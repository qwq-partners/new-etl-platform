#!/usr/bin/env python3
"""하네스 manifest — **무엇이 측정 결과를 바꾸는가를 선언한다** (9차 조치 5).

9차 교차 리뷰 P1-01. `g0-run-child.sh` 가 11개 파일을 **하드코딩**해 `harness_digest` 를
만들었다. 빠진 것이 많았다 — provider Java source, ServiceLoader resource, `build.sh`,
child JSON schema 4종, `g0-final-contract.json`, final gate, CE `suite.yaml`·시나리오 코드.

**빠진 파일을 바꿔도 digest 가 그대로다.** 그러면 M1-2 가 세운 명제("서로 다른 코드로 잰 값이
같은 판본으로 묶이는 것을 막는다")가 그 파일들에 대해서는 성립하지 않는다. 그리고 **새 파일은
목록에 자동으로 들어가지 않는다** — 조용히 빠진다.

## 이 파일이 하는 일

1. **선언**: `g0-harness-manifest.json` 이 저장소의 모든 추적 파일을 세 갈래로 나눈다.
   - `harness`  측정 결과를 바꾼다 → **digest 에 들어간다**
   - `tooling`  바꾸지 않는다(시험·생성기·lint·과거 산출물) → 들어가지 않는다
   - `excluded_globs`  문서 등 — 이유와 함께 선언한다
2. **완결성 검사**: `git ls-files` 의 모든 파일이 셋 중 정확히 하나에 있어야 한다.
   **새 파일을 만들면 이 검사가 실패한다** — 어느 쪽인지 사람이 정해야 한다.
3. **digest**: `harness` 파일들 + **manifest 자신**의 내용으로 계산한다.
   manifest 를 자기 digest 에 넣는 이유는, 파일을 harness 에서 tooling 으로 옮기는 것만으로
   digest 를 바꾸지 않고 실행 의미를 바꿀 수 있기 때문이다.

    python3 g0-harness-manifest.py            # 완결성 검사
    python3 g0-harness-manifest.py --digest   # digest 만 출력 (래퍼가 쓴다)
    python3 g0-harness-manifest.py --scaffold # 미선언 파일을 tooling 으로 붙여 초안 생성
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent
MANIFEST = ROOT / "g0-harness-manifest.json"


def tracked_files() -> list[str]:
    """추적 파일 **+ 아직 커밋하지 않은 새 파일**(gitignore 제외).

    `--others` 를 넣는 이유: 새 behavior 파일을 만들고 커밋하기 전에 잡아야 한다.
    커밋된 뒤에 잡으면 이미 그 파일로 측정을 한 회차가 있을 수 있다.
    """
    r = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard"],
                       cwd=str(ROOT), capture_output=True, text=True, check=True)
    return sorted({x for x in r.stdout.splitlines() if x.strip()})


def load() -> dict:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def _excluded(path: str, doc: dict) -> bool:
    for e in doc.get("excluded_globs", []):
        if fnmatch.fnmatch(path, e["glob"]):
            return True
    return False


def completeness(doc: dict | None = None) -> tuple[list[str], list[str]]:
    """(미선언 파일, 선언됐지만 없는 파일)."""
    doc = doc or load()
    declared = {e["path"] for e in doc.get("harness", [])} | \
               {e["path"] for e in doc.get("tooling", [])}
    tracked = [p for p in tracked_files() if not _excluded(p, doc)]
    undeclared = sorted(set(tracked) - declared)
    missing = sorted(declared - set(tracked_files()))
    return undeclared, missing


def overlap(doc: dict | None = None) -> list[str]:
    doc = doc or load()
    h = {e["path"] for e in doc.get("harness", [])}
    t = {e["path"] for e in doc.get("tooling", [])}
    return sorted(h & t)


def digest(doc: dict | None = None) -> str:
    """`harness` 파일 + manifest 자신의 내용 digest.

    경로를 함께 넣는 이유는, 내용이 같은 두 파일의 이름을 바꿔치기해도 digest 가 같아지는
    것을 막기 위해서다.
    """
    doc = doc or load()
    h = hashlib.sha256()
    for e in sorted(doc.get("harness", []), key=lambda x: x["path"]):
        p = ROOT / e["path"]
        content = hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else "MISSING"
        h.update(e["path"].encode("utf-8") + b"\0" + content.encode("ascii") + b"\n")
    # manifest 자신. 이것이 없으면 파일을 harness → tooling 으로 옮기는 것만으로
    # digest 를 그대로 둔 채 실행 의미를 바꿀 수 있다.
    h.update(b"__manifest__\0" + hashlib.sha256(
        MANIFEST.read_bytes()).hexdigest().encode("ascii") + b"\n")
    return h.hexdigest()


def main(argv: list[str]) -> int:
    if "--scaffold" in argv:
        doc = load() if MANIFEST.is_file() else {
            "schema_version": "1.0.0", "excluded_globs": [], "harness": [], "tooling": []}
        und, _ = completeness(doc)
        for p in und:
            doc.setdefault("tooling", []).append(
                {"path": p, "why": "TODO — harness 인지 tooling 인지 정하라"})
        MANIFEST.write_text(json.dumps(doc, ensure_ascii=False, indent=1) + "\n",
                            encoding="utf-8")
        print(f"[ok] 미선언 {len(und)}건을 tooling 에 TODO 로 붙였다. 손으로 분류하라")
        return 0

    if not MANIFEST.is_file():
        print(f"[fatal] {MANIFEST.name} 이 없다. --scaffold 로 초안을 만들어라", file=sys.stderr)
        return 2
    doc = load()

    if "--digest" in argv:
        # digest 를 낼 때도 완결성을 먼저 본다. **미선언 파일이 있는 채로 digest 를 내면
        # 그 digest 는 "이 코드로 쟀다" 를 말하지 못한다.**
        und, miss = completeness(doc)
        if und or miss:
            print(f"[fatal] manifest 가 불완전하다 — 미선언 {und[:5]} 없는파일 {miss[:5]}",
                  file=sys.stderr)
            return 2
        print(digest(doc))
        return 0

    ok = True
    und, miss = completeness(doc)
    if und:
        print(f"[fatal] 선언되지 않은 파일 {len(und)}건 — harness 인지 tooling 인지 정하라:",
              file=sys.stderr)
        for p in und:
            print(f"    {p}", file=sys.stderr)
        ok = False
    if miss:
        print(f"[fatal] manifest 에 있는데 저장소에 없는 파일: {miss}", file=sys.stderr)
        ok = False
    ov = overlap(doc)
    if ov:
        print(f"[fatal] harness 와 tooling 양쪽에 있는 파일: {ov}", file=sys.stderr)
        ok = False
    todo = [e["path"] for e in doc.get("tooling", []) if "TODO" in str(e.get("why", ""))]
    if todo:
        print(f"[fatal] 분류가 TODO 로 남은 파일: {todo}", file=sys.stderr)
        ok = False
    if not ok:
        return 1
    print(f"[ok] harness {len(doc['harness'])}건 · tooling {len(doc['tooling'])}건 "
          f"· digest {digest(doc)[:16]}…")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
