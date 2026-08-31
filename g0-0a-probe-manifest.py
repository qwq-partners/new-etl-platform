#!/usr/bin/env python3
"""G0-0A probe 목록 manifest 생성기 (9차 조치 3).

9차 교차 리뷰 P0-01. `cov_a()` 는 A 산출물의 **probe 집합**을 검증하지 않았다.
sentinel·중복·summary 개수·`manifest_ok` 만 보고, 다음은 하나도 안 봤다.

  · 파싱된 probe 수 == `summary.emitted`
  · `summary.expected` == SQL 의 `c_expected`
  · 빠진 ID · 알 수 없는 ID

그래서 **probe 3건 + `summary{expected:86, emitted:86}` 이 `MEASURED` 가 됐다.**
같은 파일의 `cov_b0`·`cov_c00` 은 그 검사를 한다 — 87 probe 를 내는 가장 큰 child 가
가장 약한 검사를 받고 있었다.

## 왜 목록을 손으로 적지 않는가

`c_expected` 를 **세 번 틀렸다**(56 → 78 → 86 → 87). 매번 `grep` 이 어떤 호출 형태를
놓쳤다. 사람이 세는 숫자는 또 틀린다.

그래서 목록을 SQL 에서 **뽑는다.** 이 스크립트가 생성기이고, 생성물은
`g0-child-schemas/g0-0a-probe-manifest.json` 이다. 집계기는 그 JSON 을 계약으로 읽고,
회귀 시험이 **JSON 과 SQL 이 어긋났는지** 확인한다 — SQL 을 고치고 manifest 를
재생성하지 않으면 시험이 실패한다.

    python3 g0-0a-probe-manifest.py            # 현재 SQL 과 manifest 를 대조만
    python3 g0-0a-probe-manifest.py --write    # manifest 재생성
"""
from __future__ import annotations

import hashlib
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent
SQL = ROOT / "g0-0a-capability-inventory.sql"
MANIFEST = ROOT / "g0-child-schemas" / "g0-0a-probe-manifest.json"

# `p_scalar('id', …)` · `p_stmt('id', …)`. **공백에 무관해야 한다** — `p_stmt  (` 처럼
# 공백이 낀 호출을 놓쳐서 c_expected 를 세 번 틀렸다(HANDOFF §반복된 실수).
CALL = re.compile(r"^\s*p_(?:scalar|stmt)\s*\(\s*'([^']+)'", re.M)
C_EXPECTED = re.compile(r"c_expected\s+CONSTANT\s+PLS_INTEGER\s*:=\s*(\d+)")


def derive(sql_path: pathlib.Path = SQL) -> dict:
    src = sql_path.read_text(encoding="utf-8")
    ids = CALL.findall(src)
    m = C_EXPECTED.search(src)
    dups = sorted({i for i in ids if ids.count(i) > 1})
    return {
        "$comment": "**손으로 고치지 마라.** g0-0a-probe-manifest.py 가 SQL 에서 생성한다. "
                    "SQL 을 고쳤으면 --write 로 재생성하고, 그 diff 를 리뷰에 함께 올린다. "
                    "집계기(cov_a)가 이 목록을 계약으로 읽어 산출물의 probe 집합을 대조한다"
                    "(9차 조치 3 · P0-01).",
        "generated_from": sql_path.name,
        "source_sha256": hashlib.sha256(sql_path.read_bytes()).hexdigest(),
        "c_expected_in_sql": int(m.group(1)) if m else None,
        "probe_count": len(ids),
        "duplicate_ids_in_sql": dups,
        "probe_ids": ids,
    }


def load() -> dict:
    """집계기가 쓰는 진입점. 없거나 깨졌으면 예외를 올린다 — 조용히 통과시키지 않는다."""
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def required_ids() -> list[str]:
    return list(load()["probe_ids"])


def _write(doc: dict) -> None:
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(doc, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


def main(argv: list[str]) -> int:
    doc = derive()
    if doc["duplicate_ids_in_sql"]:
        print(f"[fatal] SQL 에 중복 probe id 가 있다: {doc['duplicate_ids_in_sql']}",
              file=sys.stderr)
        return 2
    if doc["c_expected_in_sql"] != doc["probe_count"]:
        print(f"[fatal] SQL 의 c_expected={doc['c_expected_in_sql']} 인데 실제 호출은 "
              f"{doc['probe_count']}건이다. **SQL 을 먼저 고쳐라** — 이 값이 틀리면 첫 실행에서 "
              f"manifest_ok=false 가 떠 측정 결과 전체가 폐기된다.", file=sys.stderr)
        return 2

    if "--write" in argv:
        _write(doc)
        print(f"[ok] {MANIFEST.name} 생성 — probe {doc['probe_count']}건")
        return 0

    if not MANIFEST.is_file():
        print(f"[fatal] {MANIFEST} 가 없다. --write 로 생성하라", file=sys.stderr)
        return 2
    cur = load()
    if cur.get("probe_ids") != doc["probe_ids"]:
        only_sql = [i for i in doc["probe_ids"] if i not in set(cur.get("probe_ids") or [])]
        only_man = [i for i in (cur.get("probe_ids") or []) if i not in set(doc["probe_ids"])]
        print(f"[fatal] manifest 가 SQL 과 다르다. --write 로 재생성하라\n"
              f"  SQL 에만: {only_sql}\n  manifest 에만: {only_man}", file=sys.stderr)
        return 1
    if cur.get("source_sha256") != doc["source_sha256"]:
        print("[fatal] probe 목록은 같은데 SQL digest 가 다르다 — SQL 본문이 바뀌었다. "
              "--write 로 재생성하라", file=sys.stderr)
        return 1
    print(f"[ok] manifest 와 SQL 이 같다 — probe {doc['probe_count']}건")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
