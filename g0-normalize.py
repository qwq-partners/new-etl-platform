#!/usr/bin/env python3
"""G0-0 산출물 → `g0_0_evidence` 레코드 정규화 (strict aggregator).

**2026-08-27 재작성.** 7차 교차 리뷰 P0-02·P0-03·P0-04 의 조치다. 이전 판은 다음을 전부
통과시켰다 — B0 로그 한 줄, B1 의 `{"verdict":{"coverage":"PROVEN"}}` 뿐인 파일, C00 의
summary 한 줄, scenario 0개인 CE `pass=true`, schema 위반에도 exit 0. 그리고 어제 로그를
오늘 `versions.lock` 과 함께 정규화해도 아무 말이 없었다.

**규율**
  · 없는 입력을 낙관적으로 채우지 않는다. 안 잰 것은 NOT_RUN, 못 정한 것은 UNDETERMINED.
  · **완결을 선언하지 않은 산출물은 MEASURED 가 되지 않는다.** 한 줄로는 부족하다.
  · child manifest(g0-child-contract.md)가 없으면 그 child 는 FAILED 다 —
    '안 돌렸다'(NOT_RUN)와 '계약을 안 지켰다'는 다르다.
  · 계약 위반은 warning 이 아니라 **거부**다(exit 4). 무효한 레코드를 최종 경로에 쓰지 않는다.
  · G0-0 은 G0 의 부분집합이다 — `gate_eligible` 은 schema 의 const false 다.

**2026-08-30 보강(8차 M3).** 다섯 가지를 더했다.

  M3-1  child schema 를 통과하지 못한 산출물은 **본문 집계에 들어가지도 않는다**.
        이전 판은 집계한 뒤 coverage 만 FAILED 로 덮었고, 그 사이에 뽑힌 probe·축·source 는
        레코드에 남았다.
  M3-2  probe 별 typed predicate 와 SQLCODE taxonomy(g0_axes.py) — 권한 부족·대상 부재·
        프로브 결함을 '기능 부재' 로 강등하지 않는다.
  M3-3  `effective_value` 가 실제로 floor 로 내려간다. child 미완결·unbound·stale·
        신선도 근거 부재·비권위 profile 이 사유이며, **요약과 판정은 `value` 가 아니라
        `effective_value` 를 읽는다**.
  M3-4  `not_covered` 를 `g0-final-contract.json` 에서 읽고 덮은 것과의 합이 최종 계약과
        같은지 기계가 검사한다. 최종 게이트 입구는 `g0_final_gate.py` 로 분리했다.
  M3-5  `--out` 은 run 별 경로여야 하고 덮어쓰지 않는다. `--current` 포인터는 성공 시에만
        VALID 가 되고 **거부 시 INVALIDATED 로 덮인다** — 무효한 재실행 뒤에 이전 회차
        결과가 current 로 읽히는 일이 없다.

종료 코드 (g0-child-contract.md §4)
  0  유효한 레코드를 썼다
  3  불완전 — child 중 NOT_RUN/PARTIAL 이 있다. 레코드는 쓴다(completeness=INCOMPLETE)
  4  무효 — 계약 위반 또는 schema 위반. **최종 경로에 쓰지 않고** <out>.rejected.json 에만 남긴다
  2  실행 전 조건 미비(인자·파일 없음 등)

**exit 3 은 측정 완결성만 말한다**(8차 P0-04). "capability 를 못 정했다" 는 exit 에 섞지
않고 레코드의 `outcome.capability` 로 따로 낸다 — 두 가지를 한 코드에 섞으면 운영자가
어느 쪽을 고쳐야 하는지 알 수 없다.

사용:
  python3 g0-normalize.py --report-id NORM-2026-08-27-01 --run-id RUN-2026-08-27-01 \\
      --profile LOCAL_WSL --a g0-0a.log --b0 b0.json --b1 g0-0b1-evidence.json \\
      --c00 c00.log --c-suite g0-0c-counterexamples/evidence.json \\
      --versions-lock versions.lock --out evidence/RUN-2026-08-27-01/g0-0-evidence.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import sys
from datetime import datetime, timezone

# 축 파생은 별도 모듈이다 — 표 기반 pure function(7차 리뷰 P0-01·P0-05 조치).
# 파일명에 밑줄을 쓰는 이유는 import 때문이다(g0_axes.py 의 머리말 참조).
import g0_axes
# 최종 G0 계약(항목 집합)과 그 게이트 입구. **여기서 최종 레코드를 만들지 않는다** —
# 이 모듈을 import 하는 이유는 `not_covered` 를 자유 목록이 아니라 계약과의 차집합으로
# 만들기 위해서다(8차 M3-4).
import g0_final_gate

SCHEMA_FILE = "g0-0-evidence.schema.json"
SCHEMA_VERSION = "2.3.0"   # 9차 조치 9 — not_covered 항목 식별자에서 표시 이름을 뗐다
FINAL_CONTRACT_FILE = "g0-final-contract.json"

# capability 값의 유효기간에는 **기본값이 없다**(9차 조치 8 / P1-03).
# 이전 판은 30일을 자동으로 적용하면서 `freshness.basis` 에 `OPERATOR_DECLARED_TTL` 이라고
# 적었다. 운영자가 선언한 적이 없는데 선언했다고 기록하는 것이다 — 확인되지 않은 것을
# 확인된 것처럼 쓰는 문장을, 그것도 기계가 자동으로 쓰고 있었다.
# 미선언이면 신선도를 판정할 수 없고, 그러면 모든 확정값이 floor 로 내려간다.
# 얼마가 옳은 유효기간인지는 측정 분포가 나와야 답할 수 있다(M5).
DEFAULT_TTL_DAYS = 0

CHILD_KEYS = [("g0_0a", "G0_0A", "a"), ("g0_0b0", "G0_0B0", "b0"), ("g0_0b1", "G0_0B1", "b1"),
              ("g0_0c00", "G0_0C00", "c00"), ("g0_0c_suite", "G0_0C_SUITE", "c_suite")]
# check_run_set 이 '계약에 없는 child' 를 가리는 데 쓴다(8차 M1-3).
CHILD_CONSTS = [k for k, _c, _a in CHILD_KEYS]

# ── 9차 조치 7: environment_scope ────────────────────────────────────
#
# 9차 P0-07. CE 는 **폐기용 쓰기 가능 primary** 에서 돌고 나머지는 사내 standby 를 본다.
# `check_run_set` 이 `source_id` 균일성을 요구하므로 한 회차에 CE 를 넣으면 CE 가 원천
# 이름을 거짓 신고하거나 회차 전체가 거부됐다. **계약에 "이 child 는 다른 환경의 것"을
# 표현할 자리가 없었다.**
#
# 자리를 만들되 **선언이 아니라 유도**한다. 운영자가 고르는 값이면 그것은 또 하나의
# 자가선언이고 — profile 이 그래서 P0-02 가 됐다 — CE 를 SOURCE 로 신고하는 순간
# 원래 결함으로 되돌아간다. child 이름은 계약이 정한 것이므로 유도가 안전하다.
SCOPE_SOURCE = "SOURCE"
SCOPE_CE = "COUNTEREXAMPLE"
CHILD_SCOPE = {"g0_0a": SCOPE_SOURCE, "g0_0b0": SCOPE_SOURCE, "g0_0b1": SCOPE_SOURCE,
               "g0_0c00": SCOPE_SOURCE, "g0_0c_suite": SCOPE_CE}
SCOPE_MEANING = {
    SCOPE_SOURCE: "이 회차가 capability 를 재는 DB. 축 값은 전부 여기서 나온다",
    SCOPE_CE: "완화책이 실제로 막는지 보이려고 파괴적 시나리오를 도는 폐기용 DB. "
              "**원천의 capability 에 대해 아무것도 말하지 않는다**",
    "UNDECLARED": "래퍼가 scope 를 적지 않았다 — 어느 환경의 것인지 말하지 못한다",
}

# **CE 는 어떤 capability 축도 올리지 못한다.** 축은 A·B0·B1 의 probe 에서만 나오고
# (`P` 사전), `cov_ce` 는 축에 손대지 않는다. 이것은 코드 구조가 이미 지키는 성질이라
# 여기서 새로 강제할 것이 없지만, 구조가 바뀌면 깨지므로 회귀 시험으로 못박는다
# (g0-normalize-tests.py [15]).

HEX64 = re.compile(r"^[0-9a-f]{64}$")

# ── 9차 조치 3: A probe 목록 계약 ────────────────────────────────────
A_PROBE_MANIFEST = pathlib.Path(__file__).resolve().parent / "g0-child-schemas" \
    / "g0-0a-probe-manifest.json"


def _load_a_required_ids() -> list[str] | None:
    """계약이 요구하는 A probe id 목록. 읽지 못하면 None — 그것은 위반이다."""
    try:
        doc = json.loads(A_PROBE_MANIFEST.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    ids = doc.get("probe_ids")
    return list(ids) if isinstance(ids, list) and ids else None


def sha(p: pathlib.Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else "MISSING"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_pointer(path: pathlib.Path, body: dict) -> None:
    """'현재 유효한 G0-0 레코드' 포인터를 쓴다 (8차 M3-5).

    **거부된 회차에서도 반드시 쓴다.** 이전 판은 거부 시 `<out>.rejected.json` 만 남기고
    최종 경로를 건드리지 않았다. 그러면 이전 회차가 만든 `g0-0-evidence.json` 이 그대로
    남아 소비자에게는 여전히 '현재' 로 보인다 — 무효한 재실행이 있었다는 사실은 어디에도
    나타나지 않는다. 그 별칭을 없애는 것이 이 함수의 존재 이유다.

    직전 포인터가 있으면 `previous` 로 접어 넣는다(무엇을 대체했는지 남기기 위해).
    """
    prev = None
    if path.is_file():
        try:
            prev = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            prev = {"status": "UNREADABLE"}
        if isinstance(prev, dict):
            prev.pop("previous", None)      # 무한 중첩을 막는다
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({**body, "previous": prev}, ensure_ascii=False, indent=1),
                    encoding="utf-8")


def jsonl(path: pathlib.Path | None, key: str = "probe") -> list[dict]:
    """DBMS_OUTPUT spool 이나 stdout 에서 JSON 객체 줄만 뽑는다. 다른 줄은 무시한다."""
    out: list[dict] = []
    if not path or not path.is_file():
        return out
    for ln in path.read_text(encoding="utf-8", errors="replace").splitlines():
        ln = ln.strip()
        i = ln.find("{")
        if i < 0:
            continue
        try:
            o = json.loads(ln[i:])
        except json.JSONDecodeError:
            continue
        if isinstance(o, dict):
            out.append(o)
    return out


UNSET_VALUE = re.compile(r"(?::|^\s*-)\s*UNSET\s*$")


def lock_has_unset(text: str) -> bool:
    """`UNSET` 이 **값 토큰 자체**로 남아 있는가.

    이전 판은 파일 전체를 문자열 검색해서, 8행 주석의 "UNSET 은 빈칸이 아니라 판정이다"
    때문에 모든 값을 채워도 경고가 사라지지 않았다(7차 리뷰 P2). 주석을 떼고, 그 다음
    **값 토큰 전체가 UNSET 인 줄만** 센다 — 설명문에 그 단어가 들어간 값(`note: "… UNSET …"`)은
    미측정 항목이 아니다. YAML 파서를 쓰지 않는 이유는 의존성을 늘리지 않기 위해서이고,
    그래서 이것은 완전한 판정이 아니라 **보수적 근사**다. 놓치는 쪽이 아니라 과탐지 쪽으로
    틀리도록 정규식을 좁게 잡았다.
    """
    for ln in text.splitlines():
        if UNSET_VALUE.search(ln.split("#", 1)[0].rstrip()):
            return True
    return False


# ── child manifest ───────────────────────────────────────────────────
def read_manifest(art: pathlib.Path) -> dict | None:
    m = art.with_name(art.name + ".manifest.json")
    if not m.is_file():
        return None
    try:
        o = json.loads(m.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return o if isinstance(o, dict) else None


def check_child(child_const: str, art: pathlib.Path,
                run_id: str, profile: str, lock_digest: str,
                source_id: str = "", harness_digest: str = "") -> tuple[dict, list[str]]:
    """manifest 를 읽고 계약을 대조한다.

    반환: (children[...] 레코드, 계약 위반 목록). 위반이 있으면 그 child 는 coverage 에서
    FAILED 다 — 본문이 아무리 그럴듯해도.
    """
    V: list[str] = []
    man = read_manifest(art)
    if man is None:
        V.append(f"{child_const}: manifest 사이드카가 없다({art.name}.manifest.json). "
                 f"g0-run-child.sh 로 실행하지 않았다 — 이 산출물은 언제·어느 판본으로 "
                 f"만들어졌는지 스스로 말하지 못한다")
        return {"present": False}, V

    actual = sha(art)
    declared = str((man.get("artifact") or {}).get("sha256", "MISSING"))
    rec: dict = {
        "present": True,
        "run_id": str(man.get("run_id", "")),
        "exit_code": man.get("exit_code") if isinstance(man.get("exit_code"), int) else -1,
        "versions_lock_digest": str(man.get("versions_lock_digest", "MISSING")),
        "artifact_sha256": declared if (HEX64.match(declared) or declared == "MISSING") else "MISSING",
        "artifact_verified": declared == actual,
        "runtime": {k: str(v) for k, v in (man.get("runtime") or {}).items()
                    if isinstance(k, str)},
        # M1-2 — 어느 원천에서, 어느 하네스 코드로 잰 값인가.
        "source_id": str(man.get("source_id", "")),
        "harness_digest": str(man.get("harness_digest", "MISSING")),
        "started_at": str(man.get("started_at", "")),
        "ended_at": str(man.get("ended_at", "")),
        "overwrote_existing": bool(man.get("overwrote_existing", False)),
        # 9차 조치 4 — 래퍼가 **관측한** 실행 환경. caller 가 고른 profile label 과 다르다.
        "env_kind": str(man.get("env_kind", "")) or "UNRECORDED",
        # 9차 조치 7 — 이 child 가 **어떤 환경의 것인가**. SOURCE 인지 COUNTEREXAMPLE 인지.
        "environment_scope": str(man.get("environment_scope", "")) or "UNDECLARED",
    }
    measured = str(man.get("ended_at") or man.get("started_at") or "")
    if measured:
        rec["measured_at"] = measured
    if not (HEX64.match(rec["versions_lock_digest"]) or rec["versions_lock_digest"] == "MISSING"):
        rec["versions_lock_digest"] = "MISSING"

    if man.get("child") != child_const:
        V.append(f"{child_const}: manifest 의 child 가 {man.get('child')!r} 이다 — 다른 산출물의 manifest 다")
    if rec["run_id"] != run_id:
        V.append(f"{child_const}: run_id 불일치 (manifest={rec['run_id']!r}, 요청={run_id!r}) — "
                 f"서로 다른 회차의 산출물을 섞었다")
    if man.get("profile") != profile:
        V.append(f"{child_const}: profile 불일치 (manifest={man.get('profile')!r}, 요청={profile!r})")
    if str(man.get("versions_lock_digest", "")) != lock_digest:
        V.append(f"{child_const}: versions_lock_digest 불일치 — 실행 시점 판본과 집계 시점 판본이 "
                 f"다르다 (child={str(man.get('versions_lock_digest'))[:16]}…, 지금={lock_digest[:16]}…)")
    if not rec["artifact_verified"]:
        V.append(f"{child_const}: 산출물이 실행 후 변경됐다 "
                 f"(manifest={declared[:16]}…, 지금={actual[:16]}…)")
    if rec["exit_code"] != 0:
        V.append(f"{child_const}: exit_code={rec['exit_code']} — 실행이 성공으로 끝나지 않았다")

    # ── 9차 조치 7: scope 는 유도값이므로 manifest 가 계약과 같아야 한다 ──
    want_scope = SCOPE_CE if child_const == "G0_0C_SUITE" else SCOPE_SOURCE
    if rec["environment_scope"] == "UNDECLARED":
        V.append(f"{child_const}: manifest 에 environment_scope 가 없다 — 9차 조치 7 이전 "
                 f"판본의 래퍼로 실행했는가. **이 산출물이 어느 환경의 것인지 말하지 "
                 f"못하면 원천 증거와 반례 증거를 가를 수 없다**(9차 P0-07)")
    elif rec["environment_scope"] != want_scope:
        V.append(f"{child_const}: environment_scope={rec['environment_scope']!r} 인데 계약이 "
                 f"정한 값은 {want_scope!r} 다. scope 는 child 로부터 **유도**되므로 이것이 "
                 f"다르다는 것은 manifest 를 손댔다는 뜻이다 — 특히 CE 를 SOURCE 로 바꾸면 "
                 f"파괴적 시나리오의 결과가 원천 증거로 섞인다(9차 P0-07)")

    # ── M1-2: source·harness·시각 결속 ──────────────────────────────
    if not rec["source_id"]:
        V.append(f"{child_const}: manifest 에 source_id 가 없다 — **어느 원천에서 잰 값인지 "
                 f"말하지 못한다.** 여러 원천의 산출물을 한 회차로 섞어도 알 수 없다(8차 M1-2)")
    elif source_id and rec["environment_scope"] == SCOPE_SOURCE \
            and rec["source_id"] != source_id:
        # `--source-id` 는 **SOURCE scope 의 이름**이다. CE 는 다른 DB 에서 돌므로
        # 여기에 걸면 안 된다 — 그것이 P0-07 이 만든 막다른 골목이었다.
        V.append(f"{child_const}: source_id 불일치 (manifest={rec['source_id']!r}, "
                 f"요청={source_id!r}) — 서로 다른 원천의 산출물을 섞었다")
    elif source_id and rec["environment_scope"] == SCOPE_CE \
            and rec["source_id"].upper() == source_id.upper():
        # **CE 가 원천과 같은 DB 를 신고했다.** 둘 중 하나다 — 운영자가 CE 단계에서
        # G0_SOURCE_ID 를 안 바꿨거나, CE 가 정말 사내 원천에서 돌았거나.
        # 앞이면 증거가 거짓이고 뒤면 사고다. 어느 쪽이든 통과시키지 않는다.
        V.append(f"{child_const}: CE 의 source_id={rec['source_id']!r} 가 원천과 같다. "
                 f"CE 는 **폐기용 쓰기 가능 DB** 에서 돌아야 하고 파괴적 시나리오를 "
                 f"수행한다 — 사내 원천과 같은 이름을 신고하는 것은 운영자가 CE 단계에서 "
                 f"G0_SOURCE_ID 를 바꾸지 않았거나 CE 가 실제로 원천에서 돌았다는 "
                 f"뜻이다(9차 P0-07)")

    if rec["harness_digest"] in ("", "MISSING", "NO_HARNESS_FILES", "MANIFEST_INCOMPLETE"):
        V.append(f"{child_const}: manifest 에 harness_digest 가 없다 — versions.lock 은 "
                 f"실행 판본이지 하네스 코드가 아니다. 프로브를 고쳐도 lock digest 는 "
                 f"그대로이므로, 이것이 없으면 다른 코드로 잰 값이 같은 판본으로 묶인다(8차 M1-2)")
    elif harness_digest and rec["harness_digest"] != harness_digest:
        V.append(f"{child_const}: harness_digest 불일치 "
                 f"(child={rec['harness_digest'][:16]}…, 지금={harness_digest[:16]}…) — "
                 f"이 산출물을 만든 하네스와 지금 하네스가 다르다")

    if not (rec["started_at"] and rec["ended_at"]):
        V.append(f"{child_const}: manifest 에 started_at/ended_at 이 모두 있어야 한다(8차 M1-2)")

    if rec["overwrote_existing"]:
        V.append(f"{child_const}: 기존 산출물을 덮어썼다(G0_ALLOW_OVERWRITE=1). "
                 f"회차 산출물은 불변이어야 한다 — 덮인 회차의 증거는 복구할 수 없다(8차 M1-4)")

    # ── M1-4: run 별 경로 ───────────────────────────────────────────
    if run_id and run_id not in str(art):
        V.append(f"{child_const}: 산출물 경로에 run_id 가 없다({art}) — 회차마다 다른 경로가 "
                 f"아니면 이전 회차가 덮였는지 사후에 말할 수 없다(8차 M1-4)")

    return rec, V


# ── M1-1: child 별 개별 schema ───────────────────────────────────────
# 산출물 형태가 child 마다 다르므로 스키마도 child 마다 다르다. **집계 전에** 건다 —
# 형태가 계약과 다른 산출물이 집계 입력이 되면, 뒤의 완결 판정이 무엇을 세는지
# 스스로도 말할 수 없다.
CHILD_SCHEMA_DIR = pathlib.Path(__file__).resolve().parent / "g0-child-schemas"
CHILD_SCHEMA = {
    "g0_0a":   ("g0-child-a.schema.json",   "jsonl"),
    "g0_0b0":  ("g0-child-b0.schema.json",  "jsonl"),
    "g0_0b1":  ("g0-child-b1.schema.json",  "json"),
    "g0_0c00": ("g0-child-c00.schema.json", "jsonl"),
    # G0_0C_SUITE 는 자기 스키마를 패키지 안에 갖고 있고 runner 가 스스로 검증한다
    # (g0-0c-counterexamples/evidence.schema.json). 여기서 다시 정의하지 않는다 —
    # 같은 것을 두 곳에서 정의하던 것이 7차 P0-04 였다.
}


def validate_child_artifact(key: str, art: pathlib.Path) -> list[str]:
    """child 산출물을 자기 스키마로 검증한다 (8차 M1-1)."""
    spec = CHILD_SCHEMA.get(key)
    if spec is None:
        return []
    fname, kind = spec
    sp = CHILD_SCHEMA_DIR / fname
    if not sp.is_file():
        return [f"{key}: child 스키마가 없다({sp}) — 검증 없이 집계하지 않는다"]
    try:
        import jsonschema
    except ImportError:
        return [f"{key}: jsonschema 가 없어 child 산출물을 검증하지 못했다. "
                f"**검증하지 못한 것은 통과가 아니다** — 설치 후 다시 돌려라"]
    try:
        schema = json.loads(sp.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        return [f"{key}: child 스키마를 읽지 못했다 — {type(e).__name__}"]

    V: list[str] = []
    validator = jsonschema.Draft202012Validator(schema)
    if kind == "json":
        try:
            doc = json.loads(art.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            return [f"{key}: 산출물이 JSON 이 아니다 — {type(e).__name__}"]
        for err in list(validator.iter_errors(doc))[:5]:
            V.append(f"{key}: child schema 위반 — {'/'.join(str(x) for x in err.path) or '(root)'}: {err.message}")
        return V

    # jsonl — 줄마다 검증한다. 한 줄이라도 어긋나면 그 파일은 계약 밖이다.
    bad = 0
    for i, line in enumerate(art.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
        t = line.strip()
        if not t or not t.startswith("{"):
            continue          # 로그 잡음은 무시한다. 판정은 파싱된 레코드로만 한다.
        try:
            rec = json.loads(t)
        except json.JSONDecodeError:
            continue
        errs = list(validator.iter_errors(rec))
        if errs:
            bad += 1
            if bad <= 3:
                V.append(f"{key}: child schema 위반 (line {i}) — {errs[0].message}")
    if bad > 3:
        V.append(f"{key}: child schema 위반이 총 {bad}줄이다(앞 3건만 표시)")
    return V


# ── 9차 조치 4: profile attestation 과 서버 신원 결속 ────────────────
#
# 9차 P0-02 가 잡은 것 둘.
#
#   ① manifest 의 `source_id` 를 **서버가 밝힌 `DB_UNIQUE_NAME` 과 대조하지 않았다.**
#      그래서 한 레코드 안에 `source_id=TESTSTBY` 와 `db_unique_name=ETLSTB` 가 공존하고
#      위반이 0건이었다. child 들이 같은 **거짓** 이름을 공유하는 것은 같은 원천에서
#      나왔다는 증명이 아니다.
#   ② profile 이 caller 가 고르는 label 이라 WSL 에서 `CORP_POC` 로 재라벨하면 막을 수
#      없었다 — `PROFILE_NOT_AUTHORITATIVE` floor 는 정직하게 신고한 회차만 막고 있었다.
#
# 완전한 attestation 은 이 저장소 범위 밖이다. 여기서 하는 것은 **알려진 모순을 거부**하는
# 것이고, 그 판정은 비대칭이다 — `wsl`·`container` 관측은 `CORP_POC` 를 반증하지만
# `host` 는 아무것도 입증하지 않는다.
PROFILE_ENV = {
    "LOCAL_WSL": {"wsl"},
    "SANDBOX_CONTAINER": {"container"},
    # CORP_POC 는 어떤 fingerprint 로도 **입증되지 않는다.** 아래 표는 반증용이다.
}
# 이 환경이 관측되면 그 profile 은 거짓이다.
PROFILE_CONTRADICTED_BY = {
    "CORP_POC": {"wsl", "container"},
    "LOCAL_WSL": {"container"},
    "SANDBOX_CONTAINER": {"wsl"},
}


def check_profile_attestation(children: dict[str, dict], profile: str) -> list[str]:
    """선언된 profile 이 래퍼가 관측한 환경과 모순되는가 (9차 조치 4)."""
    V: list[str] = []
    bad = PROFILE_CONTRADICTED_BY.get(profile, set())
    for name, rec in children.items():
        if not (isinstance(rec, dict) and rec.get("present")):
            continue
        kind = rec.get("env_kind") or "UNRECORDED"
        if kind in bad:
            V.append(f"{name}: profile={profile} 인데 래퍼가 관측한 실행 환경은 {kind!r} 다 — "
                     f"**로컬 증거를 사내 회차로 재라벨한 것이다**(9차 P0-02). profile 은 "
                     f"caller 가 고르는 label 이지만 환경은 관측된 사실이다")
        elif kind == "UNRECORDED":
            V.append(f"{name}: manifest 에 env_kind 가 없다 — 래퍼가 실행 환경을 기록하지 "
                     f"않았다(9차 조치 4 이전 판본으로 실행했는가). **관측하지 못한 것을 "
                     f"통과로 두지 않는다**")
    return V


def check_source_identity(P: dict[str, dict], children: dict[str, dict],
                          declared_source_id: str) -> tuple[list[str], list[str]]:
    """manifest 의 `source_id` 를 **서버가 밝힌 신원**과 대조한다 (9차 조치 4 · P0-02).

    반환은 (위반, 경고). 서버 신원을 못 읽은 회차는 위반이 아니라 **미확인**이며,
    그 사실이 capability floor 사유가 된다 — 호출자가 그것을 판단한다.
    """
    V: list[str] = []
    W: list[str] = []
    r = P.get("userenv.DB_UNIQUE_NAME")
    server = str(r.get("value")) if (r and r.get("query_ok") is True and
                                     r.get("value") is not None) else ""
    if not server:
        W.append("서버가 밝힌 DB_UNIQUE_NAME 이 없다(G0-0A 미실행 또는 그 probe 실패) — "
                 "**manifest 의 source_id 를 대조할 상대가 없다.** 이 회차의 원천 신원은 "
                 "운영자 신고값뿐이며 그것은 증거가 아니다(9차 P0-02)")
        return V, W

    for name, rec in children.items():
        if not (isinstance(rec, dict) and rec.get("present")):
            continue
        # **SOURCE scope 만 본다**(9차 조치 7 · P0-07). A 가 읽은 DB_UNIQUE_NAME 은
        # 원천의 것이고, CE 는 폐기용 DB 에서 돈다 — 그 둘을 대조하면 정상 회차가
        # 거부된다. CE 의 신원은 `check_ce_identity()` 가 CE 자신의 증거로 대조한다.
        if str(rec.get("environment_scope") or "") != SCOPE_SOURCE:
            continue
        sid = str(rec.get("source_id") or "")
        if sid and sid.upper() != server.upper():
            V.append(f"{name}: manifest 의 source_id={sid!r} 인데 서버가 밝힌 "
                     f"DB_UNIQUE_NAME 은 {server!r} 다 — **한 레코드가 두 원천을 말한다.** "
                     f"child 들이 같은 이름을 공유하는 것은 같은 원천에서 나왔다는 증명이 "
                     f"아니다(9차 P0-02)")
    if declared_source_id and declared_source_id.upper() != server.upper():
        V.append(f"--source-id={declared_source_id!r} 인데 서버가 밝힌 DB_UNIQUE_NAME 은 "
                 f"{server!r} 다 — 운영자가 지정한 원천과 실제로 붙은 원천이 다르다")
    return V, W


def check_ce_identity(children: dict[str, dict], ce_env: dict) -> tuple[list[str], list[str]]:
    """CE manifest 의 `source_id` 를 **CE runner 가 서버에서 읽은 값**과 대조한다.

    9차 조치 7. SOURCE scope 를 A 의 서버 신원에 묶었듯이(조치 4), CE 도 자기 환경의
    서버 신원에 묶여야 한다. 그러지 않으면 scope 를 도입한 것이 **CE 만 신원 대조에서
    면제해 주는 결과**가 된다 — 파괴적 시나리오를 도는 쪽을 느슨하게 두는 것은 방향이
    거꾸로다.

    CE runner 는 `CE_DSN` 으로 직접 붙어 `V$DATABASE` 에서 `db_unique_name` 을 읽고
    증거의 `environment.primary_db_unique_name` 에 남긴다. 그것이 대조 상대다.

    반환은 (위반, 경고). CE 를 안 돌린 회차는 둘 다 비어 있다.
    """
    V: list[str] = []
    W: list[str] = []
    rec = children.get("g0_0c_suite")
    if not (isinstance(rec, dict) and rec.get("present")):
        return V, W

    observed = str((ce_env or {}).get("primary_db_unique_name") or "")
    sid = str(rec.get("source_id") or "")
    if not observed:
        W.append("CE 증거에 environment.primary_db_unique_name 이 없다 — **CE manifest 의 "
                 "source_id 를 대조할 상대가 없다.** CE 가 어느 DB 에서 돌았는지는 "
                 "운영자 신고값뿐이며 그것은 증거가 아니다(9차 조치 7)")
        return V, W
    if sid and sid.upper() != observed.upper():
        V.append(f"g0_0c_suite: manifest 의 source_id={sid!r} 인데 CE runner 가 서버에서 "
                 f"읽은 db_unique_name 은 {observed!r} 다 — **CE 증거와 그 manifest 가 "
                 f"서로 다른 DB 를 말한다**(9차 조치 7)")
    return V, W


def check_run_set(children: dict[str, dict]) -> list[str]:
    """**회차 집합 전체**를 본다 (8차 M1-3).

    check_child 는 child 하나씩만 본다. 그것만으로는 잡히지 않는 것이 셋이다.
      · duplicate  — 같은 child 가 두 번 들어온 경우(집계기가 뒤엣것만 쓰면 앞엣것이 사라진다)
      · unknown    — 계약에 없는 child 이름
      · concatenated — child 들이 서로 다른 회차·원천·하네스인데 각자는 자기 manifest 와
                      일관돼서 개별 검사를 다 통과하는 경우. **이것이 가장 위험하다** —
                      각 child 를 따로 보면 아무 문제가 없다.
    """
    V: list[str] = []
    known = set(CHILD_CONSTS)

    for name in children:
        if name not in known:
            V.append(f"알 수 없는 child: {name!r}. 계약에 있는 것은 {sorted(known)} 뿐이다(8차 M1-3)")

    present = {k: v for k, v in children.items()
               if isinstance(v, dict) and v.get("present")}
    if not present:
        return V

    def spread(field: str, pool: dict) -> list[str]:
        return sorted({str(v.get(field, "")) for v in pool.values()})

    # ── 회차 전체에서 균일해야 하는 것 ─────────────────────────────────
    # scope 가 갈려도 이 셋은 같아야 한다. **같은 회차·같은 코드·같은 실행 판본**이라야
    # "CE 가 보인 완화책은 이 하네스의 것이다" 를 말할 수 있다. scope 를 도입한다고 해서
    # 이 결속까지 푸는 것이 아니다.
    for field, why in (("run_id", "서로 다른 회차"),
                       ("versions_lock_digest", "서로 다른 실행 판본"),
                       ("harness_digest", "서로 다른 하네스 코드")):
        vals = [v for v in spread(field, present) if v and v != "MISSING"]
        if len(vals) > 1:
            V.append(f"child 들이 {why}의 것이다 — {field}: {vals}. "
                     f"각 child 는 자기 manifest 와 일관되므로 개별 검사로는 잡히지 않는다. "
                     f"**이어 붙인 회차는 하나의 회차가 아니다**(8차 M1-3)")

    # ── 9차 조치 7: `source_id` 는 **scope 안에서** 균일해야 한다 ──────
    #
    # 이전 판은 회차 전체에서 균일할 것을 요구했다. CE 는 폐기용 DB 에서 도는데도
    # 그랬으므로, 한 회차에 CE 를 넣으려면 CE 가 원천 이름을 거짓 신고하는 수밖에
    # 없었다(9차 P0-07). scope 를 도입한 이유가 이것이다.
    by_scope: dict[str, dict] = {}
    for k, v in present.items():
        by_scope.setdefault(str(v.get("environment_scope") or "UNDECLARED"), {})[k] = v

    for scope, pool in sorted(by_scope.items()):
        vals = [v for v in spread("source_id", pool) if v and v != "MISSING"]
        if len(vals) > 1:
            V.append(f"scope={scope} 안에서 child 들이 서로 다른 원천의 것이다 — "
                     f"source_id: {vals}. 같은 scope 는 같은 DB 를 봐야 한다(8차 M1-3)")

    # 그리고 **scope 가 다르면 원천도 달라야 한다.** 같으면 CE 가 사내 원천에서 돌았다는
    # 뜻이고, CE 는 DDL/DML 을 한다. 위의 per-child 검사가 `--source-id` 를 준 회차에서
    # 잡지만, 주지 않은 회차에서는 여기가 유일한 방어다.
    def names(scope: str) -> set[str]:
        return {n.upper() for n in spread("source_id", by_scope.get(scope, {}))
                if n and n != "MISSING"}

    overlap = names(SCOPE_SOURCE) & names(SCOPE_CE)
    if overlap:
        V.append(f"CE 와 원천이 같은 DB 를 가리킨다 — {sorted(overlap)}. **CE 는 파괴적 "
                 f"시나리오를 도는 폐기용 환경 전용이다.** 사내 원천에서 돌았거나, CE "
                 f"단계에서 G0_SOURCE_ID 를 바꾸지 않은 것이다(9차 P0-07)")

    return V


# ── child 별 완결 판정 ────────────────────────────────────────────────
def cov_a(path: pathlib.Path | None) -> tuple[dict, dict[str, dict], list[str]]:
    """G0-0A. 완결 조건: sentinel ∧ manifest_ok ∧ 중복 0 ∧ summary 정확히 1개
    ∧ **probe 집합이 계약과 정확히 같음**(9차 조치 3).

    9차 P0-01 — 마지막 항이 없었다. `cov_b0` 는 `expected_steps` 와 `emitted_steps` 의
    차집합을 보고 `cov_c00` 은 `expected_probe_ids` 를 id 집합으로 대조하는데 **A 만
    안 봤다.** 87 probe 를 내는 가장 큰 child 가 가장 약한 검사를 받고 있었고, 그래서
    **probe 3건 + `summary{expected:86, emitted:86}` 이 `MEASURED`** 가 됐다.
    """
    if path is None:
        return {"status": "NOT_RUN"}, {}, []
    recs = jsonl(path)
    V: list[str] = []
    summaries = [r["probe_summary"] for r in recs
                 if isinstance(r.get("probe_summary"), dict)]
    sentinel = any("probe_run_end" in r for r in recs)
    probes = [r for r in recs if isinstance(r.get("probe"), str)]

    ids = [r["probe"] for r in probes]
    dups = sorted({i for i in ids if ids.count(i) > 1})
    if dups:
        V.append(f"G0_0A: probe id 가 중복이다 {dups[:5]} — 여러 회차의 로그가 한 파일에 섞였을 수 "
                 f"있다. 마지막 값이 이기는 조립은 증거가 아니다")
    if len(summaries) > 1:
        V.append(f"G0_0A: probe_summary 가 {len(summaries)}개다 — 한 파일에 한 회차만 있어야 한다")

    # ── 9차 조치 3: probe 집합을 계약과 정확히 대조한다 ─────────────
    # 목록의 권위는 `g0-child-schemas/g0-0a-probe-manifest.json` 이고, 그것은 SQL 에서
    # 생성된다(`g0-0a-probe-manifest.py`). **손으로 센 숫자를 쓰지 않는다** — 이 저장소는
    # `c_expected` 를 세 번 틀렸다.
    req = _load_a_required_ids()
    if req is None:
        V.append("G0_0A: probe manifest 를 읽지 못했다"
                 f"({A_PROBE_MANIFEST.name}) — **검증하지 못한 것은 통과가 아니다**. "
                 f"`python3 g0-0a-probe-manifest.py --write` 로 생성하라(9차 조치 3)")
    else:
        got, want = set(ids), set(req)
        missing, unknown = sorted(want - got), sorted(got - want)
        if missing:
            V.append(f"G0_0A: 계약에 있는 probe {len(missing)}건이 산출물에 없다 "
                     f"{missing[:5]}{'…' if len(missing) > 5 else ''} — 블록이 중간에 끊겼거나 "
                     f"다른 판본의 SQL 로 돌렸다(9차 P0-01)")
        if unknown:
            V.append(f"G0_0A: 계약에 없는 probe {len(unknown)}건이 있다 "
                     f"{unknown[:5]}{'…' if len(unknown) > 5 else ''} — 이 산출물을 만든 SQL 이 "
                     f"manifest 와 다르다. `g0-0a-probe-manifest.py` 를 재생성했는가")
        # summary 가 스스로 신고한 수도 대조한다. **자기 신고를 그대로 믿지 않는다** —
        # 3건짜리 로그가 "86건 냈다" 고 적어도 통과하던 것이 P0-01 이다.
        if summaries:
            s0 = summaries[0]
            for key in ("expected", "emitted"):
                v = s0.get(key)
                if isinstance(v, int) and v != len(req):
                    V.append(f"G0_0A: probe_summary.{key}={v} 인데 계약은 {len(req)}건이다 — "
                             f"산출물이 스스로 다른 수를 신고했다")
            emitted = s0.get("emitted")
            if isinstance(emitted, int) and emitted != len(ids):
                V.append(f"G0_0A: probe_summary.emitted={emitted} 인데 실제 파싱된 probe 는 "
                         f"{len(ids)}건이다 — **자기 신고와 실물이 다르다**")

    P = {r["probe"]: r for r in probes}
    if not summaries:
        return ({"status": "PARTIAL",
                 "reason": "probe_summary sentinel 이 없다 — 블록이 끝까지 갔는지 확인 불가",
                 "counts": {"parsed": len(P)}}, P, V)
    s = summaries[0]
    counts = {k: int(s.get(k) or 0) for k in ("expected", "emitted", "query_failed", "value_mismatch")
              if s.get(k) is not None}
    if s.get("manifest_ok") is False:
        return ({"status": "FAILED",
                 "reason": "manifest_ok=false — 블록이 중간에 끊겼다. 결과 전체를 폐기한다.",
                 "counts": counts}, {}, V)
    if not sentinel:
        return ({"status": "PARTIAL",
                 "reason": "probe_run_end sentinel 이 없다 — 스크립트가 끝까지 도달했다는 증거가 없다",
                 "counts": counts}, P, V)
    return {"status": "MEASURED", "counts": counts}, P, V


def cov_b0(path: pathlib.Path | None) -> dict:
    """G0-0B0. **완결 sentinel 이 없으면 MEASURED 가 아니다.**

    이전 판은 파싱 가능한 줄이 1개만 있어도 MEASURED 였다(7차 리뷰 P0-02).
    """
    if path is None:
        return {"status": "NOT_RUN"}
    recs = jsonl(path)
    if not recs:
        return {"status": "FAILED", "reason": "파싱 가능한 결과 줄이 없다"}
    summ = next((r["b0_summary"] for r in recs if isinstance(r.get("b0_summary"), dict)), None)
    if summ is None:
        return {"status": "PARTIAL",
                "reason": "b0_summary 완결 sentinel 이 없다 — 몇 개를 낼 예정이었는지 산출물이 "
                          "말하지 않으므로 완주 여부를 판정할 수 없다",
                "counts": {"records": len(recs)}}
    exp = [x for x in (summ.get("expected_steps") or []) if isinstance(x, str)]
    got = [x for x in (summ.get("emitted_steps") or []) if isinstance(x, str)]
    missing = sorted(set(exp) - set(got))
    if missing:
        return {"status": "PARTIAL", "reason": f"미출력 step: {missing[:6]}",
                "counts": {"expected": len(exp), "emitted": len(got)}}
    return {"status": "MEASURED", "counts": {"expected": len(exp), "emitted": len(got)}}


def cov_b1(path: pathlib.Path | None) -> tuple[dict, dict]:
    """G0-0B1. 필수 키가 다 있고 두 회차가 다 관측돼야 MEASURED."""
    if path is None:
        return {"status": "NOT_RUN"}, {}
    try:
        e = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as ex:
        return {"status": "FAILED", "reason": f"{type(ex).__name__}"}, {}
    if not isinstance(e, dict):
        return {"status": "FAILED", "reason": "최상위가 객체가 아니다"}, {}

    v = e.get("verdict") if isinstance(e.get("verdict"), dict) else {}
    summary = {"verdict": v.get("coverage"), "blocking": v.get("blocking", []),
               "by_path": e.get("by_path", {}),
               "preamble_ok_by_path": e.get("preamble_ok_by_path", {}),
               "runs_seen": e.get("runs_seen", {})}

    need = [k for k in ("verdict", "by_path", "preamble_ok_by_path", "runs_seen") if k not in e]
    if need:
        return ({"status": "FAILED",
                 "reason": f"필수 키 누락 {need} — verdict 만 있는 파일은 증거가 아니다"}, summary)
    runs = e.get("runs_seen") if isinstance(e.get("runs_seen"), dict) else {}
    # **회차 이름을 정확히 맞추지 않는다.** 조치 5 로 failclosed 가 경로별로 갈렸다 —
    # failclosed_schema / failclosed_task. 정확한 키 "failclosed" 를 찾으면 새 하네스의
    # 정상 실행이 영원히 PARTIAL 이 된다(2026-08-27 runbook 작성 중 발견한 회귀다).
    missing_runs = []
    if not runs.get("coverage"):
        missing_runs.append("coverage")
    if not any(k.startswith("failclosed") and v for k, v in runs.items()):
        missing_runs.append("failclosed*")
    if missing_runs:
        return ({"status": "PARTIAL",
                 "reason": f"관측되지 않은 회차 {missing_runs} — fail-closed 를 시험하지 않았으면 "
                           f"그 질문은 미확정이지 통과가 아니다"}, summary)
    if v.get("coverage") != "PROVEN":
        return {"status": "PARTIAL", "reason": str(v.get("reason", ""))[:200]}, summary
    return {"status": "MEASURED"}, summary


def cov_c00(path: pathlib.Path | None) -> tuple[dict, dict]:
    if path is None:
        return {"status": "NOT_RUN"}, {}
    recs = jsonl(path)
    fence = {r["probe"]: r for r in recs if isinstance(r.get("probe"), str)}
    if not fence:
        return {"status": "FAILED", "reason": "파싱 가능한 결과 줄이 없다"}, {}
    summ = fence.get("fence.summary")
    emitted = len([k for k in fence if k != "fence.summary"])
    if summ is None:
        return ({"status": "PARTIAL", "reason": "fence.summary 가 없다 — 블록이 끝까지 갔는지 확인 불가",
                 "counts": {"probes": emitted}}, fence)
    exp_ids = [x for x in (summ.get("expected_probe_ids") or []) if isinstance(x, str)]
    exp = summ.get("expected_probes")
    if not exp_ids and not isinstance(exp, int):
        return ({"status": "PARTIAL",
                 "reason": "fence.summary 에 expected_probe_ids 가 없다 — 무엇을 낼 예정이었는지 "
                           "산출물이 말하지 않으므로 완주 여부를 판정할 수 없다",
                 "counts": {"probes": emitted}}, fence)
    skipped = sum(1 for k, r in fence.items() if k != "fence.summary" and r.get("skipped"))
    # **개수가 아니라 id 집합을 본다.** 개수만 맞고 다른 probe 가 나온 경우를 놓치지 않는다.
    missing = [i for i in exp_ids if i not in fence]
    if missing:
        return ({"status": "PARTIAL", "reason": f"미출력 probe: {missing[:6]} — 블록이 중간에 끊겼다",
                 "counts": {"expected": len(exp_ids), "emitted": emitted, "skipped": skipped}}, fence)
    if isinstance(exp, int) and emitted != exp:
        return ({"status": "PARTIAL", "reason": f"expected={exp} emitted={emitted}",
                 "counts": {"expected": exp, "emitted": emitted, "skipped": skipped}}, fence)
    if skipped:
        return ({"status": "PARTIAL", "reason": "ACK_FULL_SCAN=N — 전수 스캔 계열이 건너뛰어졌다",
                 "counts": {"expected": len(exp_ids) or (exp or 0), "emitted": emitted,
                            "skipped": skipped}}, fence)
    return ({"status": "MEASURED",
             "counts": {"expected": len(exp_ids) or (exp or 0), "emitted": emitted}}, fence)


def cov_ce(path: pathlib.Path | None) -> tuple[dict, dict, list[str]]:
    """G0-0C suite. scenario 0개 pass 를 거부한다(7차 리뷰 P0-02)."""
    if path is None:
        return {"status": "NOT_RUN"}, {}, []
    V: list[str] = []
    try:
        e = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as ex:
        return {"status": "FAILED", "reason": f"{type(ex).__name__}"}, {}, V
    if not isinstance(e, dict):
        return {"status": "FAILED", "reason": "최상위가 객체가 아니다"}, {}, V

    v = e.get("suite_verdict") if isinstance(e.get("suite_verdict"), dict) else {}
    scen = [s for s in (e.get("scenarios") or []) if isinstance(s, dict)]
    ces = {"pass": v.get("pass"), "reason": str(v.get("reason", ""))[:300],
           "outcomes": {s.get("id"): s.get("outcome") for s in scen},
           "scenario_count": len(scen),
           # 9차 조치 7 — **CE 가 어느 DB 에서 돌았는가.** manifest 의 source_id 를
           # 이 값과 대조한다(`check_ce_identity`). 이것을 버리면 CE 만 신원 대조에서
           # 빠지고, 파괴적 시나리오를 도는 쪽을 느슨하게 두는 셈이 된다.
           "environment": e.get("environment") if isinstance(e.get("environment"), dict)
                          else {},
           # allowlist 가 **증명하지 못하는 것**도 함께 옮긴다(P0-07 후반부).
           "env_allowlist_sha256": str(e.get("env_allowlist_sha256") or ""),
           "env_allowlist_attestation": e.get("env_allowlist_attestation")
                                        if isinstance(e.get("env_allowlist_attestation"), dict)
                                        else {}}

    if not scen:
        return ({"status": "FAILED",
                 "reason": "scenario 가 0개다. suite_verdict.pass 가 참이어도 아무것도 실행되지 "
                           "않았다 — 이것은 통과가 아니라 미실행이다"}, ces, V)
    bad_rc = [s.get("id") for s in scen
              if isinstance(s.get("child_returncode"), int) and s["child_returncode"] != 0]
    if bad_rc:
        V.append(f"G0_0C_SUITE: child 프로세스가 0 이 아닌 코드로 끝난 시나리오 {bad_rc[:5]} — "
                 f"통과 모양의 결과를 찍고 죽어도 PASS 후보가 되던 경로다")
    no_rc = [s.get("id") for s in scen if "child_returncode" not in s]
    if no_rc:
        return ({"status": "PARTIAL",
                 "reason": f"child_returncode 를 기록하지 않은 시나리오 {no_rc[:5]} — "
                           f"runner 를 갱신하고 다시 실행하라",
                 "counts": {"scenarios": len(scen)}}, ces, V)
    if not v.get("pass"):
        return ({"status": "PARTIAL", "reason": str(v.get("reason", ""))[:200],
                 "counts": {"scenarios": len(scen)}}, ces, V)
    return {"status": "MEASURED", "counts": {"scenarios": len(scen)}}, ces, V


# ── main ─────────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-id", required=True, help="이 정규화 회차의 식별자.")
    ap.add_argument("--run-id", required=True,
                    help="child 들이 공유하는 실행 회차 식별자. manifest 와 대조한다.")
    ap.add_argument("--profile", required=True,
                    choices=["LOCAL_WSL", "CORP_POC", "SANDBOX_CONTAINER"])
    ap.add_argument("--versions-lock", default="versions.lock")
    ap.add_argument("--a", help="G0-0A spool 로그")
    ap.add_argument("--b0", help="G0-0B0 출력")
    ap.add_argument("--b1", help="G0-0B1 g0-0b1-evidence.json")
    ap.add_argument("--c00", help="G0-0C00 spool 로그")
    ap.add_argument("--c-suite", help="G0-0C evidence.json")
    ap.add_argument("--out", default="g0-0-evidence.json")
    # **테이블 단위 축은 묶이지 않으면 확정값을 내지 않는다**(P0-03). 그 대상을 여기서 받는다.
    # G0-0A 가 어떤 테이블을 쟀는지는 로그에 없으므로 운영자가 알려 줘야 한다.
    ap.add_argument("--source-id", default="",
                    help="이 회차의 대상 원천 DB_UNIQUE_NAME(8차 M1-2). 주면 모든 child manifest 의 "
                         "source_id 와 대조한다. 주지 않아도 child 들 사이의 불일치는 잡는다")
    ap.add_argument("--harness-digest", default="",
                    help="기대 harness digest(8차 M1-2). 주면 모든 child manifest 와 대조한다")
    ap.add_argument("--target-owner", help="G0-0A 가 실측한 대상 스키마. 없으면 테이블 단위 축은 UNDETERMINED")
    ap.add_argument("--target-table", help="대상 테이블")
    ap.add_argument("--wm-column", help="watermark 컬럼")
    ap.add_argument("--allow-missing-manifest", action="store_true",
                    help="manifest 없는 산출물을 계약 위반이 아니라 경고로 낮춘다. "
                         "**증거 생성에 쓰지 마라** — 계약 도입 이전 로그를 살펴볼 때만 쓴다.")
    # ── 8차 M3-5: stale final alias ────────────────────────────────
    ap.add_argument("--current", default="",
                    help="'현재 유효한 G0-0 레코드' 포인터 파일(8차 M3-5). 기본값은 --out 과 "
                         "같은 디렉터리의 g0-0-evidence.current.json 이다. 성공하면 이 회차를 "
                         "가리키고, **거부되면 INVALIDATED 로 덮인다** — 무효한 재실행 뒤에 "
                         "이전 회차가 current 로 읽히지 않게 하는 것이 이 파일의 존재 이유다.")
    ap.add_argument("--allow-overwrite", action="store_true",
                    help="이미 있는 --out 을 덮어쓴다. 회차 산출물은 불변이어야 하므로 "
                         "**증거 생성에 쓰지 마라**(8차 M1-4 와 같은 이유).")
    # ── 8차 M3-3: 신선도 ───────────────────────────────────────────
    ap.add_argument("--capability-ttl-days", type=int, default=DEFAULT_TTL_DAYS,
                    help="capability 값의 유효기간(일). **기본값이 없다**(9차 조치 8) — 주지 "
                         "않거나 0 을 주면 TTL 미선언이고, 그러면 신선도를 판정할 수 없으므로 "
                         "**모든 확정값의 effective_value 가 floor 로 내려간다**(모르는 것을 "
                         "신선하다고 가정하지 않는다). 양수를 주면 그 값이 운영자 선언값으로 "
                         "레코드에 남고, measured_at + TTL 이 지난 축은 stale 이 되어 역시 "
                         "floor 로 내려간다.")
    a = ap.parse_args()

    V: list[str] = []   # contract_violations — 하나라도 있으면 exit 4
    W: list[str] = []   # warnings

    # ── 최종 G0 계약 ───────────────────────────────────────────────
    contract_path = pathlib.Path(__file__).resolve().parent / FINAL_CONTRACT_FILE
    if not contract_path.is_file():
        print(f"[fatal] {FINAL_CONTRACT_FILE} 이 없다 — not_covered 를 계약과 대조하지 못한다. "
              f"자유 목록으로 되돌리지 않는다(8차 M3-4)", file=sys.stderr)
        return 2
    try:
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        print(f"[fatal] {FINAL_CONTRACT_FILE} 이 JSON 이 아니다: {e}", file=sys.stderr)
        return 2
    contract_digest = sha(contract_path)
    covered_spec = g0_final_gate.covered_items(contract)
    not_covered = [{"item": str(i["item"]), "why": str(i["why"])}
                   for i in g0_final_gate.not_covered_items(contract)]
    all_items = set(g0_final_gate.contract_items(contract))
    cov_names = {str(i["item"]) for i in covered_spec}
    nc_names = {i["item"] for i in not_covered}
    # **차집합 자동 검사**(8차 M3-4). 덮은 것과 못 덮은 것의 합이 최종 계약과 같아야 한다 —
    # 어느 항목이 조용히 어느 쪽에도 없는 상태를 만들지 않는다.
    if cov_names & nc_names:
        V.append(f"최종 계약에서 같은 항목이 COVERED 이면서 NOT_COVERED 다 "
                 f"{sorted(cov_names & nc_names)}")
    if (cov_names | nc_names) != all_items:
        V.append(f"최종 계약 항목과 COVERED∪NOT_COVERED 가 다르다 — "
                 f"빠짐={sorted(all_items - (cov_names | nc_names))}, "
                 f"군더더기={sorted((cov_names | nc_names) - all_items)}(8차 M3-4)")

    # ── 출력 경로 (8차 M3-5) ───────────────────────────────────────
    out = pathlib.Path(a.out)
    if a.run_id and a.run_id not in str(out):
        V.append(f"--out 경로에 run_id 가 없다({out}) — 회차마다 다른 경로가 아니면 "
                 f"고정 이름 하나가 여러 회차의 별칭이 되고, 그 별칭을 읽는 쪽은 자기가 "
                 f"어느 회차를 보는지 알 수 없다(8차 M3-5)")
    if out.exists() and not a.allow_overwrite:
        V.append(f"--out 이 이미 있다({out}) — 회차 산출물은 불변이다. 덮어쓰려면 "
                 f"--allow-overwrite 를 명시하라(8차 M3-5)")
    current = pathlib.Path(a.current) if a.current else \
        out.parent / "g0-0-evidence.current.json"

    lock = pathlib.Path(a.versions_lock)
    if not lock.is_file():
        print(f"[fatal] versions.lock 이 없다: {lock}", file=sys.stderr)
        return 2
    lock_text = lock.read_text(encoding="utf-8")
    lock_digest = sha(lock)
    if not HEX64.match(lock_digest):
        print("[fatal] versions.lock 을 해시하지 못했다", file=sys.stderr)
        return 2
    if lock_has_unset(lock_text):
        W.append("versions.lock 의 **값 자리에** UNSET 이 남아 있다 — 그 항목에 의존하는 측정은 미확정이다")

    # ── child 대조 ────────────────────────────────────────────────
    paths: dict[str, pathlib.Path | None] = {}
    arts: dict = {}
    children: dict = {}
    contract_ok: dict[str, bool] = {}
    manifest_waived = False     # --allow-missing-manifest 를 실제로 쓴 회차인가(M3-3 floor)

    for key, const, argname in CHILD_KEYS:
        raw = getattr(a, argname)
        if not raw:
            paths[key] = None
            children[key] = {"present": False}
            contract_ok[key] = True     # 안 돌린 것은 위반이 아니다
            continue
        p = pathlib.Path(raw)
        if not p.is_file():
            paths[key] = None
            children[key] = {"present": False}
            contract_ok[key] = False
            V.append(f"{const}: 지정한 산출물이 없다({raw})")
            continue
        paths[key] = p
        rec, viol = check_child(const, p, a.run_id, a.profile, lock_digest,
                                source_id=a.source_id, harness_digest=a.harness_digest)
        children[key] = rec
        if viol and a.allow_missing_manifest and not rec.get("present"):
            W.append(f"[--allow-missing-manifest] {viol[0]}")
            contract_ok[key] = True
            manifest_waived = True
        else:
            V.extend(viol)
            contract_ok[key] = not viol
        # **집계 전에** child 스키마로 검증한다(8차 M1-1).
        sv = validate_child_artifact(key, p)
        if sv:
            V.extend(sv)
            contract_ok[key] = False
        arts[key] = {"path": str(p), "sha256": sha(p),
                     "lines": len(p.read_text(encoding="utf-8", errors="replace").splitlines())}

    # ── 회차 집합 검사 (8차 M1-3) ─────────────────────────────────
    # child 하나씩 보는 검사로는 "각자는 일관되지만 서로 다른 회차" 를 잡지 못한다.
    V.extend(check_run_set(children))

    # ── profile attestation (9차 조치 4) ──────────────────────────
    V.extend(check_profile_attestation(children, a.profile))

    # ── 본문 판정 ─────────────────────────────────────────────────
    # **M3-1: 계약·schema 를 통과한 산출물만 집계 입력이 된다.**
    #
    # 이전 판은 순서가 반대였다 — 먼저 전부 집계하고, 그 다음 coverage 만 FAILED 로 덮었다.
    # 그러면 coverage 는 정직해지지만 `account_privs`·`capability_axes`·`source`·
    # `spark_paths`·`fence_facts`·`counterexamples` 는 **schema 를 통과하지 못한 파일에서
    # 뽑힌 값 그대로** 레코드에 남았다. 형태가 계약과 다른 산출물을 파싱한 결과를 싣고서
    # "이 레코드는 무엇을 세었는가" 를 말할 수 없다.
    def gated(key: str) -> pathlib.Path | None:
        """계약·schema 를 통과했을 때만 경로를 준다. 아니면 None — 즉 집계하지 않는다."""
        return paths[key] if contract_ok.get(key) else None

    skipped = [k for k in ("g0_0a", "g0_0b0", "g0_0b1", "g0_0c00", "g0_0c_suite")
               if paths[k] is not None and not contract_ok.get(k)]
    if skipped:
        W.append(f"계약·schema 를 통과하지 못한 child {skipped} 는 **집계 입력에서 제외했다** — "
                 f"coverage 만 FAILED 로 덮고 본문은 싣던 것이 8차 M3-1 이다")

    ca, P, va = cov_a(gated("g0_0a"))
    V.extend(va)
    cb0 = cov_b0(gated("g0_0b0"))
    cb1, spark_paths = cov_b1(gated("g0_0b1"))
    cc00, fence = cov_c00(gated("g0_0c00"))
    ccs, ces, vce = cov_ce(gated("g0_0c_suite"))
    V.extend(vce)

    coverage = {"g0_0a": ca, "g0_0b0": cb0, "g0_0b1": cb1, "g0_0c00": cc00, "g0_0c_suite": ccs}

    # 계약을 못 지킨 child 는 본문이 아무리 그럴듯해도 FAILED 다. 위 `gated` 때문에
    # 그런 child 의 상태는 NOT_RUN 으로 나오는데, **'안 돌렸다' 와 '계약을 안 지켰다' 는
    # 다르다** — 여기서 FAILED 로 되돌린다.
    for key in coverage:
        if not contract_ok[key] and paths[key] is not None:
            coverage[key] = {"status": "FAILED", "reason": "child 계약 위반 — contract_violations 참조"}

    # A 가 MEASURED 가 아니면 그 산출물에서 나온 값을 확정으로 쓰지 않는다.
    a_not_measured = coverage["g0_0a"]["status"] != "MEASURED"
    if coverage["g0_0a"]["status"] == "FAILED":
        P = {}
        W.append("G0-0A 가 FAILED 다 → account_privs 와 그로부터 파생될 값을 신뢰하지 않는다")

    # source — 서버가 스스로 밝힌 신원만.
    src = {}
    for k, pid in (("db_unique_name", "userenv.DB_UNIQUE_NAME"),
                   ("database_role", "userenv.DATABASE_ROLE"),
                   ("instance_name", "userenv.INSTANCE_NAME"),
                   ("oracle_version", "ver.product_component"),
                   ("characterset", "nls.characterset"),
                   # P §8.1 의 oracle_env 세 값 중 둘. G0-0A 가 실제로 수집한다 —
                   # not_covered 에 "probe 가 없다" 고 쓰면 사실이 아니다.
                   ("nchar_characterset", "nls.nchar_characterset"),
                   ("max_string_size", "v$parameter.max_string")):
        r = P.get(pid)
        if r and r.get("query_ok") is True and r.get("value") is not None:
            src[k] = str(r["value"])

    complete = all(c["status"] == "MEASURED" for c in coverage.values())

    # 테이블 단위 축의 binding. db_identity 는 **서버가 밝힌** 이름으로 만든다 —
    # 운영자 신고값으로 묶으면 잘못 지정한 실행이 일관돼 보인다.
    binding = None
    if a.target_owner and a.target_table and src.get("db_unique_name"):
        binding = {"db_identity": src["db_unique_name"], "owner": a.target_owner,
                   "object": a.target_table, "object_type": "TABLE"}
        if a.wm_column:
            binding["column"] = a.wm_column
        src["target_owner"], src["target_table"] = a.target_owner, a.target_table
        if a.wm_column:
            src["wm_column"] = a.wm_column
    elif a.target_owner or a.target_table:
        W.append("--target-owner/--target-table 중 일부만 주었거나 서버가 db_unique_name 을 "
                 "밝히지 않았다 → 테이블 단위 축을 묶지 못해 UNDETERMINED 로 둔다")

    # ── 서버 신원 결속 (9차 조치 4 · P0-02) ───────────────────────
    # manifest 의 source_id 는 **운영자 신고값**이다. 서버가 스스로 밝힌 값과 대조해야
    # 증거가 된다 — 그 대조가 없어서 한 레코드 안에 두 원천이 공존했다.
    v_src, w_src = check_source_identity(P, children, a.source_id)
    V.extend(v_src)
    W.extend(w_src)
    source_identity_verified = not v_src and not w_src

    # ── CE 신원 결속 (9차 조치 7 · P0-07) ─────────────────────────
    # scope 를 도입하면 CE 는 A 의 서버 신원 대조에서 빠진다. 거기서 끝내면 **파괴적
    # 시나리오를 도는 쪽만 신원 대조를 면제받는 것**이 되므로, CE 는 자기 환경의 서버
    # 신원(CE runner 가 V$DATABASE 에서 읽은 값)에 묶는다.
    v_ce, w_ce = check_ce_identity(children, ces.get("environment") or {})
    V.extend(v_ce)
    W.extend(w_ce)

    # 레코드가 **스스로 scope 를 말한다.** 이것이 없으면 읽는 쪽이 source_id 하나만 보고
    # 회차 전체가 그 DB 의 것이라고 읽는다 — P0-07 이 만든 오독이 정확히 그것이다.
    environment_scopes = {}
    for name, rec in sorted(children.items()):
        if not (isinstance(rec, dict) and rec.get("present")):
            continue
        sc = str(rec.get("environment_scope") or "UNDECLARED")
        e = environment_scopes.setdefault(
            sc, {"children": [], "source_ids": [], "means": SCOPE_MEANING.get(sc, "")})
        e["children"].append(name)
        sid = str(rec.get("source_id") or "")
        if sid and sid not in e["source_ids"]:
            e["source_ids"].append(sid)

    # ── 축 파생 + effective floor (8차 M3-3) ──────────────────────
    # `value` 는 관측 사실이고 `effective_value` 는 실행에 쓰는 값이다. 아래 사유가 하나라도
    # 붙으면 그 축의 effective_value 는 floor 로 내려간다 — **요약도 exit 판정도
    # effective_value 를 읽는다**(8차 §6: 구현이 모든 축에 effective_value=value 를 넣고
    # summary 는 value 를 읽던 것이 결함이었다).
    floor_reasons: list[str] = []
    if a_not_measured:
        floor_reasons.append("CHILD_NOT_MEASURED")
    if manifest_waived:
        floor_reasons.append("SOURCE_UNVERIFIED")
    if a.profile in ("LOCAL_WSL", "SANDBOX_CONTAINER"):
        # 이 레코드는 스스로 "하네스 동작 확인용이며 설계 주장의 근거가 아니다" 라고 적는다.
        # 그렇게 적으면서 확정 capability 를 publish 값으로 내보내면 두 말이 어긋난다.
        floor_reasons.append("PROFILE_NOT_AUTHORITATIVE")
    if not source_identity_verified:
        # 9차 조치 4 — 서버가 밝힌 신원과 대조하지 못했으면 이 회차가 **어느 원천의
        # capability 인지** 말할 수 없다. `CORP_POC` 라고 적혀 있어도 마찬가지다.
        floor_reasons.append("SOURCE_IDENTITY_UNVERIFIED")
    # 사유 이름은 g0_axes 의 표가 권위다. 여기서 새 이름을 지어내면 레코드를 읽는 쪽이
    # 뜻을 찾을 곳이 없다.
    undeclared = [r for r in floor_reasons if r not in g0_axes.FLOOR_REASONS]
    if undeclared:
        V.append(f"선언되지 않은 floor 사유 {undeclared} — g0_axes.FLOOR_REASONS 에 뜻을 "
                 f"적지 않은 이름은 쓰지 않는다")
    ttl_seconds = a.capability_ttl_days * 86400 if a.capability_ttl_days > 0 else None
    if ttl_seconds is None:
        W.append("--capability-ttl-days 미선언 — 이 인자에는 기본값이 없다(9차 조치 8). "
                 "신선도를 판정할 수 없으므로 모든 확정값의 effective_value 가 floor 로 "
                 "내려간다. 유효기간을 주장하려면 운영자가 일수를 명시해야 한다")
    evaluated_at = now_iso()

    axes = g0_axes.derive_axes(
        P, binding=binding,
        measured_at=children.get("g0_0a", {}).get("measured_at"),
        now=evaluated_at, ttl_seconds=ttl_seconds, floor_reasons=floor_reasons)

    def determinate(v: str) -> bool:
        return v not in ("UNDETERMINED", "UNDEFINED")

    eff = {k: v["effective_value"] for k, v in axes.items()}
    n_graded = sum(1 for v in eff.values() if determinate(v))
    capability = ("GRADED" if n_graded == len(eff) else
                  "UNGRADED" if n_graded == 0 else "PARTIALLY_GRADED")

    rec = {
        "schema_version": SCHEMA_VERSION,
        "record_type": "g0_0_evidence",
        "scope": "CAPABILITY_INVENTORY",
        "gate_eligible": False,
        "completeness": "COMPLETE" if complete else "INCOMPLETE",
        "g0_report_id": a.report_id,
        "run_id": a.run_id,
        "profile": a.profile,
        "normalized_at": now_iso(),
        "versions_lock_digest": lock_digest,
        "children": children,
        # 9차 조치 7 — **이 레코드가 몇 개 환경의 증거를 담고 있는가.** source_id 하나만
        # 보고 회차 전체를 그 DB 의 것으로 읽지 못하게 한다(P0-07).
        "environment_scopes": environment_scopes,
        "coverage": coverage,
        "not_covered": not_covered,
        "final_contract_digest": contract_digest,
        "freshness": {
            "basis": "OPERATOR_DECLARED_TTL" if ttl_seconds else "NO_TTL_DECLARED",
            "ttl_seconds": ttl_seconds,
            "evaluated_at": evaluated_at,
            # 미선언일 때 "운영자가 선언한 상한" 이라고 적으면 basis 와 어긋난다 —
            # P1-03 이 지적한 것과 같은 종류의 거짓이므로 basis 별로 나눈다(9차 조치 8).
            "note": ("**측정 분포로 정한 값이 아니다.** capability 값이 얼마나 오래 유효한지는 "
                     "실측을 모아야 알 수 있고(M5), 그 전까지 이것은 운영자가 선언한 상한이다."
                     if ttl_seconds else
                     "**유효기간이 선언되지 않았다.** 운영자가 --capability-ttl-days 를 주지 "
                     "않았으므로 이 회차는 신선도를 판정하지 못했고, 확정값은 전부 floor 로 "
                     "내려가 있다. 얼마가 옳은 값인지는 측정 분포가 나와야 답할 수 있다(M5)."),
        },
        "outcome": {
            # 네 값을 분리한다(8차 P0-04 권고). 이전 판은 exit 3 하나에 "정상 측정 결과로
            # capability 가 없음/미확정" 과 "child 실행 불완전" 을 섞었다.
            "process": "CLEAN",           # 아래 schema 검증 뒤에 확정한다
            "measurement": "COMPLETE" if complete else "INCOMPLETE",
            "capability": capability,
            "final_gate": "REJECTED_BY_CONTRACT",
        },
        "account_privs": list(P.values()),
        "capability_axes": axes,
        "artifacts": arts,
        "spark_paths": spark_paths, "fence_facts": fence, "counterexamples": ces,
        "contract_violations": V,
        "warnings": W,
    }
    if src:
        rec["source"] = src

    # **덮었다고 선언한 항목이 실제로 이 레코드에 있는가**(8차 M3-4). 없으면 위반이 아니라
    # 사실로 적는다 — 그 child 를 안 돌린 회차는 없는 것이 정상이고, 거짓 주장을 막는 것이
    # 목적이지 실행 범위를 강제하는 것이 목적이 아니다.
    rec["covered"] = [
        {"item": str(i["item"]), "where": str(i["where"]),
         "present": bool(g0_final_gate.resolve(rec, str(i["where"])))}
        for i in covered_spec
    ]

    if a.profile in ("LOCAL_WSL", "SANDBOX_CONTAINER"):
        rec["warnings"].append(
            f"profile={a.profile} — 이 증거는 **하네스 동작 확인용**이며 설계 주장의 근거가 아니다. "
            + ("원천 capability 값·ADG 거동·규모는 사내 환경에서만 잴 수 있다."
               if a.profile == "LOCAL_WSL" else
               "이 환경에는 Oracle 서버가 없다. 원천에 붙는 모든 측정은 미실행이며, 확인된 것은 "
               "코드가 그 Spark 판본에 대해 컴파일·배선되는가 뿐이다. LOCAL_WSL 보다 제약이 강하다."))

    # ── schema 검증 — 위반이면 최종 경로에 쓰지 않는다 ──────────────
    schema_errs: list[str] = []
    sp = pathlib.Path(__file__).resolve().parent / SCHEMA_FILE
    if not sp.is_file():
        schema_errs.append(f"{SCHEMA_FILE} 이 없다 — 계약으로 검증하지 못했다")
    else:
        try:
            import jsonschema
        except ImportError:
            schema_errs.append("jsonschema 미설치 — 이 레코드를 계약으로 검증하지 못했다. "
                               "검증하지 못한 것을 통과로 두지 않는다")
        else:
            sc = json.loads(sp.read_text(encoding="utf-8"))
            # **schema 의 not_covered enum 과 최종 계약이 같은 것을 말하는가**(8차 M3-4).
            # 같은 목록을 두 파일이 각자 적으면 조용히 갈라진다 — 그것이 7차 P0-04 였다.
            try:
                enum = set(sc["properties"]["not_covered"]["items"]
                             ["properties"]["item"]["enum"])
            except (KeyError, TypeError):
                enum = None
            if enum is not None and enum != nc_names:
                V.append(f"{SCHEMA_FILE} 의 not_covered enum 과 {FINAL_CONTRACT_FILE} 의 "
                         f"NOT_COVERED 가 다르다 — schema에만={sorted(enum - nc_names)}, "
                         f"계약에만={sorted(nc_names - enum)}(8차 M3-4)")
            for e in sorted(jsonschema.Draft202012Validator(sc).iter_errors(rec),
                            key=lambda x: list(x.path))[:8]:
                schema_errs.append(f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message[:160]}")

    if V or schema_errs:
        rec["outcome"]["process"] = "REJECTED"
        out.parent.mkdir(parents=True, exist_ok=True)
        rejected = out.with_name(out.name + ".rejected.json")
        rejected.write_text(json.dumps(rec, ensure_ascii=False, indent=1), encoding="utf-8")
        # **M3-5: current 포인터를 무효화한다.** 이전 회차의 유효 포인터를 그대로 두면
        # 무효한 재실행 뒤에도 소비자는 옛 레코드를 "현재" 로 읽는다 — 그리고 그 사실을
        # 어디에서도 알 수 없다. 거부는 조용하면 안 된다.
        write_pointer(current, {
            "status": "INVALIDATED",
            "invalidated_at": now_iso(),
            "rejected_run_id": a.run_id,
            "rejected_report_id": a.report_id,
            "rejected_copy": str(rejected),
            "reason_count": {"contract_violations": len(V), "schema_errors": len(schema_errs)},
            "note": "이 회차가 거부됐다. **이전 회차 레코드를 current 로 읽지 마라** — "
                    "그것은 이 회차가 무효라는 사실을 반영하지 않는다. 유효한 레코드를 "
                    "원하면 위반을 고쳐 다시 정규화하라.",
        })
        for e in schema_errs:
            print(f"[schema] {e}", file=sys.stderr)
        for v in V:
            print(f"[contract] {v}", file=sys.stderr)
        print(json.dumps({
            "verdict": "REJECTED",
            "why": "계약 위반 또는 schema 위반이 있다. **최종 경로에 쓰지 않았다.**",
            "rejected_copy": str(rejected),
            "current_pointer": str(current),
            "current_status": "INVALIDATED",
            "contract_violations": V,
            "schema_errors": schema_errs,
        }, ensure_ascii=False, indent=1))
        return 4

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rec, ensure_ascii=False, indent=1), encoding="utf-8")
    write_pointer(current, {
        "status": "VALID",
        "record_type": rec["record_type"],
        "gate_eligible": rec["gate_eligible"],
        "run_id": a.run_id,
        "g0_report_id": a.report_id,
        "path": str(out),
        "sha256": sha(out),
        "normalized_at": rec["normalized_at"],
        "outcome": rec["outcome"],
        "note": "**gate_eligible=false 다.** 이 포인터는 '가장 최근에 유효했던 G0-0 레코드' 를 "
                "가리킬 뿐이며 G0 PASS 를 뜻하지 않는다. 최종 게이트 입력 자격은 "
                "g0_final_gate.admit 이 판정하고, 이 record_type 은 항상 거절된다.",
    })
    print(json.dumps({
        "out": str(out),
        "current_pointer": str(current),
        "record_type": rec["record_type"],
        "gate_eligible": rec["gate_eligible"],
        "completeness": rec["completeness"],
        "outcome": rec["outcome"],
        "coverage": {k: v["status"] for k, v in coverage.items()},
        # **effective_value 를 낸다.** `value` 는 audit·표시용이며 여기에 싣지 않는다 —
        # 요약이 value 를 읽던 것이 8차 §6 의 지적이다.
        "capability_axes_effective": eff,
        "floored_axes": {k: v["floor_reasons"] for k, v in axes.items() if v["floor_reasons"]},
        "undetermined_axes": [k for k, v in eff.items() if not determinate(v)],
        "warnings": rec["warnings"],
    }, ensure_ascii=False, indent=1))
    return 0 if complete else 3


if __name__ == "__main__":
    sys.exit(main())
