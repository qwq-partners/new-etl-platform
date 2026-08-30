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

종료 코드 (g0-child-contract.md §4)
  0  유효한 레코드를 썼다
  3  불완전 — child 중 NOT_RUN/PARTIAL 이 있다. 레코드는 쓴다(completeness=INCOMPLETE)
  4  무효 — 계약 위반 또는 schema 위반. **최종 경로에 쓰지 않고** <out>.rejected.json 에만 남긴다
  2  실행 전 조건 미비(인자·파일 없음 등)

사용:
  python3 g0-normalize.py --report-id NORM-2026-08-27-01 --run-id RUN-2026-08-27-01 \\
      --profile LOCAL_WSL --a g0-0a.log --b0 b0.json --b1 g0-0b1-evidence.json \\
      --c00 c00.log --c-suite g0-0c-counterexamples/evidence.json \\
      --versions-lock versions.lock --out g0-0-evidence.json
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

SCHEMA_FILE = "g0-0-evidence.schema.json"
SCHEMA_VERSION = "2.1.0"

# P §8.1 의 g0_evidence 항목 중 G0-0 이 도달하지 못하는 것.
# **고정 집합이다.** item 값은 schema 의 enum 과 정확히 일치해야 한다(7차 리뷰 P0-04).
NOT_COVERED = [
    {"item": "hash_vector_result (V-01~V-16)",
     "why": "canonical hash 벡터 시험은 G0-3 소관이다. G0-0 은 STANDARD_HASH 가용성만 본다."},
    {"item": "ddl_digest",
     "why": "target(Iceberg) DDL 이 아직 없다. 플랫폼을 세운 뒤에 생긴다."},
    {"item": "verdict_sql_digest",
     "why": "판정 SQL 이 규범 확정 후에 나온다."},
    {"item": "canonical_hash_spec_digest",
     "why": "ETL_CANON 함수·pinned 매핑표가 아직 없다(원천 DDL 불가로 보류)."},
    {"item": "submission_path_result",
     "why": "Dagster 제출 경로 시험은 G1 소관이다."},
    {"item": "source_kind",
     "why": "ORACLE_TEST_INSTANCE / ORACLE_COMPATIBLE_STUB 구분은 G0-1~5 aggregator 가 정한다. "
            "G0-0 은 접속한 서버가 스스로 밝힌 신원만 기록한다."},
    {"item": "oracle_env.nls_nchar_characterset",
     "why": "**값은 G0-0A 가 수집한다**(probe `nls.nchar_characterset`) — source 에 담는다. "
            "덮지 못하는 것은 그 값이 STRING 경로의 TO_NCHAR 결과에 미치는 영향 판정이며 "
            "그것은 G0-3 V-01~V-16 소관이다."},
    {"item": "oracle_env.max_string_size",
     "why": "**값은 G0-0A 가 수집한다**(probe `v$parameter.max_string`) — source 에 담는다. "
            "덮지 못하는 것은 그 값이 canonical hash 규격에 미치는 영향 판정이며 G0-3 소관이다."},
    {"item": "same_lock (G0-5)",
     "why": "동일 lock 실증은 G0-1~G0-4 산출물이 모두 있어야 성립한다. G0-0 하나로는 불가능하다."},
]

CHILD_KEYS = [("g0_0a", "G0_0A", "a"), ("g0_0b0", "G0_0B0", "b0"), ("g0_0b1", "G0_0B1", "b1"),
              ("g0_0c00", "G0_0C00", "c00"), ("g0_0c_suite", "G0_0C_SUITE", "c_suite")]
# check_run_set 이 '계약에 없는 child' 를 가리는 데 쓴다(8차 M1-3).
CHILD_CONSTS = [k for k, _c, _a in CHILD_KEYS]

HEX64 = re.compile(r"^[0-9a-f]{64}$")


def sha(p: pathlib.Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else "MISSING"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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

    # ── M1-2: source·harness·시각 결속 ──────────────────────────────
    if not rec["source_id"]:
        V.append(f"{child_const}: manifest 에 source_id 가 없다 — **어느 원천에서 잰 값인지 "
                 f"말하지 못한다.** 여러 원천의 산출물을 한 회차로 섞어도 알 수 없다(8차 M1-2)")
    elif source_id and rec["source_id"] != source_id:
        V.append(f"{child_const}: source_id 불일치 (manifest={rec['source_id']!r}, "
                 f"요청={source_id!r}) — 서로 다른 원천의 산출물을 섞었다")

    if rec["harness_digest"] in ("", "MISSING", "NO_HARNESS_FILES"):
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

    def spread(field: str) -> list[str]:
        return sorted({str(v.get(field, "")) for v in present.values()})

    for field, why in (("run_id", "서로 다른 회차"),
                       ("source_id", "서로 다른 원천"),
                       ("versions_lock_digest", "서로 다른 실행 판본"),
                       ("harness_digest", "서로 다른 하네스 코드")):
        vals = [v for v in spread(field) if v and v != "MISSING"]
        if len(vals) > 1:
            V.append(f"child 들이 {why}의 것이다 — {field}: {vals}. "
                     f"각 child 는 자기 manifest 와 일관되므로 개별 검사로는 잡히지 않는다. "
                     f"**이어 붙인 회차는 하나의 회차가 아니다**(8차 M1-3)")

    return V


# ── child 별 완결 판정 ────────────────────────────────────────────────
def cov_a(path: pathlib.Path | None) -> tuple[dict, dict[str, dict], list[str]]:
    """G0-0A. 완결 조건: sentinel ∧ manifest_ok ∧ 중복 0 ∧ summary 정확히 1개."""
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
           "scenario_count": len(scen)}

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
    a = ap.parse_args()

    V: list[str] = []   # contract_violations — 하나라도 있으면 exit 4
    W: list[str] = []   # warnings

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

    # ── 본문 판정 ─────────────────────────────────────────────────
    ca, P, va = cov_a(paths["g0_0a"])
    V.extend(va)
    cb0 = cov_b0(paths["g0_0b0"])
    cb1, spark_paths = cov_b1(paths["g0_0b1"])
    cc00, fence = cov_c00(paths["g0_0c00"])
    ccs, ces, vce = cov_ce(paths["g0_0c_suite"])
    V.extend(vce)

    coverage = {"g0_0a": ca, "g0_0b0": cb0, "g0_0b1": cb1, "g0_0c00": cc00, "g0_0c_suite": ccs}

    # 계약을 못 지킨 child 는 본문이 아무리 그럴듯해도 FAILED 다.
    for key in coverage:
        if not contract_ok[key] and coverage[key]["status"] != "NOT_RUN":
            coverage[key] = {"status": "FAILED", "reason": "child 계약 위반 — contract_violations 참조"}

    # A 가 실패했으면 그 산출물에서 나온 것을 쓰지 않는다.
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

    axes = g0_axes.derive_axes(
        P, binding=binding,
        measured_at=children.get("g0_0a", {}).get("measured_at"))

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
        "coverage": coverage,
        "not_covered": NOT_COVERED,
        "account_privs": list(P.values()),
        "capability_axes": axes,
        "artifacts": arts,
        "spark_paths": spark_paths, "fence_facts": fence, "counterexamples": ces,
        "contract_violations": V,
        "warnings": W,
    }
    if src:
        rec["source"] = src

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
            for e in sorted(jsonschema.Draft202012Validator(sc).iter_errors(rec),
                            key=lambda x: list(x.path))[:8]:
                schema_errs.append(f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message[:160]}")

    out = pathlib.Path(a.out)
    if V or schema_errs:
        rejected = out.with_name(out.name + ".rejected.json")
        rejected.write_text(json.dumps(rec, ensure_ascii=False, indent=1), encoding="utf-8")
        for e in schema_errs:
            print(f"[schema] {e}", file=sys.stderr)
        for v in V:
            print(f"[contract] {v}", file=sys.stderr)
        print(json.dumps({
            "verdict": "REJECTED",
            "why": "계약 위반 또는 schema 위반이 있다. **최종 경로에 쓰지 않았다.**",
            "rejected_copy": str(rejected),
            "contract_violations": V,
            "schema_errors": schema_errs,
        }, ensure_ascii=False, indent=1))
        return 4

    out.write_text(json.dumps(rec, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({
        "out": str(out),
        "record_type": rec["record_type"],
        "gate_eligible": rec["gate_eligible"],
        "completeness": rec["completeness"],
        "coverage": {k: v["status"] for k, v in coverage.items()},
        "capability_axes": {k: v["value"] for k, v in axes.items()},
        "undetermined_axes": [k for k, v in axes.items() if v["value"] == "UNDETERMINED"],
        "warnings": rec["warnings"],
    }, ensure_ascii=False, indent=1))
    return 0 if complete else 3


if __name__ == "__main__":
    sys.exit(main())
