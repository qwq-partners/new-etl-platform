#!/usr/bin/env python3
"""G0-0 산출물 → `g0_evidence` 레코드 정규화.

probe-README 가 "두 로그를 g0_evidence 로 정규화한다" 를 첫 단계로 지시하는데
그 형식의 스키마도 도구도 없었다(2026-08-27 리뷰 확정). 이 파일이 그 도구다.

**규율**
  · 없는 입력을 낙관적으로 채우지 않는다. 안 잰 것은 NOT_RUN, 못 정한 것은 UNDETERMINED.
  · capability 축은 **probe 결과에서 파생**하며, 어떤 probe 로 정했는지(derived_from)를
    반드시 남긴다. 사람이 재판정할 수 있어야 한다.
  · G0-0A 의 `manifest_ok=false` 는 그 산출물 전체를 무효로 만든다(문서 규칙).
  · G0-0 은 G0 의 부분집합이다 — 덮지 못하는 항목을 not_covered 에 명시한다.

사용:
  python3 g0-normalize.py --report-id RUN-2026-08-27-01 --profile LOCAL_WSL \\
      --a g0-0a.log --b0 b0.json --b1 g0-0b1-evidence.json \\
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

# P §8.1 의 g0_evidence 항목 중 G0-0 이 도달하지 못하는 것.
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
]


def sha(p: pathlib.Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else "MISSING"


def jsonl(path: pathlib.Path, key: str = "probe") -> list[dict]:
    """DBMS_OUTPUT spool 이나 stdout 에서 JSON 객체 줄만 뽑는다. 다른 줄은 무시한다."""
    out = []
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
        if isinstance(o, dict) and (key in o or "probe_summary" in o):
            out.append(o)
    return out


def by_id(recs: list[dict]) -> dict[str, dict]:
    return {r["probe"]: r for r in recs if isinstance(r.get("probe"), str)}


def ok(p: dict | None) -> bool:
    return bool(p) and p.get("query_ok") is True


def val(p: dict | None):
    return p.get("value") if p else None


# ── 실패의 성격 구분 ─────────────────────────────────────────────────
# "기능이 없다" 와 "측정하지 못했다" 는 다르다. 뒤엣것을 NONE 으로 적으면
# 미확정을 확정으로 바꾸는 것이고, 그게 이 저장소가 금지하는 바로 그 일이다.
ABSENCE_ORA = {942, 1031, 900, 6550, 439, 2003, 1918, 990}   # 없음·권한없음·구문 미지원
def absent(p: dict | None) -> bool:
    """이 probe 의 실패가 '기능 부재' 로 읽혀도 되는가."""
    if not p or p.get("query_ok") is True:
        return False
    o = p.get("ora")
    return isinstance(o, int) and abs(o) in ABSENCE_ORA


def unknown(p: dict | None) -> bool:
    """probe 가 없거나, 실패했는데 그 원인이 기능 부재가 아니다(타임아웃·접속단절 등)."""
    return p is None or (p.get("query_ok") is not True and not absent(p))


# ── capability 축 파생 ───────────────────────────────────────────────
def derive_axes(P: dict[str, dict]) -> dict:
    """capability-overlay 의 축을 실제 probe id 에서 파생한다.

    **규칙 셋 (2026-08-27 7차 교차 리뷰 P0-01 반영)**
      1. 어떤 등급도 **그 등급 자신의 probe 가 성공했을 때만** 준다.
         상위가 실패했다고 하위로 떨어뜨리지 않는다 — 하위도 실패했을 수 있다.
      2. 실패가 '기능 부재'(ORA-00942/01031/00900 등)로 확인될 때만 NONE 이다.
         원인 불명 실패·probe 부재는 UNDETERMINED.
      3. `COUNT(*)` 가 되는 것과 그 뷰에서 **값을 읽을 수 있는 것**은 다르다.
    """
    A: dict[str, dict] = {}

    def put(axis, value, used, note=None):
        A[axis] = {"value": value, "derived_from": used, **({"note": note} if note else {})}

    # ── snapshot_read ────────────────────────────────────────────────
    # 주의: view.v_database 는 `SELECT COUNT(*) FROM v$database` 일 뿐 CURRENT_SCN 을
    #       읽지 않는다. **SCN 원점이 아니다** — 여기서 제외한다(P0-01).
    asof = P.get("as_of_timestamp.target")
    scn = P.get("dbms_flashback.get_scn")
    ro, ro_sel = P.get("txn.set_read_only"), P.get("txn.select_inside")
    reissue = P.get("txn.set_read_only.reissue")
    used = ["as_of_timestamp.target", "dbms_flashback.get_scn",
            "txn.set_read_only", "txn.select_inside", "txn.set_read_only.reissue"]
    if ok(asof) and ok(scn):
        put("snapshot_read", "AS_OF_SCN", used,
            "AS OF 조회와 SCN 원점이 모두 성공했다. 다만 **여러 connection 이 같은 anchor 를 "
            "공유하는지**는 이 probe 로 증명되지 않는다(G0-0B1 소관).")
    elif ok(asof):
        put("snapshot_read", "AS_OF_TIMESTAMP", used,
            "AS OF 는 되지만 SCN 원점이 없다. SCN_TO_TIMESTAMP 계열은 약 3초 근삿값이라 "
            "AS_OF_SCN 으로 올리지 않는다.")
    elif ok(ro) and ok(ro_sel):
        # ORA-01453(두 번째 SET TRANSACTION 거부)이 나와야 첫 트랜잭션이 실제로 열렸다는 양성 대조다.
        pc = isinstance((reissue or {}).get("ora"), int) and abs(reissue["ora"]) == 1453
        put("snapshot_read", "READ_ONLY_TXN", used,
            "양성 대조(재발행 ORA-01453) 확인됨" if pc else
            "**양성 대조 없음** — 재발행에서 ORA-01453 이 관측되지 않아 트랜잭션이 실제로 "
            "열렸는지 확정되지 않았다. 이 값은 잠정이다.")
    elif all(absent(x) for x in (asof, ro, ro_sel) if x is not None) and not all(
            x is None for x in (asof, ro, ro_sel)):
        put("snapshot_read", "NONE", used, "AS OF 와 READ ONLY 가 모두 기능 부재로 확인됐다")
    else:
        put("snapshot_read", "UNDETERMINED", used,
            "상위 등급이 실패했다고 하위로 내리지 않는다 — 하위 probe 도 성공하지 않았다")

    # ── row_hash ─────────────────────────────────────────────────────
    h = P.get("feat.standard_hash_sha256")
    used = ["feat.standard_hash_sha256"]
    if ok(h) and h.get("value_interpretable") is True:
        put("row_hash", "SHA256", used,
            "표준 시험 벡터와 일치. **cross-engine canonical row hash 는 별개다** — "
            "G0-3 의 V-01~V-16 전까지 행 대조 가능성으로 승격하지 마라.")
    elif absent(h):
        put("row_hash", "NONE", used + ["feat.ora_hash"],
            "ORA_HASH 는 32비트라 대조용 대체재가 아니다 — Reconciliation 이 건수+PK 로 강등된다")
    else:
        put("row_hash", "UNDETERMINED", used)

    # ── row_change_scn ───────────────────────────────────────────────
    dep, rs = P.get("feat.rowdependencies_target"), P.get("feat.ora_rowscn_target")
    used = ["feat.rowdependencies_target", "feat.ora_rowscn_target"]
    if ok(rs) and ok(dep) and str(val(dep) or "").upper() == "ENABLED":
        put("row_change_scn", "ROW_LEVEL", used)
    elif ok(rs) and ok(dep):
        put("row_change_scn", "BLOCK_LEVEL", used,
            f"ROWDEPENDENCIES={val(dep)!r} — 블록 단위 SCN 은 상한만 보장한다. 사후 변경 불가")
    elif absent(rs):
        put("row_change_scn", "NONE", used)
    else:
        put("row_change_scn", "UNDETERMINED", used)

    # ── lag: 관측과 강제는 **독립 축**이다 (P0-05) ──────────────────────
    dg = P.get("view.v_dataguard_stats")
    used = ["view.v_dataguard_stats"]
    if ok(dg):
        try:
            rows = int(str(val(dg)))
        except (TypeError, ValueError):
            rows = -1
        if rows >= 1:
            put("lag_observation", "DG_STATS", used,
                "뷰에 행이 있다. 다만 이 probe 는 COUNT(*) 라 **lag 값·DATUM_TIME 을 해석하지 않는다** — "
                "값 해석 가능성은 별도 probe 가 필요하다.")
        else:
            put("lag_observation", "UNDETERMINED", used,
                f"뷰는 읽히지만 행이 {rows}건이다. 읽을 수 있다는 것과 lag 을 잴 수 있다는 것은 다르다.")
    elif absent(dg):
        put("lag_observation", "NONE", used)
    else:
        put("lag_observation", "UNDETERMINED", used)

    d, zero = P.get("alter.STANDBY_MAX_DATA_DELAY.D"), P.get("max_delay_zero.touch_target")
    used = ["alter.STANDBY_MAX_DATA_DELAY.D", "max_delay_zero.touch_target"]
    pc3172 = isinstance((zero or {}).get("ora"), int) and abs(zero["ora"]) == 3172
    if ok(d) and pc3172:
        put("lag_admission", "ENFORCED", used, "ORA-03172 양성 대조 확보 — 강제가 실제로 걸린다")
    elif ok(d):
        put("lag_admission", "ACCEPTED_UNVERIFIED", used,
            "**ALTER 가 수락됐을 뿐이다.** ORA-03172 양성 대조가 없으면 '오류가 안 났다' 가 "
            "유일한 근거이고, 그것은 강제가 걸린다는 증거가 아니다. lag 이 큰 시간대에 재실행하라.")
    elif absent(d):
        put("lag_admission", "NONE", used)
    else:
        put("lag_admission", "UNDETERMINED", used)

    # ── watermark_commit_bound — **측정 수단이 없다** (P0-05) ──────────
    put("watermark_commit_bound", "UNDETERMINED", [],
        "A 의 max_commit_minus_watermark_seconds 는 apply lag 과 **독립**이다 "
        "(lag=0 이어도 오래된 UPDATE_DT 를 가진 트랜잭션이 늦게 commit 하면 overlap 밖 누락). "
        "이 축을 재는 probe 가 G0-0 에 **없다**. lag_visibility 로 대체할 수 없다.")

    # ── wm_granularity ───────────────────────────────────────────────
    t = P.get("wm_column.type_facts")
    used = ["wm_column.type_facts", "feat.interval_ns_successor", "feat.timestamp9_precision"]
    if not ok(t):
        put("wm_granularity", "UNDETERMINED", used, "wm_column.type_facts 를 읽지 못했다")
    else:
        raw = str(val(t) or "")
        dt = raw.split("|")[0].upper()
        m = re.search(r"scale=(-?\d+)", raw)
        scale = int(m.group(1)) if m and m.group(1) != "-" else None
        if dt.startswith("TIMESTAMP"):
            g = {9: "NS", 6: "US", 3: "MS", 0: "SEC"}.get(scale if scale is not None else 6, "US")
            put("wm_granularity", g, used, f"data_type={dt}, scale={scale}")
        elif dt == "DATE":
            put("wm_granularity", "SEC", used, "DATE 의 최소 단위는 1초다")
        elif dt == "NUMBER" and scale is not None and scale >= 0:
            put("wm_granularity", "SEC", used, f"NUMBER(*,{scale}) — 고정 scale 이라 successor 가 정의된다")
        else:
            put("wm_granularity", "UNDEFINED", used,
                f"data_type={dt} scale={scale} — 고정 granularity 가 없어 successor(M)==M 이 된다(CE01)")

    # ── sql_dialect / charset ────────────────────────────────────────
    ff = P.get("feat.fetch_first")
    put("sql_dialect", "UNDETERMINED" if unknown(ff) else ("12C_PLUS" if ok(ff) else "11G"),
        ["feat.fetch_first"])

    cs = P.get("nls.characterset")
    used = ["nls.characterset", "nls.comp", "nls.sort"]
    if not ok(cs):
        put("charset_class", "UNDETERMINED", used)
    else:
        v = str(val(cs) or "").upper()
        put("charset_class", "AL32UTF8" if v == "AL32UTF8" else "OTHER", used,
            f"NLS_CHARACTERSET={v}. **이 DB 하나의 charset 이며 원천 간 비교 가능성은 별개다**")
    return A


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-id", required=True, help="회차 식별자. 재실행마다 달라야 한다.")
    ap.add_argument("--profile", required=True, choices=["LOCAL_WSL", "CORP_POC"])
    ap.add_argument("--versions-lock", default="versions.lock")
    ap.add_argument("--a", help="G0-0A spool 로그")
    ap.add_argument("--b0", help="G0-0B0 출력")
    ap.add_argument("--b1", help="G0-0B1 g0-0b1-evidence.json")
    ap.add_argument("--c00", help="G0-0C00 spool 로그")
    ap.add_argument("--c-suite", help="G0-0C evidence.json")
    ap.add_argument("--target", help="SCHEMA.TABLE — target.identity probe 가 없는 옛 로그용 보조 입력")
    ap.add_argument("--wm", help="워터마크 컬럼명 — --target 과 함께 쓴다")
    ap.add_argument("--out", default="g0-0-evidence.json")
    a = ap.parse_args()

    warn: list[str] = []
    arts: dict = {}

    def art(name, path):
        if not path:
            return None
        p = pathlib.Path(path)
        if not p.is_file():
            warn.append(f"{name}: 파일 없음 ({path})")
            return None
        arts[name] = {"path": str(p), "sha256": sha(p),
                      "lines": len(p.read_text(encoding='utf-8', errors='replace').splitlines())}
        return p

    lock = pathlib.Path(a.versions_lock)
    lock_digest = sha(lock) if lock.is_file() else "UNSET"
    if lock_digest == "UNSET":
        warn.append("versions.lock 이 없다 — 이 레코드는 '어느 판본에서 잰 값인가' 를 답하지 못한다")
    elif "UNSET" in lock.read_text(encoding="utf-8"):
        warn.append("versions.lock 에 UNSET 항목이 남아 있다 — 그 항목에 의존하는 측정은 미확정이다")

    pa, pb0, pb1, pc00, pcs = (art(k, getattr(a, v)) for k, v in
                               (("g0_0a", "a"), ("g0_0b0", "b0"), ("g0_0b1", "b1"),
                                ("g0_0c00", "c00"), ("g0_0c_suite", "c_suite")))

    # ── G0-0A ────────────────────────────────────────────────────────
    arecs = jsonl(pa) if pa else []
    P = by_id(arecs)
    summ = next((r["probe_summary"] for r in arecs if "probe_summary" in r), None)
    if not pa:
        cov_a = {"status": "NOT_RUN"}
    elif summ and summ.get("manifest_ok") is False:
        cov_a = {"status": "FAILED", "reason": "manifest_ok=false — 블록이 중간에 끊겼다. 결과 전체를 폐기한다.",
                 "counts": {"expected": summ.get("expected", 0), "emitted": summ.get("emitted", 0)}}
        warn.append("G0-0A manifest_ok=false → account_privs 와 capability_axes 를 신뢰할 수 없다")
        P = {}
    elif summ:
        cov_a = {"status": "MEASURED",
                 "counts": {"emitted": summ.get("emitted", 0),
                            "query_failed": summ.get("query_failed", 0),
                            "value_mismatch": summ.get("value_mismatch", 0)}}
    else:
        cov_a = {"status": "PARTIAL", "reason": "probe_summary sentinel 이 없다 — 블록이 끝까지 갔는지 확인 불가",
                 "counts": {"parsed": len(P)}}
        warn.append("G0-0A 에 probe_summary 가 없다. exit code 와 sentinel 을 함께 확인하라")

    src = {}
    for k, pid in (("db_unique_name", "userenv.DB_UNIQUE_NAME"), ("database_role", "userenv.DATABASE_ROLE"),
                   ("instance_name", "userenv.INSTANCE_NAME"), ("oracle_version", "ver.product_component"),
                   ("characterset", "nls.characterset")):
        if ok(P.get(pid)):
            src[k] = str(val(P[pid]))
    # 측정 대상 식별자 — 테이블 단위 축이 어느 테이블의 것인지 못 박는다(P0-03).
    ti = P.get("target.identity")
    if ok(ti) and "#" in str(val(ti)):
        objpart, _, wmcol = str(val(ti)).partition("#")
        owner, _, table = objpart.partition(".")
        src.update({"target_owner": owner, "target_table": table, "wm_column": wmcol})
    elif a.target:
        owner, _, table = a.target.partition(".")
        src.update({"target_owner": owner, "target_table": table, "wm_column": a.wm or ""})

    # ── B0 / B1 / C00 / C suite ──────────────────────────────────────
    cov_b0 = {"status": "NOT_RUN"}
    if pb0:
        b0 = jsonl(pb0)
        # **한 줄짜리 산출물을 MEASURED 로 만들지 않는다**(P0-02). B0 는 S0~S5 계열을 낸다.
        sids = {str(r.get("probe", "")).split(".")[0] for r in b0}
        if len(b0) >= 4 and len([x for x in sids if x.startswith("S")]) >= 2:
            cov_b0 = {"status": "MEASURED", "counts": {"probes": len(b0), "step_groups": len(sids)}}
        elif b0:
            cov_b0 = {"status": "PARTIAL", "counts": {"probes": len(b0), "step_groups": len(sids)},
                      "reason": "S 계열 step 이 2개 미만이거나 probe 가 4건 미만이다 — 완주로 볼 수 없다"}
        else:
            cov_b0 = {"status": "FAILED", "reason": "파싱 가능한 결과 줄이 없다"}

    spark_paths, cov_b1 = {}, {"status": "NOT_RUN"}
    if pb1:
        try:
            e = json.loads(pb1.read_text(encoding="utf-8"))
            v = e.get("verdict", {})
            spark_paths = {"verdict": v.get("coverage"), "blocking": v.get("blocking", []),
                           "by_path": e.get("by_path", {}),
                           "preamble_ok_by_path": e.get("preamble_ok_by_path", {})}
            # verdict 만 있는 파일을 MEASURED 로 만들지 않는다 — 경로별 관측이 있어야 한다.
            has_detail = bool(e.get("by_path")) and bool(e.get("preamble_ok_by_path"))
            cov_b1 = {"status": "MEASURED" if (v.get("coverage") == "PROVEN" and has_detail)
                      else "PARTIAL",
                      "reason": v.get("reason", "")[:200] if has_detail
                      else "by_path·preamble_ok_by_path 가 없다 — verdict 만으로는 측정으로 보지 않는다"}
        except Exception as ex:  # noqa: BLE001
            cov_b1 = {"status": "FAILED", "reason": f"{type(ex).__name__}"}
            warn.append(f"G0-0B1 증거를 읽지 못했다: {ex}")

    fence, cov_c00 = {}, {"status": "NOT_RUN"}
    if pc00:
        recs = jsonl(pc00)
        fence = {r["probe"]: r for r in recs if isinstance(r.get("probe"), str)}
        skipped = sum(1 for r in fence.values() if r.get("skipped"))
        # summary 한 줄만 있는 것을 MEASURED 로 만들지 않는다. 실제 fact probe 가 있어야 한다.
        facts = [k for k in fence if k.startswith("fence.") and k != "fence.summary"]
        got = [k for k in facts if fence[k].get("query_ok") is True]
        if skipped or not got:
            cov_c00 = {"status": "PARTIAL" if fence else "FAILED",
                       "counts": {"probes": len(fence), "facts": len(facts), "measured": len(got),
                                  "skipped": skipped},
                       "reason": ("ACK_FULL_SCAN=N — 전수 스캔 계열이 건너뛰어졌다" if skipped
                                  else "값이 나온 fence fact 가 없다")}
        else:
            cov_c00 = {"status": "MEASURED",
                       "counts": {"probes": len(fence), "facts": len(facts), "measured": len(got)}}

    ces, cov_cs = {}, {"status": "NOT_RUN"}
    if pcs:
        try:
            e = json.loads(pcs.read_text(encoding="utf-8"))
            v = e.get("suite_verdict", {})
            ces = {"pass": v.get("pass"), "reason": v.get("reason", "")[:300],
                   "outcomes": {s.get("id"): s.get("outcome") for s in e.get("scenarios", [])}}
            # scenario 0개인데 pass=true 인 파일을 MEASURED 로 만들지 않는다.
            nsc = len(e.get("scenarios", []))
            if v.get("pass") and nsc >= 9:
                cov_cs = {"status": "MEASURED", "counts": {"scenarios": nsc}}
            elif nsc == 0:
                cov_cs = {"status": "FAILED", "counts": {"scenarios": 0},
                          "reason": "scenario 가 0건이다 — suite_verdict 만으로는 측정이 아니다"}
            else:
                cov_cs = {"status": "PARTIAL", "counts": {"scenarios": nsc},
                          "reason": v.get("reason", "")[:200]}
        except Exception as ex:  # noqa: BLE001
            cov_cs = {"status": "FAILED", "reason": f"{type(ex).__name__}"}

    rec = {
        "schema_version": "1.0.0", "record_type": "g0_0_evidence",
        "scope": "CAPABILITY_INVENTORY", "gate_eligible": False,
        "g0_report_id": a.report_id,
        "executed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "normalized_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "profile": a.profile,
        "versions_lock_digest": lock_digest,
        "source": src,
        "coverage": {"g0_0a": cov_a, "g0_0b0": cov_b0, "g0_0b1": cov_b1,
                     "g0_0c00": cov_c00, "g0_0c_suite": cov_cs},
        "not_covered": NOT_COVERED,
        "account_privs": [r for r in arecs if "probe" in r],
        "capability_axes": derive_axes(P),
        "artifacts": arts,
        "spark_paths": spark_paths, "fence_facts": fence, "counterexamples": ces,
        "warnings": warn,
    }
    if a.profile == "LOCAL_WSL":
        rec["warnings"].append(
            "profile=LOCAL_WSL — 이 증거는 **하네스 동작 확인용**이며 설계 주장의 근거가 아니다. "
            "원천 capability 값·ADG 거동·규모는 사내 환경에서만 잴 수 있다.")

    # ── 검증을 **쓰기 전에** 한다. 위반한 레코드를 최종 경로에 두지 않는다(P0-02) ──
    schema_errors: list[str] = []
    try:
        import jsonschema
        sp = pathlib.Path(__file__).with_name("g0-0-evidence.schema.json")
        sc = json.loads(sp.read_text(encoding="utf-8"))
        probe = json.loads(json.dumps(rec, default=str))
        schema_errors = [f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message[:160]}"
                         for e in sorted(jsonschema.Draft202012Validator(sc).iter_errors(probe),
                                         key=lambda e: list(e.path))]
    except ImportError:
        schema_errors = ["jsonschema 미설치 — 계약 검증을 못 했다. 검증 없는 레코드는 근거가 아니다"]
    except Exception as e:  # noqa: BLE001
        schema_errors = [f"검증 중 오류: {type(e).__name__}: {e}"]

    out = pathlib.Path(a.out)
    if schema_errors:
        bad = out.with_suffix(out.suffix + ".invalid")
        rec["warnings"].extend(schema_errors)
        bad.write_text(json.dumps(rec, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
        for e in schema_errors[:5]:
            print(f"[schema] {e}", file=sys.stderr)
        print(f"[invalid] 계약 위반 {len(schema_errors)}건 — {bad} 에만 썼다. "
              f"최종 경로({out})에는 쓰지 않는다.", file=sys.stderr)
        return 4

    out.write_text(json.dumps(rec, ensure_ascii=False, indent=1, default=str), encoding="utf-8")

    axes = {k: v["value"] for k, v in rec["capability_axes"].items()}
    incomplete = [k for k, v in rec["coverage"].items() if v["status"] != "MEASURED"]
    und = [k for k, v in axes.items() if v == "UNDETERMINED"]
    print(json.dumps({"out": str(out), "gate_eligible": False,
                      "coverage": {k: v["status"] for k, v in rec["coverage"].items()},
                      "capability_axes": axes,
                      "incomplete": incomplete, "undetermined_axes": und,
                      "warnings": rec["warnings"]}, ensure_ascii=False, indent=1))
    # 0 = 유효하고 완결 / 3 = 유효하나 불완전 / 4 = 계약 위반
    return 0 if not (incomplete or und) else 3


if __name__ == "__main__":
    sys.exit(main())
