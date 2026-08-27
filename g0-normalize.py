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
      --versions-lock versions.lock --out g0-evidence.json
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


# ── capability 축 파생 ────────────────────────────────────────────────
def derive_axes(P: dict[str, dict]) -> dict:
    """capability-overlay §3 의 축을 실제 probe id 에서 파생한다.
    입력이 없으면 UNDETERMINED — 낙관적으로 채우지 않는다."""
    A: dict[str, dict] = {}

    def put(axis, value, used, note=None):
        A[axis] = {"value": value, "derived_from": used, **({"note": note} if note else {})}

    # snapshot_read — AS OF 는 SCN 원점이 함께 있어야 성립한다(권한 판정서 §3).
    asof, scn1, scn2 = P.get("as_of_timestamp.target"), P.get("dbms_flashback.get_scn"), P.get("view.v_database")
    ro, ro_sel = P.get("txn.set_read_only"), P.get("txn.select_inside")
    used = ["as_of_timestamp.target", "dbms_flashback.get_scn", "view.v_database",
            "txn.set_read_only", "txn.select_inside"]
    if asof is None and ro is None:
        put("snapshot_read", "UNDETERMINED", used, "G0-0A 결과가 없다")
    elif ok(asof) and (ok(scn1) or ok(scn2)):
        put("snapshot_read", "AS_OF_SCN", used)
    elif ok(asof):
        put("snapshot_read", "READ_ONLY_TXN", used,
            "AS OF 는 되지만 SCN 원점이 없어 AS OF TIMESTAMP(±3초 근삿값)뿐이다 — AS_OF_SCN 으로 올리지 않는다")
    elif ok(ro) and ok(ro_sel):
        put("snapshot_read", "READ_ONLY_TXN", used)
    else:
        put("snapshot_read", "NONE", used)

    # row_hash — 표준 시험 벡터와 일치해야 SHA256 이다(값이 맞아야지 오류 부재로는 부족).
    h = P.get("feat.standard_hash_sha256")
    if h is None:
        put("row_hash", "UNDETERMINED", ["feat.standard_hash_sha256"])
    elif ok(h) and h.get("value_interpretable") is True:
        put("row_hash", "SHA256", ["feat.standard_hash_sha256"])
    else:
        put("row_hash", "NONE", ["feat.standard_hash_sha256", "feat.ora_hash"],
            "ORA_HASH 는 32비트라 대조용 해시로 쓸 수 없다 — Reconciliation 이 건수+PK 로 강등된다")

    # row_change_scn — ROWDEPENDENCIES 는 테이블 생성 시 결정되고 사후 변경 불가다.
    dep, rs = P.get("feat.rowdependencies_target"), P.get("feat.ora_rowscn_target")
    used = ["feat.rowdependencies_target", "feat.ora_rowscn_target"]
    if dep is None and rs is None:
        put("row_change_scn", "UNDETERMINED", used)
    elif ok(rs) and str(val(dep) or "").upper() == "ENABLED":
        put("row_change_scn", "ROW_LEVEL", used)
    elif ok(rs):
        put("row_change_scn", "BLOCK_LEVEL", used,
            "ROWDEPENDENCIES 가 ENABLED 가 아니다 — 블록 단위 SCN 은 상한만 보장한다. 사후 변경 불가")
    else:
        put("row_change_scn", "NONE", used)

    # lag_visibility — DG_STATS 는 측정, MAX_DELAY_ONLY 는 강제다(성질이 다르다).
    dg, d = P.get("view.v_dataguard_stats"), P.get("alter.STANDBY_MAX_DATA_DELAY.D")
    used = ["view.v_dataguard_stats", "alter.STANDBY_MAX_DATA_DELAY.D"]
    if dg is None and d is None:
        put("lag_visibility", "UNDETERMINED", used)
    elif ok(dg):
        put("lag_visibility", "DG_STATS", used)
    elif ok(d):
        put("lag_visibility", "MAX_DELAY_ONLY", used,
            "값을 읽는 것이 아니라 임계 초과 시 ORA-03172 로 실패시킬 뿐이다. **측정이 아니라 강제**다")
    else:
        put("lag_visibility", "NONE", used)

    # wm_granularity — 결정자는 컬럼 타입이다. interval/timestamp9 probe 는 구문 지원 확인용.
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
                f"data_type={dt} scale={scale} — 고정 granularity 가 없어 successor(M)==M 이 된다(CE01). "
                "반개구간 seal 대상에서 제외하고 overlap 재적재로만 처리한다")

    # sql_dialect
    ff = P.get("feat.fetch_first")
    put("sql_dialect", "UNDETERMINED" if ff is None else ("12C_PLUS" if ok(ff) else "11G"),
        ["feat.fetch_first"])

    # charset_class — 원천 간 해시 정본화 비교 가능성
    cs = P.get("nls.characterset")
    used = ["nls.characterset", "nls.comp", "nls.sort"]
    if not ok(cs):
        put("charset_class", "UNDETERMINED", used)
    else:
        v = str(val(cs) or "").upper()
        put("charset_class", "AL32UTF8" if v == "AL32UTF8" else "OTHER", used, f"NLS_CHARACTERSET={v}")
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
    ap.add_argument("--out", default="g0-evidence.json")
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

    # ── B0 / B1 / C00 / C suite ──────────────────────────────────────
    cov_b0 = {"status": "NOT_RUN"}
    if pb0:
        b0 = jsonl(pb0)
        cov_b0 = {"status": "MEASURED" if b0 else "FAILED",
                  "counts": {"probes": len(b0)},
                  **({} if b0 else {"reason": "파싱 가능한 결과 줄이 없다"})}

    spark_paths, cov_b1 = {}, {"status": "NOT_RUN"}
    if pb1:
        try:
            e = json.loads(pb1.read_text(encoding="utf-8"))
            v = e.get("verdict", {})
            spark_paths = {"verdict": v.get("coverage"), "blocking": v.get("blocking", []),
                           "by_path": e.get("by_path", {}),
                           "preamble_ok_by_path": e.get("preamble_ok_by_path", {})}
            cov_b1 = {"status": "MEASURED" if v.get("coverage") == "PROVEN" else "PARTIAL",
                      "reason": v.get("reason", "")[:200]}
        except Exception as ex:  # noqa: BLE001
            cov_b1 = {"status": "FAILED", "reason": f"{type(ex).__name__}"}
            warn.append(f"G0-0B1 증거를 읽지 못했다: {ex}")

    fence, cov_c00 = {}, {"status": "NOT_RUN"}
    if pc00:
        recs = jsonl(pc00)
        fence = {r["probe"]: r for r in recs if isinstance(r.get("probe"), str)}
        skipped = sum(1 for r in fence.values() if r.get("skipped"))
        cov_c00 = {"status": "PARTIAL" if skipped else ("MEASURED" if fence else "FAILED"),
                   "counts": {"probes": len(fence), "skipped": skipped},
                   **({"reason": "ACK_FULL_SCAN=N — 전수 스캔 계열이 전부 건너뛰어졌다"} if skipped else {})}

    ces, cov_cs = {}, {"status": "NOT_RUN"}
    if pcs:
        try:
            e = json.loads(pcs.read_text(encoding="utf-8"))
            v = e.get("suite_verdict", {})
            ces = {"pass": v.get("pass"), "reason": v.get("reason", "")[:300],
                   "outcomes": {s.get("id"): s.get("outcome") for s in e.get("scenarios", [])}}
            cov_cs = {"status": "MEASURED" if v.get("pass") else "PARTIAL",
                      "reason": v.get("reason", "")[:200]}
        except Exception as ex:  # noqa: BLE001
            cov_cs = {"status": "FAILED", "reason": f"{type(ex).__name__}"}

    rec = {
        "schema_version": "1.0.0", "record_type": "g0_evidence",
        "g0_report_id": a.report_id,
        "executed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
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

    out = pathlib.Path(a.out)
    out.write_text(json.dumps(rec, ensure_ascii=False, indent=1), encoding="utf-8")

    # 자기 검증
    try:
        import jsonschema
        sc = json.loads(pathlib.Path("g0-evidence.schema.json").read_text(encoding="utf-8"))
        errs = sorted(jsonschema.Draft202012Validator(sc).iter_errors(rec), key=lambda e: list(e.path))
        for e in errs[:5]:
            print(f"[schema] {'/'.join(map(str, e.path)) or '<root>'}: {e.message[:160]}", file=sys.stderr)
        if errs:
            print(f"[schema] {len(errs)}건 위반 — 레코드는 썼으나 계약을 지키지 못했다", file=sys.stderr)
    except ImportError:
        rec["warnings"].append("jsonschema 미설치 — 이 레코드를 계약으로 검증하지 못했다")
        out.write_text(json.dumps(rec, ensure_ascii=False, indent=1), encoding="utf-8")

    axes = {k: v["value"] for k, v in rec["capability_axes"].items()}
    print(json.dumps({"out": str(out), "coverage": {k: v["status"] for k, v in rec["coverage"].items()},
                      "capability_axes": axes, "warnings": rec["warnings"]},
                     ensure_ascii=False, indent=1))
    und = [k for k, v in axes.items() if v == "UNDETERMINED"]
    return 0 if not und else 3


if __name__ == "__main__":
    sys.exit(main())
