#!/usr/bin/env python3
"""`g0-normalize.py` 반례 회귀 시험.

7차 교차 리뷰 §5.1 이 요구한 표를 실행 가능한 시험으로 옮긴 것이다. **각 시험은 "이전 판이
통과시켰던 입력"을 그대로 넣고 지금은 막히는지 본다.** 통과가 아니라 거부가 기대값이다.

    python3 g0-normalize-tests.py

의존: jsonschema(4.x). 없으면 정규화기가 스스로 exit 4 를 내므로 시험 대부분이 무의미해진다 —
그 사실 자체를 첫 시험으로 확인한다.
"""
from __future__ import annotations

import hashlib
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent
NORM = ROOT / "g0-normalize.py"
RUN_ID = "RUN-TEST-0001"
PROFILE = "CORP_POC"

FAIL: list[str] = []
PASS = 0


def sha(p: pathlib.Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def write_manifest(art: pathlib.Path, child: str, *, run_id=RUN_ID, profile=PROFILE,
                   lock_digest: str, exit_code=0, artifact_sha: str | None = None) -> None:
    art.with_name(art.name + ".manifest.json").write_text(json.dumps({
        "schema_version": "1.0.0", "record_type": "g0_child_manifest",
        "child": child, "run_id": run_id, "profile": profile,
        "started_at": "2026-08-27T00:00:00+00:00", "ended_at": "2026-08-27T00:01:00+00:00",
        "exit_code": exit_code, "versions_lock_digest": lock_digest,
        "artifact": {"path": str(art), "sha256": artifact_sha or sha(art),
                     "lines": len(art.read_text(encoding="utf-8").splitlines())},
        "runtime": {"uname": "test"}, "command": ["test"],
    }, ensure_ascii=False), encoding="utf-8")


def run_norm(work: pathlib.Path, **kw) -> tuple[int, dict, str]:
    cmd = [sys.executable, str(NORM), "--report-id", "NORM-TEST", "--run-id", kw.pop("run_id", RUN_ID),
           "--profile", kw.pop("profile", PROFILE),
           "--versions-lock", str(work / "versions.lock"),
           "--out", str(work / "out.json")]
    for k, v in kw.items():
        cmd += [f"--{k.replace('_', '-')}", str(v)]
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=str(work))
    try:
        out = json.loads(r.stdout)
    except json.JSONDecodeError:
        out = {}
    return r.returncode, out, r.stderr


def new_work() -> pathlib.Path:
    w = pathlib.Path(tempfile.mkdtemp(prefix="g0norm-"))
    (w / "versions.lock").write_text(
        # UNSET 을 세 가지 자리에 둔다 — 주석, 값 문자열 안, 그리고 어디에도 없는 실제 값.
        # 셋 다 "미측정 항목"이 아니므로 경고가 나오면 안 된다.
        'schema_version: "1.0.0"   # UNSET 은 빈칸이 아니라 판정이다\n'
        'profile: CORP_POC\n'
        'note: "설명문에 UNSET 이라는 단어가 들어갈 수 있다"\n',
        encoding="utf-8")
    # 정규화기는 자기 옆의 스키마를 읽는다. 작업 디렉터리에서 돌려도 되도록 복사해 둔다.
    return w


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL.append(f"{name} — {detail}")
        print(f"  FAIL  {name}  {detail}")


# ── 시험 ─────────────────────────────────────────────────────────────
def t_jsonschema_present() -> None:
    print("\n[0] 검증 도구가 있는가 (없으면 나머지 시험의 의미가 약해진다)")
    try:
        import jsonschema  # noqa: F401
        check("jsonschema 설치됨", True)
    except ImportError:
        check("jsonschema 설치됨", False, "미설치 — 정규화기가 schema 위반을 잡지 못한다")


def t_b0_one_line() -> None:
    print("\n[1] B0 한 줄 → MEASURED 금지 (이전 판은 통과시켰다)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / "b0.json"; art.write_text('{"step":"S1","ok":true}\n', encoding="utf-8")
    write_manifest(art, "G0_0B0", lock_digest=ld)
    rc, out, _ = run_norm(w, b0=art)
    st = (out.get("coverage") or {}).get("g0_0b0")
    check("b0 가 MEASURED 가 아니다", st != "MEASURED", f"status={st}")
    check("exit 3(불완전)", rc == 3, f"rc={rc}")
    shutil.rmtree(w)


def t_b1_fabricated() -> None:
    print("\n[2] B1 이 verdict 만 담은 파일 → FAILED (이전 판은 MEASURED)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / "b1.json"; art.write_text('{"verdict":{"coverage":"PROVEN"}}', encoding="utf-8")
    write_manifest(art, "G0_0B1", lock_digest=ld)
    rc, out, _ = run_norm(w, b1=art)
    st = (out.get("coverage") or {}).get("g0_0b1")
    check("b1 이 FAILED 다", st == "FAILED", f"status={st}")
    shutil.rmtree(w)


def t_b1_no_failclosed() -> None:
    print("\n[3] B1 이 coverage 회차만 관측 → MEASURED 금지")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / "b1.json"
    art.write_text(json.dumps({"verdict": {"coverage": "PROVEN"}, "by_path": {"SCHEMA": 3, "TASK": 4},
                               "preamble_ok_by_path": {"SCHEMA": "3/3"},
                               "runs_seen": {"coverage": 7}}), encoding="utf-8")
    write_manifest(art, "G0_0B1", lock_digest=ld)
    rc, out, _ = run_norm(w, b1=art)
    st = (out.get("coverage") or {}).get("g0_0b1")
    check("failclosed 미관측이면 MEASURED 가 아니다", st == "PARTIAL", f"status={st}")
    shutil.rmtree(w)


def t_c00_summary_only() -> None:
    print("\n[4] C00 summary 한 줄 → MEASURED 금지 (이전 판은 통과)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / "c00.log"
    art.write_text('{"probe":"fence.summary","ack_full_scan":true}\n', encoding="utf-8")
    write_manifest(art, "G0_0C00", lock_digest=ld)
    rc, out, _ = run_norm(w, c00=art)
    st = (out.get("coverage") or {}).get("g0_0c00")
    check("c00 가 MEASURED 가 아니다", st != "MEASURED", f"status={st}")
    shutil.rmtree(w)


def t_ce_empty_pass() -> None:
    print("\n[5] CE 가 scenario 0개로 pass=true → FAILED (이전 판은 MEASURED)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / "ce.json"
    art.write_text(json.dumps({"suite_verdict": {"pass": True}, "scenarios": []}), encoding="utf-8")
    write_manifest(art, "G0_0C_SUITE", lock_digest=ld)
    rc, out, _ = run_norm(w, c_suite=art)
    st = (out.get("coverage") or {}).get("g0_0c_suite")
    check("ce 가 FAILED 다", st == "FAILED", f"status={st}")
    shutil.rmtree(w)


def t_ce_bad_returncode() -> None:
    print("\n[6] CE 시나리오가 exit != 0 → 계약 위반으로 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / "ce.json"
    art.write_text(json.dumps({"suite_verdict": {"pass": True}, "scenarios": [
        {"id": "CE01", "outcome": "MITIGATION_HOLDS", "child_returncode": 1}]}), encoding="utf-8")
    write_manifest(art, "G0_0C_SUITE", lock_digest=ld)
    rc, out, err = run_norm(w, c_suite=art)
    check("exit 4(거부)", rc == 4, f"rc={rc}")
    check("거부 사유가 child 종료 코드다", "0 이 아닌 코드로 끝난" in err, err[:160])
    shutil.rmtree(w)


def t_a_no_sentinel() -> None:
    print("\n[7] A 가 sentinel 없음 → PARTIAL ∧ 축은 전부 UNDETERMINED")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / "a.log"
    art.write_text('{"probe":"userenv.DB_UNIQUE_NAME","query_ok":true,"value":"ORCL"}\n'
                   '{"probe_summary":{"expected":86,"emitted":86,"manifest_ok":true}}\n',
                   encoding="utf-8")
    write_manifest(art, "G0_0A", lock_digest=ld)
    rc, out, _ = run_norm(w, a=art)
    st = (out.get("coverage") or {}).get("g0_0a")
    check("sentinel 없으면 PARTIAL", st == "PARTIAL", f"status={st}")
    rec = json.loads((w / "out.json").read_text(encoding="utf-8"))
    vals = {v["value"] for v in rec["capability_axes"].values()}
    check("축이 전부 UNDETERMINED", vals == {"UNDETERMINED"}, str(vals))
    check("gate_eligible 은 false", rec["gate_eligible"] is False)
    check("record_type 은 g0_0_evidence", rec["record_type"] == "g0_0_evidence")
    shutil.rmtree(w)


def t_a_duplicate_probe() -> None:
    print("\n[8] A 에 probe id 중복 → 거부 (마지막 값이 이기는 조립 금지)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / "a.log"
    art.write_text('{"probe":"x","query_ok":true,"value":"1"}\n'
                   '{"probe":"x","query_ok":true,"value":"2"}\n'
                   '{"probe_summary":{"expected":2,"emitted":2,"manifest_ok":true}}\n'
                   '{"probe_run_end":"G0-0A"}\n', encoding="utf-8")
    write_manifest(art, "G0_0A", lock_digest=ld)
    rc, out, err = run_norm(w, a=art)
    check("exit 4(거부)", rc == 4, f"rc={rc}")
    check("최종 경로에 쓰지 않았다", not (w / "out.json").exists())
    check("거부 사본은 남는다", (w / "out.json.rejected.json").exists())
    shutil.rmtree(w)


def t_missing_manifest() -> None:
    print("\n[9] manifest 사이드카 없음 → 계약 위반 거부")
    w = new_work()
    art = w / "b0.json"; art.write_text('{"step":"S1"}\n', encoding="utf-8")
    rc, out, err = run_norm(w, b0=art)
    check("exit 4(거부)", rc == 4, f"rc={rc}")
    check("사유가 manifest 부재", "manifest" in err, err[:120])
    shutil.rmtree(w)


def t_lock_mismatch() -> None:
    print("\n[10] child 실행 시점 lock 과 집계 시점 lock 이 다름 → 거부")
    w = new_work()
    art = w / "b0.json"; art.write_text('{"step":"S1"}\n', encoding="utf-8")
    write_manifest(art, "G0_0B0", lock_digest="0" * 64)
    rc, out, err = run_norm(w, b0=art)
    check("exit 4(거부)", rc == 4, f"rc={rc}")
    check("사유가 lock 불일치", "versions_lock_digest" in err, err[:160])
    shutil.rmtree(w)


def t_run_id_mismatch() -> None:
    print("\n[11] run_id 불일치 → 거부 (다른 회차 산출물 혼합)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / "b0.json"; art.write_text('{"step":"S1"}\n', encoding="utf-8")
    write_manifest(art, "G0_0B0", run_id="RUN-OTHER", lock_digest=ld)
    rc, out, err = run_norm(w, b0=art)
    check("exit 4(거부)", rc == 4, f"rc={rc}")
    check("사유가 run_id 불일치", "run_id" in err, err[:160])
    shutil.rmtree(w)


def t_artifact_tampered() -> None:
    print("\n[12] 실행 후 산출물이 바뀜 → 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / "b0.json"; art.write_text('{"step":"S1"}\n', encoding="utf-8")
    write_manifest(art, "G0_0B0", lock_digest=ld)
    art.write_text('{"step":"S1","ok":true}\n', encoding="utf-8")   # 사후 변조
    rc, out, err = run_norm(w, b0=art)
    check("exit 4(거부)", rc == 4, f"rc={rc}")
    check("사유가 산출물 변경", "변경" in err, err[:160])
    shutil.rmtree(w)


def t_child_nonzero_exit() -> None:
    print("\n[13] child 가 0 이 아닌 코드로 끝남 → 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / "b0.json"; art.write_text('{"step":"S1"}\n', encoding="utf-8")
    write_manifest(art, "G0_0B0", lock_digest=ld, exit_code=2)
    rc, out, err = run_norm(w, b0=art)
    check("exit 4(거부)", rc == 4, f"rc={rc}")
    check("사유가 exit_code", "exit_code" in err, err[:160])
    shutil.rmtree(w)


def t_nothing_run() -> None:
    print("\n[14] 아무것도 안 돌린 회차 → exit 3, gate_eligible=false, 축 UNDETERMINED")
    w = new_work()
    rc, out, _ = run_norm(w)
    check("exit 3(불완전)", rc == 3, f"rc={rc}")
    rec = json.loads((w / "out.json").read_text(encoding="utf-8"))
    check("completeness=INCOMPLETE", rec["completeness"] == "INCOMPLETE")
    check("모든 child 가 NOT_RUN",
          all(v["status"] == "NOT_RUN" for v in rec["coverage"].values()))
    check("not_covered 가 9항", len(rec["not_covered"]) == 9, str(len(rec["not_covered"])))
    shutil.rmtree(w)


def t_lock_unset_comment_only() -> None:
    print("\n[15] versions.lock 주석에만 UNSET → 경고 없음 (이전 판은 영원히 경고)")
    w = new_work()
    rc, out, _ = run_norm(w)
    warns = " ".join(out.get("warnings") or [])
    check("UNSET 경고가 없다", "UNSET" not in warns, warns[:160])
    # 값 자리에 넣으면 경고가 나와야 한다
    (w / "versions.lock").write_text('a: UNSET   # 주석\n', encoding="utf-8")
    rc, out, _ = run_norm(w)
    warns = " ".join(out.get("warnings") or [])
    check("값 자리 UNSET 은 경고한다", "UNSET" in warns, warns[:160])
    shutil.rmtree(w)


def t_positive_control() -> None:
    """**양성 대조.** 거부만 하는 도구는 무조건 거부하는 도구와 구분되지 않는다.

    이 저장소의 규칙 — "0건 조건에는 양성 대조를 함께 둔다"(README §4) — 을 시험에도 적용한다.
    완결을 제대로 선언한 산출물은 MEASURED 에 도달해야 하고, 다섯이 다 서면 exit 0 이어야 한다.
    """
    print("\n[16] 양성 대조 — 제대로 된 산출물은 통과해야 한다")
    w = new_work(); ld = sha(w / "versions.lock")

    a_art = w / "a.log"
    a_art.write_text(
        '{"probe":"userenv.DB_UNIQUE_NAME","query_ok":true,"value":"ETLSTB"}\n'
        '{"probe":"userenv.DATABASE_ROLE","query_ok":true,"value":"PHYSICAL STANDBY"}\n'
        '{"probe_summary":{"expected":86,"emitted":86,"manifest_ok":true,'
        '"query_failed":0,"value_mismatch":0}}\n'
        '{"probe_run_end":"G0-0A","status":"reached_end"}\n', encoding="utf-8")
    write_manifest(a_art, "G0_0A", lock_digest=ld)

    b0 = w / "b0.json"
    b0.write_text(json.dumps({"b0_summary": {"expected_steps": ["S0", "S1"],
                                             "emitted_steps": ["S0", "S1"]}}) + "\n",
                  encoding="utf-8")
    write_manifest(b0, "G0_0B0", lock_digest=ld)

    b1 = w / "b1.json"
    b1.write_text(json.dumps({"verdict": {"coverage": "PROVEN"},
                              "by_path": {"SCHEMA": 3, "TASK": 4},
                              "preamble_ok_by_path": {"SCHEMA": "3/3", "TASK": "4/4"},
                              "runs_seen": {"coverage": 7, "failclosed": 3}}), encoding="utf-8")
    write_manifest(b1, "G0_0B1", lock_digest=ld)

    c00 = w / "c00.log"
    c00.write_text(
        '{"probe":"fence.max_wm","query_ok":true,"value":"2026-08-27 00:00:00.000000"}\n'
        '{"probe":"fence.rows_at_max_wm","query_ok":true,"value":1}\n'
        '{"probe":"fence.null_wm_rows","query_ok":true,"value":0}\n'
        '{"probe":"fence.future_wm_rows","query_ok":true,"value":0}\n'
        '{"probe":"fence.summary","ack_full_scan":true,"exact_mode":true,"expected_probes":4,'
        '"expected_probe_ids":["fence.max_wm","fence.null_wm_rows","fence.future_wm_rows",'
        '"fence.rows_at_max_wm"]}\n', encoding="utf-8")
    write_manifest(c00, "G0_0C00", lock_digest=ld)

    ce = w / "ce.json"
    ce.write_text(json.dumps({"suite_verdict": {"pass": True}, "scenarios": [
        {"id": "CE01", "outcome": "MITIGATION_HOLDS", "child_returncode": 0},
        {"id": "CE02", "outcome": "MITIGATION_HOLDS", "child_returncode": 0}]}), encoding="utf-8")
    write_manifest(ce, "G0_0C_SUITE", lock_digest=ld)

    rc, out, err = run_norm(w, a=a_art, b0=b0, b1=b1, c00=c00, c_suite=ce,
                            target_owner="APP", target_table="T1", wm_column="UPDATE_DT")
    check("exit 0(완결)", rc == 0, f"rc={rc} stderr={err[:200]}")
    cov = out.get("coverage") or {}
    check("다섯 child 가 전부 MEASURED",
          all(v == "MEASURED" for v in cov.values()), str(cov))
    rec = json.loads((w / "out.json").read_text(encoding="utf-8"))
    check("completeness=COMPLETE", rec["completeness"] == "COMPLETE")
    check("그래도 gate_eligible 은 false", rec["gate_eligible"] is False)
    check("계약 위반 0건", rec["contract_violations"] == [], str(rec["contract_violations"]))
    check("source 를 서버 신고값에서 채웠다",
          rec.get("source", {}).get("db_unique_name") == "ETLSTB", str(rec.get("source")))
    check("child measured_at 이 보존됐다",
          rec["children"]["g0_0a"].get("measured_at", "").startswith("2026-08-27"),
          str(rec["children"]["g0_0a"]))
    check("normalized_at 은 그와 별개다",
          rec["normalized_at"] != rec["children"]["g0_0a"].get("measured_at"))
    axes = rec["capability_axes"]
    check("13축이 전부 있다", len(axes) == 13, str(len(axes)))
    check("watermark_commit_bound 축이 복원돼 있다", "watermark_commit_bound" in axes)
    check("lag_visibility 라는 합쳐진 축은 없다", "lag_visibility" not in axes)
    check("확정된 축에는 measured_at 이 붙는다",
          all(v["measured_at"] for v in axes.values() if v["value"] != "UNDETERMINED")
          or all(v["value"] == "UNDETERMINED" for v in axes.values()),
          str({k: (v["value"], v["measured_at"]) for k, v in axes.items()})[:200])
    shutil.rmtree(w)


def t_axes_derived_in_record() -> None:
    """축이 실제 probe 에서 파생돼 레코드에 담기는가 — 조치 3 통합 확인."""
    print("\n[17] 축 파생이 레코드에 반영된다 (조치 3)")
    w = new_work(); ld = sha(w / "versions.lock")
    a_art = w / "a.log"
    a_art.write_text(
        '{"probe":"userenv.DB_UNIQUE_NAME","query_ok":true,"value":"ETLSTB"}\n'
        '{"probe":"dbms_flashback.get_scn","query_ok":true,"value":"9912345",'
        '"value_interpretable":true}\n'
        '{"probe":"as_of_timestamp.target","query_ok":true,"value":"1"}\n'
        '{"probe":"feat.standard_hash_sha256","query_ok":true,"value":"ba7816bf",'
        '"value_interpretable":true}\n'
        '{"probe":"feat.fetch_first","query_ok":true,"value":"1"}\n'
        '{"probe":"nls.characterset","query_ok":true,"value":"AL32UTF8"}\n'
        '{"probe":"nls.nchar_characterset","query_ok":true,"value":"AL16UTF16"}\n'
        '{"probe":"v$parameter.max_string","query_ok":true,"value":"STANDARD"}\n'
        '{"probe":"wm_column.type_facts","query_ok":true,"value":"TIMESTAMP(2)|scale=2"}\n'
        '{"probe":"feat.ora_rowscn_target","query_ok":true,"value":"9912000"}\n'
        '{"probe":"feat.rowdependencies_target","query_ok":true,"value":"DISABLED"}\n'
        '{"probe":"alter.STANDBY_MAX_DATA_DELAY.D","query_ok":true,"value":"ok"}\n'
        '{"probe":"max_delay_zero.touch_target","query_ok":true,"value":"1"}\n'
        '{"probe_summary":{"expected":13,"emitted":13,"manifest_ok":true}}\n'
        '{"probe_run_end":"G0-0A","status":"reached_end"}\n', encoding="utf-8")
    write_manifest(a_art, "G0_0A", lock_digest=ld)
    rc, out, err = run_norm(w, a=a_art, target_owner="APP", target_table="T1",
                            wm_column="UPDATE_DT")
    rec = json.loads((w / "out.json").read_text(encoding="utf-8"))
    ax = {k: v["value"] for k, v in rec["capability_axes"].items()}
    check("snapshot_anchor=SCN", ax["snapshot_anchor"] == "SCN", str(ax))
    check("hash_function=SHA256", ax["hash_function"] == "SHA256", str(ax))
    check("sql_dialect=12C_PLUS", ax["sql_dialect"] == "12C_PLUS", str(ax))
    check("db_charset=AL32UTF8", ax["db_charset"] == "AL32UTF8", str(ax))
    check("row_change_scn=BLOCK_LEVEL(ROWDEPENDENCIES DISABLED)",
          ax["row_change_scn"] == "BLOCK_LEVEL", str(ax))
    check("wm_successor=TIMESTAMP(2) — US 가 아니다",
          ax["wm_successor"] == "TIMESTAMP(2)", str(ax))
    check("lag_admission 은 ORA-03172 없이 승격되지 않는다",
          ax["lag_admission"] == "UNDETERMINED", str(ax))
    check("snapshot_scope 가 JOB 이 아니다", ax["snapshot_scope"] != "JOB", str(ax))
    check("canonical_row_compare 가 VECTORS_PROVEN 이 아니다",
          ax["canonical_row_compare"] != "VECTORS_PROVEN", str(ax))
    check("source 에 nchar_characterset 이 담긴다",
          rec["source"].get("nchar_characterset") == "AL16UTF16", str(rec.get("source")))
    check("source 에 max_string_size 가 담긴다",
          rec["source"].get("max_string_size") == "STANDARD", str(rec.get("source")))
    check("테이블 단위 축에 binding 이 붙는다",
          rec["capability_axes"]["row_change_scn"]["binding"]["object"] == "T1",
          str(rec["capability_axes"]["row_change_scn"]["binding"]))
    shutil.rmtree(w)


def main() -> int:
    print("=" * 70)
    print("g0-normalize.py 반례 회귀 시험 — 7차 교차 리뷰 §5.1")
    print("=" * 70)
    for t in (t_jsonschema_present, t_b0_one_line, t_b1_fabricated, t_b1_no_failclosed,
              t_c00_summary_only, t_ce_empty_pass, t_ce_bad_returncode, t_a_no_sentinel,
              t_a_duplicate_probe, t_missing_manifest, t_lock_mismatch, t_run_id_mismatch,
              t_artifact_tampered, t_child_nonzero_exit, t_nothing_run,
              t_lock_unset_comment_only, t_positive_control, t_axes_derived_in_record):
        t()
    print("\n" + "=" * 70)
    print(f"통과 {PASS}건 · 실패 {len(FAIL)}건")
    for f in FAIL:
        print(f"  - {f}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
