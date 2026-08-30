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


SOURCE_ID = "TESTSTBY"
HARNESS = "b" * 64

# **고정 날짜를 쓰지 않는다.** M3-3 이 TTL 기반 stale 판정을 넣었으므로, 픽스처의 측정
# 시각을 2026-08-27 로 못 박아 두면 시험이 어느 날부터 갑자기 STALE 로 갈린다 —
# 시간이 지나면 저절로 깨지는 시험은 회귀 시험이 아니다. stale 은 아래에서 **일부러 오래된
# 시각을 준 시험**으로만 낸다.
def iso(delta_days: float = 0.0) -> str:
    from datetime import datetime, timedelta, timezone as _tz
    return (datetime.now(_tz.utc) - timedelta(days=delta_days)).isoformat(timespec="seconds")


def write_manifest(art: pathlib.Path, child: str, *, run_id=RUN_ID, profile=PROFILE,
                   lock_digest: str, exit_code=0, artifact_sha: str | None = None,
                   source_id: str = SOURCE_ID, harness_digest: str = HARNESS,
                   overwrote: bool = False, drop: tuple[str, ...] = (),
                   age_days: float = 0.02) -> None:
    """`drop` 으로 필드를 빼면 M1-2 결속 검사를 반례로 시험할 수 있다.

    `age_days` 는 측정 시각을 과거로 미는 값이다 — M3-3 stale 시험용. 기본값이 0 이 아닌
    이유는 **측정이 정규화보다 먼저 일어나기 때문**이다. 0 으로 두면 두 시각이 초 단위로
    같아져서 "normalized_at 은 measured_at 이 아니다" 를 시험할 수 없다.
    """
    man = {
        "schema_version": "1.0.0", "record_type": "g0_child_manifest",
        "child": child, "run_id": run_id, "profile": profile,
        "started_at": iso(age_days + 0.001), "ended_at": iso(age_days),
        "exit_code": exit_code, "versions_lock_digest": lock_digest,
        "source_id": source_id, "harness_digest": harness_digest,
        "overwrote_existing": overwrote,
        "artifact": {"path": str(art), "sha256": artifact_sha or sha(art),
                     "lines": len(art.read_text(encoding="utf-8").splitlines())},
        "runtime": {"uname": "test"}, "command": ["test"],
    }
    for k in drop:
        man.pop(k, None)
    art.with_name(art.name + ".manifest.json").write_text(
        json.dumps(man, ensure_ascii=False), encoding="utf-8")


def run_norm(work: pathlib.Path, **kw) -> tuple[int, dict, str]:
    """정규화기를 한 번 돌린다.

    **`--out` 은 run 별 경로여야 한다**(8차 M3-5) — 고정 이름 하나가 여러 회차의 별칭이
    되면 무효한 재실행 뒤에 이전 회차가 current 로 읽힌다. 그래서 시험도 실제 사용법과
    같은 형태로 부른다. `out` 을 직접 주면 그 경로를 쓴다(경로 규칙 자체를 시험할 때).
    """
    run_id = kw.pop("run_id", RUN_ID)
    flags = kw.pop("flags", ())
    out = kw.pop("out", None) or (work / f"{run_id}-out.json")
    cmd = [sys.executable, str(NORM), "--report-id", "NORM-TEST", "--run-id", run_id,
           "--profile", kw.pop("profile", PROFILE),
           "--versions-lock", str(work / "versions.lock"),
           "--out", str(out)]
    for k, v in kw.items():
        cmd += [f"--{k.replace('_', '-')}", str(v)]
    cmd += list(flags)
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
    art = w / f"b0-{RUN_ID}.json"; art.write_text('{"probe":"S1","ok":true}\n', encoding="utf-8")
    write_manifest(art, "G0_0B0", lock_digest=ld)
    rc, out, _ = run_norm(w, b0=art)
    st = (out.get("coverage") or {}).get("g0_0b0")
    check("b0 가 MEASURED 가 아니다", st != "MEASURED", f"status={st}")
    check("exit 3(불완전)", rc == 3, f"rc={rc}")
    shutil.rmtree(w)


def t_b1_fabricated() -> None:
    print("\n[2] B1 이 verdict 만 담은 파일 → **child schema 단계에서 거부**(8차 M1-1)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"b1-{RUN_ID}.json"; art.write_text('{"verdict":{"coverage":"PROVEN"}}', encoding="utf-8")
    write_manifest(art, "G0_0B1", lock_digest=ld)
    rc, out, _ = run_norm(w, b1=art)
    # 7차 판에서는 집계까지 가서 coverage=FAILED 였다. 8차 M1-1 로 **집계 전에** 걸린다 —
    # connections·by_path·findings·verdicts 가 없는 파일은 B1 산출물의 형태가 아니다.
    check("exit 4(계약 위반) — 집계까지 가지 않는다", rc == 4, f"rc={rc}")
    check("사유가 child schema 위반",
          any("child schema" in v for v in out.get("contract_violations", [])),
          str(out.get("contract_violations"))[:220])
    shutil.rmtree(w)


def t_b1_no_failclosed() -> None:
    print("\n[3] B1 이 coverage 회차만 관측 → MEASURED 금지")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"b1-{RUN_ID}.json"
    # child schema 를 만족하는 **현실적인** 산출물이어야 한다(8차 M1-1). 그래야 이 시험이
    # 형태가 아니라 **판정**(failclosed 미관측 → MEASURED 금지)을 시험하게 된다.
    art.write_text(json.dumps({
        "connections": [{"path_guess": "SCHEMA", "run": "coverage", "injection_applied": False},
                        {"path_guess": "TASK", "run": "coverage", "injection_applied": False}],
        "by_path": {"SCHEMA": 3, "TASK": 4},
        "preamble_ok_by_path": {"SCHEMA": "3/3"},
        "findings": [],
        "verdicts": {"provider_reachability": "PROVEN", "fail_closed": "NOT_TESTED"},
        "verdict": {"coverage": "PROVEN"},
        "runs_seen": {"coverage": 7}}), encoding="utf-8")
    write_manifest(art, "G0_0B1", lock_digest=ld)
    rc, out, _ = run_norm(w, b1=art)
    st = (out.get("coverage") or {}).get("g0_0b1")
    check("failclosed 미관측이면 MEASURED 가 아니다", st == "PARTIAL", f"status={st}")
    shutil.rmtree(w)


def t_c00_summary_only() -> None:
    print("\n[4] C00 summary 한 줄 → MEASURED 금지 (이전 판은 통과)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"c00-{RUN_ID}.log"
    art.write_text('{"probe":"fence.summary","ack_full_scan":true}\n', encoding="utf-8")
    write_manifest(art, "G0_0C00", lock_digest=ld)
    rc, out, _ = run_norm(w, c00=art)
    st = (out.get("coverage") or {}).get("g0_0c00")
    check("c00 가 MEASURED 가 아니다", st != "MEASURED", f"status={st}")
    shutil.rmtree(w)


def t_ce_empty_pass() -> None:
    print("\n[5] CE 가 scenario 0개로 pass=true → FAILED (이전 판은 MEASURED)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"ce-{RUN_ID}.json"
    art.write_text(json.dumps({"suite_verdict": {"pass": True}, "scenarios": []}), encoding="utf-8")
    write_manifest(art, "G0_0C_SUITE", lock_digest=ld)
    rc, out, _ = run_norm(w, c_suite=art)
    st = (out.get("coverage") or {}).get("g0_0c_suite")
    check("ce 가 FAILED 다", st == "FAILED", f"status={st}")
    shutil.rmtree(w)


def t_ce_bad_returncode() -> None:
    print("\n[6] CE 시나리오가 exit != 0 → 계약 위반으로 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"ce-{RUN_ID}.json"
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
    art = w / f"a-{RUN_ID}.log"
    art.write_text('{"probe":"userenv.DB_UNIQUE_NAME","query_ok":true,"value":"ORCL"}\n'
                   '{"probe_summary":{"expected":86,"emitted":86,"manifest_ok":true}}\n',
                   encoding="utf-8")
    write_manifest(art, "G0_0A", lock_digest=ld)
    rc, out, _ = run_norm(w, a=art)
    st = (out.get("coverage") or {}).get("g0_0a")
    check("sentinel 없으면 PARTIAL", st == "PARTIAL", f"status={st}")
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    vals = {v["value"] for v in rec["capability_axes"].values()}
    check("축이 전부 UNDETERMINED", vals == {"UNDETERMINED"}, str(vals))
    check("gate_eligible 은 false", rec["gate_eligible"] is False)
    check("record_type 은 g0_0_evidence", rec["record_type"] == "g0_0_evidence")
    shutil.rmtree(w)


def t_a_duplicate_probe() -> None:
    print("\n[8] A 에 probe id 중복 → 거부 (마지막 값이 이기는 조립 금지)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"a-{RUN_ID}.log"
    art.write_text('{"probe":"x","query_ok":true,"value":"1"}\n'
                   '{"probe":"x","query_ok":true,"value":"2"}\n'
                   '{"probe_summary":{"expected":2,"emitted":2,"manifest_ok":true}}\n'
                   '{"probe_run_end":"G0-0A"}\n', encoding="utf-8")
    write_manifest(art, "G0_0A", lock_digest=ld)
    rc, out, err = run_norm(w, a=art)
    check("exit 4(거부)", rc == 4, f"rc={rc}")
    check("최종 경로에 쓰지 않았다", not (w / f"{RUN_ID}-out.json").exists())
    check("거부 사본은 남는다", (w / f"{RUN_ID}-out.json.rejected.json").exists())
    shutil.rmtree(w)


def t_missing_manifest() -> None:
    print("\n[9] manifest 사이드카 없음 → 계약 위반 거부")
    w = new_work()
    art = w / f"b0-{RUN_ID}.json"; art.write_text('{"step":"S1"}\n', encoding="utf-8")
    rc, out, err = run_norm(w, b0=art)
    check("exit 4(거부)", rc == 4, f"rc={rc}")
    check("사유가 manifest 부재", "manifest" in err, err[:120])
    shutil.rmtree(w)


def t_lock_mismatch() -> None:
    print("\n[10] child 실행 시점 lock 과 집계 시점 lock 이 다름 → 거부")
    w = new_work()
    art = w / f"b0-{RUN_ID}.json"; art.write_text('{"step":"S1"}\n', encoding="utf-8")
    write_manifest(art, "G0_0B0", lock_digest="0" * 64)
    rc, out, err = run_norm(w, b0=art)
    check("exit 4(거부)", rc == 4, f"rc={rc}")
    check("사유가 lock 불일치", "versions_lock_digest" in err, err[:160])
    shutil.rmtree(w)


def t_run_id_mismatch() -> None:
    print("\n[11] run_id 불일치 → 거부 (다른 회차 산출물 혼합)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"b0-{RUN_ID}.json"; art.write_text('{"step":"S1"}\n', encoding="utf-8")
    write_manifest(art, "G0_0B0", run_id="RUN-OTHER", lock_digest=ld)
    rc, out, err = run_norm(w, b0=art)
    check("exit 4(거부)", rc == 4, f"rc={rc}")
    check("사유가 run_id 불일치", "run_id" in err, err[:160])
    shutil.rmtree(w)


def t_artifact_tampered() -> None:
    print("\n[12] 실행 후 산출물이 바뀜 → 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"b0-{RUN_ID}.json"; art.write_text('{"step":"S1"}\n', encoding="utf-8")
    write_manifest(art, "G0_0B0", lock_digest=ld)
    art.write_text('{"step":"S1","ok":true}\n', encoding="utf-8")   # 사후 변조
    rc, out, err = run_norm(w, b0=art)
    check("exit 4(거부)", rc == 4, f"rc={rc}")
    check("사유가 산출물 변경", "변경" in err, err[:160])
    shutil.rmtree(w)


def t_child_nonzero_exit() -> None:
    print("\n[13] child 가 0 이 아닌 코드로 끝남 → 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"b0-{RUN_ID}.json"; art.write_text('{"step":"S1"}\n', encoding="utf-8")
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
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
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
    # 값 자리에 넣으면 경고가 나와야 한다. **회차 산출물 경로를 바꿔서 부른다** —
    # 같은 --out 에 두 번 쓰는 것은 8차 M3-5 가 막는 것이고 여기서 시험하려는 것이 아니다.
    (w / "versions.lock").write_text('a: UNSET   # 주석\n', encoding="utf-8")
    rc, out, _ = run_norm(w, out=w / f"{RUN_ID}-out-2.json")
    warns = " ".join(out.get("warnings") or [])
    check("값 자리 UNSET 은 경고한다", "UNSET" in warns, warns[:160])
    shutil.rmtree(w)


def full_fixture(w: pathlib.Path, ld: str, **mk) -> dict:
    """다섯 child 가 모두 MEASURED 에 도달하는 산출물 한 벌.

    양성 대조([16])와 8차 M3 시험들이 같은 픽스처를 쓴다 — 두 벌을 따로 두면 한쪽만
    현실을 따라가고 다른 쪽은 조용히 낡는다. `mk` 는 `write_manifest` 로 넘어간다
    (예: `age_days` 로 측정 시각을 과거로 밀어 stale 을 낸다).
    """
    a_art = w / f"a-{RUN_ID}.log"
    a_art.write_text(
        '{"probe":"userenv.DB_UNIQUE_NAME","query_ok":true,"value":"ETLSTB"}\n'
        '{"probe":"userenv.DATABASE_ROLE","query_ok":true,"value":"PHYSICAL STANDBY"}\n'
        # 축이 하나라도 확정값을 갖게 하려면 값 probe 가 하나는 있어야 한다.
        # 전 축이 UNDETERMINED 인 픽스처로는 floor·stale 을 시험할 수 없다(내려갈 곳이 없다).
        '{"probe":"nls.characterset","query_ok":true,"value":"AL32UTF8"}\n'
        '{"probe_summary":{"expected":86,"emitted":86,"manifest_ok":true,'
        '"query_failed":0,"value_mismatch":0}}\n'
        '{"probe_run_end":"G0-0A","status":"reached_end"}\n', encoding="utf-8")
    write_manifest(a_art, "G0_0A", lock_digest=ld, **mk)

    b0 = w / f"b0-{RUN_ID}.json"
    b0.write_text(json.dumps({"b0_summary": {"expected_steps": ["S0", "S1"],
                                             "emitted_steps": ["S0", "S1"]}}) + "\n",
                  encoding="utf-8")
    write_manifest(b0, "G0_0B0", lock_digest=ld, **mk)

    b1 = w / f"b1-{RUN_ID}.json"
    b1.write_text(json.dumps({
        # child schema(8차 M1-1)를 만족하는 현실적인 산출물.
        "connections": [
            {"path_guess": "SCHEMA", "run": "coverage", "injection_applied": False},
            {"path_guess": "TASK", "run": "coverage", "injection_applied": False},
            {"path_guess": "SCHEMA", "run": "failclosed_schema", "injection_applied": True,
             "preamble_error": "forced"},
            {"path_guess": "TASK", "run": "failclosed_task", "injection_applied": True,
             "preamble_error": "forced"}],
        "by_path": {"SCHEMA": 3, "TASK": 4},
        "preamble_ok_by_path": {"SCHEMA": "3/3", "TASK": "4/4"},
        "findings": [],
        "verdicts": {"provider_reachability": "PROVEN", "session_assertion": "PROVEN",
                     "fail_closed": "PROVEN", "read_only_transaction": "NOT_IMPLEMENTED",
                     "common_snapshot": "NOT_IMPLEMENTED"},
        "verdict": {"coverage": "PROVEN"},
        # 조치 5 이후의 실제 회차 이름
        "runs_seen": {"coverage": 7, "failclosed_schema": 2,
                      "failclosed_task": 3}}), encoding="utf-8")
    write_manifest(b1, "G0_0B1", lock_digest=ld, **mk)

    c00 = w / f"c00-{RUN_ID}.log"
    c00.write_text(
        '{"probe":"fence.max_wm","query_ok":true,"value":"2026-08-27 00:00:00.000000"}\n'
        '{"probe":"fence.rows_at_max_wm","query_ok":true,"value":1}\n'
        '{"probe":"fence.null_wm_rows","query_ok":true,"value":0}\n'
        '{"probe":"fence.future_wm_rows","query_ok":true,"value":0}\n'
        '{"probe":"fence.summary","ack_full_scan":true,"exact_mode":true,"expected_probes":4,'
        '"expected_probe_ids":["fence.max_wm","fence.null_wm_rows","fence.future_wm_rows",'
        '"fence.rows_at_max_wm"]}\n', encoding="utf-8")
    write_manifest(c00, "G0_0C00", lock_digest=ld, **mk)

    ce = w / f"ce-{RUN_ID}.json"
    ce.write_text(json.dumps({"suite_verdict": {"pass": True}, "scenarios": [
        {"id": "CE01", "outcome": "MITIGATION_HOLDS", "child_returncode": 0},
        {"id": "CE02", "outcome": "MITIGATION_HOLDS", "child_returncode": 0}]}), encoding="utf-8")
    write_manifest(ce, "G0_0C_SUITE", lock_digest=ld, **mk)

    return {"a": a_art, "b0": b0, "b1": b1, "c00": c00, "c_suite": ce}


def t_positive_control() -> None:
    """**양성 대조.** 거부만 하는 도구는 무조건 거부하는 도구와 구분되지 않는다.

    이 저장소의 규칙 — "0건 조건에는 양성 대조를 함께 둔다"(README §4) — 을 시험에도 적용한다.
    완결을 제대로 선언한 산출물은 MEASURED 에 도달해야 하고, 다섯이 다 서면 exit 0 이어야 한다.
    """
    print("\n[16] 양성 대조 — 제대로 된 산출물은 통과해야 한다")
    w = new_work(); ld = sha(w / "versions.lock")

    f = full_fixture(w, ld)
    a_art, b0, b1, c00, ce = (f["a"], f["b0"], f["b1"], f["c00"], f["c_suite"])

    rc, out, err = run_norm(w, a=a_art, b0=b0, b1=b1, c00=c00, c_suite=ce,
                            target_owner="APP", target_table="T1", wm_column="UPDATE_DT")
    check("exit 0(완결)", rc == 0, f"rc={rc} stderr={err[:200]}")
    cov = out.get("coverage") or {}
    check("다섯 child 가 전부 MEASURED",
          all(v == "MEASURED" for v in cov.values()), str(cov))
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    check("completeness=COMPLETE", rec["completeness"] == "COMPLETE")
    check("그래도 gate_eligible 은 false", rec["gate_eligible"] is False)
    check("계약 위반 0건", rec["contract_violations"] == [], str(rec["contract_violations"]))
    check("source 를 서버 신고값에서 채웠다",
          rec.get("source", {}).get("db_unique_name") == "ETLSTB", str(rec.get("source")))
    # measured_at 의 권위는 래퍼가 적은 ended_at 이다. 집계기가 만들지 않는다.
    man = json.loads((a_art.parent / (a_art.name + ".manifest.json")).read_text(encoding="utf-8"))
    check("child measured_at 이 manifest 의 ended_at 그대로다",
          rec["children"]["g0_0a"].get("measured_at") == man["ended_at"],
          str(rec["children"]["g0_0a"].get("measured_at")))
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


def t_b1_path_split_runs() -> None:
    """조치 5 로 failclosed 가 경로별로 갈렸다 — 집계기가 새 이름을 인식하는가."""
    print("\n[18] B1 회차 이름이 failclosed_schema/_task 여도 인식한다 (조치 5 회귀)")
    for runs, want, label in (
            ({"coverage": 7, "failclosed_schema": 2, "failclosed_task": 3}, "MEASURED", "경로별 분리"),
            ({"coverage": 7, "failclosed": 3}, "MEASURED", "옛 이름(호환)"),
            ({"coverage": 7}, "PARTIAL", "failclosed 회차 없음")):
        w = new_work(); ld = sha(w / "versions.lock")
        art = w / f"b1-{RUN_ID}.json"
        art.write_text(json.dumps({
            "connections": [{"path_guess": "SCHEMA", "run": r, "injection_applied": False}
                            for r in runs],
            "by_path": {"SCHEMA": 3, "TASK": 4},
            "preamble_ok_by_path": {"SCHEMA": "3/3", "TASK": "4/4"},
            "findings": [],
            "verdicts": {"provider_reachability": "PROVEN"},
            "verdict": {"coverage": "PROVEN"},
            "runs_seen": runs}), encoding="utf-8")
        write_manifest(art, "G0_0B1", lock_digest=ld)
        rc, out, _ = run_norm(w, b1=art)
        st = (out.get("coverage") or {}).get("g0_0b1")
        check(f"{label} → {want}", st == want, f"status={st} runs={runs}")
        shutil.rmtree(w)


def t_axes_derived_in_record() -> None:
    """축이 실제 probe 에서 파생돼 레코드에 담기는가 — 조치 3 통합 확인."""
    print("\n[17] 축 파생이 레코드에 반영된다 (조치 3)")
    w = new_work(); ld = sha(w / "versions.lock")
    a_art = w / f"a-{RUN_ID}.log"
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
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
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


# ── 8차 M1: child evidence contract 반례 ─────────────────────────────
def t_m1_no_source_id() -> None:
    print("\n[20] M1-2 — manifest 에 source_id 가 없으면 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"b0-{RUN_ID}.json"; art.write_text('{"probe":"S1","ok":true}\n', encoding="utf-8")
    write_manifest(art, "G0_0B0", lock_digest=ld, drop=("source_id",))
    rc, out, _ = run_norm(w, b0=art)
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    check("사유가 source_id",
          any("source_id" in v for v in out.get("contract_violations", [])),
          str(out.get("contract_violations"))[:200])
    shutil.rmtree(w)


def t_m1_no_harness_digest() -> None:
    print("\n[21] M1-2 — harness_digest 가 없으면 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"b0-{RUN_ID}.json"; art.write_text('{"probe":"S1","ok":true}\n', encoding="utf-8")
    write_manifest(art, "G0_0B0", lock_digest=ld, drop=("harness_digest",))
    rc, out, _ = run_norm(w, b0=art)
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    check("사유가 harness_digest",
          any("harness_digest" in v for v in out.get("contract_violations", [])),
          str(out.get("contract_violations"))[:200])
    shutil.rmtree(w)


def t_m1_source_mismatch() -> None:
    print("\n[22] M1-3 — child 들의 source_id 가 다르면 거부 (**각자는 일관되다**)")
    w = new_work(); ld = sha(w / "versions.lock")
    a0 = w / f"b0-{RUN_ID}.json"; a0.write_text('{"step":"S1","ok":true}\n', encoding="utf-8")
    write_manifest(a0, "G0_0B0", lock_digest=ld, source_id="STBY_A")
    a1 = w / f"b1-{RUN_ID}.json"; a1.write_text('{"connections":[]}', encoding="utf-8")
    write_manifest(a1, "G0_0B1", lock_digest=ld, source_id="STBY_B")
    rc, out, _ = run_norm(w, b0=a0, b1=a1)
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    viols = " ".join(out.get("contract_violations", []))
    check("사유가 '서로 다른 원천'", "다른 원천" in viols, viols[:250])
    check("두 값을 모두 적는다", "STBY_A" in viols and "STBY_B" in viols, viols[:250])
    shutil.rmtree(w)


def t_m1_harness_mismatch() -> None:
    print("\n[23] M1-3 — child 들의 harness_digest 가 다르면 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    a0 = w / f"b0-{RUN_ID}.json"; a0.write_text('{"step":"S1","ok":true}\n', encoding="utf-8")
    write_manifest(a0, "G0_0B0", lock_digest=ld, harness_digest="a" * 64)
    a1 = w / f"b1-{RUN_ID}.json"; a1.write_text('{"connections":[]}', encoding="utf-8")
    write_manifest(a1, "G0_0B1", lock_digest=ld, harness_digest="c" * 64)
    rc, out, _ = run_norm(w, b0=a0, b1=a1)
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    check("사유가 '다른 하네스 코드'",
          "하네스 코드" in " ".join(out.get("contract_violations", [])),
          str(out.get("contract_violations"))[:250])
    shutil.rmtree(w)


def t_m1_expected_source_mismatch() -> None:
    print("\n[24] M1-2 — --source-id 를 주면 그것과도 대조한다")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"b0-{RUN_ID}.json"; art.write_text('{"probe":"S1","ok":true}\n', encoding="utf-8")
    write_manifest(art, "G0_0B0", lock_digest=ld, source_id="STBY_A")
    rc, out, _ = run_norm(w, b0=art, source_id="STBY_B")
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    check("사유가 source_id 불일치",
          any("source_id 불일치" in v for v in out.get("contract_violations", [])),
          str(out.get("contract_violations"))[:200])
    shutil.rmtree(w)


def t_m1_path_without_run_id() -> None:
    print("\n[25] M1-4 — 산출물 경로에 run_id 가 없으면 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / "b0.json"; art.write_text('{"step":"S1","ok":true}\n', encoding="utf-8")
    write_manifest(art, "G0_0B0", lock_digest=ld)
    rc, out, _ = run_norm(w, b0=art)
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    check("사유가 run_id 경로",
          any("run_id 가 없다" in v for v in out.get("contract_violations", [])),
          str(out.get("contract_violations"))[:200])
    shutil.rmtree(w)


def t_m1_overwrote() -> None:
    print("\n[26] M1-4 — 기존 산출물을 덮어썼으면 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"b0-{RUN_ID}.json"; art.write_text('{"probe":"S1","ok":true}\n', encoding="utf-8")
    write_manifest(art, "G0_0B0", lock_digest=ld, overwrote=True)
    rc, out, _ = run_norm(w, b0=art)
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    check("사유가 덮어쓰기",
          any("덮어썼다" in v for v in out.get("contract_violations", [])),
          str(out.get("contract_violations"))[:200])
    shutil.rmtree(w)


def t_m1_no_timestamps() -> None:
    print("\n[27] M1-2 — started_at/ended_at 이 없으면 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"b0-{RUN_ID}.json"; art.write_text('{"probe":"S1","ok":true}\n', encoding="utf-8")
    write_manifest(art, "G0_0B0", lock_digest=ld, drop=("started_at",))
    rc, out, _ = run_norm(w, b0=art)
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    check("사유가 started_at/ended_at",
          any("started_at" in v for v in out.get("contract_violations", [])),
          str(out.get("contract_violations"))[:200])
    shutil.rmtree(w)


def t_m1_child_schema_missing_key() -> None:
    print("\n[28] M1-1 — child 산출물이 자기 스키마를 어기면 **집계 전에** 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    # A 의 probe 줄에 probe id 가 비어 있다 — 그러면 중복 검사도 의미가 없어진다.
    art = w / f"a-{RUN_ID}.log"
    art.write_text('{"probe":"","ok":true}\n'
                   '{"probe_summary":{"expected":1,"emitted":1,"manifest_ok":true}}\n'
                   '{"probe_run_end":true}\n', encoding="utf-8")
    write_manifest(art, "G0_0A", lock_digest=ld)
    rc, out, _ = run_norm(w, a=art)
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    check("사유가 child schema 위반",
          any("child schema" in v for v in out.get("contract_violations", [])),
          str(out.get("contract_violations"))[:220])
    shutil.rmtree(w)


def t_m1_child_schema_files_exist() -> None:
    print("\n[29] M1-1 — A/B0/B1/C00 개별 스키마가 실재한다")
    d = NORM.parent / "g0-child-schemas"
    for f in ("g0-child-a.schema.json", "g0-child-b0.schema.json",
              "g0-child-b1.schema.json", "g0-child-c00.schema.json"):
        p = d / f
        ok = p.is_file()
        check(f"{f} 존재", ok)
        if ok:
            try:
                doc = json.loads(p.read_text(encoding="utf-8"))
                check(f"{f} 가 유효한 JSON Schema", "$schema" in doc and "title" in doc)
            except json.JSONDecodeError as e:
                check(f"{f} 가 유효한 JSON Schema", False, str(e))


# ── 8차 M3 ───────────────────────────────────────────────────────────
def t_m3_no_aggregation_before_schema() -> None:
    print("\n[30] M3-1 — schema 를 어긴 산출물은 **본문 집계에 들어가지 않는다**")
    w = new_work(); ld = sha(w / "versions.lock")
    f = full_fixture(w, ld)
    # A 는 멀쩡하고 C00 만 자기 스키마를 어긴다(probe 가 문자열이 아니다).
    f["c00"].write_text('{"probe":123,"query_ok":true}\n', encoding="utf-8")
    write_manifest(f["c00"], "G0_0C00", lock_digest=ld)
    rc, out, _ = run_norm(w, **{k: v for k, v in f.items()})
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    rej = w / f"{RUN_ID}-out.json.rejected.json"
    check("거부 사본이 있다", rej.is_file())
    if rej.is_file():
        rec = json.loads(rej.read_text(encoding="utf-8"))
        # 7차 판은 집계한 뒤 coverage 만 FAILED 로 덮어서, schema 를 어긴 파일에서 뽑힌
        # fence_facts 가 레코드에 그대로 남았다.
        check("fence_facts 가 비어 있다(집계 자체를 하지 않았다)",
              rec.get("fence_facts") == {}, str(rec.get("fence_facts"))[:120])
        check("coverage 는 FAILED 다('안 돌렸다'가 아니다)",
              rec["coverage"]["g0_0c00"]["status"] == "FAILED",
              str(rec["coverage"]["g0_0c00"]))
        check("제외했다는 사실을 경고에 남긴다",
              any("집계 입력에서 제외" in x for x in rec.get("warnings", [])),
              str(rec.get("warnings"))[:200])
    shutil.rmtree(w)


def t_m3_effective_floor() -> None:
    print("\n[31] M3-3 — child 가 MEASURED 가 아니면 effective_value 가 floor 로 내려간다")
    w = new_work(); ld = sha(w / "versions.lock")
    # A 만 넣되 sentinel 을 빼서 PARTIAL 로 만든다. value 는 관측대로 남고
    # effective_value 는 내려가야 한다.
    a_art = w / f"a-{RUN_ID}.log"
    a_art.write_text(
        '{"probe":"feat.fetch_first","query_ok":true,"value":"1"}\n'
        '{"probe":"nls.characterset","query_ok":true,"value":"AL32UTF8"}\n'
        '{"probe_summary":{"expected":2,"emitted":2,"manifest_ok":true}}\n', encoding="utf-8")
    write_manifest(a_art, "G0_0A", lock_digest=ld)
    rc, out, _ = run_norm(w, a=a_art)
    check("exit 3(측정 불완전)", rc == 3, f"rc={rc}")
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    ax = rec["capability_axes"]["sql_dialect"]
    check("value 는 관측대로 12C_PLUS", ax["value"] == "12C_PLUS", ax["value"])
    check("effective_value 는 floor(11G)", ax["effective_value"] == "11G", ax["effective_value"])
    check("사유가 CHILD_NOT_MEASURED", "CHILD_NOT_MEASURED" in ax["floor_reasons"],
          str(ax["floor_reasons"]))
    check("요약은 effective_value 를 낸다",
          (out.get("capability_axes_effective") or {}).get("sql_dialect") == "11G",
          str(out.get("capability_axes_effective"))[:120])
    check("요약에 value 키를 싣지 않는다", "capability_axes" not in out, str(list(out))[:160])
    shutil.rmtree(w)


def t_m3_floor_never_raises() -> None:
    print("\n[32] M3-3 — floor 는 값을 **올리지 않는다**")
    w = new_work(); ld = sha(w / "versions.lock")
    a_art = w / f"a-{RUN_ID}.log"
    # 아무 probe 도 없다 → 전 축 UNDETERMINED. sql_dialect 의 floor 는 11G 지만
    # UNDETERMINED 를 11G 로 올리면 그것은 floor 가 아니라 승격이다.
    a_art.write_text('{"probe_summary":{"expected":0,"emitted":0,"manifest_ok":true}}\n',
                     encoding="utf-8")
    write_manifest(a_art, "G0_0A", lock_digest=ld)
    rc, out, _ = run_norm(w, a=a_art)
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    ax = rec["capability_axes"]["sql_dialect"]
    check("UNDETERMINED 는 그대로 UNDETERMINED", ax["effective_value"] == "UNDETERMINED",
          ax["effective_value"])
    check("outcome.capability=UNGRADED", rec["outcome"]["capability"] == "UNGRADED",
          str(rec["outcome"]))
    shutil.rmtree(w)


def t_m3_stale() -> None:
    print("\n[33] M3-3 — TTL 이 지난 측정은 stale 이고 floor 로 내려간다")
    w = new_work(); ld = sha(w / "versions.lock")
    f = full_fixture(w, ld, age_days=400)          # 400일 전 측정
    rc, out, _ = run_norm(w, target_owner="APP", target_table="T1", **f)
    check("exit 0(측정 자체는 완결)", rc == 0, f"rc={rc}")
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    ax = rec["capability_axes"]["db_charset"]
    check("value 는 관측대로 AL32UTF8", ax["value"] == "AL32UTF8", ax["value"])
    check("stale=true", ax["stale"] is True, str(ax))
    check("expires_at 이 있다", bool(ax["expires_at"]), str(ax["expires_at"]))
    check("effective_value 가 floor", ax["effective_value"] == "UNDETERMINED",
          ax["effective_value"])
    check("사유에 STALE", "STALE" in ax["floor_reasons"], str(ax["floor_reasons"]))
    # 같은 입력을 TTL 을 늘려 다시 재면 stale 이 아니다 — 양성 대조.
    rc2, out2, _ = run_norm(w, out=w / f"{RUN_ID}-out-fresh.json", capability_ttl_days=100000,
                            target_owner="APP", target_table="T1", **f)
    rec2 = json.loads((w / f"{RUN_ID}-out-fresh.json").read_text(encoding="utf-8"))
    check("TTL 을 늘리면 stale 이 아니다",
          rec2["capability_axes"]["db_charset"]["stale"] is False,
          str(rec2["capability_axes"]["db_charset"]["stale"]))
    shutil.rmtree(w)


def t_m3_no_ttl_declared() -> None:
    print("\n[34] M3-3 — TTL 미선언이면 신선도를 판정할 수 없으므로 확정값이 내려간다")
    w = new_work(); ld = sha(w / "versions.lock")
    f = full_fixture(w, ld)
    rc, out, _ = run_norm(w, capability_ttl_days=0, target_owner="APP", target_table="T1", **f)
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    ax = rec["capability_axes"]["db_charset"]
    check("freshness.basis=NO_TTL_DECLARED", rec["freshness"]["basis"] == "NO_TTL_DECLARED",
          str(rec["freshness"]))
    check("축의 freshness_basis 도 NO_TTL_DECLARED",
          ax["freshness_basis"] == "NO_TTL_DECLARED", ax["freshness_basis"])
    check("사유에 NO_FRESHNESS_BASIS", "NO_FRESHNESS_BASIS" in ax["floor_reasons"],
          str(ax["floor_reasons"]))
    shutil.rmtree(w)


def t_m3_profile_not_authoritative() -> None:
    print("\n[35] M3-3 — 비권위 profile 의 확정값은 publish 값이 되지 않는다")
    w = new_work(); ld = sha(w / "versions.lock")
    (w / "versions.lock").write_text('profile: LOCAL_WSL\n', encoding="utf-8")
    ld = sha(w / "versions.lock")
    f = full_fixture(w, ld, profile="LOCAL_WSL")
    rc, out, _ = run_norm(w, profile="LOCAL_WSL", target_owner="APP", target_table="T1", **f)
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    ax = rec["capability_axes"]["db_charset"]
    # 레코드는 스스로 "설계 주장의 근거가 아니다" 라고 적는다. 그렇게 적으면서 확정
    # capability 를 내보내면 두 말이 어긋난다.
    check("사유에 PROFILE_NOT_AUTHORITATIVE",
          "PROFILE_NOT_AUTHORITATIVE" in ax["floor_reasons"], str(ax["floor_reasons"]))
    check("effective_value 가 내려갔다", ax["effective_value"] != ax["value"],
          f"{ax['value']} / {ax['effective_value']}")
    shutil.rmtree(w)


def t_m3_composite_follows_input() -> None:
    print("\n[36] M3-3 — 합성 축은 입력이 내려가면 같이 내려간다")
    w = new_work(); ld = sha(w / "versions.lock")
    a_art = w / f"a-{RUN_ID}.log"
    a_art.write_text(
        '{"probe":"feat.standard_hash_sha256","query_ok":true,"value":"ba7816bf",'
        '"value_interpretable":true}\n'
        '{"probe_summary":{"expected":1,"emitted":1,"manifest_ok":true}}\n', encoding="utf-8")
    write_manifest(a_art, "G0_0A", lock_digest=ld)
    rc, out, _ = run_norm(w, a=a_art)          # sentinel 없음 → PARTIAL → floor
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    comp = rec["capability_axes"]["canonical_row_compare"]
    check("value 는 PARTIAL", comp["value"] == "PARTIAL", comp["value"])
    check("effective_value 는 NONE", comp["effective_value"] == "NONE", comp["effective_value"])
    check("사유에 COMPOSITE_INPUT_FLOORED",
          "COMPOSITE_INPUT_FLOORED" in comp["floor_reasons"], str(comp["floor_reasons"]))
    shutil.rmtree(w)


def t_m3_outcome_split() -> None:
    print("\n[37] M3-4/P0-04 — process·measurement·capability·final_gate 를 섞지 않는다")
    w = new_work(); ld = sha(w / "versions.lock")
    f = full_fixture(w, ld)
    rc, out, _ = run_norm(w, target_owner="APP", target_table="T1", **f)
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    o = rec["outcome"]
    check("process=CLEAN", o["process"] == "CLEAN", str(o))
    check("measurement=COMPLETE", o["measurement"] == "COMPLETE", str(o))
    check("final_gate=REJECTED_BY_CONTRACT", o["final_gate"] == "REJECTED_BY_CONTRACT", str(o))
    # 측정이 완결돼도 capability 는 별개다 — exit 3 에 섞이지 않는다.
    check("측정 완결(exit 0)과 capability 등급이 분리돼 있다",
          rc == 0 and o["capability"] in ("GRADED", "PARTIALLY_GRADED", "UNGRADED"),
          f"rc={rc} {o}")
    shutil.rmtree(w)


def t_m3_covered_diff() -> None:
    print("\n[38] M3-4 — not_covered 는 최종 계약과의 차집합이다")
    contract = json.loads((NORM.parent / "g0-final-contract.json").read_text(encoding="utf-8"))
    items = {i["item"] for i in contract["items"]}
    cov = {i["item"] for i in contract["items"] if i["g0_0"] == "COVERED"}
    nc = {i["item"] for i in contract["items"] if i["g0_0"] == "NOT_COVERED"}
    check("COVERED 와 NOT_COVERED 는 겹치지 않는다", not (cov & nc), str(cov & nc))
    check("둘의 합이 계약 전체다", (cov | nc) == items, str(items ^ (cov | nc)))
    sc = json.loads((NORM.parent / "g0-0-evidence.schema.json").read_text(encoding="utf-8"))
    enum = set(sc["properties"]["not_covered"]["items"]["properties"]["item"]["enum"])
    check("schema 의 not_covered enum 이 계약과 같다", enum == nc, str(enum ^ nc))
    cenum = set(sc["properties"]["covered"]["items"]["properties"]["item"]["enum"])
    check("schema 의 covered enum 이 계약과 같다", cenum == cov, str(cenum ^ cov))

    w = new_work(); ld = sha(w / "versions.lock")
    f = full_fixture(w, ld)
    rc, out, _ = run_norm(w, **f)
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    check("레코드의 not_covered 가 계약에서 나온다",
          {i["item"] for i in rec["not_covered"]} == nc, str(rec["not_covered"])[:120])
    check("레코드가 계약 digest 를 남긴다", len(rec["final_contract_digest"]) == 64)
    present = {i["item"]: i["present"] for i in rec["covered"]}
    check("실제로 있는 항목만 present=true",
          present.get("g0_report_id") is True and present.get("executed_at") is True,
          str(present))
    check("A 가 charset 을 읽었으면 present=true",
          present.get("oracle_env.nls_characterset") is True, str(present))
    # 음성 대조 — 그 probe 가 없는 회차에서는 present=false 여야 한다. 항상 true 를 적으면
    # 이 필드는 아무것도 말하지 않는다.
    w2 = new_work(); ld2 = sha(w2 / "versions.lock")
    a2 = w2 / f"a-{RUN_ID}.log"
    a2.write_text('{"probe_summary":{"expected":0,"emitted":0,"manifest_ok":true}}\n'
                  '{"probe_run_end":"G0-0A","status":"reached_end"}\n', encoding="utf-8")
    write_manifest(a2, "G0_0A", lock_digest=ld2)
    run_norm(w2, a=a2)
    rec2 = json.loads((w2 / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    present2 = {i["item"]: i["present"] for i in rec2["covered"]}
    check("charset 을 안 읽은 회차에서는 present=false",
          present2.get("oracle_env.nls_characterset") is False, str(present2))
    shutil.rmtree(w2)
    shutil.rmtree(w)


def t_m3_final_gate_rejects() -> None:
    print("\n[39] M3-4 — G0-0 레코드는 최종 G0 게이트에서 **항상** 거부된다")
    sys.path.insert(0, str(NORM.parent))
    import importlib
    gate = importlib.import_module("g0_final_gate")
    w = new_work(); ld = sha(w / "versions.lock")
    f = full_fixture(w, ld)
    rc, out, _ = run_norm(w, target_owner="APP", target_table="T1", **f)
    check("정규화는 성공했다(exit 0)", rc == 0, f"rc={rc}")
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    ok, reasons = gate.admit(rec)
    check("게이트가 거부한다", ok is False, str(reasons)[:160])
    check("사유에 record_type", any("record_type" in r for r in reasons), str(reasons)[:200])
    # gate_eligible 을 손으로 true 로 바꿔도 record_type 때문에 여전히 거부다.
    forged = dict(rec); forged["gate_eligible"] = True
    ok2, reasons2 = gate.admit(forged)
    check("gate_eligible 을 위조해도 거부", ok2 is False, str(reasons2)[:160])
    # 최종 aggregator 는 아직 없다 — 없다고 말한다.
    try:
        gate.aggregate()
        check("aggregate 는 NotImplementedError", False, "예외가 나지 않았다")
    except NotImplementedError:
        check("aggregate 는 NotImplementedError", True)
    shutil.rmtree(w)


def t_m3_out_path_and_pointer() -> None:
    print("\n[40] M3-5 — 무효한 재실행 뒤에 이전 회차가 current 로 읽히지 않는다")
    w = new_work(); ld = sha(w / "versions.lock")
    f = full_fixture(w, ld)
    ptr = w / "g0-0-evidence.current.json"
    rc, out, _ = run_norm(w, current=ptr, **f)
    check("첫 회차 성공", rc == 0, f"rc={rc}")
    p1 = json.loads(ptr.read_text(encoding="utf-8"))
    check("포인터가 VALID", p1["status"] == "VALID", str(p1)[:120])
    check("포인터가 이 회차를 가리킨다", p1["run_id"] == RUN_ID, str(p1)[:120])

    # 이제 무효한 재실행 — 다른 회차 id 로, 계약을 어긴 산출물(source_id 없는 manifest).
    bad = w / "RUN-TEST-0002-b0.json"
    bad.write_text('{"probe":"S1","ok":true}\n', encoding="utf-8")
    write_manifest(bad, "G0_0B0", lock_digest=ld, run_id="RUN-TEST-0002",
                   drop=("source_id",))
    rc2, out2, _ = run_norm(w, run_id="RUN-TEST-0002", b0=bad, current=ptr)
    check("두 번째 회차는 거부(exit 4)", rc2 == 4, f"rc={rc2}")
    p2 = json.loads(ptr.read_text(encoding="utf-8"))
    check("포인터가 INVALIDATED 로 덮였다", p2["status"] == "INVALIDATED", str(p2)[:160])
    check("무효화한 회차를 적는다", p2["rejected_run_id"] == "RUN-TEST-0002", str(p2)[:160])
    check("이전 포인터를 previous 로 보존한다",
          (p2.get("previous") or {}).get("run_id") == RUN_ID, str(p2.get("previous"))[:160])
    # 첫 회차 레코드 자체는 남아 있다 — 지우는 것이 아니라 **current 가 아니게** 한다.
    check("첫 회차 레코드는 그대로 있다", (w / f"{RUN_ID}-out.json").is_file())
    shutil.rmtree(w)


def t_m3_out_path_rules() -> None:
    print("\n[41] M3-5 — --out 은 run 별 경로여야 하고 덮어쓰지 않는다")
    w = new_work(); ld = sha(w / "versions.lock")
    rc, out, _ = run_norm(w, out=w / "g0-0-evidence.json")
    check("run_id 없는 --out 은 거부", rc == 4, f"rc={rc}")
    check("사유가 --out 경로",
          any("--out 경로에 run_id" in v for v in out.get("contract_violations", [])),
          str(out.get("contract_violations"))[:200])

    rc, _, _ = run_norm(w)
    check("정상 경로는 통과", rc in (0, 3), f"rc={rc}")
    rc2, out2, _ = run_norm(w)                     # 같은 경로에 두 번
    check("같은 --out 두 번은 거부", rc2 == 4, f"rc={rc2}")
    check("사유가 불변 산출물",
          any("이미 있다" in v for v in out2.get("contract_violations", [])),
          str(out2.get("contract_violations"))[:200])
    rc3, _, _ = run_norm(w, flags=("--allow-overwrite",))
    check("--allow-overwrite 를 명시하면 통과", rc3 in (0, 3), f"rc={rc3}")
    shutil.rmtree(w)


def main() -> int:
    print("=" * 70)
    print("g0-normalize.py 반례 회귀 시험 — 7차 §5.1 + 8차 M1·M3")
    print("=" * 70)
    for t in (t_jsonschema_present, t_b0_one_line, t_b1_fabricated, t_b1_no_failclosed,
              t_c00_summary_only, t_ce_empty_pass, t_ce_bad_returncode, t_a_no_sentinel,
              t_a_duplicate_probe, t_missing_manifest, t_lock_mismatch, t_run_id_mismatch,
              t_artifact_tampered, t_child_nonzero_exit, t_nothing_run,
              t_lock_unset_comment_only, t_positive_control, t_b1_path_split_runs,
              t_axes_derived_in_record,
              # 8차 M1
              t_m1_no_source_id, t_m1_no_harness_digest, t_m1_source_mismatch,
              t_m1_harness_mismatch, t_m1_expected_source_mismatch,
              t_m1_path_without_run_id, t_m1_overwrote, t_m1_no_timestamps,
              t_m1_child_schema_missing_key, t_m1_child_schema_files_exist,
              # 8차 M3
              t_m3_no_aggregation_before_schema, t_m3_effective_floor,
              t_m3_floor_never_raises, t_m3_stale, t_m3_no_ttl_declared,
              t_m3_profile_not_authoritative, t_m3_composite_follows_input,
              t_m3_outcome_split, t_m3_covered_diff, t_m3_final_gate_rejects,
              t_m3_out_path_and_pointer, t_m3_out_path_rules):
        t()
    print("\n" + "=" * 70)
    print(f"통과 {PASS}건 · 실패 {len(FAIL)}건")
    for f in FAIL:
        print(f"  - {f}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
