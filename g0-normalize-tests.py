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


# **서버가 밝히는 값과 같아야 한다**(9차 조치 4 · P0-02). 이전 픽스처는 manifest 에
# TESTSTBY, A probe 에 ETLSTB 를 넣고도 통과했다 — 한 레코드가 두 원천을 말했다.
SOURCE_ID = "ETLSTB"
# 9차 조치 7 — CE 는 **폐기용 쓰기 가능 DB** 에서 돈다. 원천과 같은 이름을 쓰면
# 집계기가 거부한다(P0-07). 픽스처도 실제 절차와 같은 이름을 쓴다.
CE_SOURCE_ID = "CEFREE1"
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
                   age_days: float = 0.02, env_kind: str = "host",
                   env_scope: str | None = None) -> None:
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
        # 9차 조치 4 — 래퍼가 관측한 실행 환경. 시험은 'host'(반증 불가)로 둔다.
        "env_kind": env_kind,
        # 9차 조치 7 — scope 는 래퍼가 CHILD 에서 **유도**한다. 시험도 같은 규칙을 쓴다.
        "environment_scope": env_scope or
                             ("COUNTEREXAMPLE" if child == "G0_0C_SUITE" else "SOURCE"),
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
    # **sentinel 부재만 격리한다.** 나머지는 정상이어야 그 성질을 시험한 것이 된다 —
    # 이전 판은 probe 1건에 summary 86 이라 9차 조치 3 이후 다른 이유로 거부된다.
    art.write_text(a_log(sentinel=False), encoding="utf-8")
    write_manifest(art, "G0_0A", lock_digest=ld)
    rc, out, _ = run_norm(w, a=art)
    st = (out.get("coverage") or {}).get("g0_0a")
    check("sentinel 없으면 PARTIAL", st == "PARTIAL", f"status={st}")
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    # **`value` 가 아니라 `effective_value` 를 본다**(8차 M3-3). 87 probe 를 실제로 낸
    # 픽스처에서는 관측값이 나오는 것이 정상이다 — `value` 는 감사용이고, PARTIAL 인
    # child 에서 나온 값은 `effective_value` 가 floor 로 내려가야 한다.
    ax = rec["capability_axes"]
    floored = [k for k, v in ax.items() if "CHILD_NOT_MEASURED" in v["floor_reasons"]]
    check("전 축에 CHILD_NOT_MEASURED floor 가 걸린다", len(floored) == len(ax),
          f"안 걸린 축 {sorted(set(ax) - set(floored))}")
    determinate = {k: v["effective_value"] for k, v in ax.items()
                   if v["effective_value"] not in ("UNDETERMINED", "UNDEFINED")}
    # floor 로 내려간 값은 남을 수 있다(sql_dialect 의 floor 는 11G 다). 확인할 것은
    # **관측값이 그대로 실행값이 되지 않는다**는 것이다.
    same = [k for k, v in ax.items()
            if v["value"] != "UNDETERMINED" and v["effective_value"] == v["value"]]
    check("관측값이 그대로 실행값이 되지 않는다", not same,
          f"floor 가 안 걸린 축 {same} / 확정 실행값 {determinate}")
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


# ── 9차 조치 3: A 픽스처는 **실제 87개 probe** 를 낸다 ──────────────────
# 이전 픽스처는 probe 3건에 `summary{expected:86, emitted:86}` 였고, 그것이 `MEASURED` 로
# 통과했다. **저장소가 자기 결함을 양성 대조로 고정해 두고 있었다**(9차 P0-01).
# 목록은 SQL 에서 생성된 계약에서 읽는다 — 여기에 87개를 베껴 적으면 그것도 같이 낡는다.
A_REQUIRED = json.loads(
    (ROOT / "g0-child-schemas" / "g0-0a-probe-manifest.json").read_text(encoding="utf-8")
)["probe_ids"]

# 축 파생이 확정값을 내려면 값이 필요한 probe 들. 나머지는 형태만 맞춘다.
A_VALUES = {
    "userenv.DB_UNIQUE_NAME": "ETLSTB",
    "userenv.DATABASE_ROLE": "PHYSICAL STANDBY",
    "nls.characterset": "AL32UTF8",
}


def a_log(*, drop: tuple[str, ...] = (), extra: tuple[str, ...] = (),
          expected: int | None = None, emitted: int | None = None,
          sentinel: bool = True, values: dict | None = None,
          failed_ora: int | None = None) -> str:
    """계약이 요구하는 87개를 그대로 내는 A 로그.

    `drop`/`extra` 로 집합 반례를, `values` 로 특정 probe 의 값을 준다 — 축 파생을
    시험하려면 값이 필요한데, **그렇다고 87개 집합을 깨서는 안 된다**(9차 조치 3).
    """
    vals = dict(A_VALUES)
    vals.update(values or {})
    ids = [i for i in A_REQUIRED if i not in set(drop)] + list(extra)
    lines = []
    for i in ids:
        if failed_ora:
            # 전 probe 가 transient 로 실패한 회차. **TRANSIENT 는 절대 NONE 이 아니므로**
            # 모든 축이 UNDETERMINED 가 된다 — floor 가 값을 올리지 않는지 보는 데 쓴다.
            lines.append(json.dumps({"probe": i, "query_ok": False, "ora": -failed_ora,
                                     "msg": f"ORA-{failed_ora:05d}: test"}, ensure_ascii=False))
            continue
        rec = {"probe": i, "query_ok": True}
        v = vals.get(i)
        if isinstance(v, dict):
            rec.update(v)
        elif v is not None:
            rec["value"] = v
        lines.append(json.dumps(rec, ensure_ascii=False))
    lines.append(json.dumps({"probe_summary": {
        "expected": len(A_REQUIRED) if expected is None else expected,
        "emitted": len(ids) if emitted is None else emitted,
        "manifest_ok": True, "query_failed": 0, "value_mismatch": 0}}))
    if sentinel:
        lines.append(json.dumps({"probe_run_end": "G0-0A", "status": "reached_end"}))
    return "\n".join(lines) + "\n"


def full_fixture(w: pathlib.Path, ld: str, **mk) -> dict:
    """다섯 child 가 모두 MEASURED 에 도달하는 산출물 한 벌.

    양성 대조([16])와 8차 M3 시험들이 같은 픽스처를 쓴다 — 두 벌을 따로 두면 한쪽만
    현실을 따라가고 다른 쪽은 조용히 낡는다. `mk` 는 `write_manifest` 로 넘어간다
    (예: `age_days` 로 측정 시각을 과거로 밀어 stale 을 낸다).
    """
    a_art = w / f"a-{RUN_ID}.log"
    a_art.write_text(a_log(), encoding="utf-8")
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
    ce.write_text(json.dumps({
        # CE 는 자기 환경의 서버 신원을 증거에 남긴다 — 집계기가 CE manifest 의
        # source_id 를 이 값과 대조한다(9차 조치 7).
        "environment": {"primary_db_unique_name": CE_SOURCE_ID},
        "suite_verdict": {"pass": True}, "scenarios": [
        {"id": "CE01", "outcome": "MITIGATION_HOLDS", "child_returncode": 0},
        {"id": "CE02", "outcome": "MITIGATION_HOLDS", "child_returncode": 0}]}), encoding="utf-8")
    write_manifest(ce, "G0_0C_SUITE", lock_digest=ld,
                   **{**mk, "source_id": CE_SOURCE_ID})

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
    # **87 집합을 지키면서 값만 준다**(9차 조치 3). 이전 판은 13개짜리 로그였고,
    # 집합 검사가 생긴 뒤로는 그것이 축 파생이 아니라 집합 위반을 시험하게 된다.
    a_art.write_text(a_log(values={
        "userenv.DB_UNIQUE_NAME": "ETLSTB",
        "dbms_flashback.get_scn": {"value": "9912345", "value_interpretable": True},
        "as_of_timestamp.target": "1",
        "feat.standard_hash_sha256": {"value": "ba7816bf", "value_interpretable": True},
        "feat.fetch_first": "1",
        "nls.characterset": "AL32UTF8",
        "nls.nchar_characterset": "AL16UTF16",
        "v$parameter.max_string": "STANDARD",
        "wm_column.type_facts": "TIMESTAMP(2)|scale=2",
        "feat.ora_rowscn_target": "9912000",
        "feat.rowdependencies_target": "DISABLED",
        "alter.STANDBY_MAX_DATA_DELAY.D": "ok",
        "max_delay_zero.touch_target": "1",
    }), encoding="utf-8")
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
    a_art.write_text(a_log(sentinel=False,
                           values={"feat.fetch_first": "1",
                                   "nls.characterset": "AL32UTF8"}), encoding="utf-8")
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
    # 87건을 다 내되 **전부 transient 실패**로 만든다(ORA-03135 연결 단절).
    # 집합 검사는 통과하고 축은 전부 UNDETERMINED 가 된다.
    a_art.write_text(a_log(failed_ora=3135), encoding="utf-8")
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
    # TTL 을 **명시로** 준다. 9차 조치 8 전에는 이 줄이 없어도 기본값 30일이 자동으로
    # 붙어 stale 이 만들어졌다 — 이 시험이 시험하려는 것은 만료 판정이지 기본값이 아니다.
    rc, out, _ = run_norm(w, capability_ttl_days=30, target_owner="APP", target_table="T1", **f)
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
    a_art.write_text(a_log(sentinel=False, values={
        "feat.standard_hash_sha256": {"value": "ba7816bf", "value_interpretable": True},
    }), encoding="utf-8")
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
    # 87건을 다 내되 전부 transient 실패 — charset 값이 없는 회차를 만든다.
    # 집합 검사는 통과해야 이 시험이 `covered.present` 를 시험한 것이 된다.
    a2.write_text(a_log(failed_ora=3135), encoding="utf-8")
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


# ── 9차 조치 3 ───────────────────────────────────────────────────────
def t_a9_probe_manifest_matches_sql() -> None:
    print("\n[42] 조치 3 — probe manifest 가 SQL 과 같은가")
    r = subprocess.run([sys.executable, str(ROOT / "g0-0a-probe-manifest.py")],
                       capture_output=True, text=True, cwd=str(ROOT))
    check("manifest 와 SQL 이 일치한다", r.returncode == 0,
          f"{r.stdout.strip()} {r.stderr.strip()}"[:300])
    check("87건이다", len(A_REQUIRED) == 87, str(len(A_REQUIRED)))
    sql = (ROOT / "g0-0a-capability-inventory.sql").read_text(encoding="utf-8")
    check("SQL 의 c_expected 와 목록 길이가 같다",
          f":= {len(A_REQUIRED)};" in sql, "c_expected 를 함께 고쳐라")


def t_a9_missing_probe() -> None:
    print("\n[43] 조치 3 — required probe 하나가 빠지면 거부 (9차 §9-1)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"a-{RUN_ID}.log"
    art.write_text(a_log(drop=("feat.standard_hash_sha256",)), encoding="utf-8")
    write_manifest(art, "G0_0A", lock_digest=ld)
    rc, out, _ = run_norm(w, a=art)
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    check("사유가 '계약에 있는 probe 가 없다'",
          any("산출물에 없다" in v for v in out.get("contract_violations", [])),
          str(out.get("contract_violations"))[:200])
    shutil.rmtree(w)


def t_a9_unknown_probe() -> None:
    print("\n[44] 조치 3 — 알 수 없는 probe 가 있으면 거부 (9차 §9-2)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"a-{RUN_ID}.log"
    art.write_text(a_log(extra=("feat.made_up_probe",)), encoding="utf-8")
    write_manifest(art, "G0_0A", lock_digest=ld)
    rc, out, _ = run_norm(w, a=art)
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    check("사유가 '계약에 없는 probe'",
          any("계약에 없는 probe" in v for v in out.get("contract_violations", [])),
          str(out.get("contract_violations"))[:200])
    shutil.rmtree(w)


def t_a9_lying_summary() -> None:
    print("\n[45] 조치 3 — probe 3건 + summary 87 → 거부 (9차 §9-3, **저장소가 스스로 통과시키던 것**)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"a-{RUN_ID}.log"
    # 9차가 잡은 실물 반례. 이전 판은 이것을 MEASURED 로 만들었고, 그 픽스처가
    # full_fixture() 에 양성 대조로 박혀 있었다.
    art.write_text(
        '{"probe":"userenv.DB_UNIQUE_NAME","query_ok":true,"value":"ETLSTB"}\n'
        '{"probe":"userenv.DATABASE_ROLE","query_ok":true,"value":"PHYSICAL STANDBY"}\n'
        '{"probe":"nls.characterset","query_ok":true,"value":"AL32UTF8"}\n'
        '{"probe_summary":{"expected":87,"emitted":87,"manifest_ok":true}}\n'
        '{"probe_run_end":"G0-0A","status":"reached_end"}\n', encoding="utf-8")
    write_manifest(art, "G0_0A", lock_digest=ld)
    rc, out, _ = run_norm(w, a=art)
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    v = " ".join(out.get("contract_violations", []))
    check("빠진 probe 를 지적한다", "산출물에 없다" in v, v[:200])
    check("자기 신고와 실물이 다름을 지적한다", "자기 신고와 실물이 다르다" in v, v[:300])
    check("최종 경로에 쓰지 않았다", not (w / f"{RUN_ID}-out.json").exists())
    shutil.rmtree(w)


def t_a9_summary_count_mismatch() -> None:
    print("\n[46] 조치 3 — 87건을 다 냈어도 summary 가 다른 수를 신고하면 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"a-{RUN_ID}.log"
    art.write_text(a_log(expected=86, emitted=86), encoding="utf-8")
    write_manifest(art, "G0_0A", lock_digest=ld)
    rc, out, _ = run_norm(w, a=art)
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    check("계약 건수와 다름을 지적한다",
          any("계약은" in v for v in out.get("contract_violations", [])),
          str(out.get("contract_violations"))[:200])
    shutil.rmtree(w)


def t_a9_positive_control() -> None:
    print("\n[47] 조치 3 — **양성 대조**: 87건을 정확히 내면 MEASURED")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"a-{RUN_ID}.log"
    art.write_text(a_log(), encoding="utf-8")
    write_manifest(art, "G0_0A", lock_digest=ld)
    rc, out, _ = run_norm(w, a=art)
    check("g0_0a=MEASURED", (out.get("coverage") or {}).get("g0_0a") == "MEASURED",
          f"{out.get('coverage')} rc={rc}")
    check("계약 위반 0건", rc == 3, f"rc={rc} — 다른 child 미실행으로 3 이어야 한다")
    shutil.rmtree(w)


# ── 9차 조치 4 ───────────────────────────────────────────────────────
def t_a9_source_identity_mismatch() -> None:
    print("\n[48] 조치 4 — manifest source_id ≠ 서버 DB_UNIQUE_NAME 이면 거부 (9차 §9-4)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"a-{RUN_ID}.log"
    art.write_text(a_log(), encoding="utf-8")          # 서버는 ETLSTB 라고 말한다
    write_manifest(art, "G0_0A", lock_digest=ld, source_id="TESTSTBY")
    rc, out, _ = run_norm(w, a=art)
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    v = " ".join(out.get("contract_violations", []))
    check("한 레코드가 두 원천을 말한다고 지적한다", "두 원천을 말한다" in v, v[:250])
    check("최종 경로에 쓰지 않았다", not (w / f"{RUN_ID}-out.json").exists())
    shutil.rmtree(w)


def t_a9_declared_source_mismatch() -> None:
    print("\n[49] 조치 4 — **M1 을 통과하는 구멍**: manifest 와 --source-id 는 일치하는데 둘 다 서버와 다르다")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"a-{RUN_ID}.log"
    art.write_text(a_log(), encoding="utf-8")          # 서버는 ETLSTB 라고 말한다
    # M1-2 는 manifest ↔ --source-id 만 본다. 둘을 같은 거짓 이름으로 맞추면 통과했다 —
    # **같은 이름을 공유하는 것은 같은 원천에서 나왔다는 증명이 아니다.**
    write_manifest(art, "G0_0A", lock_digest=ld, source_id="OTHERDB")
    rc, out, _ = run_norm(w, a=art, source_id="OTHERDB")
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    v = " ".join(out.get("contract_violations", []))
    check("M1-2 의 manifest↔인자 대조는 통과했다", "source_id 불일치" not in v, v[:200])
    check("서버 신원과 다름을 지적한다(9차 조치 4 가 잡는다)",
          "두 원천을 말한다" in v, v[:300])
    shutil.rmtree(w)


def t_a9_no_server_identity_floors() -> None:
    print("\n[50] 조치 4 — 서버 신원을 못 읽으면 위반이 아니라 **floor** 다")
    w = new_work(); ld = sha(w / "versions.lock")
    # A 를 안 돌린 회차. 대조할 상대가 없다 — 그것은 계약 위반이 아니라 미확인이다.
    b0 = w / f"b0-{RUN_ID}.json"
    b0.write_text(json.dumps({"b0_summary": {"expected_steps": ["S0"],
                                             "emitted_steps": ["S0"]}}) + "\n", encoding="utf-8")
    write_manifest(b0, "G0_0B0", lock_digest=ld)
    rc, out, _ = run_norm(w, b0=b0)
    check("계약 위반은 아니다(exit 3)", rc == 3, f"rc={rc}")
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    check("대조할 상대가 없다고 경고한다",
          any("대조할 상대가 없다" in x for x in rec.get("warnings", [])),
          str(rec.get("warnings"))[:250])
    reasons = {r for v in rec["capability_axes"].values() for r in v["floor_reasons"]}
    check("SOURCE_IDENTITY_UNVERIFIED floor 가 걸린다",
          "SOURCE_IDENTITY_UNVERIFIED" in reasons, str(sorted(reasons)))
    shutil.rmtree(w)


def t_a9_profile_relabel() -> None:
    print("\n[51] 조치 4 — WSL 에서 CORP_POC 로 재라벨하면 거부 (9차 §9-5)")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"a-{RUN_ID}.log"
    art.write_text(a_log(), encoding="utf-8")
    # 래퍼가 관측한 환경은 wsl 인데 caller 는 CORP_POC 라고 선언했다.
    write_manifest(art, "G0_0A", lock_digest=ld, profile="CORP_POC", env_kind="wsl")
    rc, out, _ = run_norm(w, a=art, profile="CORP_POC")
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    check("재라벨을 지적한다",
          any("재라벨한 것이다" in x for x in out.get("contract_violations", [])),
          str(out.get("contract_violations"))[:250])
    shutil.rmtree(w)


def t_a9_env_kind_required() -> None:
    print("\n[52] 조치 4 — env_kind 를 기록하지 않은 manifest 는 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"a-{RUN_ID}.log"
    art.write_text(a_log(), encoding="utf-8")
    write_manifest(art, "G0_0A", lock_digest=ld, drop=("env_kind",))
    rc, out, _ = run_norm(w, a=art)
    check("exit 4(계약 위반)", rc == 4, f"rc={rc}")
    check("관측하지 못한 것을 통과로 두지 않는다",
          any("env_kind 가 없다" in x for x in out.get("contract_violations", [])),
          str(out.get("contract_violations"))[:250])
    shutil.rmtree(w)


def t_a9_profile_positive_control() -> None:
    print("\n[53] 조치 4 — **양성 대조**: 환경이 profile 을 반증하지 않으면 통과")
    w = new_work(); ld = sha(w / "versions.lock")
    art = w / f"a-{RUN_ID}.log"
    art.write_text(a_log(), encoding="utf-8")
    # host 는 CORP_POC 를 **입증하지 않지만 반증하지도 않는다** — 판정은 비대칭이다.
    write_manifest(art, "G0_0A", lock_digest=ld, profile="CORP_POC", env_kind="host")
    rc, out, _ = run_norm(w, a=art, profile="CORP_POC")
    check("통과한다(exit 3 — 다른 child 미실행)", rc == 3, f"rc={rc}")
    # LOCAL_WSL 선언 + wsl 관측도 정상이다.
    w2 = new_work(); ld2 = sha(w2 / "versions.lock")
    (w2 / "versions.lock").write_text("profile: LOCAL_WSL\n", encoding="utf-8")
    ld2 = sha(w2 / "versions.lock")
    a2 = w2 / f"a-{RUN_ID}.log"
    a2.write_text(a_log(), encoding="utf-8")
    write_manifest(a2, "G0_0A", lock_digest=ld2, profile="LOCAL_WSL", env_kind="wsl")
    rc2, out2, _ = run_norm(w2, a=a2, profile="LOCAL_WSL")
    check("LOCAL_WSL + wsl 관측은 정상", rc2 == 3, f"rc={rc2}")
    shutil.rmtree(w); shutil.rmtree(w2)


# ── 9차 조치 7 ───────────────────────────────────────────────────────
def _ce_pair(w: pathlib.Path, ld: str, *, ce_source: str, ce_observed: str | None = None,
             ce_scope: str | None = None) -> dict:
    """A(SOURCE) 와 CE(COUNTEREXAMPLE) 를 한 회차에 넣는 최소 픽스처."""
    a = w / f"a-{RUN_ID}.log"
    a.write_text(a_log(), encoding="utf-8")
    write_manifest(a, "G0_0A", lock_digest=ld)
    ce = w / f"ce-{RUN_ID}.json"
    ce.write_text(json.dumps({
        "environment": {"primary_db_unique_name": ce_observed
                        if ce_observed is not None else ce_source},
        "suite_verdict": {"pass": True},
        "scenarios": [{"id": "CE01", "outcome": "MITIGATION_HOLDS",
                       "child_returncode": 0}]}), encoding="utf-8")
    write_manifest(ce, "G0_0C_SUITE", lock_digest=ld, source_id=ce_source,
                   env_scope=ce_scope)
    return {"a": a, "c_suite": ce}


def t_a9_scope_allows_two_environments() -> None:
    """**양성 대조가 먼저다.** 조치 7 의 요점은 거부가 아니라 *정상 회차를 통과시키는 것*이다.

    이전 판은 CE 를 한 회차에 넣는 순간 `source_id` 균일성 검사가 거부했다. 그래서 운영자
    앞에 남은 선택지가 **CE 가 원천 이름을 거짓 신고하는 것** 뿐이었다(9차 P0-07).
    """
    print("\n[56] 조치 7 — CE 와 원천을 한 회차에 넣어도 통과한다 (P0-07 의 본체)")
    w = new_work(); ld = sha(w / "versions.lock")
    f = _ce_pair(w, ld, ce_source=CE_SOURCE_ID)
    rc, out, err = run_norm(w, **f)
    check("서로 다른 원천인데 회차가 거부되지 않는다", rc == 3,
          f"rc={rc} {err[:300]}")
    rec = json.loads(pathlib.Path(out["out"]).read_text(encoding="utf-8"))
    check("contract_violations 가 비어 있다", not rec.get("contract_violations"),
          str(rec.get("contract_violations"))[:300])
    shutil.rmtree(w)


def t_a9_scope_recorded_in_record() -> None:
    print("\n[57] 조치 7 — 레코드가 **스스로** 몇 개 환경의 증거인지 말한다")
    w = new_work(); ld = sha(w / "versions.lock")
    f = _ce_pair(w, ld, ce_source=CE_SOURCE_ID)
    rc, out, _ = run_norm(w, **f)
    rec = json.loads(pathlib.Path(out["out"]).read_text(encoding="utf-8"))
    es = rec.get("environment_scopes") or {}
    check("SOURCE·COUNTEREXAMPLE 두 scope 가 기록된다",
          set(es) == {"SOURCE", "COUNTEREXAMPLE"}, str(sorted(es)))
    check("각 scope 가 자기 원천 이름을 적는다",
          es.get("SOURCE", {}).get("source_ids") == [SOURCE_ID] and
          es.get("COUNTEREXAMPLE", {}).get("source_ids") == [CE_SOURCE_ID], str(es)[:300])
    check("scope 의 뜻이 레코드 안에 있다 — 다른 문서를 찾지 않아도 된다",
          "capability" in es.get("COUNTEREXAMPLE", {}).get("means", ""),
          str(es.get("COUNTEREXAMPLE", {}).get("means"))[:200])
    shutil.rmtree(w)


def t_a9_ce_same_source_rejected() -> None:
    print("\n[58] 조치 7 — CE 가 원천과 **같은 DB** 를 신고하면 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    # 운영자가 CE 단계에서 G0_SOURCE_ID 를 바꾸지 않은 경우. 그러면 파괴적 시나리오가
    # 사내 원천에서 돌았다는 뜻이 되고, 그것은 통과시킬 수 있는 값이 아니다.
    f = _ce_pair(w, ld, ce_source=SOURCE_ID)
    rc, out, err = run_norm(w, **f)
    check("exit 4(거부)", rc == 4, f"rc={rc}")
    check("사유가 CE 와 원천의 동일 DB 다",
          "폐기용" in err and "P0-07" in err, err[:300])
    shutil.rmtree(w)


def t_a9_ce_identity_bound_to_own_server() -> None:
    """scope 를 도입하면 CE 는 A 의 서버 신원 대조에서 빠진다. 거기서 끝내면 **파괴적
    시나리오를 도는 쪽만 신원 대조를 면제받는다.** CE 는 자기 환경의 서버 값에 묶인다."""
    print("\n[59] 조치 7 — CE manifest 가 CE 증거의 서버 신원과 다르면 거부")
    w = new_work(); ld = sha(w / "versions.lock")
    f = _ce_pair(w, ld, ce_source=CE_SOURCE_ID, ce_observed="SOMEOTHERDB")
    rc, out, err = run_norm(w, **f)
    check("exit 4(거부)", rc == 4, f"rc={rc}")
    check("사유가 CE manifest ↔ CE 증거 불일치다",
          "CE runner 가 서버에서" in err, err[:300])
    shutil.rmtree(w)


def t_a9_scope_is_derived_not_declared() -> None:
    """scope 가 선언값이면 CE 를 SOURCE 로 신고해 원래 결함으로 되돌아갈 수 있다."""
    print("\n[60] 조치 7 — CE 를 SOURCE 로 재라벨하면 거부 (scope 는 유도값이다)")
    w = new_work(); ld = sha(w / "versions.lock")
    f = _ce_pair(w, ld, ce_source=CE_SOURCE_ID, ce_scope="SOURCE")
    rc, out, err = run_norm(w, **f)
    check("exit 4(거부)", rc == 4, f"rc={rc}")
    check("사유가 계약이 정한 scope 와 다름이다",
          "계약이 정한 값은" in err, err[:300])

    # scope 를 아예 빼도 거부한다 — 조치 7 이전 판본의 래퍼로 돌린 경우.
    w2 = new_work(); ld2 = sha(w2 / "versions.lock")
    a2 = w2 / f"a-{RUN_ID}.log"
    a2.write_text(a_log(), encoding="utf-8")
    write_manifest(a2, "G0_0A", lock_digest=ld2, drop=("environment_scope",))
    rc2, _, err2 = run_norm(w2, a=a2)
    check("scope 가 없으면 거부", rc2 == 4, f"rc={rc2}")
    check("사유가 옛 래퍼다", "이전 판본의 래퍼" in err2, err2[:300])
    shutil.rmtree(w); shutil.rmtree(w2)


def t_a9_ce_raises_no_axis() -> None:
    """**CE 는 어떤 capability 축도 올리지 못한다.**

    지금은 코드 구조가 이것을 지킨다 — 축은 A·B0·B1 의 probe 사전에서만 나오고 `cov_ce`
    는 축에 손대지 않는다. 구조가 바뀌면 조용히 깨지므로 여기서 못박는다.
    """
    print("\n[61] 조치 7 — CE 만 있는 회차는 축을 하나도 올리지 못한다")
    w = new_work(); ld = sha(w / "versions.lock")
    ce = w / f"ce-{RUN_ID}.json"
    ce.write_text(json.dumps({
        "environment": {"primary_db_unique_name": CE_SOURCE_ID},
        "suite_verdict": {"pass": True},
        "scenarios": [{"id": "CE01", "outcome": "MITIGATION_HOLDS",
                       "child_returncode": 0}]}), encoding="utf-8")
    write_manifest(ce, "G0_0C_SUITE", lock_digest=ld, source_id=CE_SOURCE_ID)
    rc, out, _ = run_norm(w, c_suite=ce)
    eff = out.get("capability_axes_effective") or {}
    decided = [k for k, v in eff.items() if v not in (None, "UNDETERMINED")]
    check("확정된 축이 하나도 없다", not decided, str(decided)[:300])
    check("축이 실제로 존재하기는 한다 — 빈 사전으로 통과하지 않는다", bool(eff))
    shutil.rmtree(w)


# ── 9차 조치 5 ───────────────────────────────────────────────────────
def t_a9_harness_manifest_complete() -> None:
    print("\n[54] 조치 5 — harness manifest 가 저장소 전체를 덮는가 (9차 §9-6)")
    r = subprocess.run([sys.executable, str(ROOT / "g0-harness-manifest.py")],
                       capture_output=True, text=True, cwd=str(ROOT))
    check("미선언 파일이 없다", r.returncode == 0,
          f"{r.stdout.strip()} {r.stderr.strip()}"[:400])

    man = json.loads((ROOT / "g0-harness-manifest.json").read_text(encoding="utf-8"))
    h = {e["path"] for e in man["harness"]}
    # 9차가 "빠져 있다" 고 지목한 파일들이 이제 들어 있는가.
    for must in ("g0-0b1-connection-provider/src/main/java/etl/g0b1/Preamble.java",
                 "g0-0b1-connection-provider/src/main/resources/META-INF/services/"
                 "org.apache.spark.sql.jdbc.JdbcConnectionProvider",
                 "g0-0b1-connection-provider/build.sh",
                 "g0-child-schemas/g0-child-a.schema.json",
                 "g0-final-contract.json",
                 "g0_final_gate.py",
                 "g0-0c-counterexamples/suite.yaml"):
        check(f"harness 에 있다: {must.split('/')[-1]}", must in h, must)
    check("이전 판(11건)보다 늘었다", len(h) > 11, str(len(h)))


def t_a9_harness_digest_changes() -> None:
    print("\n[55] 조치 5 — behavior 파일을 바꾸면 digest 가 바뀐다")
    import importlib.util
    spec = importlib.util.spec_from_file_location("hm", ROOT / "g0-harness-manifest.py")
    hm = importlib.util.module_from_spec(spec); spec.loader.exec_module(hm)
    base = hm.digest()

    # 9차가 지목한 "빠져 있던" 파일 하나를 건드려 본다. 되돌린다.
    target = ROOT / "g0-0b1-connection-provider/src/main/java/etl/g0b1/Preamble.java"
    orig = target.read_bytes()
    try:
        target.write_bytes(orig + b"\n// wiring test touch\n")
        after = hm.digest()
        check("Preamble.java 를 바꾸면 digest 가 바뀐다", after != base,
              "이전 판은 이 파일이 목록에 없어 digest 가 그대로였다")
    finally:
        target.write_bytes(orig)
    check("되돌리면 digest 가 복원된다", hm.digest() == base)


def t_a9_undeclared_file_fails() -> None:
    print("\n[56] 조치 5 — **음성 대조**: 선언되지 않은 파일이 생기면 실패한다")
    newf = ROOT / "g0-zz-undeclared-probe.py"
    assert not newf.exists()
    newf.write_text("# 9차 조치 5 음성 대조용 임시 파일\n", encoding="utf-8")
    try:
        r = subprocess.run([sys.executable, str(ROOT / "g0-harness-manifest.py")],
                           capture_output=True, text=True, cwd=str(ROOT))
        check("검사가 실패한다", r.returncode != 0, f"rc={r.returncode}")
        check("그 파일을 지목한다", "g0-zz-undeclared-probe.py" in r.stderr, r.stderr[:200])
        rd = subprocess.run([sys.executable, str(ROOT / "g0-harness-manifest.py"), "--digest"],
                            capture_output=True, text=True, cwd=str(ROOT))
        check("불완전하면 digest 를 내주지 않는다", rd.returncode != 0, rd.stdout[:80])
    finally:
        newf.unlink()
    r2 = subprocess.run([sys.executable, str(ROOT / "g0-harness-manifest.py")],
                        capture_output=True, text=True, cwd=str(ROOT))
    check("지우면 다시 통과한다", r2.returncode == 0, r2.stderr[:200])


def t_a9_ttl_undeclared_by_default() -> None:
    print("\n[62] 조치 8 — TTL 을 주지 않으면 미선언이고 전 축이 floor 로 내려간다")
    w = new_work(); ld = sha(w / "versions.lock")
    f = full_fixture(w, ld)
    # **--capability-ttl-days 를 주지 않는다.** 9차 조치 8 전에는 여기서 조용히 30일이
    # 적용되고 레코드에 `OPERATOR_DECLARED_TTL` 이 박혔다 — 운영자는 선언한 적이 없다.
    rc, out, _ = run_norm(w, target_owner="APP", target_table="T1", **f)
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    check("freshness.basis=NO_TTL_DECLARED", rec["freshness"]["basis"] == "NO_TTL_DECLARED",
          str(rec["freshness"]))
    check("ttl_seconds 가 비어 있다", rec["freshness"]["ttl_seconds"] is None,
          str(rec["freshness"]["ttl_seconds"]))
    check("미선언이라고 경고한다",
          any("capability-ttl-days 미선언" in x for x in rec["warnings"]),
          str(rec["warnings"])[:200])
    # note 도 basis 와 같은 말을 해야 한다 — 미선언인데 "운영자가 선언한 상한" 이라고
    # 적으면 P1-03 이 지적한 것과 같은 종류의 거짓이 레코드에 남는다.
    check("note 가 선언했다고 말하지 않는다",
          "선언되지 않았다" in rec["freshness"]["note"], rec["freshness"]["note"][:120])
    ax = rec["capability_axes"]
    det = [k for k, v in ax.items() if v["value"] not in ("UNDETERMINED", "UNDEFINED")]
    # **빈 집합으로 통과하지 않는다.** 확정값이 하나도 없으면 "전 축 floor" 는 공허하다.
    check("확정값 축이 실제로 있다", len(det) > 0, f"determinate={len(det)}")
    missing = [k for k in det if "NO_FRESHNESS_BASIS" not in ax[k]["floor_reasons"]]
    check("확정값 축 전부가 NO_FRESHNESS_BASIS 로 내려갔다", not missing, str(missing))
    unfloored = [k for k in det if not ax[k]["floor_reasons"]]
    check("floor 사유가 빈 확정값 축이 없다", not unfloored, str(unfloored))
    shutil.rmtree(w)


def t_a9_ttl_declared_positive_control() -> None:
    print("\n[63] 조치 8 — 양성 대조: 명시로 선언하면 그 축은 내려가지 않는다")
    w = new_work(); ld = sha(w / "versions.lock")
    f = full_fixture(w, ld)
    # [62] 와 **같은 픽스처**다. 달라지는 것은 운영자의 선언 하나뿐이다 — 그래야
    # [62] 의 floor 가 TTL 미선언 때문이지 픽스처가 원래 못 쓸 것이어서가 아님이 선다.
    rc, out, _ = run_norm(w, capability_ttl_days=30, target_owner="APP", target_table="T1", **f)
    rec = json.loads((w / f"{RUN_ID}-out.json").read_text(encoding="utf-8"))
    check("freshness.basis=OPERATOR_DECLARED_TTL",
          rec["freshness"]["basis"] == "OPERATOR_DECLARED_TTL", str(rec["freshness"]))
    check("ttl_seconds 가 선언대로다", rec["freshness"]["ttl_seconds"] == 30 * 86400,
          str(rec["freshness"]["ttl_seconds"]))
    check("미선언 경고가 없다",
          not any("capability-ttl-days 미선언" in x for x in rec["warnings"]),
          str(rec["warnings"])[:200])
    check("note 가 운영자 선언값이라고 적는다",
          "운영자가 선언한 상한" in rec["freshness"]["note"], rec["freshness"]["note"][:120])
    ax = rec["capability_axes"]
    det = [k for k, v in ax.items() if v["value"] not in ("UNDETERMINED", "UNDEFINED")]
    fresh = [k for k in det if not ax[k]["floor_reasons"]]
    check("내려가지 않은 확정값 축이 있다", len(fresh) > 0,
          f"determinate={len(det)} unfloored={len(fresh)}")
    check("어느 축에도 NO_FRESHNESS_BASIS 가 없다",
          not [k for k in det if "NO_FRESHNESS_BASIS" in ax[k]["floor_reasons"]],
          str([k for k in det if "NO_FRESHNESS_BASIS" in ax[k]["floor_reasons"]]))
    shutil.rmtree(w)


def _gate():
    sys.path.insert(0, str(NORM.parent))
    import importlib
    return importlib.import_module("g0_final_gate")


def _forged_5_1() -> dict:
    """9차 판정 §5-1 이 실증한 그 레코드. **전부 쓰레기 문자열이다.**

    이전 판은 이것을 `admitted = True, reasons = []` 로 승인했다. 값을 안 봐서만이
    아니라 `admit()` 이 `item` 이름을 경로로 해석했기 때문이다 — 계약이 표시용으로 적어
    둔 `hash_vector_result (V-01~V-16)` 이라는 이름의 키 하나면 그 항목이 충족됐다.
    """
    return {
        "record_type": "g0_evidence", "gate_eligible": True,
        "g0_report_id": "쓰레기", "executed_at": "언젠가",
        "versions_lock_digest": "digest아님",
        "oracle_env": {"nls_characterset": "아무거나",
                       "nls_nchar_characterset": "아무거나",
                       "max_string_size": "아무거나"},
        "hash_vector_result (V-01~V-16)": "FORGED", "ddl_digest": "FORGED",
        "verdict_sql_digest": "FORGED", "canonical_hash_spec_digest": "FORGED",
        "submission_path_result": "FORGED", "source_kind": "FORGED",
        "same_lock (G0-5)": "FORGED",
    }


def t_a9_forged_record_rejected() -> None:
    print("\n[64] 조치 9 — §5-1 의 위조 레코드는 무조건 거부된다 (§8 재리뷰 기준 12)")
    gate = _gate()
    ok, reasons = gate.admit(_forged_5_1())
    check("승인하지 않는다", ok is False, str(reasons)[:160])
    check("게이트가 닫혀 있다고 말한다",
          any("GATE_OPEN=False" in r for r in reasons), str(reasons)[:200])
    # 문이 닫힌 것 하나에 기대지 않는다 — `where` 검사도 같은 레코드를 독립으로 잡아야 한다.
    check("where 로도 잡는다 — executed_at",
          any("executed_at@children[*].measured_at" in r for r in reasons),
          str(reasons)[:300])
    check("where 로도 잡는다 — nls_characterset",
          any("oracle_env.nls_characterset@source.characterset" in r for r in reasons),
          str(reasons)[:300])
    # 표시 이름을 키로 박아 넣어도 그 항목은 충족되지 않는다(P2-4).
    check("표시 이름 키로는 충족되지 않는다",
          any("hash_vector_result" in r and "위치가 정해지지 않은" in r for r in reasons),
          str(reasons)[:300])
    shutil.rmtree(new_work())


def t_a9_where_is_the_only_path_authority() -> None:
    print("\n[65] 조치 9 — 경로 권위는 `where` 하나다 (§5-1)")
    gate = _gate()
    # **양성 대조.** `where` 가 가리키는 자리에 실제로 값을 두면 그 항목은 누락에서 빠진다.
    # 이것이 서지 않으면 [64] 의 '누락' 은 무엇을 넣어도 실패하는 공허한 검사다.
    good = {"record_type": "g0_evidence", "gate_eligible": True,
            "g0_report_id": "G0-0-20260831T000000Z",
            "versions_lock_digest": "a" * 64,
            "children": {"g0_0a": {"measured_at": "2026-08-31T00:00:00+00:00"}},
            "source": {"characterset": "AL32UTF8"}}
    ok, reasons = gate.admit(good)
    missing = [r for r in reasons if r.startswith("최종 계약 항목 누락")]
    check("where 를 채우면 누락 사유가 사라진다", not missing, str(missing)[:300])
    # 같은 값을 **item 이름 자리**에 두면 충족되지 않는다 — 이전 판이 통과시킨 형태다.
    by_name = {"record_type": "g0_evidence", "gate_eligible": True,
               "g0_report_id": "G0-0-20260831T000000Z",
               "versions_lock_digest": "a" * 64,
               "executed_at": "2026-08-31T00:00:00+00:00",
               "oracle_env": {"nls_characterset": "AL32UTF8"}}
    ok2, reasons2 = gate.admit(by_name)
    m2 = [r for r in reasons2 if r.startswith("최종 계약 항목 누락")]
    check("item 이름 자리에 두면 충족되지 않는다", len(m2) == 1, str(reasons2)[:300])
    check("둘 다 어차피 거부다", ok is False and ok2 is False, f"{ok} {ok2}")
    shutil.rmtree(new_work())


def t_a9_gate_closed_admits_nothing() -> None:
    print("\n[66] 조치 9 — 문이 닫혀 있는 동안에는 무엇도 승인되지 않는다")
    gate = _gate()
    check("GATE_OPEN 이 False 다", gate.GATE_OPEN is False, str(gate.GATE_OPEN))
    # 계약의 COVERED 항목을 where 기준으로 전부 채운 '가장 좋은' 레코드도 거부다.
    best = {"record_type": "g0_evidence", "gate_eligible": True,
            "g0_report_id": "G0-0-20260831T000000Z",
            "versions_lock_digest": "a" * 64,
            "children": {"g0_0a": {"measured_at": "2026-08-31T00:00:00+00:00"}},
            "source": {"characterset": "AL32UTF8"}}
    ok, reasons = gate.admit(best)
    check("그래도 거부한다", ok is False, str(reasons)[:200])
    # **문을 억지로 열어도** 위치 미정 항목이 남아 여전히 거부다 — 방벽이 하나가 아니다.
    gate.GATE_OPEN = True
    try:
        ok2, reasons2 = gate.admit(best)
        check("문을 열어도 위치 미정 때문에 거부", ok2 is False, str(reasons2)[:200])
        check("사유가 위치 미정이다",
              any("위치가 정해지지 않은" in r for r in reasons2), str(reasons2)[:200])
        # 위조 레코드는 문이 열려도 거부다.
        ok3, _ = gate.admit(_forged_5_1())
        check("문을 열어도 위조는 거부", ok3 is False)
    finally:
        gate.GATE_OPEN = False
    check("시험이 문을 되돌려 놓았다", gate.GATE_OPEN is False)
    shutil.rmtree(new_work())


def main() -> int:
    print("=" * 70)
    print("g0-normalize.py 반례 회귀 시험 — 7차 §5.1 + 8차 M1·M3 + 9차 조치 3·4·5")
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
              t_m3_out_path_and_pointer, t_m3_out_path_rules,
              # 9차 조치 3
              t_a9_probe_manifest_matches_sql, t_a9_missing_probe,
              t_a9_unknown_probe, t_a9_lying_summary,
              t_a9_summary_count_mismatch, t_a9_positive_control,
              # 9차 조치 4
              t_a9_source_identity_mismatch, t_a9_declared_source_mismatch,
              t_a9_no_server_identity_floors, t_a9_profile_relabel,
              t_a9_env_kind_required, t_a9_profile_positive_control,
              # 9차 조치 5
              t_a9_harness_manifest_complete, t_a9_harness_digest_changes,
              t_a9_undeclared_file_fails,
              # 9차 조치 7
              t_a9_scope_allows_two_environments, t_a9_scope_recorded_in_record,
              t_a9_ce_same_source_rejected, t_a9_ce_identity_bound_to_own_server,
              t_a9_scope_is_derived_not_declared, t_a9_ce_raises_no_axis,
              # 9차 조치 8
              t_a9_ttl_undeclared_by_default, t_a9_ttl_declared_positive_control,
              # 9차 조치 9
              t_a9_forged_record_rejected, t_a9_where_is_the_only_path_authority,
              t_a9_gate_closed_admits_nothing):
        t()
    print("\n" + "=" * 70)
    print(f"통과 {PASS}건 · 실패 {len(FAIL)}건")
    for f in FAIL:
        print(f"  - {f}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
