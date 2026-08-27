#!/usr/bin/env python3
"""`g0-0b1-connection-provider/analyze-trace.py` 반례 회귀 시험.

7차 교차 리뷰 **P0-06(a)** 의 조치를 검증한다. 판정기가 다음 두 경우에 `PROVEN` 을 내던 것을
막았는지 본다.

1. `MIXED` 추적 한 건을 SCHEMA·TASK **양쪽 관측**으로 세던 것
2. `fail=all` 회차에서 task connection 에 **주입이 닿지도 않았는데** fail-closed 를 `YES` 로 두던 것

두 번째가 특히 중요하다. `fail=all` 은 provider 가 처음 불린 connection 에서 즉시 던지므로,
각 step 이 schema 해석에서 막혀 task connection 을 열지 못할 수 있다. 그 회차에서
"전 step 이 실패했다"는 **task 경로의 fail-closed 를 하나도 말해 주지 않는다.**

    python3 g0-b1-analyzer-tests.py
"""
from __future__ import annotations

import json
import pathlib
import shutil
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent
ANALYZER = ROOT / "g0-0b1-connection-provider" / "analyze-trace.py"

FAIL: list[str] = []
PASS = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL.append(f"{name} — {detail}")
        print(f"  FAIL  {name}  {detail}")


def conn(run: str, path: str, *, preamble_ok=True, preamble_error=None,
         fail_mode=None, injection_target=None):
    r = {"event": "connection", "run": run, "conn_id": f"{run}-{path}-x",
            "path_guess": path, "jvm": "1@t", "thread": "main",
            "url_host": "@//h:1521/s", "open_error": None,
            "preamble": ({"ok": preamble_ok, "db_unique_name": "ETLSTB",
                          "database_role": "PHYSICAL STANDBY", "instance_name": "i",
                          "sid": "42", "server_time": "t", "session_tz": "+00:00",
                          "error": preamble_error} if preamble_ok or preamble_error else None),
            "preamble_error": preamble_error, "elapsed_ms": 1,
            "driver_props_passed": ["user"], "raw_stack": ["x.y"]}
    if fail_mode is not None:
        # tracer 가 남기는 주입 사실. 판정기가 preamble_error 로 추정하지 않게 한다.
        r["fail_mode"] = fail_mode
        r["injection_target"] = bool(injection_target)
        r["injection_applied"] = bool(injection_target) and preamble_error is not None
    return r


def result(mode: str, status: str, ok_steps=None):
    r = {"mode": mode, "spark_version": "4.2.0", "disabled_providers": "basic",
         "num_partitions_requested": 4, "steps": [], "status": status}
    if ok_steps is not None:
        r["ok_steps_under_fail_all"] = ok_steps
    return r


def run_analyzer(traces, results):
    w = pathlib.Path(tempfile.mkdtemp(prefix="g0b1an-"))
    td = w / "trace"; td.mkdir()
    (td / "g0-0b1-trace-t-1@t.jsonl").write_text(
        "\n".join(json.dumps(t, ensure_ascii=False) for t in traces) + "\n", encoding="utf-8")
    log = w / "run.log"
    log.write_text("\n".join("G0B1_RESULT " + json.dumps(r, ensure_ascii=False)
                             for r in results) + "\n", encoding="utf-8")
    out = w / "ev.json"
    r = subprocess.run([sys.executable, str(ANALYZER), "--trace-dir", str(td),
                        "--result-log", str(log), "--out", str(out)],
                       capture_output=True, text=True)
    ev = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    shutil.rmtree(w)
    return r.returncode, ev


# ─────────────────────────────────────────────────────────────────────
def t_mixed_not_double_counted():
    print("\n[1] MIXED 한 건을 SCHEMA·TASK 양쪽으로 세지 않는다 (P0-06)")
    # coverage 회차에 MIXED 만 있다. 이전 판은 seen_schema=1, seen_task=1 로 세어 통과시켰다.
    traces = [conn("coverage", "MIXED")]
    traces += [conn("failclosed", "SCHEMA", preamble_ok=False, preamble_error="forced"),
               conn("failclosed", "TASK", preamble_ok=False, preamble_error="forced")]
    rc, ev = run_analyzer(traces, [result("coverage", "OK"),
                                   result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])])
    f = next(x for x in ev["findings"] if "schema 경로와 task" in x["q"])
    check("경로 커버리지가 NO", f["answer"] == "NO", f["answer"])
    check("SCHEMA 관측이 0", f["observed"]["SCHEMA"] == 0, str(f["observed"]))
    check("TASK 관측이 0", f["observed"]["TASK"] == 0, str(f["observed"]))
    check("MIXED 건수는 따로 남는다", f["observed"]["MIXED"] == 1, str(f["observed"]))
    check("재판정 필요를 명시한다", "재판정" in f["note"], f["note"][:80])
    check("PROVEN 이 아니다", ev["verdict"]["coverage"] == "NOT_PROVEN", ev["verdict"]["coverage"])
    check("exit 3", rc == 3, f"rc={rc}")


def t_failclosed_without_task_injection():
    print("\n[2] fail=all 이 task 에 닿지 않았는데 fail-closed=YES 금지 (P0-06 핵심)")
    # coverage 는 두 경로를 다 덮었고 프리앰블도 전부 적용됐다.
    traces = [conn("coverage", "SCHEMA"), conn("coverage", "TASK"), conn("coverage", "TASK")]
    # failclosed 회차는 SCHEMA 에서 전부 막혀 TASK connection 을 연 적이 없다.
    traces += [conn("failclosed", "SCHEMA", preamble_ok=False, preamble_error="forced"),
               conn("failclosed", "SCHEMA", preamble_ok=False, preamble_error="forced")]
    rc, ev = run_analyzer(traces, [result("coverage", "OK"),
                                   result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])])
    f = next(x for x in ev["findings"] if "fail-closed" in x["q"])
    check("YES 가 아니다", f["answer"] != "YES", f["answer"])
    check("NOT_OBSERVED 다", f["answer"] == "NOT_OBSERVED", f["answer"])
    check("어느 경로에 안 닿았는지 남긴다",
          f["observed"]["injection_missing_paths"] == ["TASK"],
          str(f["observed"].get("injection_missing_paths")))
    check("PROVEN 이 아니다", ev["verdict"]["coverage"] == "NOT_PROVEN", ev["verdict"]["coverage"])
    check("fail_closed verdict 이 NOT_TESTED", ev["verdicts"]["fail_closed"] == "NOT_TESTED",
          str(ev["verdicts"]))
    check("그러나 provider_reachability 는 PROVEN — 성질을 섞지 않는다",
          ev["verdicts"]["provider_reachability"] == "PROVEN", str(ev["verdicts"]))
    check("exit 3", rc == 3, f"rc={rc}")


def t_failclosed_positive():
    print("\n[3] 양성 대조 — 두 경로에 다 닿고 전부 실패하면 PROVEN")
    traces = [conn("coverage", "SCHEMA"), conn("coverage", "TASK"), conn("coverage", "TASK")]
    traces += [conn("failclosed", "SCHEMA", preamble_ok=False, preamble_error="forced"),
               conn("failclosed", "TASK", preamble_ok=False, preamble_error="forced")]
    rc, ev = run_analyzer(traces, [result("coverage", "OK"),
                                   result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])])
    f = next(x for x in ev["findings"] if "fail-closed" in x["q"])
    check("fail-closed=YES", f["answer"] == "YES", f["answer"])
    check("PROVEN", ev["verdict"]["coverage"] == "PROVEN",
          f'{ev["verdict"]["coverage"]} blocking={ev["verdict"].get("blocking")}')
    check("exit 0", rc == 0, f"rc={rc}")


def t_verdicts_separated():
    print("\n[4] verdict 를 성질별로 분리한다 (P0-06)")
    traces = [conn("coverage", "SCHEMA"), conn("coverage", "TASK")]
    traces += [conn("failclosed", "SCHEMA", preamble_ok=False, preamble_error="f"),
               conn("failclosed", "TASK", preamble_ok=False, preamble_error="f")]
    rc, ev = run_analyzer(traces, [result("coverage", "OK"),
                                   result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])])
    v = ev["verdicts"]
    check("다섯 verdict 이 있다", set(v) == {
        "provider_reachability", "session_assertion", "fail_closed",
        "read_only_transaction", "common_snapshot"}, str(set(v)))
    check("read_only_transaction=NOT_IMPLEMENTED", v["read_only_transaction"] == "NOT_IMPLEMENTED")
    check("common_snapshot=NOT_IMPLEMENTED", v["common_snapshot"] == "NOT_IMPLEMENTED")
    check("PROVEN 이어도 snapshot 은 증명되지 않았다고 남는다",
          "snapshot capability 의 증거가 아니다" in ev["scope"]["note"], ev["scope"]["note"][:80])
    check("METADATA 가 미유발 경로로 명시된다",
          ev["scope"]["paths_not_exercised"] == ["METADATA"], str(ev["scope"]))


def t_swallowed_path_still_no():
    print("\n[5] fail=all 인데 일부 step 이 성공하면 NO (기존 동작 유지)")
    traces = [conn("coverage", "SCHEMA"), conn("coverage", "TASK")]
    traces += [conn("failclosed", "SCHEMA", preamble_ok=False, preamble_error="f"),
               conn("failclosed", "TASK", preamble_ok=False, preamble_error="f")]
    rc, ev = run_analyzer(traces, [result("coverage", "OK"),
                                   result("failclosed", "FAIL_CLOSED_PARTIAL", ["schema_only"])])
    f = next(x for x in ev["findings"] if "fail-closed" in x["q"])
    check("fail-closed=NO", f["answer"] == "NO", f["answer"])
    check("P0 로 표시한다", "P0" in f["note"], f["note"][:60])
    check("exit 3", rc == 3, f"rc={rc}")


def t_no_failclosed_run():
    print("\n[6] failclosed 회차 자체가 없으면 NOT_TESTED (통과가 아니다)")
    traces = [conn("coverage", "SCHEMA"), conn("coverage", "TASK")]
    rc, ev = run_analyzer(traces, [result("coverage", "OK")])
    f = next(x for x in ev["findings"] if "fail-closed" in x["q"])
    check("NOT_TESTED", f["answer"] == "NOT_TESTED", f["answer"])
    check("PROVEN 이 아니다", ev["verdict"]["coverage"] == "NOT_PROVEN", ev["verdict"]["coverage"])
    check("exit 3", rc == 3, f"rc={rc}")


def t_measurement_failed():
    print("\n[7] 추적 0건은 MEASUREMENT_FAILED(5) — NOT_PROVEN(3) 과 다르다")
    w = pathlib.Path(tempfile.mkdtemp(prefix="g0b1an-"))
    td = w / "trace"; td.mkdir()
    out = w / "ev.json"
    r = subprocess.run([sys.executable, str(ANALYZER), "--trace-dir", str(td), "--out", str(out)],
                       capture_output=True, text=True)
    ev = json.loads(out.read_text(encoding="utf-8"))
    check("exit 5", r.returncode == 5, f"rc={r.returncode}")
    check("MEASUREMENT_FAILED", ev["verdict"]["coverage"] == "MEASUREMENT_FAILED")
    shutil.rmtree(w)


def t_path_specific_injection():
    """조치 5 — 경로별 회차를 돌리면 task 경로 fail-closed 가 실증된다."""
    print("\n[8] 경로별 주입 회차(failclosed_schema · failclosed_task) — 조치 5")
    traces = [conn("coverage", "SCHEMA"), conn("coverage", "TASK")]
    # schema 만 주입한 회차: schema 는 죽고 task 는 애초에 열리지 않는다
    traces += [conn("failclosed_schema", "SCHEMA", preamble_ok=False, preamble_error="f",
                    fail_mode="schema", injection_target=True)]
    # task 만 주입한 회차: schema 는 **정상 통과**하고 task 에서만 죽는다
    traces += [conn("failclosed_task", "SCHEMA", preamble_ok=True,
                    fail_mode="task", injection_target=False),
               conn("failclosed_task", "TASK", preamble_ok=False, preamble_error="f",
                    fail_mode="task", injection_target=True)]
    rc, ev = run_analyzer(traces, [result("coverage", "OK"),
                                   result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])])
    f = next(x for x in ev["findings"] if "fail-closed" in x["q"])
    check("두 회차가 모두 집계된다",
          f["observed"]["failclosed_runs"] == ["failclosed_schema", "failclosed_task"],
          str(f["observed"].get("failclosed_runs")))
    check("주입 근거가 추정이 아니라 tracer flag 다",
          f["observed"]["injection_evidence"] == "tracer flag",
          f["observed"].get("injection_evidence"))
    check("SCHEMA·TASK 양쪽에 주입이 닿았다",
          f["observed"]["injection_reached_paths"] == ["SCHEMA", "TASK"],
          str(f["observed"].get("injection_reached_paths")))
    check("fail-closed=YES", f["answer"] == "YES", f["answer"])
    check("PROVEN", ev["verdict"]["coverage"] == "PROVEN", ev["verdict"]["coverage"])
    check("exit 0", rc == 0, f"rc={rc}")


def t_injection_target_false_is_not_reach():
    """주입 대상이 아니어서 통과한 connection 을 '닿았다' 로 세지 않는다."""
    print("\n[9] 주입 대상이 아닌 통과를 '주입이 닿았다' 로 세지 않는다")
    traces = [conn("coverage", "SCHEMA"), conn("coverage", "TASK")]
    # task 만 주입했는데 task connection 이 아예 안 열린 회차
    traces += [conn("failclosed_task", "SCHEMA", preamble_ok=True,
                    fail_mode="task", injection_target=False)]
    rc, ev = run_analyzer(traces, [result("coverage", "OK"),
                                   result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])])
    f = next(x for x in ev["findings"] if "fail-closed" in x["q"])
    check("어느 경로에도 주입이 닿지 않았다",
          f["observed"]["injection_reached_paths"] == [],
          str(f["observed"].get("injection_reached_paths")))
    check("NOT_OBSERVED", f["answer"] == "NOT_OBSERVED", f["answer"])
    check("다음에 무엇을 돌릴지 알려 준다", "g0b1.fail=" in f["note"], f["note"][:100])
    check("PROVEN 이 아니다", ev["verdict"]["coverage"] == "NOT_PROVEN")


def main() -> int:
    print("=" * 70)
    print("analyze-trace.py 반례 회귀 시험 — 7차 교차 리뷰 P0-06(a)")
    print("=" * 70)
    for t in (t_mixed_not_double_counted, t_failclosed_without_task_injection,
              t_failclosed_positive, t_verdicts_separated, t_swallowed_path_still_no,
              t_no_failclosed_run, t_measurement_failed, t_path_specific_injection,
              t_injection_target_false_is_not_reach):
        t()
    print("\n" + "=" * 70)
    print(f"통과 {PASS}건 · 실패 {len(FAIL)}건")
    for f in FAIL:
        print(f"  - {f}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
