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


def conn(run: str, path: str, *, phase: str = "schema_only",
         preamble_ok=True, preamble_error=None,
         fail_mode=None, injection_target=None):
    """추적 라인 하나.

    `path`(= path_guess)는 **진단 라벨**이고 `phase`(= declared_phase)가 판정 입력이다
    (8차 M2-3). 두 값을 일부러 어긋나게 줘서 판정기가 어느 쪽을 보는지 시험한다.
    """
    r = {"event": "connection", "run": run, "conn_id": f"{run}-{path}-{phase}",
            "path_guess": path, "declared_phase": phase,
            "jvm": "1@t", "thread": "main",
            "url_host": "@//h:1521/s", "open_error": None,
            "preamble": ({"ok": preamble_ok, "db_unique_name": "ETLSTB",
                          "database_role": "PHYSICAL STANDBY", "instance_name": "i",
                          "sid": "42", "server_time": "t", "session_tz": "+00:00",
                          "error": preamble_error} if preamble_ok or preamble_error else None),
            "preamble_error": preamble_error, "elapsed_ms": 1,
            "driver_props_passed": ["user"], "raw_stack": ["x.y"]}
    if fail_mode is not None:
        r["fail_mode"] = fail_mode
        r["injection_target"] = bool(injection_target)
        r["injection_applied"] = bool(injection_target) and preamble_error is not None
    return r


def trace_end(run="t", lines=1):
    """추적 완결 sentinel (8차 M2-5). 없으면 판정기가 MEASUREMENT_FAILED 로 간다."""
    return {"event": "trace_end", "run": run, "jvm": "1@t", "lines_written": lines}


def result(mode: str, status: str, ok_steps=None):
    r = {"mode": mode, "spark_version": "4.2.0", "disabled_providers": None,
         "num_partitions_requested": 4, "steps": [], "status": status}
    if ok_steps is not None:
        r["ok_steps_under_fail_all"] = ok_steps
    return r


def terminal(run: str, status: str, rows_read=0, attempts=1):
    """driver 가 선언하는 종료 토큰 (8차 M2-5)."""
    return {"run": run, "scenario": "x", "status": status,
            "steps_total": attempts, "steps_ok": 0,
            "business_sql_attempts": attempts, "rows_read_total": rows_read}


def fc_runs_ok():
    """두 failclosed 회차가 **정상적으로 확정되는** 최소 구성."""
    return ([conn("failclosed_schema", "SCHEMA", phase="schema_only",
                  preamble_ok=False, preamble_error="forced",
                  fail_mode="all", injection_target=True),
             conn("failclosed_task", "TASK", phase="partitioned_count",
                  preamble_ok=False, preamble_error="forced",
                  fail_mode="phase", injection_target=True)],
            [terminal("failclosed_schema", "EXPECTED_FAILURE_OBSERVED"),
             terminal("failclosed_task", "EXPECTED_FAILURE_OBSERVED")])


def run_analyzer_streams(streams: dict, results, terminals=()):
    """**stream(=파일) 여러 개**를 쓴다 (9차 조치 10 / P1-02).

    `streams` 는 `{파일접미사: [추적 레코드…]}` 다. 파일이 곧 JVM 이므로, driver 와
    executor 를 따로 두어야 "driver 만 정상 종료" 를 재현할 수 있다. 한 파일에 다 넣는
    기존 하네스로는 이 결함이 원리상 드러나지 않는다.
    """
    w = pathlib.Path(tempfile.mkdtemp(prefix="g0b1an-"))
    td = w / "trace"; td.mkdir()
    for suffix, recs in streams.items():
        (td / f"g0-0b1-trace-t-{suffix}.jsonl").write_text(
            "\n".join(json.dumps(t, ensure_ascii=False) for t in recs) + "\n",
            encoding="utf-8")
    log = w / "run.log"
    lines = ["G0B1_RESULT " + json.dumps(r, ensure_ascii=False) for r in results]
    lines += ["G0B1_TERMINAL " + json.dumps(t, ensure_ascii=False) for t in terminals]
    log.write_text("\n".join(lines) + "\n", encoding="utf-8")
    out = w / "ev.json"
    r = subprocess.run([sys.executable, str(ANALYZER), "--trace-dir", str(td),
                        "--result-log", str(log), "--out", str(out)],
                       capture_output=True, text=True)
    ev = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    shutil.rmtree(w)
    return r.returncode, ev


def run_analyzer(traces, results, terminals=()):
    w = pathlib.Path(tempfile.mkdtemp(prefix="g0b1an-"))
    td = w / "trace"; td.mkdir()
    (td / "g0-0b1-trace-t-1@t.jsonl").write_text(
        "\n".join(json.dumps(t, ensure_ascii=False) for t in traces) + "\n", encoding="utf-8")
    log = w / "run.log"
    lines = ["G0B1_RESULT " + json.dumps(r, ensure_ascii=False) for r in results]
    lines += ["G0B1_TERMINAL " + json.dumps(t, ensure_ascii=False) for t in terminals]
    log.write_text("\n".join(lines) + "\n", encoding="utf-8")
    out = w / "ev.json"
    r = subprocess.run([sys.executable, str(ANALYZER), "--trace-dir", str(td),
                        "--result-log", str(log), "--out", str(out)],
                       capture_output=True, text=True)
    ev = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    shutil.rmtree(w)
    return r.returncode, ev


# ─────────────────────────────────────────────────────────────────────
def t_path_guess_not_a_predicate():
    print("\n[1] 판정은 declared_phase 로 한다 — path_guess 를 쓰지 않는다 (8차 M2-3)")
    # **path_guess 는 다 맞게 주고 declared_phase 만 비운다.** 옛 판정기는 통과시켰다.
    tr = [conn("coverage", "SCHEMA", phase="UNDECLARED"),
          conn("coverage", "TASK", phase="UNDECLARED")]
    fcs, terms = fc_runs_ok(); tr += fcs
    tr.append(trace_end())
    rc, ev = run_analyzer(tr, [result("coverage", "OK"),
                               result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])], terms)
    f = next(x for x in ev["findings"] if "schema 경로와 task" in x["q"])
    check("경로 커버리지가 NO — path_guess 가 맞아도 선언이 없으면 못 센다",
          f["answer"] == "NO", str(f["observed"]))
    check("선언되지 않은 건수를 따로 남긴다",
          f["observed"]["undeclared_or_between"] == 2, str(f["observed"]))
    check("path_guess 는 진단으로만 남는다",
          "path_guess_distribution_diagnostic" in f["observed"], str(f["observed"].keys()))
    check("PROVEN 이 아니다", ev["verdict"]["coverage"] == "NOT_PROVEN", ev["verdict"]["coverage"])


def t_declared_phase_drives_coverage():
    print("\n[2] declared_phase 가 맞으면 path_guess 가 틀려도 센다")
    # path_guess 를 일부러 UNKNOWN 으로 준다. 분류기가 갈피를 못 잡아도 판정은 선다.
    tr = [conn("coverage", "UNKNOWN", phase="schema_only"),
          conn("coverage", "UNKNOWN", phase="partitioned_count")]
    fcs, terms = fc_runs_ok(); tr += fcs
    tr.append(trace_end())
    rc, ev = run_analyzer(tr, [result("coverage", "OK"),
                               result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])], terms)
    f = next(x for x in ev["findings"] if "schema 경로와 task" in x["q"])
    check("경로 커버리지가 YES", f["answer"] == "YES", str(f["observed"]))
    check("schema phase 1건", f["observed"]["schema_phase_connections"] == 1, str(f["observed"]))
    check("task phase 1건", f["observed"]["task_phase_connections"] == 1, str(f["observed"]))
    check("task phase 의 한계를 적는다", "다시 해석" in f["note"], f["note"][:120])


def t_failclosed_needs_terminal_token():
    print("\n[3] terminal token 이 없으면 fail-closed 는 확정되지 않는다 (8차 M2-5)")
    tr = [conn("coverage", "SCHEMA", phase="schema_only"),
          conn("coverage", "TASK", phase="partitioned_count")]
    fcs, terms = fc_runs_ok(); tr += fcs
    tr.append(trace_end())
    # 토큰을 주지 않는다.
    rc, ev = run_analyzer(tr, [result("coverage", "OK"),
                               result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])], [])
    f = next(x for x in ev["findings"] if "fail-closed" in x["q"])
    check("NOT_PROVEN", f["answer"] == "NOT_PROVEN", f["answer"])
    check("사유가 terminal token 부재", "terminal token" in f["note"], f["note"][:160])
    check("verdicts.fail_closed 가 PROVEN 이 아니다",
          ev["verdicts"]["fail_closed"] != "PROVEN", ev["verdicts"]["fail_closed"])


def t_rows_read_under_injection_is_fence_escape():
    print("\n[4] 주입 회차에서 행을 읽었으면 fence 밖 읽기다 (8차 M2-5)")
    tr = [conn("coverage", "SCHEMA", phase="schema_only"),
          conn("coverage", "TASK", phase="partitioned_count")]
    fcs, terms = fc_runs_ok(); tr += fcs
    tr.append(trace_end())
    terms = [terminal("failclosed_schema", "EXPECTED_FAILURE_OBSERVED"),
             terminal("failclosed_task", "EXPECTED_FAILURE_OBSERVED", rows_read=17)]
    rc, ev = run_analyzer(tr, [result("coverage", "OK"),
                               result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])], terms)
    f = next(x for x in ev["findings"] if "fail-closed" in x["q"])
    check("NOT_PROVEN", f["answer"] == "NOT_PROVEN", f["answer"])
    check("사유가 fence 밖 읽기", "fence 밖" in f["note"], f["note"][:200])
    check("몇 행인지 적는다", "17" in f["note"], f["note"][:200])


def t_injection_must_actually_apply():
    print("\n[5] 주입이 닿지 않은 회차는 통과가 아니다")
    tr = [conn("coverage", "SCHEMA", phase="schema_only"),
          conn("coverage", "TASK", phase="partitioned_count")]
    # failclosed_task 회차의 connection 에 주입이 닿지 않았다(injection_applied=False).
    tr += [conn("failclosed_schema", "SCHEMA", phase="schema_only",
                preamble_ok=False, preamble_error="forced",
                fail_mode="all", injection_target=True),
           conn("failclosed_task", "TASK", phase="partitioned_count",
                fail_mode="phase", injection_target=False)]
    tr.append(trace_end())
    terms = [terminal("failclosed_schema", "EXPECTED_FAILURE_OBSERVED"),
             terminal("failclosed_task", "EXPECTED_FAILURE_OBSERVED")]
    rc, ev = run_analyzer(tr, [result("coverage", "OK"),
                               result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])], terms)
    f = next(x for x in ev["findings"] if "fail-closed" in x["q"])
    check("NOT_PROVEN", f["answer"] == "NOT_PROVEN", f["answer"])
    check("사유가 주입 미도달", "주입이 한 건도" in f["note"], f["note"][:200])
    check("proven_runs 에 schema 만 있다",
          f["observed"]["proven_runs"] == ["failclosed_schema"], str(f["observed"]["proven_runs"]))


def t_trace_incomplete_is_measurement_failed():
    print("\n[6] trace_end sentinel 이 없으면 측정 실패다 (8차 M2-5 · 9차 조치 10)")
    tr = [conn("coverage", "SCHEMA", phase="schema_only"),
          conn("coverage", "TASK", phase="partitioned_count")]
    fcs, terms = fc_runs_ok(); tr += fcs
    # trace_end 를 넣지 않는다.
    rc, ev = run_analyzer(tr, [result("coverage", "OK"),
                               result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])], terms)
    check("trace_complete=false", ev.get("trace_complete") is False, str(ev.get("trace_complete")))
    # **NOT_PROVEN(3) 이 아니라 MEASUREMENT_FAILED(5) 다**(9차 조치 10). 이전 판은
    # 완결성 finding 만 남기고 판정을 계속해서 NOT_PROVEN·exit 3 으로 끝냈다 —
    # 이 파일 머리말이 스스로 금지한 혼동이다("덮지 못한다"와 "측정하지 못했다").
    check("verdict 가 MEASUREMENT_FAILED",
          ev["verdict"]["coverage"] == "MEASUREMENT_FAILED", str(ev["verdict"])[:160])
    check("exit 5", rc == 5, f"rc={rc}")
    g = next(x for x in ev["findings"] if "끝까지 쓰였는가" in x["q"])
    check("완결성 finding 이 따로 있다", g["answer"] == "NO", g["answer"])
    check("'없다'와 '못 봤다'를 구분할 수 없다고 적는다",
          "구분할 수 없다" in g["note"], g["note"][:160])


def t_failclosed_positive():
    print("\n[7] 양성 대조 — 두 회차가 다 확정되면 PROVEN")
    tr = [conn("coverage", "SCHEMA", phase="schema_only"),
          conn("coverage", "TASK", phase="partitioned_count")]
    fcs, terms = fc_runs_ok(); tr += fcs
    tr.append(trace_end())
    rc, ev = run_analyzer(tr, [result("coverage", "OK"),
                               result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])], terms)
    f = next(x for x in ev["findings"] if "fail-closed" in x["q"])
    check("fail-closed YES", f["answer"] == "YES", f["answer"] + " " + f["note"][:150])
    check("두 회차 다 proven", sorted(f["observed"]["proven_runs"]) ==
          ["failclosed_schema", "failclosed_task"], str(f["observed"]["proven_runs"]))
    check("path_guess 를 쓰지 않았다고 적는다", "path_guess 를" in f["note"], f["note"][:200])
    check("verdicts.fail_closed = PROVEN", ev["verdicts"]["fail_closed"] == "PROVEN",
          ev["verdicts"]["fail_closed"])
    check("exit 0", rc == 0, f"rc={rc}")


def t_verdicts_separated():
    print("\n[8] verdict 를 성질별로 나눈다 (7차 P0-06 회귀)")
    tr = [conn("coverage", "SCHEMA", phase="schema_only"),
          conn("coverage", "TASK", phase="partitioned_count")]
    fcs, terms = fc_runs_ok(); tr += fcs
    tr.append(trace_end())
    rc, ev = run_analyzer(tr, [result("coverage", "OK"),
                               result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])], terms)
    v = ev["verdicts"]
    for k in ("provider_reachability", "session_assertion", "fail_closed",
              "read_only_transaction", "common_snapshot"):
        check(f"{k} 가 있다", k in v)
    check("read_only_transaction 은 NOT_IMPLEMENTED",
          v["read_only_transaction"] == "NOT_IMPLEMENTED", v["read_only_transaction"])
    check("common_snapshot 은 NOT_IMPLEMENTED",
          v["common_snapshot"] == "NOT_IMPLEMENTED", v["common_snapshot"])
    check("PROVEN 이 snapshot 의 증거가 아님을 적는다",
          "NOT_IMPLEMENTED" in ev["verdict"]["reason"], ev["verdict"]["reason"][:120])


def t_no_failclosed_run():
    print("\n[9] failclosed 회차가 아예 없으면 NOT_TESTED (통과가 아니다)")
    tr = [conn("coverage", "SCHEMA", phase="schema_only"),
          conn("coverage", "TASK", phase="partitioned_count"), trace_end()]
    rc, ev = run_analyzer(tr, [result("coverage", "OK")], [])
    f = next(x for x in ev["findings"] if "fail-closed" in x["q"])
    check("NOT_TESTED", f["answer"] == "NOT_TESTED", f["answer"])
    check("시험하지 않은 것은 통과가 아니라고 적는다", "통과가 아니다" in f["note"], f["note"][:120])
    check("PROVEN 이 아니다", ev["verdict"]["coverage"] == "NOT_PROVEN", ev["verdict"]["coverage"])
    check("exit 3", rc == 3, f"rc={rc}")


def t_measurement_failed():
    print("\n[10] 추적 0건은 NOT_PROVEN 이 아니라 MEASUREMENT_FAILED")
    rc, ev = run_analyzer([], [result("coverage", "OK")], [])
    check("MEASUREMENT_FAILED", ev["verdict"]["coverage"] == "MEASUREMENT_FAILED",
          ev["verdict"]["coverage"])
    check("exit 5 — NOT_PROVEN(3) 과 다르다", rc == 5, f"rc={rc}")


def t_swallowed_path_still_no():
    print("\n[11] 주입했는데 step 이 살아남으면 NO (P0)")
    tr = [conn("coverage", "SCHEMA", phase="schema_only"),
          conn("coverage", "TASK", phase="partitioned_count")]
    fcs, terms = fc_runs_ok(); tr += fcs
    tr.append(trace_end())
    rc, ev = run_analyzer(tr, [result("coverage", "OK"),
                               result("failclosed", "FAIL_CLOSED_PARTIAL", ["partitioned_count"])],
                          terms)
    f = next(x for x in ev["findings"] if "fail-closed" in x["q"])
    check("NO", f["answer"] == "NO", f["answer"])
    check("P0 로 표시한다", "P0" in f["note"], f["note"][:120])
    check("살아남은 step 을 적는다", "partitioned_count" in str(f["observed"]), str(f["observed"])[:200])


def _two_stream_run(executor_end: bool, exec_lines_written=None):
    """driver 와 executor 를 **다른 파일**에 둔 한 회차.

    driver 는 언제나 정상 종료한다 — 실제로도 거의 항상 그렇다. 그래서 executor 쪽만
    잘렸을 때 이전 판이 완결이라고 답했다.
    """
    drv = [conn("coverage", "SCHEMA", phase="schema_only")]
    fcs, terms = fc_runs_ok()
    drv += fcs
    drv.append(trace_end(lines=len(drv)))
    ex = [conn("coverage", "TASK", phase="partitioned_count")]
    if executor_end:
        ex.append(trace_end(lines=len(ex) if exec_lines_written is None
                            else exec_lines_written))
    return {"1@driver": drv, "2@exec": ex}, terms


def t_a9_one_stream_missing_sentinel():
    print("\n[12] 조치 10 — executor stream 하나만 sentinel 이 없어도 전체가 측정 실패다")
    streams, terms = _two_stream_run(executor_end=False)
    rc, ev = run_analyzer_streams(streams, [result("coverage", "OK"),
                                            result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])],
                                  terms)
    check("trace_complete=false", ev.get("trace_complete") is False, str(ev.get("trace_complete")))
    check("verdict 가 MEASUREMENT_FAILED",
          ev["verdict"]["coverage"] == "MEASUREMENT_FAILED", str(ev["verdict"])[:200])
    check("exit 5", rc == 5, f"rc={rc}")
    check("어느 stream 인지 지목한다",
          ev["verdict"]["incomplete_streams"] == ["g0-0b1-trace-t-2@exec.jsonl"],
          str(ev["verdict"].get("incomplete_streams")))
    # **driver 는 멀쩡하다.** 그 사실이 executor 를 대신 말해 주지 않는다는 것이 요지다.
    rows = {r["file"]: r for r in ev["trace_streams"]}
    check("driver stream 은 complete", rows["g0-0b1-trace-t-1@driver.jsonl"]["complete"] is True,
          str(rows["g0-0b1-trace-t-1@driver.jsonl"]))
    check("executor stream 만 incomplete",
          rows["g0-0b1-trace-t-2@exec.jsonl"]["complete"] is False,
          str(rows["g0-0b1-trace-t-2@exec.jsonl"]))


def t_a9_all_streams_complete_positive():
    print("\n[13] 조치 10 — 양성 대조: 두 stream 이 다 온전하면 판정이 진행된다")
    streams, terms = _two_stream_run(executor_end=True)
    rc, ev = run_analyzer_streams(streams, [result("coverage", "OK"),
                                            result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])],
                                  terms)
    # 이게 서지 않으면 [12] 의 실패는 '파일을 나누면 무조건 실패' 라는 뜻일 뿐이다.
    check("trace_complete=true", ev.get("trace_complete") is True, str(ev.get("trace_complete")))
    check("MEASUREMENT_FAILED 가 아니다",
          ev["verdict"]["coverage"] != "MEASUREMENT_FAILED", str(ev["verdict"])[:160])
    check("exit 5 가 아니다", rc != 5, f"rc={rc}")
    check("stream 2개를 다 봤다", len(ev["trace_streams"]) == 2, str(len(ev["trace_streams"])))
    check("둘 다 complete", all(r["complete"] for r in ev["trace_streams"]),
          str(ev["trace_streams"]))


def t_a9_lost_lines_detected():
    print("\n[14] 조치 10 — sentinel 이 있어도 줄이 모자라면 측정 실패다")
    # sentinel 은 붙어 있는데 tracer 가 센 줄 수가 파일보다 많다 = 쓰다 만 줄이 있다.
    # write 실패는 삼켜지므로(`rawLine` 의 catch) 이 구멍은 sentinel 만으로는 안 보인다.
    streams, terms = _two_stream_run(executor_end=True, exec_lines_written=99)
    rc, ev = run_analyzer_streams(streams, [result("coverage", "OK"),
                                            result("failclosed", "EXPECTED_FAILURE_OBSERVED", [])],
                                  terms)
    check("trace_complete=false", ev.get("trace_complete") is False, str(ev.get("trace_complete")))
    check("exit 5", rc == 5, f"rc={rc}")
    rows = {r["file"]: r for r in ev["trace_streams"]}
    ex = rows["g0-0b1-trace-t-2@exec.jsonl"]
    check("sentinel 은 있었다", ex["trace_end_records"] == 1, str(ex))
    check("모자란 줄 수를 센다", ex["lost_lines"] == 99 + 1 - ex["lines"], str(ex))
    check("사유가 줄 부족이다", "모자란다" in (ex["why"] or ""), str(ex["why"]))


def main() -> int:
    print("=" * 70)
    print("analyze-trace.py 반례 회귀 시험 — 7차 교차 리뷰 P0-06(a)")
    print("=" * 70)
    for t in (t_path_guess_not_a_predicate, t_declared_phase_drives_coverage,
              t_failclosed_needs_terminal_token, t_rows_read_under_injection_is_fence_escape,
              t_injection_must_actually_apply, t_trace_incomplete_is_measurement_failed,
              t_failclosed_positive, t_verdicts_separated, t_no_failclosed_run,
              t_measurement_failed, t_swallowed_path_still_no,
              # 9차 조치 10
              t_a9_one_stream_missing_sentinel, t_a9_all_streams_complete_positive,
              t_a9_lost_lines_detected):
        t()
    print("\n" + "=" * 70)
    print(f"통과 {PASS}건 · 실패 {len(FAIL)}건")
    for f in FAIL:
        print(f"  - {f}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
