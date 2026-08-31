#!/usr/bin/env python3
"""G0-0B1 판정기 — 추적 파일과 실행 결과를 함께 보고 결론을 낸다.

**판정 규칙은 하나다: 증명되지 않은 것은 미확정이다.**
provider 가 호출되지 않았으면 "덮지 못한다" 가 아니라 "측정 실패" 다. 둘을 구분한다.
"""
import argparse
import json
import pathlib
import sys
from collections import Counter, defaultdict


def load_traces(d):
    out = []
    for p in sorted(pathlib.Path(d).glob("g0-0b1-trace-*.jsonl")):
        for i, ln in enumerate(p.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            ln = ln.strip()
            if not ln:
                continue
            try:
                out.append(json.loads(ln))
            except json.JSONDecodeError as e:
                print(f"[warn] {p.name}:{i} JSON 아님 — {e}", file=sys.stderr)
    return out


def load_results(paths):
    out = []
    for p in paths:
        for ln in pathlib.Path(p).read_text(encoding="utf-8", errors="replace").splitlines():
            if ln.startswith("G0B1_RESULT "):
                try:
                    out.append(json.loads(ln[len("G0B1_RESULT "):]))
                except json.JSONDecodeError:
                    pass
    return out


def load_terminals(paths):
    """driver 가 낸 G0B1_TERMINAL 토큰을 모은다 (8차 M2-5)."""
    out = []
    for p in paths:
        try:
            for line in pathlib.Path(p).read_text(encoding="utf-8", errors="replace").splitlines():
                i = line.find("G0B1_TERMINAL ")
                if i >= 0:
                    try:
                        out.append(json.loads(line[i + len("G0B1_TERMINAL "):]))
                    except json.JSONDecodeError:
                        pass
        except OSError:
            pass
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trace-dir", required=True)
    ap.add_argument("--result-log", nargs="*", default=[],
                    help="spark-submit 표준출력을 저장한 파일들(G0B1_RESULT 라인 포함)")
    ap.add_argument("--out", default="g0-0b1-evidence.json")
    a = ap.parse_args()

    tr = load_traces(a.trace_dir)
    res = load_results(a.result_log)

    # ── M2-5: 추적 완결성 ────────────────────────────────────────────
    # 잘린 추적 파일과 "connection 이 원래 없었다" 는 겉모습이 같다. tracer 가 종료 시
    # 남기는 trace_end sentinel 이 있어야 **파일이 끝까지 쓰였다**고 말할 수 있다.
    ends = [t for t in tr if t.get("event") == "trace_end"]
    trace_complete = bool(ends)

    # ── M2-5: terminal failure token ────────────────────────────────
    # driver 가 자기가 어떻게 끝났는지 선언한 토큰. 판정기가 step 성공 여부로
    # **추론**하지 않고 이 값을 읽는다.
    terminals = load_terminals(a.result_log)

    ev = {"trace_lines": len(tr), "results": res, "findings": [], "verdict": {},
          "trace_complete": trace_complete,
          "trace_end_records": ends,
          "terminal_tokens": terminals}

    if not trace_complete and tr:
        ev["findings"].append({
            "q": "추적 파일이 끝까지 쓰였는가",
            "observed": {"trace_end_records": 0, "trace_lines": len(tr)},
            "answer": "NO",
            "note": ("trace_end sentinel 이 없다 — JVM 이 비정상 종료했거나 파일이 잘렸다. "
                     "**이 추적으로는 '없다' 와 '못 봤다' 를 구분할 수 없다**(8차 M2-5)."),
        })

    if not tr:
        ev["verdict"] = {
            "coverage": "MEASUREMENT_FAILED",
            "reason": ("추적 라인이 0건이다. provider 가 한 번도 호출되지 않았다는 뜻이며, "
                       "이는 '덮지 못한다' 가 아니라 '측정하지 못했다' 이다. 확인할 것: "
                       "(1) jar 가 --jars 로 올라갔는가 (2) META-INF/services 가 jar 안에 있는가 "
                       "(3) spark.sql.sources.disabledJdbcConnProviderList=basic 을 줬는가 "
                       "(4) canHandle 의 URL 접두사가 실제 URL 과 맞는가"),
        }
        pathlib.Path(a.out).write_text(json.dumps(ev, ensure_ascii=False, indent=1), encoding="utf-8")
        print(json.dumps(ev["verdict"], ensure_ascii=False, indent=1))
        return 5   # MEASUREMENT_FAILED 는 NOT_PROVEN(3) 과 다른 것이다. 섞으면 안 된다.

    # ── 회차(run)로 먼저 가른다 ────────────────────────────────────────
    # coverage 와 failclosed 를 합산하면 failclosed 의 **의도된** 실패가 coverage 통계로
    # 새어 들어가, 완벽한 실행도 영원히 NOT_PROVEN 이 된다.
    conns_all = [t for t in tr if t.get("event") == "connection"]
    runs = defaultdict(list)
    for c in conns_all:
        runs[c.get("run") or "unspecified"].append(c)
    ev["runs_seen"] = {k: len(v) for k, v in runs.items()}

    cov = runs.get("coverage") or runs.get("unspecified") or []
    # **failclosed 회차가 여럿이다**(2026-08-27, 조치 5). 경로별 주입을 쓰면
    # failclosed_schema / failclosed_task 처럼 나뉘므로 접두사로 모은다.
    fcl = [c for k, v in runs.items() if k.startswith("failclosed") for c in v]
    if not cov:
        ev["verdict"] = {"coverage": "MEASUREMENT_FAILED",
                         "reason": ("coverage 회차의 추적이 없다. run.sh 가 -Dg0b1.run=coverage 를 "
                                    f"넘겼는지 확인하라. 관측된 회차: {list(runs)}")}
        pathlib.Path(a.out).write_text(json.dumps(ev, ensure_ascii=False, indent=1), encoding="utf-8")
        print(json.dumps(ev["verdict"], ensure_ascii=False, indent=1))
        return 5   # MEASUREMENT_FAILED 는 NOT_PROVEN(3) 과 다른 것이다. 섞으면 안 된다.

    by_path = Counter(c.get("path_guess") for c in cov)
    by_jvm = Counter(c.get("jvm") for c in cov)
    sids = {(c.get("preamble") or {}).get("sid") for c in cov if (c.get("preamble") or {}).get("sid")}
    ok_by_path = defaultdict(lambda: [0, 0])
    for c in cov:
        pth = c.get("path_guess")
        ok_by_path[pth][1] += 1
        if (c.get("preamble") or {}).get("ok"):
            ok_by_path[pth][0] += 1

    ev["connections_coverage"] = len(cov)
    ev["connections_failclosed"] = len(fcl)
    ev["by_path"] = dict(by_path)
    ev["by_jvm"] = dict(by_jvm)
    ev["distinct_server_sids"] = len(sids)
    ev["preamble_ok_by_path"] = {k: f"{v[0]}/{v[1]}" for k, v in ok_by_path.items()}

    # ── 질문 1: 경로 커버리지 ─────────────────────────────────────────
    # ── 8차 M2-3: 경로 커버리지도 declared_phase 로 판정한다 ───────────
    #
    # v1 은 `by_path`(= Counter(path_guess)) 로 "schema·task 경로에서 provider 가 불렸는가"
    # 를 판정했다. **분류기가 곧 판정이었다.** 이제 driver 가 선언한 phase 로 센다 —
    # driver 는 자기가 `.schema` 를 부르는지 `.count()` 를 부르는지 알고 있다.
    #
    # path_guess 는 아래 observed 에 **진단용**으로만 남는다.
    by_phase = Counter(c.get("declared_phase") for c in cov)
    SCHEMA_PHASES = {"schema_only"}
    TASK_PHASES = {"partitioned_count", "second_action"}
    seen_schema_phase = sum(v for k, v in by_phase.items() if k in SCHEMA_PHASES)
    seen_task_phase = sum(v for k, v in by_phase.items() if k in TASK_PHASES)
    undeclared = sum(v for k, v in by_phase.items()
                     if k in (None, "", "UNDECLARED", "BETWEEN_STEPS"))

    missing_paths = [n for n, v in (("SCHEMA", seen_schema_phase),
                                    ("TASK", seen_task_phase)) if not v]
    ev["findings"].append({
        "q": "provider 가 schema 경로와 task 경로 모두에서 호출되는가",
        "observed": {"by_declared_phase": dict(by_phase),
                     "schema_phase_connections": seen_schema_phase,
                     "task_phase_connections": seen_task_phase,
                     "undeclared_or_between": undeclared,
                     # 진단용. 판정에 쓰지 않는다(8차 M2-3).
                     "path_guess_distribution_diagnostic": dict(by_path)},
        "answer": "YES" if not missing_paths else "NO",
        "note": (("선언된 phase 로 셌다 — 분류기를 쓰지 않았다. "
                  "**다만 task phase 의 connection 이 전부 task 경로인 것은 아니다** — "
                  "`.load()` 가 그 안에서 schema 를 다시 해석할 수 있다. "
                  "task 경로의 확정 증거는 failclosed_task 회차다(주입이 그 phase 에만 걸렸고 "
                  "읽은 행이 0). METADATA 는 이 하네스가 유발하지 않으므로 게이트에 넣지 않는다."
                  if not missing_paths else
                  f"선언된 phase 기준으로 관측되지 않은 경로: {missing_paths}")
                 + (f" 선언되지 않은 구간의 connection {undeclared}건은 어느 쪽에도 세지 않았다."
                    if undeclared else "")),
    })

    # ── 질문 2: 프리앰블 적용 (coverage 회차 한정) ──────────────────────
    missing = {k: v for k, v in ok_by_path.items() if v[0] != v[1]}
    ev["findings"].append({
        "q": "coverage 회차의 모든 물리 connection 이 프리앰블을 받았는가",
        "observed": ev["preamble_ok_by_path"],
        "answer": "YES" if not missing else "NO",
        "note": "" if not missing else f"프리앰블이 실패했거나 누락된 경로: {list(missing)}",
    })

    # ── 질문 3: fail-closed. 시험하지 않았으면 **미확정이지 통과가 아니다.** ──
    fc = [r for r in res if r.get("mode") == "failclosed"]
    if not fc:
        ev["findings"].append({
            "q": "프리앰블이 실패하면 job 이 정말 죽는가(fail-closed)",
            "observed": None, "answer": "NOT_TESTED",
            "note": ("failclosed 회차를 돌리지 않았다. **시험하지 않은 것은 통과가 아니다** — "
                     "이 항목이 미확정인 한 coverage 는 PROVEN 이 될 수 없다."),
        })
    else:
        # PARTIAL 도 깨진 것이다 — fail=all 인데 일부 step 이 성공했다면
        # 그 step 이 쓰는 경로가 예외를 삼킨 것이고, 그게 이 실험의 표적이다.
        # ── 8차 M2-3: 판정에서 path_guess 를 뺀다 ───────────────────
        #
        # v1 은 `applied_by_path = Counter(c["path_guess"] …)` 로 "어느 경로에 주입이
        # 닿았는가" 를 세고 그것을 PASS 술어에 넣었다. **분류기가 틀리면 판정이 틀린다**
        # — 그리고 그 판정으로 분류기를 검증할 수도 없다(순환).
        #
        # 이제 경로 귀속은 **실행 구성**에서 온다:
        #   failclosed_schema → scenario=schema_only 회차. action 이 없으므로 그 회차의
        #                       connection 은 전부 schema 경로다. 분류기 불필요.
        #   failclosed_task   → scenario=task_only + fail.phase=partitioned_count.
        #                       driver 가 선언한 phase 에만 주입이 걸린다.
        #
        # path_guess 는 observed 에 진단용으로만 남는다.
        broken = [r for r in fc if r.get("status") in ("FAIL_CLOSED_BROKEN", "FAIL_CLOSED_PARTIAL")]
        ok_steps = sorted({st for r in fc for st in (r.get("ok_steps_under_fail_all") or [])})
        swallowed = [c for c in fcl if (c.get("preamble_error") or c.get("open_error"))]

        REQUIRED_RUNS = {"failclosed_schema": "SCHEMA", "failclosed_task": "TASK"}
        per_run = {}
        for rname, pathname in REQUIRED_RUNS.items():
            conns = runs.get(rname) or []
            applied = [c for c in conns if c.get("injection_applied") is True]
            tok = next((t for t in terminals if t.get("run") == rname), None)
            per_run[rname] = {
                "path_under_test": pathname,
                "connections": len(conns),
                "injection_applied": len(applied),
                # M2-5 — driver 가 선언한 종료 상태. step 성공 여부로 추론하지 않는다.
                "terminal_token_present": tok is not None,
                "terminal_status": (tok or {}).get("status"),
                # M2-5 — 주입 회차에서 원천으로부터 받은 행 수. **0 이어야 한다.**
                "rows_read_total": (tok or {}).get("rows_read_total"),
                # 진단용. 판정에 쓰지 않는다.
                "path_guess_distribution": dict(Counter(c.get("path_guess") for c in conns)),
            }

        def run_proven(r):
            d = per_run[r]
            return (d["injection_applied"] > 0
                    and d["terminal_token_present"]
                    and d["terminal_status"] == "EXPECTED_FAILURE_OBSERVED"
                    and d["rows_read_total"] == 0)

        proven = [r for r in REQUIRED_RUNS if run_proven(r)]
        not_proven = [r for r in REQUIRED_RUNS if r not in proven]

        if broken:
            answer = "NO"
            note = (f"**P0** — 주입했는데 {ok_steps or '일부'} step 이 성공했다. "
                    "그 step 이 쓰는 경로가 connection 예외를 삼킨다.")
        elif not trace_complete:
            answer = "MEASUREMENT_FAILED"
            note = ("추적에 trace_end sentinel 이 없다 — 파일이 끝까지 쓰였다는 증거가 없으므로 "
                    "'주입이 닿지 않았다' 와 '기록이 잘렸다' 를 구분할 수 없다(8차 M2-5).")
        elif not_proven:
            answer = "NOT_PROVEN"
            reasons = []
            for r in not_proven:
                d = per_run[r]
                if d["connections"] == 0:
                    reasons.append(f"{r}: 회차 자체가 없다")
                elif d["injection_applied"] == 0:
                    reasons.append(f"{r}: 주입이 한 건도 닿지 않았다")
                elif not d["terminal_token_present"]:
                    reasons.append(f"{r}: terminal token 이 없다 — driver 가 어떻게 끝났는지 "
                                   f"선언하지 않았다")
                elif d["terminal_status"] != "EXPECTED_FAILURE_OBSERVED":
                    reasons.append(f"{r}: terminal status={d['terminal_status']} "
                                   f"(EXPECTED_FAILURE_OBSERVED 여야 한다)")
                elif d["rows_read_total"]:
                    reasons.append(f"{r}: **주입 회차인데 {d['rows_read_total']} 행을 읽었다** — "
                                   f"fence 밖 읽기다")
            note = ("경로별 fail-closed 가 확정되지 않았다: " + "; ".join(reasons)
                    + ". **시험하지 않은 것은 통과가 아니다.**")
        else:
            answer = "YES"
            note = ("schema·task 두 경로 모두 — 주입이 닿았고(tracer 사실), driver 가 "
                    "EXPECTED_FAILURE_OBSERVED 를 선언했으며, 읽은 행이 0 이다. "
                    "**경로 귀속은 실행 구성(시나리오·선언 phase)에서 왔고 path_guess 를 "
                    "쓰지 않았다**(8차 M2-3).")

        ev["findings"].append({
            "q": "프리앰블이 실패하면 job 이 정말 죽는가(fail-closed)",
            "observed": {"statuses": [r.get("status") for r in fc],
                         "failed_connections_in_failclosed": len(swallowed),
                         "succeeded_steps_under_injection": ok_steps,
                         "failclosed_runs": sorted(k for k in runs if k.startswith("failclosed")),
                         "per_run": per_run,
                         "trace_complete": trace_complete,
                         "proven_runs": proven,
                         "not_proven_runs": not_proven},
            "answer": answer,
            "note": note,
        })

    # ── 질문 4: 세션 수(참고값, 게이트 아님) ────────────────────────────
    ev["findings"].append({
        "q": "한 회차가 여는 물리 connection·서버 세션 수(참고)",
        "observed": {"connections": len(cov), "distinct_sids": len(sids), "jvms": len(by_jvm)},
        "answer": "MEASURED",
        "note": ("서버 SID 는 재사용될 수 있어 connection 수와 다를 수 있다. "
                 "Control 의 동시 세션 예산은 총 개수가 아니라 **동시 피크**로 잡아야 하며, "
                 "local[N] 실행은 피크를 재지 않는다."),
    })

    # **통과로 인정하는 값은 YES 와 MEASURED 뿐이다.** NOT_TESTED·NOT_OBSERVED 는
    # "아직 모른다" 이고, 이 도구의 규칙상 모르는 것은 통과가 아니다.
    PASSING = {"YES", "MEASURED"}
    answers = {f["q"]: f["answer"] for f in ev["findings"]}
    blocking = [q for q, v in answers.items() if v not in PASSING]

    # **성질이 다른 것을 한 verdict 로 내지 않는다**(7차 교차 리뷰 P0-06).
    # 이전 판은 coverage 하나였다. provider 가 닿는가 / 단언이 걸리는가 /
    # 실패하면 죽는가 / 읽기가 한 시점인가는 서로 다른 질문이고, 하나가 참이어도
    # 나머지를 시사하지 않는다.
    q_paths = "provider 가 schema 경로와 task 경로 모두에서 호출되는가"
    q_pre = "coverage 회차의 모든 물리 connection 이 프리앰블을 받았는가"
    q_fc = "프리앰블이 실패하면 job 이 정말 죽는가(fail-closed)"

    def verdict_of(q):
        v = answers.get(q)
        return "PROVEN" if v in PASSING else ("NOT_TESTED" if v in ("NOT_TESTED", "NOT_OBSERVED")
                                              else "NOT_PROVEN")

    ev["verdicts"] = {
        "provider_reachability": verdict_of(q_paths),
        "session_assertion": verdict_of(q_pre),
        "fail_closed": verdict_of(q_fc),
        # 아래 둘은 이 하네스가 **시험하지 않는다.** 없는 것을 미확정으로 두는 것과
        # 시험 자체가 없는 것을 구분한다.
        "read_only_transaction": "NOT_IMPLEMENTED",
        "common_snapshot": "NOT_IMPLEMENTED",
    }
    ev["scope"] = {
        "paths_exercised": ["SCHEMA", "TASK"],
        "paths_not_exercised": ["METADATA"],
        "note": ("이 하네스는 DSv1(spark.read.format('jdbc'))만 쓰고 spark.sql.catalog.* 를 "
                 "설정하지 않으므로 DSv2 METADATA 경로를 **유발하지 않는다**. "
                 "그 경로가 0건인 것은 '덮지 못한다'가 아니라 '측정하지 않았다'다. "
                 "read_only_transaction·common_snapshot 은 Preamble 에 "
                 "SET TRANSACTION READ ONLY 가 없으므로 아예 시험 대상이 아니다 — "
                 "**B1 통과는 snapshot capability 의 증거가 아니다.**"),
    }
    ev["verdict"] = {
        "coverage": "PROVEN" if not blocking else "NOT_PROVEN",
        "blocking": blocking,
        "reason": ("schema·task 경로에서 provider 가 호출되고, 프리앰블이 전부 적용됐으며, "
                   "fail-closed 가 의도대로 동작했다. **이것은 verdicts 세 항에 대한 것이며 "
                   "read_only_transaction·common_snapshot 은 여전히 NOT_IMPLEMENTED 다.**"
                   if not blocking else
                   "아래 질문이 미해결이다. 해결 전에는 세션 단언 위의 모든 보장이 미확정이다."),
    }
    pathlib.Path(a.out).write_text(json.dumps(ev, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"verdict": ev["verdict"], "verdicts": ev["verdicts"],
                      "runs_seen": ev.get("runs_seen"), "by_path": ev["by_path"],
                      "preamble_ok_by_path": ev["preamble_ok_by_path"]},
                     ensure_ascii=False, indent=1))
    return 0 if ev["verdict"]["coverage"] == "PROVEN" else 3


if __name__ == "__main__":
    sys.exit(main())
