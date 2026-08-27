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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trace-dir", required=True)
    ap.add_argument("--result-log", nargs="*", default=[],
                    help="spark-submit 표준출력을 저장한 파일들(G0B1_RESULT 라인 포함)")
    ap.add_argument("--out", default="g0-0b1-evidence.json")
    a = ap.parse_args()

    tr = load_traces(a.trace_dir)
    res = load_results(a.result_log)

    ev = {"trace_lines": len(tr), "results": res, "findings": [], "verdict": {}}

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
        return 3

    # ── 회차(run)로 먼저 가른다 ────────────────────────────────────────
    # coverage 와 failclosed 를 합산하면 failclosed 의 **의도된** 실패가 coverage 통계로
    # 새어 들어가, 완벽한 실행도 영원히 NOT_PROVEN 이 된다.
    conns_all = [t for t in tr if t.get("event") == "connection"]
    runs = defaultdict(list)
    for c in conns_all:
        runs[c.get("run") or "unspecified"].append(c)
    ev["runs_seen"] = {k: len(v) for k, v in runs.items()}

    cov = runs.get("coverage") or runs.get("unspecified") or []
    fcl = runs.get("failclosed") or []
    if not cov:
        ev["verdict"] = {"coverage": "MEASUREMENT_FAILED",
                         "reason": ("coverage 회차의 추적이 없다. run.sh 가 -Dg0b1.run=coverage 를 "
                                    f"넘겼는지 확인하라. 관측된 회차: {list(runs)}")}
        pathlib.Path(a.out).write_text(json.dumps(ev, ensure_ascii=False, indent=1), encoding="utf-8")
        print(json.dumps(ev["verdict"], ensure_ascii=False, indent=1))
        return 3

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

    # ── 질문 1: 경로 커버리지. **SCHEMA 만 보고 "세 경로" 라 하지 않는다.** ──
    seen_schema = by_path.get("SCHEMA", 0) + by_path.get("MIXED", 0)
    seen_task = by_path.get("TASK", 0) + by_path.get("MIXED", 0)
    seen_meta = by_path.get("METADATA", 0)
    missing_paths = [n for n, v in (("SCHEMA", seen_schema), ("TASK", seen_task)) if not v]
    ev["findings"].append({
        "q": "provider 가 schema 경로와 task 경로 모두에서 호출되는가",
        "observed": {"SCHEMA": seen_schema, "TASK": seen_task, "METADATA": seen_meta,
                     "UNKNOWN": by_path.get("UNKNOWN", 0)},
        "answer": "YES" if not missing_paths else "NO",
        "note": ("METADATA 경로는 DSv2 카탈로그를 쓰지 않으면 나타나지 않는다 — 0 이라고 해서 "
                 "덮지 못한 것이 아니다. 그래서 게이트에 넣지 않는다. "
                 "path_guess 는 스택 추정이므로 UNKNOWN 이 있으면 raw_stack 을 직접 보라."
                 if not missing_paths else f"관측되지 않은 경로: {missing_paths}"),
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
        broken = [r for r in fc if r.get("status") == "FAIL_CLOSED_BROKEN"]
        swallowed = [c for c in fcl if (c.get("preamble_error") or c.get("open_error"))]
        ev["findings"].append({
            "q": "프리앰블이 실패하면 job 이 정말 죽는가(fail-closed)",
            "observed": {"statuses": [r.get("status") for r in fc],
                         "failed_connections_in_failclosed": len(swallowed)},
            "answer": "NO" if broken else "YES",
            "note": ("**P0** — 프리앰블을 강제 실패시켰는데 읽기가 성공했다. 그 경로는 "
                     "connection 예외를 삼킨다. 어느 경로인지는 failclosed 회차의 "
                     "path_guess 별 실패 건수와 job 성공 여부를 대조해 좁혀라."
                     if broken else "의도대로 실패했다."),
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
    ev["verdict"] = {
        "coverage": "PROVEN" if not blocking else "NOT_PROVEN",
        "blocking": blocking,
        "reason": ("schema·task 경로에서 provider 가 호출되고, 프리앰블이 전부 적용됐으며, "
                   "fail-closed 가 의도대로 동작했다."
                   if not blocking else
                   "아래 질문이 미해결이다. 해결 전에는 세션 단언 위의 모든 보장이 미확정이다."),
    }
    pathlib.Path(a.out).write_text(json.dumps(ev, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"verdict": ev["verdict"], "runs_seen": ev.get("runs_seen"),
                      "by_path": ev["by_path"],
                      "preamble_ok_by_path": ev["preamble_ok_by_path"]}, ensure_ascii=False, indent=1))
    return 0 if ev["verdict"]["coverage"] == "PROVEN" else 3


if __name__ == "__main__":
    sys.exit(main())
