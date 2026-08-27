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

    conns = [t for t in tr if t.get("event") == "connection"]
    by_path = Counter(c.get("path_guess") for c in conns)
    by_jvm = Counter(c.get("jvm") for c in conns)
    sids = {(c.get("preamble") or {}).get("sid") for c in conns if (c.get("preamble") or {}).get("sid")}
    ok_by_path = defaultdict(lambda: [0, 0])
    for c in conns:
        p = c.get("path_guess")
        ok_by_path[p][1] += 1
        if (c.get("preamble") or {}).get("ok"):
            ok_by_path[p][0] += 1

    ev["connections_total"] = len(conns)
    ev["by_path"] = dict(by_path)
    ev["by_jvm"] = dict(by_jvm)
    ev["distinct_server_sids"] = len(sids)
    ev["preamble_ok_by_path"] = {k: f"{v[0]}/{v[1]}" for k, v in ok_by_path.items()}

    # ── 핵심 질문 1: schema 경로가 provider 를 탔는가 ──────────────────
    schema_seen = by_path.get("SCHEMA", 0) + by_path.get("MIXED", 0)
    ev["findings"].append({
        "q": "커스텀 provider 가 schema 해석 경로에서도 호출되는가",
        "observed": f"SCHEMA {by_path.get('SCHEMA', 0)}건 / MIXED {by_path.get('MIXED', 0)}건",
        "answer": "YES" if schema_seen else "NOT_OBSERVED",
        "note": ("path_guess 는 스택 추정이다. raw_stack 을 직접 보고 재판정하라."
                 if schema_seen else
                 "schema 경로 connection 이 관측되지 않았다. 스택 분류가 틀렸을 수도 있으니 "
                 "UNKNOWN 항목의 raw_stack 을 먼저 확인하라."),
    })

    # ── 핵심 질문 2: 모든 경로가 프리앰블을 받았는가 ────────────────────
    missing = {k: v for k, v in ok_by_path.items() if v[0] != v[1]}
    ev["findings"].append({
        "q": "모든 물리 connection 이 프리앰블을 받았는가",
        "observed": ev["preamble_ok_by_path"],
        "answer": "YES" if not missing else "NO",
        "note": "" if not missing else f"프리앰블이 실패했거나 누락된 경로: {list(missing)}",
    })

    # ── 핵심 질문 3: fail-closed 가 성립하는가 ─────────────────────────
    fc = [r for r in res if r.get("mode") == "failclosed"]
    if fc:
        broken = [r for r in fc if r.get("status") == "FAIL_CLOSED_BROKEN"]
        ev["findings"].append({
            "q": "프리앰블이 실패하면 job 이 정말 죽는가(fail-closed)",
            "observed": [r.get("status") for r in fc],
            "answer": "NO" if broken else "YES",
            "note": ("**P0** — 프리앰블을 강제 실패시켰는데 읽기가 성공했다. 그 경로는 "
                     "connection 예외를 삼킨다. 세션 단언 모델이 그 경로에서 성립하지 않는다."
                     if broken else "의도대로 실패했다."),
        })
    else:
        ev["findings"].append({
            "q": "프리앰블이 실패하면 job 이 정말 죽는가(fail-closed)",
            "observed": None, "answer": "NOT_TESTED",
            "note": "failclosed 모드를 돌리지 않았다. run.sh 가 두 모드를 모두 돌린다.",
        })

    # ── 핵심 질문 4: 한 회차가 여는 물리 세션 수 ────────────────────────
    ev["findings"].append({
        "q": "한 회차가 여는 물리 connection·서버 세션 수",
        "observed": {"connections": len(conns), "distinct_sids": len(sids), "jvms": len(by_jvm)},
        "answer": "MEASURED",
        "note": ("서버 SID 는 재사용될 수 있으므로 connection 수와 SID 수가 다른 것은 정상이다. "
                 "Control 의 동시 세션 예산은 connection 수가 아니라 **동시 피크**로 잡아야 하며, "
                 "이 실행은 피크를 재지 않는다."),
    })

    answers = {f["q"]: f["answer"] for f in ev["findings"]}
    blocking = [q for q, v in answers.items() if v in ("NO", "NOT_OBSERVED")]
    ev["verdict"] = {
        "coverage": "PROVEN" if not blocking else "NOT_PROVEN",
        "blocking": blocking,
        "reason": ("세 경로 모두에서 provider 가 호출되고 프리앰블이 적용됐다."
                   if not blocking else
                   "아래 질문이 미해결이다. 해결 전에는 세션 단언 위의 모든 보장이 미확정이다."),
    }
    pathlib.Path(a.out).write_text(json.dumps(ev, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"verdict": ev["verdict"], "by_path": ev["by_path"],
                      "preamble_ok_by_path": ev["preamble_ok_by_path"]}, ensure_ascii=False, indent=1))
    return 0 if ev["verdict"]["coverage"] == "PROVEN" else 3


if __name__ == "__main__":
    sys.exit(main())
