#!/usr/bin/env python3
"""G0-0B1 — 커스텀 JdbcConnectionProvider 가 Spark 의 3경로를 모두 덮는지 실측한다.

이 스크립트는 **판정하지 않는다.** Spark 를 돌려 사실을 만들고, 판정은
`analyze-trace.py` 가 추적 파일과 이 스크립트의 결과를 함께 보고 한다.

모드
  coverage       정상 프리앰블. connection 이 몇 개·어느 경로에서 열리는지 본다.
  failclosed     -Dg0b1.fail=all 과 함께 실행한다(run.sh 가 건다).
                 **job 이 죽어야 정상이다.** 살아남으면 그 경로가 예외를 삼킨 것이고,
                 그건 세션 단언 모델이 그 경로에서 성립하지 않는다는 뜻이다.
  initstatement  provider 없이 sessionInitStatement 만으로 같은 읽기를 한다(대조군).

안전 규칙
  · 읽기 전용. DDL/DML 없음. 잘못된 비밀번호를 시도하지 않는다.
  · 대상 테이블은 --limit 행으로 제한해 읽는다. 전수 스캔하지 않는다.
  · 비밀번호는 argv 가 아니라 환경변수로만 받는다.
"""
import argparse
import json
import os
import sys


def emit(rec):
    print("G0B1_RESULT " + json.dumps(rec, ensure_ascii=False))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password-env", default="ORA_PW",
                    help="비밀번호가 담긴 환경변수 이름. 비밀번호 자체를 argv 로 넘기지 마라.")
    ap.add_argument("--table", required=True, help="SCHEMA.TABLE")
    ap.add_argument("--num-partitions", type=int, default=4)
    ap.add_argument("--limit", type=int, default=1000, help="읽을 행 수 상한")
    ap.add_argument("--mode", choices=["coverage", "failclosed", "initstatement"], default="coverage")
    ap.add_argument("--trace-dir", default=None,
                    help="추적 디렉터리. step 경계 마커를 여기에 남겨 connection 을 step 에 귀속시킨다.")
    a = ap.parse_args()

    pw = os.environ.get(a.password_env)
    if not pw:
        emit({"mode": a.mode, "status": "ABORT",
              "reason": f"환경변수 {a.password_env} 가 비어 있다"})
        return 2

    try:
        from pyspark.sql import SparkSession
    except ImportError as e:
        emit({"mode": a.mode, "status": "ABORT", "reason": f"pyspark 없음: {e}"})
        return 2

    spark = SparkSession.builder.appName(f"g0-0b1-{a.mode}").getOrCreate()
    conf = spark.sparkContext.getConf()
    rec = {
        "mode": a.mode,
        "spark_version": spark.version,
        "disabled_providers": conf.get("spark.sql.sources.disabledJdbcConnProviderList", None),
        "num_partitions_requested": a.num_partitions,
        "steps": [],
    }

    # partition 컬럼을 원천에 요구하지 않기 위해 파생 컬럼을 만든다.
    # ROWNUM 으로 먼저 잘라 전수 스캔을 막는다.
    # partition 키는 **행 자체의 함수**여야 한다. MOD(ROWNUM, n) 은 파티션 쿼리마다
    # 독립 재실행되어 같은 행이 매번 다른 파티션에 배정될 수 있고, 그러면 파티션들의
    # 합집합이 원래 행 집합과 달라진다. ORA_HASH(ROWID) 는 재실행해도 같다.
    dbtable = (f"(SELECT t.*, MOD(ORA_HASH(ROWIDTOCHAR(t.ROWID)), {a.num_partitions}) AS g0b1_part "
               f"FROM (SELECT * FROM {a.table} WHERE ROWNUM <= {a.limit}) t) x")

    opts = {"url": a.url, "user": a.user, "password": pw, "dbtable": dbtable}
    if a.mode == "initstatement":
        # 대조군: provider 없이 sessionInitStatement 만. schema 경로가 이것을 실행하지 않는다는
        # 사실(NEW-04)을 tracer 없이 다시 보이기 위한 것이다.
        opts["sessionInitStatement"] = "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '. '"

    def marker(name, phase):
        """step 경계를 추적 파일에 남긴다. 이게 없으면 어떤 connection 이 어느 step 것인지
        귀속할 수 없다(provider 는 step 을 모른다)."""
        if not a.trace_dir:
            return
        import pathlib as _p, time as _t
        f = _p.Path(a.trace_dir) / f"g0-0b1-marker-{a.mode}.jsonl"
        try:
            f.parent.mkdir(parents=True, exist_ok=True)
            with f.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps({"event": "step", "run": a.mode, "step": name,
                                     "phase": phase, "mono_ns": _t.monotonic_ns()},
                                    ensure_ascii=False) + "\n")
        except OSError:
            pass

    # 자격증명 오류는 한 회차 안에서도 여러 step 이 각각 로그온을 시도해 누적된다.
    # 한 번 관측되면 남은 step 을 돌리지 않는다(계정 잠금 방지).
    FATAL = ("ORA-01017", "ORA-28000", "ORA-01005", "ORA-28001", "invalid username")
    state = {"abort": None}

    def step(name, fn):
        if state["abort"]:
            rec["steps"].append({"step": name, "ok": False, "error": "SKIPPED: " + state["abort"]})
            return None
        marker(name, "begin")
        try:
            v = fn()
            rec["steps"].append({"step": name, "ok": True, "value": v})
            return v
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if any(k in msg for k in FATAL):
                state["abort"] = "자격증명 오류 관측 — 남은 step 을 실행하지 않는다(계정 잠금 방지)"
            rec["steps"].append({"step": name, "ok": False,
                                 "error": f"{type(e).__name__}: {msg[:400]}"})
            return None
        finally:
            marker(name, "end")

    reader = spark.read.format("jdbc").options(**opts)

    # ① schema 해석만 유발한다(action 없음). 여기서 열리는 connection 이 schema 경로다.
    step("schema_only", lambda: [f.name for f in reader.load().schema.fields][:8])

    # ② partition 병렬 읽기. numPartitions 만큼 task connection 이 열려야 한다.
    def partitioned_count():
        df = (spark.read.format("jdbc").options(**opts)
              .option("partitionColumn", "g0b1_part")
              .option("lowerBound", "0")
              .option("upperBound", str(a.num_partitions))
              .option("numPartitions", str(a.num_partitions))
              .load())
        return df.count()
    step("partitioned_count", partitioned_count)

    # ③ 같은 DataFrame 에 두 번째 action — connection 이 재사용되는지 새로 열리는지(NEW-05/18)
    def second_action():
        df = (spark.read.format("jdbc").options(**opts)
              .option("partitionColumn", "g0b1_part")
              .option("lowerBound", "0")
              .option("upperBound", str(a.num_partitions))
              .option("numPartitions", str(a.num_partitions))
              .load())
        # **cache 를 걸지 않는다.** cache 를 걸면 두 번째 action 이 원천에 가지 않아
        # "action 마다 새 connection 이 열리는가"(NEW-05/18)를 관측할 수 없다.
        n1 = df.count()
        n2 = df.filter("g0b1_part = 0").count()
        return [n1, n2]
    step("second_action", second_action)

    ok = all(s["ok"] for s in rec["steps"])
    rec["all_steps_ok"] = ok
    if state["abort"]:
        rec["status"] = "ABORT_CREDENTIALS"
        rec["note"] = state["abort"]
        emit(rec); spark.stop(); return 2
    if a.mode == "failclosed":
        # 이 모드에서는 **실패가 정상**이다.
        # 전부 실패 / 일부 성공 / 전부 성공을 구분한다. 일부 성공이면 **그 step 의 경로가
        # 예외를 삼킨 것**이고, 그게 이 실험을 넣은 이유다.
        n_ok = sum(1 for s in rec["steps"] if s["ok"])
        if n_ok == len(rec["steps"]):
            rec["status"] = "FAIL_CLOSED_BROKEN"
        elif n_ok > 0:
            rec["status"] = "FAIL_CLOSED_PARTIAL"
        else:
            rec["status"] = "EXPECTED_FAILURE_OBSERVED"
        rec["ok_steps_under_fail_all"] = [s["step"] for s in rec["steps"] if s["ok"]]
        rec["note"] = ("프리앰블을 강제로 실패시켰는데 읽기가 성공했다면, 그 경로는 "
                       "connection 예외를 삼킨 것이다 — 세션 단언이 그 경로에서 성립하지 않는다."
                       if ok else "의도한 대로 실패했다. 각 step 의 error 를 경로별로 대조하라.")
    else:
        rec["status"] = "OK" if ok else "ERROR"

    emit(rec)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
