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
import atexit
import json
import os
import re
import sys


# 대상 테이블을 크게 읽지 않는다는 표시를 유지하려면 상한이 코드에 있어야 한다(P2).
MAX_LIMIT = 100_000
# **파티션 하나가 원천 세션 하나다**(8차 M0-3). b0 와 같은 상한을 건다.
MAX_PARTITIONS = 8
# 동시 세션 상한은 **여기에 두지 않는다**. 이전 값 12 는 partitions ≤ 8 이라 도달 불가능한
# 죽은 분기였고, 그것을 `MAX_PARTITIONS + 1` 로 바꾸는 것도 고침이 아니다 — 상한이 파티션
# 상한에서 산술적으로 유도되면 파티션 검사를 통과한 입력에서 세션 검사는 **항상** 참이다.
# 살아 있는 검사는 두 값이 서로 독립인 데이터일 때만 가능하므로 `g0-source-envelope.json`
# 이 둘을 따로 선언하고 `g0_source_envelope.check_request()` 가 관계까지 본다(9차 P0-04).


def emit(rec):
    print("G0B1_RESULT " + json.dumps(rec, ensure_ascii=False))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password-env", default="ORA_PW",
                    help="비밀번호가 담긴 환경변수 이름. 비밀번호 자체를 argv 로 넘기지 마라.")
    ap.add_argument("--table", required=True, help="SCHEMA.TABLE")
    # **기본은 1이다**(9차 P0-04). b0 와 같다.
    ap.add_argument("--num-partitions", type=int, default=1,
                    help="1~%d 만 허용한다 — 파티션 하나가 원천 세션 하나다(8차 M0-3)" % MAX_PARTITIONS)
    ap.add_argument("--limit", type=int, default=1000,
                    help="읽을 행 수 상한. 1~%d 만 허용한다 — production-safe 표시를 "
                         "유지하려면 하드 상한이 있어야 한다." % MAX_LIMIT)
    # ── 9차 조치 1: run 식별자는 **하나뿐이다** ─────────────────────────
    #
    # 9차 리뷰 P0-03. 이 회차를 가리키는 이름이 세 가지로 갈려 있었다.
    #
    #   run.sh   -Dg0b1.run=failclosed_task   (JVM 이 보는 이름)
    #   python   --mode failclosed            (여기가 phase 파일·토큰에 쓰던 이름)
    #   analyzer failclosed_task 를 요구
    #
    # driver 가 쓰는 phase 파일은 `g0-0b1-phase-failclosed.txt`, provider 가 읽는 것은
    # `g0-0b1-phase-failclosed_task.txt` — **이름이 달라 주입이 걸리지 않았다.**
    # terminal token 도 analyzer 가 찾는 이름과 달라 `PROVEN` 이 도달 불가능했다.
    # `coverage` 회차만 run == mode 라 우연히 맞아서 눈에 띄지 않았다.
    #
    # **run 과 mode 는 다른 것이다.**
    #   run   이 회차의 **신원**. 추적·phase 파일·마커·terminal token·analyzer 가 공유한다
    #   mode  이 회차의 **의미**. failclosed 는 "실패가 정상" 이라는 판정 규칙을 켠다
    #
    # 하나의 run 이름이 여러 mode 를 가질 수는 없지만 그 반대는 된다 —
    # failclosed_schema 와 failclosed_task 는 mode 가 둘 다 failclosed 다.
    ap.add_argument("--run", default=None,
                    help="이 회차의 신원(run.sh 의 -Dg0b1.run 과 **같은 값**). 생략하면 --mode 를 "
                         "쓰지만, 그것은 한 mode 에 회차가 하나뿐인 경우에만 맞다. "
                         "run.sh 는 언제나 명시한다(9차 조치 1).")
    ap.add_argument("--mode", choices=["coverage", "failclosed", "initstatement"], default="coverage")
    ap.add_argument("--scenario", default="full",
                    choices=["full", "schema_only", "task_only", "metadata_only"],
                    help="**경로를 시나리오로 격리한다**(8차 M2-4). full 은 세 step 을 한 JVM 에서 "
                         "이어 돌리므로 어느 경로가 무엇을 했는지 분류기에 의존해야 한다. "
                         "*_only 는 그 경로만 유발하므로 **분류기 없이도** 경로를 안다.")
    ap.add_argument("--provider", default="g0b1tracer",
                    help="JDBC connectionProvider 옵션 값(8차 M2-1). 빈 문자열이면 옵션을 주지 "
                         "않는다 — disabledJdbcConnProviderList 전역 비활성화에 의존하는 옛 경로다.")
    ap.add_argument("--trace-dir", default=None,
                    help="추적 디렉터리. step 경계 마커를 여기에 남겨 connection 을 step 에 귀속시킨다.")
    # ── 9차 조치 6: 원천 신원과 회차 신원 ───────────────────────────────
    # 봉투를 찾으려면 **어느 원천인가**를 알아야 하고, lease 를 적으려면 **어느 회차인가**를
    # 알아야 한다. 둘 다 g0-run-child.sh 가 환경으로 넘긴다 — 호출부마다 인자를 붙이면
    # manifest 의 값과 갈릴 수 있고, 그것이 P0-03 이 만들어진 방식이다.
    ap.add_argument("--source-id", default=os.environ.get("G0_SOURCE_ID", ""),
                    help="대상 원천의 DB_UNIQUE_NAME. 기본값은 $G0_SOURCE_ID 다. "
                         "이 이름으로 g0-source-envelope.json 에서 봉투를 찾는다.")
    ap.add_argument("--run-id", default=os.environ.get("G0_RUN_ID", ""),
                    help="이 회차의 신원(래퍼의 RUN_ID). 기본값은 $G0_RUN_ID 다. "
                         "`--run` 과 다른 것이다 — `--run` 은 이 JVM 실행 하나를 가리키고 "
                         "`--run-id` 는 그것을 담은 G0-0 회차를 가리킨다.")
    a = ap.parse_args()

    # run 을 주지 않으면 mode 로 떨어진다. **한 mode 에 회차가 하나뿐일 때만 맞다.**
    if not a.run:
        a.run = a.mode
    # provider 의 `Trace.run()` 이 같은 정규화를 한다. 여기서 안 맞추면 특수문자가 든
    # run 이름에서 파일 이름이 다시 갈린다 — P0-03 과 같은 종류의 결함이다.
    a.run = re.sub(r"[^A-Za-z0-9_.-]", "_", a.run)

    # **상한을 강제한다.** type=int 만으로는 0·음수·과대값이 그대로 들어간다(7차 리뷰 P2).
    # 이 하네스가 '운영계 제한적'인 근거가 ROWNUM 제한이므로 그 제한을 실제로 건다.
    if not (1 <= a.limit <= MAX_LIMIT):
        emit({"mode": a.mode, "status": "ABORT",
              "reason": f"--limit 은 1~{MAX_LIMIT} 여야 한다(받은 값 {a.limit}). "
                        "이 하네스는 운영 원천에서도 돌 수 있으므로 상한을 코드로 강제한다."})
        return 2
    if not (1 <= a.num_partitions <= MAX_PARTITIONS):
        emit({"mode": a.mode, "status": "ABORT",
              "reason": f"--num-partitions 는 1~{MAX_PARTITIONS} 여야 한다(받은 값 {a.num_partitions}). "
                        "**파티션 하나가 원천 세션 하나다** — 상한이 없으면 이 인자 하나로 "
                        "원천 세션 예산을 넘길 수 있다(8차 M0-3)."})
        return 2

    # ── 9차 조치 6: 원천 안전 봉투 + 회차 lease ─────────────────────────
    if not a.source_id:
        emit({"mode": a.mode, "status": "ABORT",
              "reason": "--source-id 가 비어 있고 $G0_SOURCE_ID 도 없다. 어느 원천의 봉투를 "
                        "적용해야 하는지 알 수 없으면 붙지 않는다. g0-run-child.sh 를 통해 "
                        "돌려라(9차 조치 6)."})
        return 2
    if not a.run_id:
        emit({"mode": a.mode, "status": "ABORT",
              "reason": "--run-id 가 비어 있고 $G0_RUN_ID 도 없다. lease 를 쥔 회차를 이름으로 "
                        "부를 수 없으면 동시 회차 진단이 불가능하다."})
        return 2

    # `metadata_only` 는 **아무 connection 도 열지 않는다**(아래 ④ 참조 — 유발하지 못한다는
    # 사실만 기록한다). 그 회차에까지 대상 질의 승인을 요구하면, 승인 없이 돌릴 수 있는
    # 유일한 음성 대조군을 막게 된다. 실제로 대상에 붙는 시나리오에만 승인을 요구한다.
    wants_touch = a.scenario != "metadata_only"
    lease = None
    try:
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        import g0_source_envelope as envl
        viol = envl.check_request(a.source_id, partitions=a.num_partitions,
                                  wants_target_touch=wants_touch)
        if viol:
            emit({"mode": a.mode, "status": "ABORT",
                  "reason": "원천 안전 봉투 위반 — 원천에 붙지 않는다: " + " | ".join(viol)})
            return 2
        lease = envl.acquire(a.source_id, a.run_id)
        rec_envelope = envl.provenance()
        # 어떻게 끝나든 놓는다. SIGKILL 은 놓지 못하지만 `acquire()` 의 pid 확인이 치운다.
        atexit.register(envl.release, lease)
    except ImportError as e:
        emit({"mode": a.mode, "status": "ABORT",
              "reason": f"g0_source_envelope 를 읽지 못했다({e}) — **검증하지 못한 것은 통과가 "
                        f"아니다**. 원천에 붙지 않는다"})
        return 2
    except Exception as e:  # noqa: BLE001  (EnvelopeError 포함)
        emit({"mode": a.mode, "status": "ABORT", "reason": f"{type(e).__name__}: {e}"})
        return 2

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

    spark = SparkSession.builder.appName(f"g0-0b1-{a.run}").getOrCreate()
    conf = spark.sparkContext.getConf()

    # ── 9차 조치 1: run 신원 교차 확인 ─────────────────────────────────
    #
    # **이 검사가 P0-03 을 영구히 막는다.** driver JVM 이 보는 `-Dg0b1.run` 과 이 프로세스가
    # 쓰는 `--run` 이 다르면, provider 가 읽을 phase 파일과 driver 가 쓸 phase 파일의
    # 이름이 갈린다. 그 상태로 회차를 돌리면 주입이 걸리지 않는데도 "주입했다" 고
    # 기록되고, 판정기는 영원히 그 회차를 찾지 못한다.
    #
    # 조용히 어긋나게 두지 않는다 — **여기서 죽는다.**
    jvm_run = None
    try:
        jvm_run = spark.sparkContext._jvm.System.getProperty("g0b1.run")
    except Exception:  # noqa: BLE001
        # JVM 게이트웨이에 못 닿는 경우(로컬 stub 등). 확인하지 못한 것을 통과로 두지
        # 않되, 이 검사 하나 때문에 회차를 못 돌리게 하지도 않는다 — 사실로 남긴다.
        jvm_run = None
    if jvm_run is not None:
        jvm_run_norm = re.sub(r"[^A-Za-z0-9_.-]", "_", jvm_run)
        if jvm_run_norm != a.run:
            emit({"mode": a.mode, "run": a.run, "status": "ABORT",
                  "reason": f"run 신원 불일치 — driver JVM 의 -Dg0b1.run={jvm_run!r} 인데 "
                            f"--run={a.run!r} 다. provider 가 읽을 phase 파일과 driver 가 쓸 "
                            f"phase 파일의 이름이 갈려 주입이 걸리지 않는다(9차 P0-03). "
                            f"run.sh 가 두 값을 같이 넘기는지 확인하라."})
            spark.stop()
            return 2
    rec = {
        "run": a.run,
        "mode": a.mode,
        "jvm_run_property": jvm_run,
        "spark_version": spark.version,
        "scenario": a.scenario,
        "disabled_providers": conf.get("spark.sql.sources.disabledJdbcConnProviderList", None),
        "num_partitions_requested": a.num_partitions,
        # **어느 봉투가 이 회차를 지배했는가**(9차 조치 6). harness_digest 는 번들 봉투만
        # 센다 — $G0_SOURCE_ENVELOPE 로 덮으면 적용된 정책이 그 digest 밖에 있으므로,
        # 실제로 읽은 파일의 경로와 sha256 을 산출물에 남긴다.
        "source_id": a.source_id,
        "run_id": a.run_id,
        "envelope": rec_envelope,
        "steps": [],
        # M2-5 — **업무 SQL 이 원천에 몇 번 갔는가.** 주입 회차에서 이 값이 0 이어야
        # "fence 밖 읽기가 없었다" 를 추정이 아니라 사실로 말할 수 있다.
        "business_sql_attempts": 0,
        "rows_read_total": 0,
    }

    # partition 컬럼을 원천에 요구하지 않기 위해 파생 컬럼을 만든다.
    # ROWNUM 으로 먼저 잘라 전수 스캔을 막는다.
    # partition 키는 **행 자체의 함수**여야 한다. MOD(ROWNUM, n) 은 파티션 쿼리마다
    # 독립 재실행되어 같은 행이 매번 다른 파티션에 배정될 수 있고, 그러면 파티션들의
    # 합집합이 원래 행 집합과 달라진다. ORA_HASH(ROWID) 는 재실행해도 같다.
    dbtable = (f"(SELECT t.*, MOD(ORA_HASH(ROWIDTOCHAR(t.ROWID)), {a.num_partitions}) AS g0b1_part "
               f"FROM (SELECT * FROM {a.table} WHERE ROWNUM <= {a.limit}) t) x")

    opts = {"url": a.url, "user": a.user, "password": pw, "dbtable": dbtable}
    # **explicit connectionProvider 가 기본 경로다**(8차 M2-1). Spark 4.2 가 문서화한
    # JDBC 옵션으로 우리 provider 를 지목한다. `disabledJdbcConnProviderList=basic` 의
    # 전역 비활성화는 같은 JVM 의 다른 JDBC 사용자 동작까지 바꾸므로 진단 fallback 이다.
    if a.provider:
        opts["connectionProvider"] = a.provider
    rec_provider_opt = a.provider or None
    if a.mode == "initstatement":
        # 대조군: provider 없이 sessionInitStatement 만. schema 경로가 이것을 실행하지 않는다는
        # 사실(NEW-04)을 tracer 없이 다시 보이기 위한 것이다.
        # '.,' — A·P·G0-0A·G0-0B0 와 같은 값이어야 대조군이 성립한다(7차 리뷰 P0-06).
        opts["sessionInitStatement"] = "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'"

    def declare_phase(name):
        """**provider 가 읽을 phase 를 선언한다**(8차 M2-3).

        주입 대상을 스택 추정이 아니라 이 값으로 정한다. driver 는 자기가 지금
        `.schema` 를 부르는지 `.count()` 를 부르는지 **알고 있다** — 추정할 필요가 없다.
        provider 는 `Trace.declaredPhase()` 로 이 파일을 읽는다.
        """
        if not a.trace_dir:
            return
        import pathlib as _p
        # **`a.run` 이다. `a.mode` 가 아니다**(9차 조치 1). provider 의
        # `Trace.phaseFile()` 이 `g0-0b1-phase-<-Dg0b1.run>.txt` 를 읽는다.
        f = _p.Path(a.trace_dir) / f"g0-0b1-phase-{a.run}.txt"
        try:
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text(name, encoding="utf-8")
        except OSError:
            pass

    def marker(name, phase):
        """step 경계를 추적 파일에 남긴다. 이게 없으면 어떤 connection 이 어느 step 것인지
        귀속할 수 없다(provider 는 step 을 모른다)."""
        if not a.trace_dir:
            return
        import pathlib as _p, time as _t
        f = _p.Path(a.trace_dir) / f"g0-0b1-marker-{a.run}.jsonl"
        try:
            f.parent.mkdir(parents=True, exist_ok=True)
            with f.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps({"event": "step", "run": a.run, "step": name,
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
        declare_phase(name)          # ← provider 가 이 값으로 주입을 정한다(M2-3)
        marker(name, "begin")
        rec["business_sql_attempts"] += 1
        try:
            v = fn()
            rec["steps"].append({"step": name, "ok": True, "value": v})
            if isinstance(v, int):
                rec["rows_read_total"] += v
            elif isinstance(v, list) and v and all(isinstance(x, int) for x in v):
                rec["rows_read_total"] += sum(v)
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
            declare_phase("BETWEEN_STEPS")

    reader = spark.read.format("jdbc").options(**opts)

    # ── 시나리오별 격리 실행 (8차 M2-4) ───────────────────────────────
    #
    # **왜 나누는가.** full 은 세 step 을 한 JVM 에서 이어 돌린다. 그러면 "이 connection 이
    # schema 것인가 task 것인가" 를 **분류기에 물어야** 하고, 분류기가 틀리면 판정이 틀린다.
    # 시나리오를 나누면 그 회차에 열린 connection 이 어느 경로인지를 **실행 구성으로** 안다.
    #
    #   schema_only    `.schema` 만 부른다. action 이 없으므로 task connection 이 없다.
    #   task_only      schema 는 주입 없이 통과시키고 action 만 돌린다 — 그 회차의 주입
    #                  대상은 phase 선언(M2-3)으로 task step 에 한정된다.
    #   metadata_only  DSv2 카탈로그 경로. 이 하네스는 아직 유발하지 않는다(정직하게 남긴다).
    scen = a.scenario

    if scen in ("full", "schema_only", "task_only"):
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
    if scen in ("full", "task_only"):
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
    if scen == "full":
        step("second_action", second_action)

    if scen == "metadata_only":
        # **유발하지 못한다는 사실을 기록한다.** 빈 회차를 "METADATA 경로가 없다" 로
        # 읽으면 미측정을 측정으로 바꾸는 것이다(7차 P0-01 과 같은 종류의 오류).
        rec["steps"].append({
            "step": "metadata_catalog", "ok": False,
            "error": "NOT_EXERCISED: 이 하네스는 DSv2 카탈로그 경로를 유발하지 않는다. "
                     "METADATA connection 0 건은 '없다' 가 아니라 '재지 않았다' 이다."})

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

    # ── M2-5: terminal failure token ─────────────────────────────────
    #
    # v1 은 판정기가 **step 성공 여부를 보고 "job 이 죽었다" 를 추론**했다. 추론이면
    # 추적이 잘려도, 회차가 아예 안 돌아도 같은 모양이 된다. driver 가 **자기가 어떻게
    # 끝났는지 토큰으로 선언**하고, 판정기는 그 토큰이 있을 때만 fail-closed 를 인정한다.
    terminal = {
        # **analyzer 가 이 이름으로 회차를 찾는다**(9차 조치 1).
        "run": a.run,
        "mode": a.mode,
        "scenario": a.scenario,
        "status": rec["status"],
        "steps_total": len(rec["steps"]),
        "steps_ok": sum(1 for x in rec["steps"] if x["ok"]),
        # **주입 회차에서 이 둘이 핵심이다.** business_sql_attempts 는 driver 가 원천
        # 읽기를 몇 번 시도했는가, rows_read_total 은 실제로 몇 행을 받았는가.
        # 주입이 걸린 회차에서 rows_read_total > 0 이면 **fence 밖에서 읽은 것**이다.
        "business_sql_attempts": rec["business_sql_attempts"],
        "rows_read_total": rec["rows_read_total"],
        "fail_mode": os.environ.get("G0B1_FAIL_ECHO", ""),
    }
    rec["terminal_token"] = terminal
    print("G0B1_TERMINAL " + json.dumps(terminal, ensure_ascii=False))

    emit(rec)
    spark.stop()

    # **주입 회차는 0 으로 끝나지 않는다.** 의도한 실패가 났는데 프로세스가 0 으로
    # 끝나면 래퍼 manifest 에 exit_code=0 이 박히고 집계기가 완주로 읽는다(M0-2 와 같은 종류).
    if rec["status"] in ("FAIL_CLOSED_BROKEN", "FAIL_CLOSED_PARTIAL"):
        return 5          # 주입했는데 살아남았다 — 이 하네스가 찾는 결함이다
    if rec["status"] == "EXPECTED_FAILURE_OBSERVED":
        return 0          # 의도대로 죽었다. 회차 자체는 정상 완료다
    if rec["status"] == "ERROR":
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
