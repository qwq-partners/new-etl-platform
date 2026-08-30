#!/usr/bin/env python3
# =====================================================================
# G0-0B0 — stock Spark smoke probe (JDBC 경로 관측)
# =====================================================================
# **이 파일은 provider tracer가 아니다.** stock Spark의 기본 경로에서 관측 가능한
# 사실만 모으는 smoke probe이며, 커스텀 `JdbcConnectionProvider`가 schema·metadata·
# task 3경로를 실제로 덮는지는 **G0-0B1(별도 구현)**이 증명한다.
#
# 목적: Codex v2.0 교차 리뷰가 P0로 지목한 Spark 경로 주장을 **실측**한다.
#   NEW-03  sessionInitStatement는 단일 statement로 실행된다(세미콜론 나열 불가)
#   NEW-04  schema/metadata connection이 sessionInitStatement를 우회한다
#   NEW-05  Job 단위 TRANSACTION_SNAPSHOT은 성립하지 않는다(action마다 새 connection)
#   NEW-10  queryTimeout이 init에도 적용되는가(blocking preamble의 cancellation hole)
#   NEW-13  task retry가 새 connection/새 snapshot을 만든다
#   NEW-18  stock Spark는 task마다 connection을 열고 닫는다(재사용 pool 없음)
#
# 안전 규칙
#   - 읽기 전용. DDL/DML 없음. 잘못된 비밀번호 시도 없음(계정 잠금 금지).
#   - 대상 테이블은 ROWNUM으로 제한해 읽는다.
#   - 반드시 **운영에서 쓸 pinned Spark·Oracle JDBC 버전**으로 실행하라.
#     버전이 다르면 결과가 규범 근거가 되지 못한다.
#
# 실행:
#   spark-submit --jars ojdbc11.jar g0-0-probe-spark.py \
#       --url "jdbc:oracle:thin:@//host:1521/service" \
#       --user ETL_USER --password-env ORA_PW \
#       --table SCHEMA.TABLE --wm UPDATE_DT
# =====================================================================
import argparse, json, os, sys, time, traceback

RESULTS = []

# **완결 선언.** 이 목록이 없던 동안 집계기는 파싱 가능한 줄이 1개만 있어도 이 산출물을
# MEASURED 로 올렸다(7차 교차 리뷰 P0-02). 몇 개를 낼 예정인지 산출물이 스스로 말해야
# 집계기가 완주 여부를 판정할 수 있다. emit 을 추가하면 여기도 늘려라 — 늘리지 않으면
# 그 step 은 '예정에 없던 출력'이 되고, 빼먹으면 완주 판정이 PARTIAL 로 떨어진다.
EXPECTED_STEPS = [
    "env.versions", "S-1.identity_preflight", "S0.baseline_read", "S1a.semicolon_list", "S1b.single_alter",
    "S1c.plsql_block", "S2.schema_bypass", "S3.per_task_sessions",
    "S3b.action_session_reuse", "S4.init_query_timeout", "S5.max_delay_zero",
]

def emit(probe, ok, note=None, value=None, err=None, extra=None):
    rec = {"probe": probe, "ok": bool(ok)}
    if note is not None:  rec["note"] = note
    if value is not None: rec["value"] = value
    if err is not None:   rec["error"] = str(err)[:400]
    if extra:             rec.update(extra)
    RESULTS.append(rec)
    print("PROBE " + json.dumps(rec, ensure_ascii=False), flush=True)

MAX_PROBE_ROWS = 100_000
# **파티션 하나가 원천 세션 하나다.** 상한이 없으면 인자 하나로 원천 세션 예산을 넘긴다.
MAX_PARTITIONS = 8
MAX_CONCURRENT_SESSIONS = 12   # executor 파티션 + driver. A §11.2 pool_cap 과 별개인 하네스 자체 상한


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password-env", default="ORA_PW", help="비밀번호는 환경변수로만 전달한다")
    ap.add_argument("--table", required=True, help="SCHEMA.TABLE")
    ap.add_argument("--wm", required=True, help="watermark 컬럼")
    ap.add_argument("--partitions", type=int, default=4,
                    help="JDBC 읽기 파티션 수. 1..%d 만 허용한다 — 파티션 하나가 원천 세션 "
                         "하나이므로 이 값이 곧 동시 세션 상한이다(8차 M0-3)" % MAX_PARTITIONS)
    ap.add_argument("--probe-rows", type=int, default=1000,
                    help="S3가 읽는 행 수 상한. 전체 테이블을 읽지 않는다(재검증 결함 4). "
                         "1..%d 범위여야 한다 — production-safe 라벨의 근거다" % MAX_PROBE_ROWS)
    ap.add_argument("--expect-db-unique-name", required=True,
                    help="**필수.** 대상 원천의 DB_UNIQUE_NAME. 관측값이 다르면 대상 테이블을 "
                         "건드리기 전에 중단한다(8차 M0-4). 기본값을 두지 않는 이유는 "
                         "기본값이 곧 '아무 데나 붙어도 된다'이기 때문이다")
    ap.add_argument("--expect-role", default="PHYSICAL STANDBY",
                    help="기대 DATABASE_ROLE. 이 하네스는 standby 를 전제한다")
    ap.add_argument("--skip-slow", action="store_true", help="S4(timeout) 생략")
    a = ap.parse_args()

    # ── 인자 하드 상한 (8차 M0-3) ───────────────────────────────────
    # type=int 만으로는 0·음수·과대값이 그대로 들어간다. 이 스크립트가 '운영계 제한적'인
    # 근거가 ROWNUM 제한과 파티션 수뿐이므로, 그 둘을 코드로 걸지 않으면 라벨이 사실이 아니다.
    if not (1 <= a.probe_rows <= MAX_PROBE_ROWS):
        emit("args.probe_rows", False,
             err=f"--probe-rows 는 1~{MAX_PROBE_ROWS} 여야 한다(받은 값 {a.probe_rows}). "
                 "운영 원천에서도 돌 수 있으므로 상한을 코드로 강제한다.")
        _dump()
        return 2
    if not (1 <= a.partitions <= MAX_PARTITIONS):
        emit("args.partitions", False,
             err=f"--partitions 는 1~{MAX_PARTITIONS} 여야 한다(받은 값 {a.partitions}). "
                 "**파티션 하나가 원천 세션 하나다** — 상한이 없으면 이 인자 하나로 "
                 "원천 세션 예산을 넘길 수 있다(8차 M0-3).")
        _dump()
        return 2

    # 동시 세션 추정치도 함께 건다. executor 파티션 + driver 1.
    est_sessions = a.partitions + 1
    if est_sessions > MAX_CONCURRENT_SESSIONS:
        emit("args.sessions", False,
             err=f"예상 동시 세션 {est_sessions} 가 상한 {MAX_CONCURRENT_SESSIONS} 를 넘는다. "
                 "파티션 수를 줄여라.")
        _dump()
        return 2

    pw = os.environ.get(a.password_env)
    if not pw:
        print(f"환경변수 {a.password_env} 가 비어 있다. 비밀번호를 인자로 넘기지 마라.", file=sys.stderr)
        return 2

    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName("G0-0-probe")
             .config("spark.task.maxFailures", "1")     # retry가 결과를 오염시키지 않게
             .config("spark.speculation", "false")
             .getOrCreate())
    sc = spark.sparkContext
    emit("env.versions", True, value={"spark": sc.version,
                                      "scala_jars": [j for j in (sc.getConf().get("spark.jars", "") or "").split(",") if j]})

    base = {"url": a.url, "user": a.user, "password": pw, "driver": "oracle.jdbc.OracleDriver"}

    def rd(**kw):
        o = dict(base); o.update(kw)
        return spark.read.format("jdbc").options(**o)

    # 대상 테이블을 안전하게 감싼 subquery
    q10 = f"(SELECT * FROM {a.table} WHERE ROWNUM <= 10)"
    # 세션 사실을 되돌려 주는 쿼리 — 실제 data connection의 사실을 관측한다
    qsess = ("(SELECT SYS_CONTEXT('USERENV','SID')||'#'||"
             " TO_CHAR(SYS_CONTEXT('USERENV','SESSIONID')) AS sess,"
             " SYS_CONTEXT('USERENV','SID') AS sid,"
             " SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER') AS cid,"
             " SYS_CONTEXT('USERENV','MODULE') AS mdl,"
             " SYS_CONTEXT('USERENV','DATABASE_ROLE') AS dbrole FROM DUAL)")

    # ── S-1. 신원 preflight — **대상 테이블을 건드리기 전에** (8차 M0-4) ──────
    #
    # v1 은 S0 에서 곧바로 `SELECT * FROM <table> WHERE ROWNUM <= 10` 을 읽었다.
    # 신원 확인은 S1c 의 PL/SQL 블록 안에만 있었는데 그것은 **이미 대상을 읽은 뒤**이고,
    # 게다가 sessionInitStatement 는 NEW-04 대로 task 경로에서만 실행된다.
    # 즉 잘못된 DB 에 붙어도 대상 테이블을 한 번 읽고 나서야 알게 된다.
    #
    # 여기서는 DUAL 만 읽는다 — 대상 스키마·테이블을 전혀 건드리지 않는다.
    q_ident = ("(SELECT SYS_CONTEXT('USERENV','DB_UNIQUE_NAME') AS dbun,"
               " SYS_CONTEXT('USERENV','DATABASE_ROLE') AS dbrole,"
               " SYS_CONTEXT('USERENV','ISDBA') AS isdba,"
               " SYS_CONTEXT('USERENV','CON_NAME') AS con_name FROM DUAL)")
    try:
        ident = rd(dbtable=q_ident).load().collect()[0].asDict()
    except Exception as e:
        emit("S-1.identity_preflight", False, err=e,
             note="신원을 읽지 못했다. **fail-closed** — 대상 테이블을 읽지 않고 끝낸다.")
        _dump()
        return 2

    mism = []
    if str(ident.get("DBUN") or ident.get("dbun") or "") != a.expect_db_unique_name:
        mism.append(f"DB_UNIQUE_NAME 관측 {ident.get('DBUN') or ident.get('dbun')!r} != 기대 {a.expect_db_unique_name!r}")
    if str(ident.get("DBROLE") or ident.get("dbrole") or "") != a.expect_role:
        mism.append(f"DATABASE_ROLE 관측 {ident.get('DBROLE') or ident.get('dbrole')!r} != 기대 {a.expect_role!r}")
    if str(ident.get("ISDBA") or ident.get("isdba") or "").upper() != "FALSE":
        mism.append("ISDBA 가 FALSE 가 아니다 — DBA 세션으로 probe 를 돌리지 않는다")

    if mism:
        emit("S-1.identity_preflight", False, value=ident,
             err="; ".join(mism),
             note="**대상 테이블을 읽지 않았다.** 신원 불일치는 fail-closed 다(8차 M0-4).")
        _dump()
        return 2
    emit("S-1.identity_preflight", True, value=ident,
         note="대상 접촉 전에 DUAL 만으로 신원을 확인했다.")

    # ── S0. 기준선: init 없이 읽힌다 ────────────────────────────────
    try:
        n = rd(dbtable=q10).load().count()
        emit("S0.baseline_read", True, value=n)
    except Exception as e:
        emit("S0.baseline_read", False, err=e)
        print("기준선 읽기가 실패하면 이후 probe는 의미가 없다. 접속 정보를 확인하라.", file=sys.stderr)
        _dump()
        return 1

    # ── S1. sessionInitStatement 페이로드 형태 (NEW-03) ─────────────
    # (a) 세미콜론 나열 — 리뷰 주장대로면 구문 오류여야 한다
    semi = ("ALTER SESSION SET STANDBY_MAX_DATA_DELAY = 300; "
            "ALTER SESSION SET TIME_ZONE = '+00:00'")
    try:
        rd(dbtable=q10, sessionInitStatement=semi).load().count()
        emit("S1a.semicolon_list", True, note="세미콜론 나열이 통과했다 — 리뷰 NEW-03 주장과 다르다")
    except Exception as e:
        emit("S1a.semicolon_list", False, note="예상: 구문 오류(리뷰 NEW-03 확인)", err=e)

    # (b) 단일 ALTER SESSION
    try:
        rd(dbtable=q10, sessionInitStatement="ALTER SESSION SET STANDBY_MAX_DATA_DELAY = 300").load().count()
        emit("S1b.single_alter", True)
    except Exception as e:
        emit("S1b.single_alter", False, err=e)

    # (c) 익명 PL/SQL 블록(운영 preamble 후보) — 여러 단계를 한 payload로
    block = (
        "BEGIN "
        "  EXECUTE IMMEDIATE 'ALTER SESSION SET STANDBY_MAX_DATA_DELAY = 300'; "
        "  EXECUTE IMMEDIATE 'ALTER SESSION SET TIME_ZONE = ''+00:00'''; "
        "  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS = ''.,'''; "
        "  IF SYS_CONTEXT('USERENV','DATABASE_ROLE') <> 'PHYSICAL STANDBY' THEN "
        "     RAISE_APPLICATION_ERROR(-20901, 'not a physical standby'); "
        "  END IF; "
        "  IF SYS_CONTEXT('USERENV','ISDBA') <> 'FALSE' THEN "
        "     RAISE_APPLICATION_ERROR(-20902, 'unexpected dba session'); "
        "  END IF; "
        "  DBMS_SESSION.SET_IDENTIFIER('g0probe/a1'); "
        "END;")
    try:
        rows = rd(dbtable=qsess, sessionInitStatement=block).load().collect()
        emit("S1c.plsql_block", True, value=[r.asDict() for r in rows],
             note="CLIENT_IDENTIFIER가 g0probe/a1이면 preamble이 data connection에 적용된 것")
    except Exception as e:
        emit("S1c.plsql_block", False, err=e)

    # ── S2. schema connection이 preamble을 우회하는가 (NEW-04) ──────
    # 항상 실패하는 init을 건다.
    #  · schema 단계에서 실패 → schema connection도 init을 실행한다(우회 없음)
    #  · schema는 성공하고 count()에서 실패 → schema connection이 init을 우회한다(리뷰 확인)
    fail_init = "BEGIN RAISE_APPLICATION_ERROR(-20999, 'g0-0 probe: init must run here'); END;"
    schema_ok, schema_err = None, None
    try:
        df = rd(dbtable=q10, sessionInitStatement=fail_init).load()
        _ = df.schema           # resolveTable / getQueryOutputSchema 유발
        schema_ok = True
    except Exception as e:
        schema_ok, schema_err = False, e
    if schema_ok:
        try:
            rd(dbtable=q10, sessionInitStatement=fail_init).load().count()
            emit("S2.schema_bypass", False,
                 note="schema도 data도 통과했다 — init이 어디에서도 실행되지 않았을 수 있다. 조사 필요")
        except Exception as e2:
            emit("S2.schema_bypass", True,
                 note="**schema 단계는 통과, data 단계에서 실패 → schema connection이 preamble을 우회한다(NEW-04 확인)**",
                 err=e2)
    else:
        emit("S2.schema_bypass", False,
             note="schema 단계에서 실패 → schema connection도 preamble을 실행한다(NEW-04 반증)", err=schema_err)

    # ── S3. task별 connection/세션 (NEW-05·13·18) ──────────────────
    # **중요**: Spark의 lowerBound/upperBound는 stride 결정용이며 **필터가 아니다**.
    #   partitionColumn만 주면 전체 테이블을 읽는다 → 운영 부하 사고.
    #   따라서 서브쿼리 안에서 ROWNUM으로 **먼저 잘라내고**, 그 위에 합성 파티션 키를 만든다.
    #   세션 식별자로 SID + audit SESSIONID를 쓴다. **이것은 connection UUID가 아니다** —
    #   SESSIONID는 auditing session identifier이며 SID는 재사용된다. 따라서 값이 같아도
    #   connection 재사용의 증거가 되지 못한다(재검증 결함 10). 확정은 G0-0B1 provider가
    #   생성하는 UUID tracer로만 가능하다.
    n_rows = a.probe_rows
    src = (f"(SELECT SYS_CONTEXT('USERENV','SID')||'#'||"
           f"       TO_CHAR(SYS_CONTEXT('USERENV','SESSIONID')) AS sess, "
           f"       MOD(ROWNUM, {a.partitions}) AS pkey "
           f"   FROM {a.table} WHERE ROWNUM <= {n_rows})")
    try:
        multi = (rd(dbtable=src, partitionColumn="pkey", numPartitions=str(a.partitions),
                    lowerBound="0", upperBound=str(a.partitions),
                    sessionInitStatement=block)
                 .load().selectExpr("sess").distinct().collect())
        sess = sorted({r["sess"] for r in multi})
        emit("S3.per_task_sessions", True,
             value={"distinct_sessions": len(sess), "sample": sess[:8], "rows_read": n_rows},
             note="distinct > 1이면 partition마다 별도 세션·별도 snapshot이다(NEW-05·25). "
                  "단 = 1이라도 connection 재사용의 증거는 아니다 — 시각 분리 없이는 판정 불가")
    except Exception as e:
        emit("S3.per_task_sessions", False, err=e,
             note="SESSIONID가 없으면 SID 단독으로 낮춰 재시도하되 재사용 가능성을 기록하라")

    # 같은 DataFrame에 대한 두 action이 세션을 공유하는가
    try:
        df = rd(dbtable=qsess, sessionInitStatement=block).load()
        # S3와 같은 합성 식별자를 쓴다(SID 단독은 재사용 때문에 판별력이 없다).
        s1 = df.collect()[0]["sess"]; s2 = df.collect()[0]["sess"]
        emit("S3b.action_session_reuse", True, value={"sid_action1": s1, "sid_action2": s2},
             note="두 값이 다르면 action마다 새 connection이다 → Job 단위 TRANSACTION_SNAPSHOT 불가(NEW-05). "
                  "같아도 재사용의 증거는 아니다(SID 재사용 가능) — provider 생성 UUID tracer가 필요하다")
    except Exception as e:
        emit("S3b.action_session_reuse", False, err=e)

    # ── S4. queryTimeout이 init에도 적용되는가 (NEW-10) ─────────────
    if not a.skip_slow:
        slow = "BEGIN DBMS_SESSION.SLEEP(30); END;"
        t0 = time.time()
        try:
            rd(dbtable=q10, sessionInitStatement=slow, queryTimeout="5").load().count()
            emit("S4.init_query_timeout", False, value={"elapsed_s": round(time.time()-t0, 1)},
                 note="timeout이 걸리지 않았다 — init은 queryTimeout의 보호를 받지 못한다(NEW-10 위험 확인)")
        except Exception as e:
            el = round(time.time() - t0, 1)
            msg = str(e)
            # **어떤 예외든 timeout 보호 성공으로 기록하면 안 된다**(7차 리뷰 P1-08).
            # DBMS_SESSION.SLEEP 가 없거나(18c 미만) EXECUTE 권한이 없으면
            # ORA-00904/06550/01031 이 **즉시** 난다. 그건 timeout 이 아니라
            # 프리앰블 자체가 실행되지 못한 것이다.
            setup_fail = any(k in msg for k in ("ORA-00904", "ORA-06550", "ORA-01031",
                                                "ORA-00900", "PLS-00201", "DBMS_SESSION"))
            cancel_like = any(k in msg for k in ("ORA-01013", "DPY-4024", "timeout", "Timeout"))
            near = 3.0 <= el <= 12.0          # queryTimeout=5 근처인가
            if setup_fail:
                emit("S4.init_query_timeout", False, value={"elapsed_s": el},
                     note="**측정 실패**: sessionInitStatement 자체가 실행되지 못했다"
                          "(DBMS_SESSION.SLEEP 부재 또는 EXECUTE 권한 없음). "
                          "queryTimeout 이 init 에 적용되는지는 **미확정**이다 — "
                          "sleep 수단을 바꿔 재측정하라.", err=e)
            elif cancel_like and near:
                emit("S4.init_query_timeout", True, value={"elapsed_s": el},
                     note="취소 계열 오류가 queryTimeout 근처 시각에 났다 — "
                          "init 에도 queryTimeout 이 적용된다(NEW-10 보호 확인)", err=e)
            else:
                emit("S4.init_query_timeout", False, value={"elapsed_s": el},
                     note=f"중단됐으나 취소 계열이 아니거나 시각이 queryTimeout 근처가 아니다"
                          f"(elapsed={el}s). 보호 성공으로 세지 않는다.", err=e)

    # ── S5. ORA-03172 양성 대조 (U-1) ──────────────────────────────
    zero = "ALTER SESSION SET STANDBY_MAX_DATA_DELAY = 0"
    try:
        n = rd(dbtable=q10, sessionInitStatement=zero).load().count()
        emit("S5.max_delay_zero", True, value=n,
             note="성공했다 = 그 순간 apply lag 0. fence 미집행의 증거가 아니다. 재실행으로 ORA-03172를 최소 1회 확보하라")
    except Exception as e:
        emit("S5.max_delay_zero", False, note="ORA-03172라면 fence 집행의 **양성 증거**다", err=e)

    _dump(skipped_slow=a.skip_slow)
    spark.stop()

def _dump(skipped_slow=False):
    """완결 sentinel 을 **한 줄짜리 JSON 으로도** 낸다.

    집계기는 줄 단위로 파싱하므로 indent 를 준 블록은 읽지 못한다. 블록은 사람용,
    b0_summary 한 줄은 도구용이다.
    """
    emitted = [r["probe"] for r in RESULTS]
    expected = [x for x in EXPECTED_STEPS
                if not (skipped_slow and x == "S4.init_query_timeout")]
    summary = {"b0_summary": {
        "expected_steps": expected,
        "emitted_steps": emitted,
        "missing_steps": [x for x in expected if x not in emitted],
        "skipped_slow": bool(skipped_slow),
        "note": "missing_steps 가 비어 있어야 완주다. --skip-slow 를 주면 S4 는 예정에서 빠진다.",
    }}
    print("PROBE " + json.dumps(summary, ensure_ascii=False), flush=True)
    out = {"g0_0_spark_probe": RESULTS, **summary}
    print("\n===== JSON EVIDENCE =====")
    print(json.dumps(out, ensure_ascii=False, indent=1))

    # **완주하지 못했으면 0 으로 끝내지 않는다**(8차 M0-2). missing_steps 가 남았는데
    # exit 0 이면 래퍼의 manifest 에 exit_code=0 이 박히고 집계기가 완주로 읽는다.
    if summary["b0_summary"]["missing_steps"]:
        print("[b0] 예정 step 중 누락이 있다 — exit 3", file=sys.stderr)
        return 3
    return 0

if __name__ == "__main__":
    # **`sys.exit(main())` 이다**(8차 M0-2). v1 은 `main()` 만 부르고 반환값을 버려서,
    # main 이 2(인자 거부)·3(미완주)을 돌려줘도 프로세스는 0 으로 끝났다.
    # 그러면 g0-run-child.sh 가 exit_code=0 을 manifest 에 박고 집계기가 통과시킨다.
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        _dump()
        sys.exit(1)
