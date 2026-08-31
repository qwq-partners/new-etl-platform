#!/usr/bin/env bash
# G0-0B1 전체 실행 — coverage 와 failclosed 두 모드를 돌리고 판정까지 낸다.
#
# 비밀번호는 argv 가 아니라 환경변수로 넘긴다.
#   export ORA_PW='...'   (또는 read -rs)
set -uo pipefail
cd "$(dirname "$0")"

: "${SPARK_HOME:?SPARK_HOME 을 설정하라}"
: "${ORA_PW:?ORA_PW 환경변수에 비밀번호를 넣어라 (argv 금지)}"
URL="${1:?사용법: run.sh <jdbc-url> <user> <SCHEMA.TABLE> <expect_dbuname> [role] [max_delay_s|none]}"
# 회차: coverage → failclosed_schema → failclosed_task (세 번 접속한다)
USER="${2:?}"; TABLE="${3:?}"; EXPECT_DB="${4:?}"
# role 은 **공백 없이** 받는다. 공백이 든 값을 extraJavaOptions 문자열에 넣으면
# JVM 이 두 인자로 쪼개 -Dg0b1.expect.role=PHYSICAL 만 걸리고 STANDBY 는 미인식 옵션이 된다.
# Preamble 이 '_' 를 공백으로 되돌린다.
ROLE="${5:-PHYSICAL_STANDBY}"; DELAY="${6:-300}"
# DELAY 에 none|off|- 를 주면 STANDBY_MAX_DATA_DELAY 를 **아예 걸지 않는다**.
# standby 가 아닌 DB(로컬 단일 인스턴스 등)에서는 그 ALTER 가 거부될 수 있고,
# 프리앰블이 거기서 던지면 coverage 회차가 통째로 failclosed 로 변질된다.
# 그 경우 fail-closed 실험과 구분이 되지 않으므로 측정 자체가 무의미해진다.
case "$ROLE" in *" "*) echo "role 에 공백을 쓰지 마라. PHYSICAL_STANDBY 처럼 '_' 로 넘겨라." >&2; exit 2;; esac
JAR="$PWD/g0-0b1-tracer.jar"
[ -f "$JAR" ] || { echo "먼저 ./build.sh 를 실행하라" >&2; exit 2; }
OJDBC="${OJDBC_JAR:?OJDBC_JAR 에 ojdbc jar 경로를 지정하라}"

TRACE=$(mktemp -d); LOGS=$(mktemp -d)
echo "[run] trace dir: $TRACE"

submit() {  # $1=run 라벨  $2=python --mode  $3=extra -D  $4=시나리오  $5=fail.phase
  local run="$1" mode="$2" extra="$3" scen="${4:-full}" failphase="${5:-}"
  # -Dg0b1.run 이 추적 파일명과 각 라인에 회차를 새긴다. 이게 없으면 coverage 와
  # failclosed 의 추적이 한 덩어리로 합산되어 정상 실행도 영원히 NOT_PROVEN 이 된다.
  # **run 과 mode 는 다르다**(2026-08-27, 조치 5). failclosed 를 경로별로 나누면서
  # run 라벨은 failclosed_schema·failclosed_task 로 갈리지만 python 의 --mode 는
  # 둘 다 failclosed 여야 한다(그래야 "실패가 정상" 판정을 한다).
  #
  # **그래서 run 을 python 에도 넘긴다**(9차 조치 1). 이 줄이 없던 것이 P0-03 이다 —
  # python 은 run 을 몰라서 phase 파일·마커·terminal token 을 전부 mode 이름으로 썼고,
  # provider 는 -Dg0b1.run 이름으로 읽었다. 두 이름이 갈리면
  #   · failclosed_task 는 declaredPhase() 가 UNDECLARED 라 **주입이 아예 안 걸린다**
  #   · 두 회차 다 terminal token 이름이 달라 analyzer 가 못 찾는다 → PROVEN 도달 불가
  # coverage 만 run == mode 라 우연히 맞아서 눈에 띄지 않았다.
  # python 쪽에 -Dg0b1.run 과 --run 을 대조하는 fail-closed 검사도 넣었다.
  local delay_opt="-Dg0b1.max.delay=$DELAY"
  case "$DELAY" in none|off|-|"") delay_opt="" ;; esac
  local phase_opt=""
  [ -n "$failphase" ] && phase_opt="-Dg0b1.fail.phase=$failphase"
  local opts="-Dg0b1.run=$run -Dg0b1.trace.dir=$TRACE -Dg0b1.expect.dbuname=$EXPECT_DB -Dg0b1.expect.role=$ROLE $delay_opt $phase_opt $extra"
  echo "[run] run=$run mode=$mode scenario=$scen fail.phase=${failphase:-none}"
  "$SPARK_HOME"/bin/spark-submit \
    --master 'local[4]' \
    --jars "$JAR,$OJDBC" \
    --driver-class-path "$JAR:$OJDBC" \
    ${DISABLE_BASIC:+--conf "spark.sql.sources.disabledJdbcConnProviderList=$DISABLE_BASIC"} \
    --conf "spark.driver.extraJavaOptions=$opts" \
    --conf "spark.executor.extraJavaOptions=$opts" \
    run-g0-0b1.py --url "$URL" --user "$USER" --password-env ORA_PW \
      --table "$TABLE" --run "$run" --mode "$mode" --scenario "$scen" \
      --provider "${PROVIDER_OPT:-g0b1tracer}" \
      --trace-dir "$TRACE" 2>&1 | tee "$LOGS/$run.log"
  local rc=${PIPESTATUS[0]}
  echo "[run] run=$run exit=$rc"
  return $rc
}

# coverage 를 먼저 돌린다. 자격증명·접속 문제로 실패하면 **여기서 멈춘다** —
# 같은 자격증명으로 두 번째 회차를 돌려 로그온 실패를 늘리지 않는다(계정 잠금 방지).
submit coverage coverage "" full || true
if grep -qiE 'ORA-01017|ORA-28000|ORA-01005|invalid username|account is locked' "$LOGS/coverage.log" 2>/dev/null; then
  echo "[abort] 자격증명 오류가 관측됐다. 남은 회차를 실행하지 않는다(계정 잠금 방지)." >&2
  echo "[abort] 로그: $LOGS/coverage.log" >&2
  exit 2
fi

# ── 경로별 독립 시나리오 (8차 M2-3·M2-4) ───────────────────────────────
#
# **주입 대상을 스택 추정이 아니라 두 가지로 정한다.**
#   ① 시나리오가 경로를 격리한다 — schema_only 회차에는 task connection 이 없다
#   ② driver 가 phase 를 선언하고 provider 가 그것만 읽는다(-Dg0b1.fail.phase)
#
# 그래서 `-Dg0b1.fail=schema|task` 같은 **경로 이름 주입은 더 쓰지 않는다.** 그 값은
# 분류기의 결과와 대조되던 것이고, 분류기가 틀리면 주입 대상 자체가 틀렸다.
#
#   failclosed_schema : schema_only 시나리오 + fail=all
#                       → 그 회차의 모든 connection 이 schema 경로다(분류기 불필요)
#   failclosed_task   : task_only 시나리오 + fail.phase=partitioned_count
#                       → schema 는 통과시키고 task step 에서만 주입한다
#
# 두 회차 다 **실패가 정상**이다. 살아남는 step 이 있으면 그 경로가 예외를 삼킨 것이다.
submit failclosed_schema failclosed "-Dg0b1.fail=all"   schema_only || true
submit failclosed_task   failclosed "-Dg0b1.fail=phase" task_only partitioned_count || true

# METADATA 경로는 이 하네스가 유발하지 못한다. **그 사실을 회차로 남긴다** —
# 0 건을 "없다" 로 읽지 않기 위해서다.
submit metadata_probe coverage "" metadata_only || true

echo
# 산출물 경로를 밖에서 받는다(9차 조치 2). M1-4 가 **경로에 RUN_ID 를 요구**하는데
# 여기서 고정 이름으로 쓰면 래퍼가 그 산출물을 거부한다 — runbook 이 실제로 그 상태였다.
OUT="${B1_OUT:-g0-0b1-evidence.json}"
mkdir -p "$(dirname "$OUT")"
python3 analyze-trace.py --trace-dir "$TRACE" --result-log "$LOGS"/*.log --out "$OUT"
rc=$?
echo "[run] 증거: $OUT   추적 원본: $TRACE"
exit $rc
