#!/usr/bin/env bash
# G0-0B1 전체 실행 — coverage 와 failclosed 두 모드를 돌리고 판정까지 낸다.
#
# 비밀번호는 argv 가 아니라 환경변수로 넘긴다.
#   export ORA_PW='...'   (또는 read -rs)
set -uo pipefail
cd "$(dirname "$0")"

: "${SPARK_HOME:?SPARK_HOME 을 설정하라}"
: "${ORA_PW:?ORA_PW 환경변수에 비밀번호를 넣어라 (argv 금지)}"
URL="${1:?사용법: run.sh <jdbc-url> <user> <SCHEMA.TABLE> <expect_dbuname> [role] [max_delay_s]}"
USER="${2:?}"; TABLE="${3:?}"; EXPECT_DB="${4:?}"
ROLE="${5:-PHYSICAL STANDBY}"; DELAY="${6:-300}"
JAR="$PWD/g0-0b1-tracer.jar"
[ -f "$JAR" ] || { echo "먼저 ./build.sh 를 실행하라" >&2; exit 2; }
OJDBC="${OJDBC_JAR:?OJDBC_JAR 에 ojdbc jar 경로를 지정하라}"

TRACE=$(mktemp -d); LOGS=$(mktemp -d)
echo "[run] trace dir: $TRACE"

submit() {  # $1=mode  $2=extra -D
  local mode="$1" extra="$2"
  local opts="-Dg0b1.trace.dir=$TRACE -Dg0b1.expect.dbuname=$EXPECT_DB -Dg0b1.expect.role=$ROLE -Dg0b1.max.delay=$DELAY $extra"
  echo "[run] mode=$mode"
  "$SPARK_HOME"/bin/spark-submit \
    --master 'local[4]' \
    --jars "$JAR,$OJDBC" \
    --driver-class-path "$JAR:$OJDBC" \
    --conf "spark.sql.sources.disabledJdbcConnProviderList=basic" \
    --conf "spark.driver.extraJavaOptions=$opts" \
    --conf "spark.executor.extraJavaOptions=$opts" \
    run-g0-0b1.py --url "$URL" --user "$USER" --password-env ORA_PW \
      --table "$TABLE" --mode "$mode" 2>&1 | tee "$LOGS/$mode.log"
  echo "[run] mode=$mode exit=${PIPESTATUS[0]}"
}

submit coverage ""
submit failclosed "-Dg0b1.fail=all"

echo
python3 analyze-trace.py --trace-dir "$TRACE" --result-log "$LOGS"/*.log --out g0-0b1-evidence.json
rc=$?
echo "[run] 증거: g0-0b1-evidence.json   추적 원본: $TRACE"
exit $rc
