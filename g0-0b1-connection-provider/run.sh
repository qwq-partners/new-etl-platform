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

submit() {  # $1=mode  $2=extra -D
  local mode="$1" extra="$2"
  # -Dg0b1.run 이 추적 파일명과 각 라인에 회차를 새긴다. 이게 없으면 coverage 와
  # failclosed 의 추적이 한 덩어리로 합산되어 정상 실행도 영원히 NOT_PROVEN 이 된다.
  local delay_opt="-Dg0b1.max.delay=$DELAY"
  case "$DELAY" in none|off|-|"") delay_opt="" ;; esac
  local opts="-Dg0b1.run=$mode -Dg0b1.trace.dir=$TRACE -Dg0b1.expect.dbuname=$EXPECT_DB -Dg0b1.expect.role=$ROLE $delay_opt $extra"
  echo "[run] mode=$mode"
  "$SPARK_HOME"/bin/spark-submit \
    --master 'local[4]' \
    --jars "$JAR,$OJDBC" \
    --driver-class-path "$JAR:$OJDBC" \
    --conf "spark.sql.sources.disabledJdbcConnProviderList=basic" \
    --conf "spark.driver.extraJavaOptions=$opts" \
    --conf "spark.executor.extraJavaOptions=$opts" \
    run-g0-0b1.py --url "$URL" --user "$USER" --password-env ORA_PW \
      --table "$TABLE" --mode "$mode" --trace-dir "$TRACE" 2>&1 | tee "$LOGS/$mode.log"
  local rc=${PIPESTATUS[0]}
  echo "[run] mode=$mode exit=$rc"
  return $rc
}

# coverage 를 먼저 돌린다. 자격증명·접속 문제로 실패하면 **여기서 멈춘다** —
# 같은 자격증명으로 두 번째 회차를 돌려 로그온 실패를 늘리지 않는다(계정 잠금 방지).
submit coverage "" || true
if grep -qiE 'ORA-01017|ORA-28000|ORA-01005|invalid username|account is locked' "$LOGS/coverage.log" 2>/dev/null; then
  echo "[abort] 자격증명 오류가 관측됐다. failclosed 회차를 실행하지 않는다(계정 잠금 방지)." >&2
  echo "[abort] 로그: $LOGS/coverage.log" >&2
  exit 2
fi
submit failclosed "-Dg0b1.fail=all" || true

echo
python3 analyze-trace.py --trace-dir "$TRACE" --result-log "$LOGS"/*.log --out g0-0b1-evidence.json
rc=$?
echo "[run] 증거: g0-0b1-evidence.json   추적 원본: $TRACE"
exit $rc
