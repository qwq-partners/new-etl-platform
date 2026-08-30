#!/usr/bin/env bash
# G0-0 child 실행 래퍼 — 산출물 옆에 manifest 사이드카를 남긴다.
#
#   ./g0-run-child.sh <CHILD> <RUN_ID> <PROFILE> <ARTIFACT> -- <명령...>
#
# 예)
#   ./g0-run-child.sh G0_0B1 RUN-2026-08-27-01 LOCAL_WSL g0-0b1-evidence.json -- \
#       ./g0-0b1-connection-provider/run.sh "$URL" ETL_PROBE ETL_PROBE.G0_TARGET FREE PRIMARY none
#
# 이 스크립트는 **해석하지 않는다.** 실행 시점의 사실만 적는다 — 판정은 g0-normalize.py 가 한다.
# 계약: g0-child-contract.md
set -uo pipefail

CHILDREN="G0_0A G0_0B0 G0_0B1 G0_0C00 G0_0C_SUITE"
PROFILES="LOCAL_WSL CORP_POC SANDBOX_CONTAINER"

usage() {
  echo "사용법: $0 <CHILD> <RUN_ID> <PROFILE> <ARTIFACT> -- <명령...>" >&2
  echo "  CHILD  : $CHILDREN" >&2
  echo "  PROFILE: $PROFILES" >&2
  exit 2
}

[ $# -ge 6 ] || usage
CHILD="$1"; RUN_ID="$2"; PROFILE="$3"; ARTIFACT="$4"; shift 4
[ "$1" = "--" ] || usage
shift

case " $CHILDREN " in *" $CHILD "*) ;; *) echo "알 수 없는 CHILD: $CHILD" >&2; usage;; esac
case " $PROFILES " in *" $PROFILE "*) ;; *) echo "알 수 없는 PROFILE: $PROFILE" >&2; usage;; esac
case "$RUN_ID" in ""|*[!A-Za-z0-9._-]*) echo "RUN_ID 에는 [A-Za-z0-9._-] 만 쓴다: $RUN_ID" >&2; exit 2;; esac

HERE=$(cd "$(dirname "$0")" && pwd)
LOCK="${VERSIONS_LOCK:-$HERE/versions.lock}"
[ -f "$LOCK" ] || { echo "versions.lock 이 없다: $LOCK" >&2; exit 2; }

# ── M1-2: source 결속 ────────────────────────────────────────────────
# **어느 원천에서 잰 값인가.** 이것이 없으면 여러 원천의 산출물을 한 회차로 섞어도
# 집계기가 알 수 없다. 자유 문자열이 아니라 그 원천의 DB_UNIQUE_NAME 을 쓴다 —
# A 가 SYS_CONTEXT 로 읽어 산출물에 남기는 값과 같은 것이어야 대조가 성립한다.
SOURCE_ID="${G0_SOURCE_ID:-}"
[ -n "$SOURCE_ID" ] || {
  echo "G0_SOURCE_ID 가 없다. 대상 원천의 DB_UNIQUE_NAME 을 지정하라(8차 M1-2)." >&2
  echo "  예: G0_SOURCE_ID=ORCLSTBY ./g0-run-child.sh …" >&2
  exit 2
}
case "$SOURCE_ID" in *[!A-Za-z0-9._-]*) echo "G0_SOURCE_ID 에는 [A-Za-z0-9._-] 만 쓴다: $SOURCE_ID" >&2; exit 2;; esac

# ── M1-4: run 별 불변 산출물 경로 ────────────────────────────────────
# 산출물 경로에 RUN_ID 가 들어가야 한다. 같은 경로에 회차를 덮어쓰면
# **이전 회차의 증거가 사라지고**, manifest 만 새 것이라 사후에 구분할 수 없다.
case "$ARTIFACT" in
  *"$RUN_ID"*) ;;
  *) echo "산출물 경로에 RUN_ID($RUN_ID)가 들어가야 한다: $ARTIFACT" >&2
     echo "  회차마다 다른 경로여야 이전 증거가 덮이지 않는다(8차 M1-4)." >&2
     exit 2;;
esac
# 이미 있으면 덮지 않는다. 재실행은 새 RUN_ID 로 한다.
if [ -e "$ARTIFACT" ] && [ "${G0_ALLOW_OVERWRITE:-}" != "1" ]; then
  echo "산출물이 이미 있다: $ARTIFACT" >&2
  echo "  회차 산출물은 불변이다. 다시 돌리려면 새 RUN_ID 를 쓰라(8차 M1-4)." >&2
  echo "  의도적으로 덮으려면 G0_ALLOW_OVERWRITE=1 — 그 사실이 manifest 에 남는다." >&2
  exit 2
fi
OVERWROTE=$([ -e "$ARTIFACT" ] && echo true || echo false)

sha() { [ -f "$1" ] && sha256sum "$1" | cut -d' ' -f1 || echo MISSING; }
now() { date -u +%Y-%m-%dT%H:%M:%S+00:00; }
jq_str() {  # JSON 문자열 이스케이프. 의존성을 늘리지 않으려고 python 으로 한다.
  python3 -c 'import json,sys; sys.stdout.write(json.dumps(sys.argv[1], ensure_ascii=False))' "$1"
}

# **실행 전에** lock 을 해시한다. 실행 중 lock 이 바뀌어도 이 값이 이 회차의 판본이다.
LOCK_DIGEST=$(sha "$LOCK")

# ── M1-2: harness digest ─────────────────────────────────────────────
# versions.lock 은 **실행 판본**(Spark·JDK·ojdbc)이지 **하네스 코드**가 아니다.
# 프로브 SQL 이나 판정기를 고쳐도 lock digest 는 그대로다 — 그러면 서로 다른 코드로
# 잰 값이 같은 판본으로 묶인다. 하네스 자신의 digest 를 따로 남긴다.
harness_digest() {
  local files
  files=$(cd "$HERE" && ls -1 \
    g0-0a-capability-inventory.sql g0-0b0-spark-smoke.py g0-0c-fence-facts.sql \
    g0-normalize.py g0_axes.py g0-run-child.sh g0-sqlplus.sh \
    g0-0b1-connection-provider/run.sh g0-0b1-connection-provider/run-g0-0b1.py \
    g0-0b1-connection-provider/analyze-trace.py \
    g0-0c-counterexamples/runner.py 2>/dev/null | sort)
  [ -n "$files" ] || { echo NO_HARNESS_FILES; return; }
  (cd "$HERE" && printf '%s\n' $files | xargs sha256sum | sha256sum | cut -d' ' -f1)
}
HARNESS_DIGEST=$(harness_digest)

STARTED=$(now)

echo "[child] $CHILD run_id=$RUN_ID profile=$PROFILE"
echo "[child] source_id=$SOURCE_ID"
echo "[child] versions_lock_digest=$LOCK_DIGEST"
echo "[child] harness_digest=$HARNESS_DIGEST"
echo "[child] 명령: $*"

"$@"
RC=$?

ENDED=$(now)
ART_SHA=$(sha "$ARTIFACT")
ART_LINES=$([ -f "$ARTIFACT" ] && wc -l < "$ARTIFACT" | tr -d ' ' || echo 0)

MAN="$ARTIFACT.manifest.json"
{
  echo '{'
  echo '  "schema_version": "1.0.0",'
  echo '  "record_type": "g0_child_manifest",'
  echo "  \"child\": $(jq_str "$CHILD"),"
  echo "  \"run_id\": $(jq_str "$RUN_ID"),"
  echo "  \"profile\": $(jq_str "$PROFILE"),"
  echo "  \"started_at\": $(jq_str "$STARTED"),"
  echo "  \"ended_at\": $(jq_str "$ENDED"),"
  echo "  \"exit_code\": $RC,"
  echo "  \"source_id\": $(jq_str "$SOURCE_ID"),"
  echo "  \"versions_lock_digest\": $(jq_str "$LOCK_DIGEST"),"
  echo "  \"harness_digest\": $(jq_str "$HARNESS_DIGEST"),"
  echo "  \"overwrote_existing\": $OVERWROTE,"
  echo '  "artifact": {'
  echo "    \"path\": $(jq_str "$ARTIFACT"),"
  echo "    \"sha256\": $(jq_str "$ART_SHA"),"
  echo "    \"lines\": $ART_LINES"
  echo '  },'
  echo '  "runtime": {'
  echo "    \"uname\": $(jq_str "$(uname -srmo 2>/dev/null || uname -a)"),"
  echo "    \"python\": $(jq_str "$(python3 -V 2>&1 | head -1)"),"
  echo "    \"java\": $(jq_str "$(java -version 2>&1 | grep -v 'Picked up' | head -1)")",
  echo "    \"spark_home\": $(jq_str "${SPARK_HOME:-}")"
  echo '  },'
  printf '  "command": ['
  first=1
  for a in "$@"; do
    # 비밀번호가 argv 에 있으면 안 되지만(안전 규칙 §3.1-3), 만약을 위해
    # 흔한 비밀 형태는 남기지 않는다. 값이 아니라 자리만 남긴다.
    #
    # **이것은 심층 방어이지 해결책이 아니다.** sqlplus 를 돌릴 때는
    # g0-sqlplus.sh 를 써라 — 비밀번호를 stdin 으로만 넘겨 argv 에 아예 넣지 않는다.
    # 2026-08-27 runbook 초안이 `bash -c "… CONNECT $USER/$PW@$DSN …"` 형태였고,
    # 그러면 비밀번호가 manifest 에 그대로 남는다는 것을 실제 실행에서 확인했다.
    case "$a" in
      *PASSWORD=*|*PASSWD=*|*PW=*|*SECRET=*|*TOKEN=*) a="<redacted>";;
      # user/password@dsn 형태(sqlplus CONNECT, JDBC URL 등)
      */*@*) a=$(printf '%s' "$a" | sed -E 's#([A-Za-z0-9_.$-]+)/[^@[:space:]]+@#\1/<redacted>@#g');;
    esac
    [ $first -eq 1 ] || printf ', '
    printf '%s' "$(jq_str "$a")"
    first=0
  done
  echo ']'
  echo '}'
} > "$MAN"

echo "[child] manifest: $MAN (exit_code=$RC, artifact sha256=${ART_SHA:0:16}…)"
[ "$ART_SHA" = "MISSING" ] && echo "[child] 경고: 산출물이 없다 — $ARTIFACT" >&2
exit $RC
