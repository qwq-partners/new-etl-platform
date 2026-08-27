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

sha() { [ -f "$1" ] && sha256sum "$1" | cut -d' ' -f1 || echo MISSING; }
now() { date -u +%Y-%m-%dT%H:%M:%S+00:00; }
jq_str() {  # JSON 문자열 이스케이프. 의존성을 늘리지 않으려고 python 으로 한다.
  python3 -c 'import json,sys; sys.stdout.write(json.dumps(sys.argv[1], ensure_ascii=False))' "$1"
}

# **실행 전에** lock 을 해시한다. 실행 중 lock 이 바뀌어도 이 값이 이 회차의 판본이다.
LOCK_DIGEST=$(sha "$LOCK")
STARTED=$(now)

echo "[child] $CHILD run_id=$RUN_ID profile=$PROFILE"
echo "[child] versions_lock_digest=$LOCK_DIGEST"
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
  echo "  \"versions_lock_digest\": $(jq_str "$LOCK_DIGEST"),"
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
