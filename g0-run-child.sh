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

# ── 9차 조치 7: environment_scope ────────────────────────────────────
#
# 9차 P0-07. CE(G0_0C_SUITE)는 **폐기용 쓰기 가능 primary** 에서 돌고 A·B0·B1·C00 은
# 사내 standby 를 본다. 그런데 M1-3 의 `check_run_set` 은 child 들의 `source_id` 가
# 갈리면 회차를 거부한다. 그래서 한 회차에 CE 를 넣으면 둘 중 하나였다.
#
#   · CE 가 corporate source 의 이름을 **거짓으로 신고**하거나
#   · 집계가 회차 전체를 거부하거나
#
# **계약에 "이 child 는 다른 환경의 것" 을 표현할 자리가 없었다.** 그 자리를 만든다.
#
# scope 는 **선언값이 아니라 child 로부터 유도한다.** 운영자가 고르게 하면 그것은
# 또 하나의 자가선언이고(profile 이 그래서 문제였다), CE 를 SOURCE 로 신고하는 순간
# 원래 결함으로 돌아간다. child 이름은 계약이 정한 것이므로 유도가 안전하다.
#
#   SOURCE          이 회차가 capability 를 재는 그 DB. A·B0·B1·C00
#   COUNTEREXAMPLE  완화책이 실제로 막는지 보이려고 **파괴적 시나리오를 도는** 폐기용 DB.
#                   원천에 대해 아무것도 말하지 않는다. C_SUITE
case "$CHILD" in
  G0_0C_SUITE) ENV_SCOPE=COUNTEREXAMPLE ;;
  *)           ENV_SCOPE=SOURCE ;;
esac

# ── 9차 조치 4: 실행 환경을 **관측한다** ─────────────────────────────
#
# 9차 P0-02. profile 이 caller 가 고르는 label 이라 **WSL 에서 PROFILE=CORP_POC 로
# 재라벨**하면 아무도 막지 못했다. 8차의 profile relabel 반례가 그대로 살아 있었다.
#
# 완전한 attestation(승인된 launcher·서명)은 이 저장소 범위 밖이다. 대신 **래퍼가
# 자기가 도는 환경의 관측 가능한 사실**을 남기고, 집계기가 선언된 profile 과 모순되면
# 거부한다. 증명은 못 해도 **알려진 거짓말은 막는다.**
#
#   wsl        /proc/sys/kernel/osrelease 에 microsoft|WSL
#   container  /.dockerenv 또는 cgroup 에 docker|containerd|kubepods
#   host       위 둘 다 아님 — CORP_POC 를 **증명하지는 않는다**
env_kind() {
  local osr="" cg=""
  [ -r /proc/sys/kernel/osrelease ] && osr=$(cat /proc/sys/kernel/osrelease 2>/dev/null || true)
  case "$osr" in *microsoft*|*Microsoft*|*WSL*) echo wsl; return;; esac
  [ -e /.dockerenv ] || [ -e /run/.containerenv ] && { echo container; return; }
  [ -n "${container:-}" ] && { echo container; return; }
  [ -r /proc/1/cgroup ] && cg=$(cat /proc/1/cgroup 2>/dev/null || true)
  case "$cg" in *docker*|*containerd*|*kubepods*|*lxc*) echo container; return;; esac
  # **`host` 는 "컨테이너가 아니다" 를 증명하지 않는다.** cgroup v2 는 `0::/` 만 내는 경우가
  # 있어 컨테이너 안에서도 여기까지 온다(이 저장소의 샌드박스가 그렇다). 그래서 판정은
  # **비대칭**이다 — wsl·container 관측은 CORP_POC 선언을 반증하지만, host 는 아무것도
  # 입증하지 않는다. CORP_POC 의 실질 근거는 서버가 밝힌 원천 신원이다(집계기 §조치 4).
  echo host
}
ENV_KIND=$(env_kind)

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
# ── 9차 조치 5: harness digest 는 **선언된 목록**에서 나온다 ────────
#
# 9차 P1-01. 이전 판은 여기에 11개 파일을 **하드코딩**했고 provider Java source·
# ServiceLoader 등록·build.sh·child schema 4종·final contract·gate·CE 시나리오가 전부
# 빠져 있었다. **빠진 파일을 바꿔도 digest 가 그대로**여서, M1-2 가 세운 명제
# ("서로 다른 코드로 잰 값이 같은 판본으로 묶이는 것을 막는다")가 그 파일들에는
# 성립하지 않았다. 그리고 새 파일은 목록에 자동으로 들어가지 않았다.
#
# 이제 목록은 `g0-harness-manifest.json` 이 선언하고, 그 검사기가 저장소의 **모든**
# 파일(미커밋 포함)이 harness·tooling·excluded 중 하나에 속하는지 확인한다.
# 미선언 파일이 있으면 digest 를 내주지 않는다 — 그 digest 는 "이 코드로 쟀다" 를
# 말하지 못하기 때문이다.
harness_digest() {
  local d
  d=$(cd "$HERE" && python3 g0-harness-manifest.py --digest 2>/dev/null) || {
    echo "harness manifest 가 불완전하다. 다음을 실행해 원인을 보라:" >&2
    echo "  python3 $HERE/g0-harness-manifest.py" >&2
    echo "MANIFEST_INCOMPLETE"
    return
  }
  [ -n "$d" ] && echo "$d" || echo MANIFEST_INCOMPLETE
}
HARNESS_DIGEST=$(harness_digest)

STARTED=$(now)

echo "[child] $CHILD run_id=$RUN_ID profile=$PROFILE"
echo "[child] source_id=$SOURCE_ID scope=$ENV_SCOPE"
echo "[child] versions_lock_digest=$LOCK_DIGEST"
echo "[child] harness_digest=$HARNESS_DIGEST"
echo "[child] 명령: $*"

# ── 9차 조치 6: 회차 신원을 자식에게 넘긴다 ──────────────────────────
# 원천 lease(`g0_source_envelope.acquire`)는 **누가 쥐었는가**를 적어야 진단에 쓸모가
# 있다. RUN_ID 는 여기에만 있었고 자식은 그것을 볼 길이 없었다 — B0 는 lease 를
# `UNSPECIFIED` 로 쥘 뻔했다. 호출부마다 인자를 하나씩 더 붙이는 대신 환경으로 넘긴다.
# 그래야 manifest 의 `run_id` 와 lease 의 `run_id` 가 **구조적으로 같은 값**이 된다.
export G0_RUN_ID="$RUN_ID"
export G0_SOURCE_ID="$SOURCE_ID"

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
  echo "  \"env_kind\": $(jq_str "$ENV_KIND"),"
  echo "  \"environment_scope\": $(jq_str "$ENV_SCOPE"),"
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
