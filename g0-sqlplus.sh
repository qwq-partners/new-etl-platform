#!/usr/bin/env bash
# sqlplus 실행 헬퍼 — **비밀번호를 argv 에 넣지 않기 위한 것이다.**
#
#   ORA_USER=… ORA_DSN=//host:1521/svc  (ORA_PW 는 환경변수로)
#   ./g0-sqlplus.sh <sql파일> <출력파일>
#
# ── 왜 이 파일이 필요한가 ────────────────────────────────────────────
# child 계약(g0-child-contract.md)이 실행 명령을 manifest 에 기록한다. 그래서
#
#     g0-run-child.sh … -- bash -c "sqlplus … <<EOF
#     CONNECT $ORA_USER/$ORA_PW@$DSN
#     …"
#
# 처럼 쓰면 **비밀번호가 manifest 파일에 그대로 남는다.** 안전 규칙 §3.1-3
# ("비밀번호를 명령줄 인자에 넣지 않는다")을 계약이 뒤에서 깨는 셈이다.
# 2026-08-27 runbook 초안을 실제로 돌려 보다가 발견했다.
#
# 이 헬퍼는 비밀번호를 **stdin 으로만** 넘긴다. argv 에도, 프로세스 목록에도,
# manifest 에도 남지 않는다.
set -uo pipefail

SQL="${1:?사용법: g0-sqlplus.sh <sql파일> <출력파일>}"
OUT="${2:?}"
: "${ORA_USER:?ORA_USER 를 설정하라}"
: "${ORA_PW:?ORA_PW 를 설정하라 (argv 금지)}"
: "${ORA_DSN:?ORA_DSN 을 설정하라 (예: //localhost:1521/FREEPDB1)}"
[ -f "$SQL" ] || { echo "SQL 파일이 없다: $SQL" >&2; exit 2; }

command -v sqlplus >/dev/null || { echo "sqlplus 가 PATH 에 없다" >&2; exit 2; }

# WHENEVER SQLERROR 를 CONNECT 앞뒤로 나눠 건다.
#   · CONNECT 실패(ORA-01017 등)는 **종료 코드로** 나와야 한다. 그래야 계약이
#     exit_code != 0 을 잡는다. 로그만 남기고 0 으로 끝나면 집계기가 통과시킨다.
#   · 그러나 probe SQL 안의 오류는 **측정 결과**다. 그것으로 죽으면 안 되므로
#     CONNECT 직후 CONTINUE 로 되돌린다.
printf 'WHENEVER SQLERROR EXIT SQL.SQLCODE\nCONNECT %s/"%s"@%s\nWHENEVER SQLERROR CONTINUE\n@%s\nEXIT\n' \
    "$ORA_USER" "$ORA_PW" "$ORA_DSN" "$SQL" \
  | sqlplus -S /nolog > "$OUT" 2>&1
rc=$?

# 잘못된 비밀번호를 두 번 시도하지 않는다 — 계정 잠금은 전체 파이프라인 정지다.
if grep -qiE 'ORA-01017|ORA-28000|ORA-01005|ORA-28001' "$OUT" 2>/dev/null; then
  echo "[sqlplus] 자격증명 오류가 관측됐다. **재시도하지 마라** — 계정 잠금 위험." >&2
  echo "[sqlplus] 로그: $OUT" >&2
fi
echo "[sqlplus] $SQL -> $OUT (exit=$rc)"
exit $rc
