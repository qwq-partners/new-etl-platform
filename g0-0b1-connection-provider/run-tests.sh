#!/usr/bin/env bash
# G0-0B1 Java 단위 시험. build.sh 로 만든 클래스에 대고 돈다.
#   ./build.sh && ./run-tests.sh
set -euo pipefail
cd "$(dirname "$0")"
: "${SPARK_HOME:?SPARK_HOME 을 설정하라}"
[ -d build/classes ] || { echo "먼저 ./build.sh 를 실행하라" >&2; exit 2; }

# **소스가 클래스보다 새로우면 멈춘다**(2026-08-30, 8차 M2 작업 중 발견).
# 이 스크립트는 test/ 만 컴파일하고 build/classes 에 링크한다. 그래서 src/ 를 고치고
# build.sh 를 다시 돌리지 않으면 **낡은 구현에 대고 새 시험을 돌린다** — 실제로
# M2-3 작업 중에 그 상태로 통과·실패가 뒤섞여 나왔다. 시험이 무엇을 시험하는지
# 스스로 말하지 못하는 상태를 남겨 두지 않는다.
newer=$(find src -name '*.java' -newer build/classes -print -quit 2>/dev/null || true)
if [ -n "$newer" ]; then
  echo "src/ 가 build/classes 보다 새롭다: $newer" >&2
  echo "  ./build.sh 를 먼저 돌려라 — 낡은 클래스에 대고 시험하면 결과가 거짓이다." >&2
  exit 2
fi

CP="build/classes:$(find "$SPARK_HOME/jars" -name '*.jar' | tr '\n' ':')"
OUT=build/test-classes
rm -rf "$OUT"; mkdir -p "$OUT"
javac -nowarn --release "${TARGET_RELEASE:-17}" -cp "$CP" -d "$OUT" \
    $(find test -name '*.java') 2>&1 | grep -v 'bad path element' || true
# 로케일이 C 인 환경에서 한글 출력이 깨지지 않게 stdout 인코딩을 고정한다.
java -Dstdout.encoding=UTF-8 -Dfile.encoding=UTF-8 -cp "$OUT:$CP" etl.g0b1.InjectionMatrix
