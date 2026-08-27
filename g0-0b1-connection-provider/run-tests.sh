#!/usr/bin/env bash
# G0-0B1 Java 단위 시험. build.sh 로 만든 클래스에 대고 돈다.
#   ./build.sh && ./run-tests.sh
set -euo pipefail
cd "$(dirname "$0")"
: "${SPARK_HOME:?SPARK_HOME 을 설정하라}"
[ -d build/classes ] || { echo "먼저 ./build.sh 를 실행하라" >&2; exit 2; }

CP="build/classes:$(find "$SPARK_HOME/jars" -name '*.jar' | tr '\n' ':')"
OUT=build/test-classes
rm -rf "$OUT"; mkdir -p "$OUT"
javac -nowarn --release "${TARGET_RELEASE:-17}" -cp "$CP" -d "$OUT" \
    $(find test -name '*.java') 2>&1 | grep -v 'bad path element' || true
# 로케일이 C 인 환경에서 한글 출력이 깨지지 않게 stdout 인코딩을 고정한다.
java -Dstdout.encoding=UTF-8 -Dfile.encoding=UTF-8 -cp "$OUT:$CP" etl.g0b1.InjectionMatrix
