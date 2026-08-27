#!/usr/bin/env bash
# G0-0B1 tracer 빌드. Maven 을 쓰지 않는다 — **운영에서 쓸 그 Spark 판본에 대고**
# 직접 컴파일하게 하는 것이 목적이다. 버전이 다르면 결과가 규범 근거가 되지 못한다.
set -euo pipefail
cd "$(dirname "$0")"

: "${SPARK_HOME:?SPARK_HOME 을 설정하라 (예: export SPARK_HOME=/opt/spark)}"
[ -d "$SPARK_HOME/jars" ] || { echo "SPARK_HOME/jars 가 없다: $SPARK_HOME" >&2; exit 2; }

OUT=build
JAR=g0-0b1-tracer.jar
rm -rf "$OUT" "$JAR"
mkdir -p "$OUT/classes"

# javac 가 여기서 `warning: [path] bad path element ".../derby.jar"` 류 경고를 낸다.
# **우리 빌드의 결함이 아니다.** derby-*.jar·derbytools-*.jar·scala-compiler-*.jar 의
# MANIFEST Class-Path 가 버전 없는 이름(derby.jar, scala-library.jar)을 가리키는데
# 배포판은 버전 붙은 이름으로 담기 때문이다. javac 가 그 링크를 따라가 못 찾고 경고한다.
# 2026-08-27 Spark 4.2.0/3.5.9 실측에서 19건. 결과에 영향 없다 — 빌드 실패 징후로 읽지 마라.
CP=$(find "$SPARK_HOME/jars" -name '*.jar' | tr '\n' ':')
echo "[build] SPARK_HOME=$SPARK_HOME"
echo "[build] spark jars: $(find "$SPARK_HOME/jars" -name '*.jar' | wc -l)개"
"$SPARK_HOME"/bin/spark-submit --version 2>&1 | grep -iE 'version|scala' | sed 's/^/[build] /' || true

# 바이트코드 레벨을 Spark 실행 JVM 에 맞춘다. 빌드 JDK 가 실행 JVM 보다 높으면
# ServiceLoader 가 UnsupportedClassVersionError 로 조용히 실패하고,
# 그 실패는 "추적 0건" 으로만 나타나 원인을 찾기 어렵다.
: "${TARGET_RELEASE:=17}"
echo "[build] --release $TARGET_RELEASE (TARGET_RELEASE 로 바꿀 수 있다)"
javac --release "$TARGET_RELEASE" -Xlint:all -cp "$CP" -d "$OUT/classes" $(find src/main/java -name '*.java')
cp -r src/main/resources/* "$OUT/classes/"
( cd "$OUT/classes" && jar cf "../../$JAR" . )

echo "[build] 완료: $JAR"
echo "[build] 등록 확인:"
unzip -p "$JAR" META-INF/services/org.apache.spark.sql.jdbc.JdbcConnectionProvider | sed 's/^/[build]   /'
