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

CP=$(find "$SPARK_HOME/jars" -name '*.jar' | tr '\n' ':')
echo "[build] SPARK_HOME=$SPARK_HOME"
echo "[build] spark jars: $(find "$SPARK_HOME/jars" -name '*.jar' | wc -l)개"
"$SPARK_HOME"/bin/spark-submit --version 2>&1 | grep -iE 'version|scala' | sed 's/^/[build] /' || true

javac -Xlint:all -cp "$CP" -d "$OUT/classes" $(find src/main/java -name '*.java')
cp -r src/main/resources/* "$OUT/classes/"
( cd "$OUT/classes" && jar cf "../../$JAR" . )

echo "[build] 완료: $JAR"
echo "[build] 등록 확인:"
unzip -p "$JAR" META-INF/services/org.apache.spark.sql.jdbc.JdbcConnectionProvider | sed 's/^/[build]   /'
