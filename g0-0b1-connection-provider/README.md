# G0-0B1 — 커스텀 `JdbcConnectionProvider` tracer

Profile U 의 모든 보장은 **"모든 물리 connection 이 세션 프리앰블을 받는다"** 위에 서 있다. 그런데 `sessionInitStatement` 는 **task 경로에서만** 실행된다 — schema 해석·metadata 경로의 connection 은 그것을 실행하지 않으므로 신원 확인과 지연 상한 **밖에서 원천을 읽는다**(NEW-04).

이 패키지는 그 전제가 실제로 성립하는지 재는 도구다. **이것이 서기 전에는 세션 단언 위의 모든 보장이 미확정이다.**

---

## 1. 무엇을 묻는가

| # | 질문 | 아니면 무엇이 무너지는가 |
|---|---|---|
| 1 | 커스텀 provider 가 **세 경로 모두**에서 호출되는가 | schema/metadata 경로가 fence 밖에서 읽는다 |
| 2 | 프리앰블 실패 시 job 이 정말 죽는가(**fail-closed**) | 어떤 경로가 예외를 삼키면 그 경로는 단언 없이 계속 간다 |
| 3 | 한 회차가 여는 물리 connection·서버 세션은 몇 개인가 | Control 의 동시 세션 예산 근거가 없다 |

`sessionInitStatement` 로는 1번을 만족시킬 수 없다는 것이 출발점이다. Spark SPI 인 `JdbcConnectionProvider` 는 Spark 가 **모든** JDBC connection 을 만들 때 거치는 지점이므로, 여기에 붙는 것이 stock Spark 에서 세 경로를 덮는 유일한 방법이다.

---

## 2. 구성

```
src/main/java/etl/g0b1/
  TracingConnectionProvider.java   Spark SPI 구현. 모든 connection 을 가로챈다
  Preamble.java                    세션 단언(신원·시간축·지연상한). 어긋나면 **던진다**
  Trace.java                       JVM 별 JSONL 추적 기록 + 스택 분류
src/main/resources/META-INF/services/
  org.apache.spark.sql.jdbc.JdbcConnectionProvider    SPI 등록
build.sh          $SPARK_HOME/jars 에 대고 javac 로 직접 빌드
run-g0-0b1.py     Spark 실행(사실 수집만, 판정 안 함)
analyze-trace.py  판정
run.sh            두 모드를 돌리고 판정까지
```

**Maven 을 쓰지 않는다.** 운영에서 쓸 **그 Spark 판본**에 대고 컴파일하게 하는 것이 목적이다. 버전이 다르면 결과가 규범 근거가 되지 못한다.

---

## 3. 실행

```bash
export SPARK_HOME=/opt/spark
./build.sh
```

```bash
export OJDBC_JAR=/path/to/ojdbc11.jar
read -rs -p 'Oracle password: ' ORA_PW && export ORA_PW && echo
./run.sh "jdbc:oracle:thin:@//host:1521/svc" ETL_USER SCHEMA.TABLE ETLPOC_STB "PHYSICAL STANDBY" 300
```

종료 코드: **0** = 세 경로 커버 증명됨 · **3** = 미증명 · **2** = 실행 전 조건 미비

### 반드시 필요한 conf

```
--conf spark.sql.sources.disabledJdbcConnProviderList=basic
```

내장 `BasicConnectionProvider` 도 같은 옵션을 claim 한다. 이걸 주지 않으면 Spark 가 provider 중복으로 실패하거나 우리 것이 선택되지 않는다. **추적 라인이 0건이면 대개 이 conf 를 빠뜨린 것이다.**

---

## 4. 판정을 읽는 법

`analyze-trace.py` 는 세 결론만 낸다.

- **`PROVEN`** — 세 경로에서 provider 가 호출됐고 프리앰블이 전부 적용됐으며 fail-closed 가 성립한다.
- **`NOT_PROVEN`** — 위 중 하나가 아니다. 어느 질문이 걸렸는지 `blocking` 에 나온다.
- **`MEASUREMENT_FAILED`** — 추적 라인이 0건이다. 이건 **"덮지 못한다"가 아니라 "측정하지 못했다"** 이다. 둘을 섞으면 안 된다.

`path_guess` 는 **스택 추정이지 확정이 아니다.** 그래서 모든 라인에 `raw_stack` 상위 18프레임을 그대로 남긴다 — 분류기가 틀려도 사람이 재판정할 수 있어야 한다. `UNKNOWN` 이 많으면 분류기를 고칠 일이지 결론을 낼 일이 아니다.

---

## 5. fail-closed 실험

`-Dg0b1.fail=all` 로 **모든** 경로의 프리앰블을 강제 실패시킨다. 그러면 job 은 **죽어야 한다**.

살아남으면 그 경로가 connection 예외를 삼킨 것이고, 그건 **P0** 다 — 그 경로는 세션 단언 없이 원천을 읽는다. Spark 4.2.0 이 예외를 삼키는 metadata connection 을 하나 더 연다는 관측이 있어 이 실험을 넣었다.

---

## 6. 안전 규칙

1. 읽기 전용. DDL·DML 이 한 줄도 없다.
2. 대상 테이블은 `--limit` 행으로 제한해 읽는다(`WHERE ROWNUM <= N`). **전수 스캔하지 않는다.**
3. 비밀번호는 **환경변수로만** 받는다. argv·URL·로그에 넣지 않는다. 추적 라인의 URL 은 `@` 이후만 남긴다.
4. 프리앰블은 신원이 어긋나면 **읽지 않고 던진다**. 조회해서 로그에 남기는 것이 아니다.
5. `local[4]` 로 먼저 돌린다 — 한 JVM 이라 추적 파일이 한곳에 모인다. 클러스터 모드는 executor 로그 수집이 따로 필요하다.

---

## 7. 이 도구가 증명하지 못하는 것

- **운영 규모의 동시성.** `local[4]` 는 정시 burst 500건을 재현하지 않는다. 동시 세션 **피크**는 이 실행으로 알 수 없다.
- **다른 Spark 판본.** SPI 시그니처와 내부 호출 경로는 판본마다 다르다. `versions.lock` 의 그 버전으로 빌드·실행한 결과만 근거가 된다.
- **Oracle 쪽 부하.** 이건 connection 경로 실측이지 부하 시험이 아니다.
- **프리앰블 내용의 타당성.** `STANDBY_MAX_DATA_DELAY` 는 **쿼리 시작 시점에만** 평가되므로, 오래 도는 추출은 이것으로 self-fail 하지 않는다(`etl-platform-v2.0-grant-request-verdict.md` §2). 이 도구는 프리앰블이 *걸렸는지* 를 재지, 그것이 *충분한지* 를 재지 않는다.

---

## 8. 현재 상태

Java 3파일은 **Spark/Scala API 스텁에 대고 컴파일을 검증**했다(문법·시그니처 정합). 실제 Spark jar 에 대고는 아직 빌드하지 않았다 — 이 환경에 Spark 가 없다. **첫 단계는 `./build.sh` 이며, 거기서 실패하면 그것이 첫 번째 측정 결과다**(SPI 시그니처가 그 판본과 다르다는 뜻).

`analyze-trace.py` 의 판정 로직은 합성 추적으로 양·음성 모두 확인했다.
