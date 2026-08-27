# G0-0B1 — 커스텀 `JdbcConnectionProvider` tracer

Profile U 의 모든 보장은 **"모든 물리 connection 이 세션 프리앰블을 받는다"** 위에 서 있다. 그런데 `sessionInitStatement` 는 **task 경로에서만** 실행된다 — schema 해석·metadata 경로의 connection 은 그것을 실행하지 않으므로 신원 확인과 지연 상한 **밖에서 원천을 읽는다**(NEW-04).

이 패키지는 그 전제가 실제로 성립하는지 재는 도구다. **이것이 서기 전에는 세션 단언 위의 모든 보장이 미확정이다.**

---

## 1. 무엇을 묻는가

| # | 질문 | 아니면 무엇이 무너지는가 |
|---|---|---|
| 1 | 커스텀 provider 가 **`SCHEMA`·`TASK` 경로**에서 호출되는가 | 그 경로가 fence 밖에서 읽는다 |
| 2 | 프리앰블 실패 시 job 이 정말 죽는가(**fail-closed**) | 어떤 경로가 예외를 삼키면 그 경로는 단언 없이 계속 간다 |
| 3 | 한 회차가 여는 물리 connection·서버 세션은 몇 개인가 | Control 의 동시 세션 예산 근거가 없다 |

`sessionInitStatement` 로는 1번을 만족시킬 수 없다는 것이 출발점이다. Spark SPI 인 `JdbcConnectionProvider` 는 Spark 가 **모든** JDBC connection 을 만들 때 거치는 지점이므로, 여기에 붙는 것이 stock Spark 에서 여러 경로를 한 지점에서 덮는 유일한 방법이다(다만 이 하네스가 실제로 유발하는 것은 `SCHEMA`·`TASK` 두 경로다 — §7).

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
./run.sh "jdbc:oracle:thin:@//host:1521/svc" ETL_USER SCHEMA.TABLE ETLPOC_STB PHYSICAL_STANDBY 300
# role 은 **공백 없이** 넘긴다. JVM 인자에서 공백이 잘리므로 run.sh 가 공백 입력을 거부한다.
```

종료 코드
  **0** = `SCHEMA`·`TASK` 두 경로 커버 + coverage 회차 프리앰블 전면 적용 + fail-closed 성립
  **2** = 실행 전 조건 미비(빌드 안 됨·환경변수 없음 등)
  **3** = `NOT_PROVEN` — 측정은 했으나 위 조건 중 하나가 미충족
  **5** = `MEASUREMENT_FAILED` — 추적 0건. **측정 자체를 못 했다**(대개 conf 누락)

> `METADATA`(DSv2 `JDBCTableCatalog`) 경로는 이 하네스가 **유발하지 않는다** — `run-g0-0b1.py` 는
> DSv1(`spark.read.format("jdbc")`)만 쓰고 `spark.sql.catalog.*` 를 설정하지 않는다.
> 따라서 **exit 0 은 그 경로를 증명하지 않는다.** 미측정이다.

### 반드시 필요한 conf

```
--conf spark.sql.sources.disabledJdbcConnProviderList=basic
```

내장 `BasicConnectionProvider` 도 같은 옵션을 claim 한다. 이걸 주지 않으면 Spark 가 provider 중복으로 실패하거나 우리 것이 선택되지 않는다. **추적 라인이 0건이면 대개 이 conf 를 빠뜨린 것이다.**

---

## 4. 판정을 읽는 법

`analyze-trace.py` 는 세 결론만 낸다.

- **`PROVEN`** — `SCHEMA`·`TASK` 경로에서 provider 가 호출됐고 프리앰블이 전부 적용됐으며 fail-closed 가 성립한다. **`METADATA` 는 포함하지 않는다**(§7).
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

- **`METADATA`(DSv2 카탈로그) 경로.** 이 하네스는 그 경로를 **유발하지 않으므로 미측정**이다.
  재려면 `spark.sql.catalog.*` 를 등록해야 하는데, 그 conf 는 비밀번호를 Spark conf·이벤트로그·Web UI 에
  남기므로 §6-3(비밀번호는 환경변수로만)과 충돌한다. 사내 PoC 에서 별도 step 으로 다루고,
  켤 때의 노출 경로를 그때 명시하라.
- **운영 규모의 동시성.** `local[4]` 는 정시 burst 500건을 재현하지 않는다. 동시 세션 **피크**는 이 실행으로 알 수 없다.
- **다른 Spark 판본.** SPI 시그니처와 내부 호출 경로는 판본마다 다르다. `versions.lock` 의 그 버전으로 빌드·실행한 결과만 근거가 된다.
- **Oracle 쪽 부하.** 이건 connection 경로 실측이지 부하 시험이 아니다.
- **프리앰블 내용의 타당성.** `STANDBY_MAX_DATA_DELAY` 는 **쿼리 시작 시점에만** 평가되므로, 오래 도는 추출은 이것으로 self-fail 하지 않는다(`etl-platform-v2.0-grant-request-verdict.md` §2). 이 도구는 프리앰블이 *걸렸는지* 를 재지, 그것이 *충분한지* 를 재지 않는다.

---

## 8. 현재 상태

**2026-08-28 로컬 실측(Spark 4.2.0 / Scala 2.13.16 / JDK 21.0.11) — 확정된 것:**

| 항목 | 결과 | 근거 |
|---|---|---|
| SPI 시그니처 일치 | ✅ | `javap` 로 확인. 4멤버 정확히 일치. **`modifiesSecurityContext` 가 4.2.0 에서는 default 가 아니라 abstract 다** — 우리는 override 하고 있다 |
| `build.sh` 빌드 | ✅ exit 0 | jar 생성 + `META-INF/services` 등록 출력 |
| 바이트코드 타깃 | ✅ major=61(Java 17) | `--release 17` 이 실제로 적용됨 |
| ServiceLoader 발견 | ✅ | `name()=g0b1tracer`, `modifiesSecurityContext=false` |
| `canHandle` 판별 | ✅ | oracle=true / postgresql=false / mysql=false |
| **`disabledJdbcConnProviderList=basic` 필요성** | ✅ 바이트코드로 확인 | `BasicConnectionProvider.canHandle = (keytab==null ‖ principal==null)` → 비-Kerberos 에서 **true**. `SecureConnectionProvider`(Oracle/Postgres/DB2/MSSQL/MariaDB 상위) = keytab+principal 필요 → **false**. 즉 Oracle URL 을 claim 하는 것은 **Basic + 우리 것 2개**. conf 값 `basic` 은 `BasicConnectionProvider.name()` 상수와 일치 |

**아직 확정되지 않은 것:**

- 런타임에서 실제로 provider 중복 예외가 나는가 — 바이트코드 논증까지만. `spark-submit` 필요
- SCHEMA·TASK 경로 커버리지, fail-closed — Oracle 서버 필요(계획 S4~S6)
- `METADATA` 경로 — 하네스가 유발하지 않는다(§7)

> 위 측정은 **Maven 아티팩트 부분 클래스패스**로 했다(전체 배포판 아님). 그래서 "Spark 내장 provider 6개 중 몇 개가 로드되는가"는 **측정하지 못했다** — 부분 클래스패스에서 `ServiceConfigurationError` 가 났고, 그때 나온 "provider 1개" 는 측정이 아니라 클래스패스 결함이다. 증거: `evidence/g0-0b1-local-s2s3.json`

`analyze-trace.py` 의 판정 로직은 합성 추적으로 양·음성 모두 확인했다.
