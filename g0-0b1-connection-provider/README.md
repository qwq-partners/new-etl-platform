# G0-0B1 — 커스텀 `JdbcConnectionProvider` tracer

Profile U 의 모든 보장은 **"모든 물리 connection 이 세션 프리앰블을 받는다"** 위에 서 있다. 그런데 `sessionInitStatement` 는 **task 경로에서만** 실행된다 — schema 해석·metadata 경로의 connection 은 그것을 실행하지 않으므로 신원 확인과 지연 상한 **밖에서 원천을 읽는다**(NEW-04).

이 패키지는 그 전제가 실제로 성립하는지 재는 도구다. **이것이 서기 전에는 세션 단언 위의 모든 보장이 미확정이다.**

> **2026-08-30 실행·증거 수용 NO-GO**: 현재 `run.sh`가 호출하는 `failclosed_task`는
> CLI choice에 없어 Spark 시작 전에 종료하고, analyzer는 task failure를 최종 술어에 사용하지 않는다.
> `Trace.classify()`의 stack guess가 주입 대상과 proof를 함께 정하는 순환 판정도 남아 있다.
> M2에서 explicit `connectionProvider` 선택, schema/task/metadata 독립 scenario, 주입과 독립된
> server/runtime oracle을 구현하고 회귀 검증하기 전에는 exit 0이나 `PROVEN`을 증거로 수용하지 않는다.

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

## 3. M2 수정 후 목표 실행

아래는 환경변수와 목표 흐름을 보존한 예시다. **현재 `run.sh`를 사내 원천에 실행하지 않는다.**
identity hard preflight·partition/session/I/O budget도 M0에서 먼저 닫아야 한다.

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

현재 종료 코드의 의도는 다음과 같지만 **판정 계약이 결함 상태라 acceptance 의미가 없다**.
  **0** = analyzer가 `SCHEMA`·`TASK` 커버와 fail-closed를 주장함. 현재는 `PROVEN`으로 수용 금지
  **2** = 실행 전 조건 미비(빌드 안 됨·환경변수 없음 등)
  **3** = `NOT_PROVEN` — 측정은 했으나 위 조건 중 하나가 미충족
  **5** = `MEASUREMENT_FAILED` — 추적 0건. **측정 자체를 못 했다**(대개 conf 누락)

> `METADATA`(DSv2 `JDBCTableCatalog`) 경로는 이 하네스가 **유발하지 않는다** — `run-g0-0b1.py` 는
> DSv1(`spark.read.format("jdbc")`)만 쓰고 `spark.sql.catalog.*` 를 설정하지 않는다.
> 따라서 **exit 0 은 그 경로를 증명하지 않는다.** 미측정이다.

### provider 선택 목표

```
option("connectionProvider", "g0b1tracer")
```

내장 `BasicConnectionProvider` 도 같은 옵션을 claim 한다. 이걸 주지 않으면 Spark 가 provider 중복으로 `IllegalArgumentException` 을 던진다(2026-08-27 실측 — `g0-0-s1-s3-results.md` §3-F4).

> **Spark 4.2.0 에서는 그 예외 문구가 보이지 않는다.** `JdbcUtils.classifyException` 이 그것을
> `[FAILED_JDBC.CONNECTION] … Couldn't connect to the database` 로 덮어 **네트워크 장애처럼** 읽힌다.
> 3.5.9 는 원문을 그대로 보여 준다. conf 누락을 원천 장애로 오진하지 마라.

**Kerberos 원천에서는 `basic` 만으로 부족하다.** 내장 provider 중 `OracleConnectionProvider`(name = `oracle`)
가 있고, 그 `canHandle` 은 `keytab != null && principal != null` 이다. `BasicConnectionProvider` 의
`canHandle` 은 `keytab == null || principal == null` 로 **정확히 배타적**이다. 따라서

| 원천 인증 | `canHandle` 통과 provider | 꺼야 할 것 |
|---|---|---|
| Kerberos 미사용 | Basic + ours | `basic` |
| Kerberos 사용 | **Oracle** + ours | `basic,oracle` |

사내 원천의 인증 방식이 정해지기 전에는 이 conf 값을 규범에 고정하지 마라.

**추적 라인이 0건이면 provider 가 한 번도 불리지 않은 것이다.** 예외조차 없이 0건이면 jar 가 아예
classpath 에 오르지 않아 stock `BasicConnectionProvider` 가 조용히 쓰인 경우다 — 그쪽이 실제 위험이고,
`analyze-trace.py` 의 `MEASUREMENT_FAILED`(exit 5)가 그것을 `NOT_PROVEN` 과 분리해 내는 이유다.

> **다만 전역 비활성화가 기본 경로는 아니다**(8차 리뷰). Spark 4.2 가 문서화한 JDBC
> `connectionProvider` 옵션으로 `g0b1tracer` 를 **명시**하는 것이 우선이고,
> `spark.sql.sources.disabledJdbcConnProviderList=basic` 의 전역 비활성화는 같은 JVM 의
> 다른 JDBC 사용자 동작까지 바꾸므로 **격리된 진단 fallback** 으로만 쓴다.
> 플랫폼 기본값으로 규범에 넣지 마라.


---

## 4. 판정을 읽는 법

`analyze-trace.py`는 현재 세 결론을 출력하지만 **그 자체를 판정관으로 신뢰하지 않는다.**

- **`PROVEN`** — analyzer의 주장값. 현재 task failure 미사용·stack guess 순환 때문에 acceptance 불가. **`METADATA`도 포함하지 않는다**(§7).
- **`NOT_PROVEN`** — 위 중 하나가 아니다. 어느 질문이 걸렸는지 `blocking` 에 나온다.
- **`MEASUREMENT_FAILED`** — 추적 라인이 0건이다. 이건 **"덮지 못한다"가 아니라 "측정하지 못했다"** 이다. 둘을 섞으면 안 된다.

### 2026-08-27 — verdict 를 성질별로 나눴다 (7차 교차 리뷰 P0-06)

`verdict.coverage` 하나로 네 가지 다른 질문에 답하던 것을 `verdicts` 로 분리했다.

| verdict | 뜻 |
|---|---|
| `provider_reachability` | provider 가 `SCHEMA`·`TASK` 에서 불렸는가 |
| `session_assertion` | coverage 회차의 모든 connection 이 프리앰블을 받았는가 |
| `fail_closed` | 프리앰블이 실패하면 죽는가 |
| `read_only_transaction` | **`NOT_IMPLEMENTED`** — `Preamble` 에 `SET TRANSACTION READ ONLY` 가 없다 |
| `common_snapshot` | **`NOT_IMPLEMENTED`** — 시험 대상이 아니다 |

**그래서 `PROVEN` 은 snapshot capability 의 증거가 아니다.** 이 둘을 한 값에 담고 있었다.

### 두 가지 판정 오류를 고쳤다

1. **`MIXED` 를 양쪽으로 세던 것.** `seen_schema = SCHEMA + MIXED`, `seen_task = TASK + MIXED`
   라서 `MIXED` 한 건이 두 경로의 관측으로 계상됐다. 이제 `MIXED`·`UNKNOWN` 은 **어느 쪽에도
   기여하지 않고**, 사람이 `raw_stack` 을 보고 재판정해야 한다.
2. **주입이 닿지 않은 경로를 통과로 세던 것.** `fail=all` 은 provider 가 처음 불린 connection
   에서 즉시 던지므로, 각 step 이 schema 해석에서 막혀 **task connection 을 열지 못할 수 있다.**
   그 회차의 "전 step 이 실패했다"는 task 경로에 대해 아무것도 말하지 않는다. 이제
   `failclosed_by_path` 에 그 경로가 없으면 `NOT_OBSERVED` 이며 통과가 아니다.
   경로별 주입점(`fail=schema|task|metadata`)이 필요하다 — **조치 5 의 과제다.**

`path_guess` 는 **스택 추정이지 확정이 아니다.** 그래서 모든 라인에 `raw_stack` 상위 18프레임을 그대로 남긴다 — 분류기가 틀려도 사람이 재판정할 수 있어야 한다. `UNKNOWN` 이 많으면 분류기를 고칠 일이지 결론을 낼 일이 아니다.

**`path_guess` 를 판정 술어로 쓰지 마라**(8차 리뷰 M2). 이것은 진단 라벨이다. 경로별 판정은
`fail=schema|task` 로 **경로마다 독립 주입**하고 그 회차의 실패를 보는 것으로 하며,
`injection_applied` 플래그(tracer 가 남긴 사실)를 근거로 삼는다. 분류기 추정이 통과 조건에
들어가면 분류기의 오류가 곧 잘못된 통과가 된다.


---

## 5. fail-closed 실험 — **경로별로 나눠 돌린다**

프리앰블을 강제 실패시켜 job 이 정말 죽는지 본다. 살아남으면 그 경로가 connection 예외를 삼킨 것이고, 그건 **P0** 다 — 그 경로는 세션 단언 없이 원천을 읽는다.

`fail=all` 하나로는 부족하다 — 첫 connection(대개 schema)에서 죽어 뒤 경로를 독립 검증하지
못한다. 그래서 `fail=schema` · `fail=task` 로 나눠 돌린다(`run.sh`). **경로별 주입이 그 경로에
닿았다는 사실 없이 "job 이 죽었다" 만으로 그 경로의 fail-closed 를 주장하지 않는다.**


### `fail=all` 하나로는 부족했다 (2026-08-27, 7차 교차 리뷰 P0-06)

`all` 은 provider 가 **처음 불린** connection 에서 즉시 던진다. 그래서 각 step 이 schema 해석에서 막혀 **task connection 을 아예 열지 못한다.** 그 회차의 "전 step 이 실패했다"는 task 경로의 fail-closed 에 대해 **아무것도 말하지 않는데**, 판정기는 그것을 통과로 세고 있었다.

```
-Dg0b1.fail=none              주입 없음(기본)
-Dg0b1.fail=all               모든 경로
-Dg0b1.fail=schema            SCHEMA 만
-Dg0b1.fail=task              schema 는 통과시키고 TASK 에서만 던진다
                              ← **task 경로 fail-closed 의 유일한 증거**
-Dg0b1.fail=metadata          METADATA 만(이 하네스는 유발하지 않는다)
-Dg0b1.fail=schema,task       조합
```

`run.sh` 는 이제 세 회차를 돌린다 — `coverage` → `failclosed_schema` → `failclosed_task`.

**경로를 지정하면 `MIXED`·`UNKNOWN` 에는 주입하지 않는다.** 분류기가 갈피를 못 잡은 connection 에 주입하면 어느 경로를 시험한 것인지 말할 수 없다. `all` 만 예외다.

### 주입 사실을 추적에 남긴다

판정기가 "그 경로에 주입이 닿았는가"를 `preamble_error` 존재로 **추정**하던 것을 사실로 바꿨다. 추적 라인에 세 필드가 붙는다.

| 필드 | 뜻 |
|---|---|
| `fail_mode` | 이 회차의 주입 설정 |
| `injection_target` | **이 connection 이** 주입 대상인가 |
| `injection_applied` | 대상이었고 실제로 던져졌는가 |

`injection_applied=true` 인 경로만 "시험됐다"로 센다. 그 외에는 `NOT_OBSERVED` 이며 통과가 아니다.

### 단위 시험

```bash
./build.sh && ./run-tests.sh      # Preamble.shouldFail 매트릭스 26건
```

---

### 2026-08-30 — 세션 프리앰블의 `TIME_ZONE` 을 규범값으로 고친다

`Preamble` 이 `ALTER SESSION SET TIME_ZONE = DBTIMEZONE` 을 쓰고 있었다. **규범값은
`'+00:00'` 이다** — A §11.3 의 sessionInitStatement 가 그것을 고정하고(P §3.2 TIMESTAMP
실행 규격 ⑤), G0-0A(`g0-0a-capability-inventory.sql:136`)·G0-0B0
(`g0-0b0-spark-smoke.py:129,:147`) 도 그 값이다. **B1 만 달랐다.**

`DBTIMEZONE` 은 원천 DB 를 만들 때 정해진 값이라 무엇인지 모른다. A 는 그것을 등록조차 하지
않는다 — §6.1 capability 목록의 `DB 시간대` 는 필드 이름 없는 산문이고 §22-4 의 미결이다.
그러면 **B1 이 재는 세션이 규범이 규정한 세션이 아니게 되고**, B1 통과가 규범 세션의 성립을
시사하지 못한다.

**7차 리뷰 P0-06 의 `NLS_NUMERIC_CHARACTERS` `'. '` → `'.,'` 와 정확히 같은 종류의 결함**이다.
그때 NUMBER 축만 대조하고 TIMESTAMP 축을 놓쳤다. 규범이 세션 값을 고정하는 이유는 canonical
row hash 재현성이므로(A §12.3), 두 축 중 하나만 맞아서는 그 재현성이 서지 않는다.

> **G0-0C 는 아직 다르다.** `g0-0c-counterexamples/scenarios/_ce.py` 는 여전히 `DBTIMEZONE`
> 이며 그것은 실수가 아니라 CE03·CE04 의 자격 술어가 naive TIMESTAMP 와 `SYSTIMESTAMP` 를
> 직접 비교하기 때문이다. **그래서 CE 결과는 규범 세션에 대한 증거가 아니다.** 술어를
> `'+00:00'` 에서 다시 쓸지, CE 증거에 "규범 세션 아님" 을 명시할지는 **Oracle 에 붙여
> 술어를 돌려 본 뒤**(S7) 정한다 — 그 파일의 주석에 같은 내용을 남겼다.

### 2026-08-30 — 8차 M2: B1 재작성

**가장 중요한 변경은 M2-3 이다 — 주입을 스택 추정에서 떼어 냈다.**

v1 은 `Preamble.shouldFail(Trace.classify(stack))` 이었다. 그러면 **분류기의 오류가 곧
잘못된 주입**이 되고, 그 주입 결과로 분류기를 검증할 수도 없다(순환). 판정기도 같은
`path_guess` 로 PASS 를 냈으니 회로가 닫혀 있었다.

이제 경로 귀속이 두 곳에서 온다. 둘 다 **실행 구성**이지 추정이 아니다.

| 무엇 | 어떻게 |
|---|---|
| **선언된 phase**(M2-3) | driver 가 step 시작 전에 `g0-0b1-phase-<run>.txt` 에 step 이름을 쓴다. provider 는 `Trace.declaredPhase()` 로 그 값만 읽는다. driver 는 자기가 `.schema` 를 부르는지 `.count()` 를 부르는지 **알고 있다** |
| **격리된 시나리오**(M2-4) | `--scenario schema_only\|task_only\|metadata_only`. schema_only 회차에는 action 이 없으므로 task connection 자체가 없다 — 분류기에 묻지 않고도 안다 |

`path_guess` 는 추적에 그대로 남지만 **어떤 판정 술어에도 들어가지 않는다.** 판정기의
`observed` 에 `path_guess_distribution_diagnostic` 으로만 나온다.

**주입 키가 바뀌었다.** `-Dg0b1.fail=schema|task` 는 더 이상 아무것도 하지 않는다 —
그 값은 분류기 결과와 대조되던 것이라 계속 받으면 추정이 다시 actuator 로 샌다.

```text
-Dg0b1.fail=none                   주입 없음
-Dg0b1.fail=all                    선언된 phase 와 무관하게 전부(시나리오가 격리했을 때)
-Dg0b1.fail=phase -Dg0b1.fail.phase=partitioned_count
                                   그 phase 로 선언된 동안 열린 connection 만
```

**M2-1 explicit connectionProvider.** `--provider g0b1tracer` 가 기본이고 JDBC 옵션으로
직접 지목한다. `disabledJdbcConnProviderList` 전역 비활성화는 `DISABLE_BASIC` 을 줄 때만
걸리는 진단 fallback 이 됐다.

**M2-5 판정 입력 셋.** fail-closed 는 아래 넷이 다 성립할 때만 회차별로 확정된다.

1. `injection_applied` ≥ 1 — tracer 가 남긴 **사실**이다(추정 아님)
2. **terminal token** — driver 가 `G0B1_TERMINAL` 로 자기 종료 상태를 선언한다.
   판정기가 step 성공 여부로 추론하지 않는다
3. `rows_read_total == 0` — 주입 회차인데 행을 읽었으면 **fence 밖 읽기**다
4. **`trace_end` sentinel** — 없으면 `MEASUREMENT_FAILED`. 잘린 추적과
   "connection 이 원래 없었다" 는 겉모습이 같다

**그리고 시험 하네스 자체의 결함 하나를 고쳤다.** `run-tests.sh` 가 `test/` 만
컴파일하고 `build/classes` 에 링크했기 때문에, `src/` 를 고치고 `build.sh` 를 다시
돌리지 않으면 **낡은 구현에 대고 새 시험을 돌렸다.** 이 작업 중에 실제로 그 상태로
결과가 나왔다. 이제 소스가 클래스보다 새로우면 멈춘다.

## 6. 안전 규칙

1. 원천 객체 DDL·DML을 의도하지 않지만 **현재 source-safe 승인은 없다**.
2. `--limit`/`ROWNUM`은 결과 행 상한이지 Oracle I/O 하드 상한이 아니다. 실행 전 plan·partition/session·scan budget 승인이 필요하다.
3. 비밀번호는 **환경변수로만** 받는다. argv·URL·로그에 넣지 않는다. 추적 라인의 URL 은 `@` 이후만 남긴다.
4. 현재는 target 접촉 전 identity hard preflight가 증명되지 않았다. M0에서 mismatch를 모든 target read보다 먼저 차단한다.
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

## 8. 현재 상태 (2026-08-27 갱신)

**실제 Spark jar 에 대고 빌드·배선까지 확인됐다.** 근거: `g0-0-s1-s3-results.md`(profile `SANDBOX_CONTAINER`).

| | 상태 |
|---|---|
| `build.sh` (`javac --release 17 -Xlint:all`) | **exit 0** — Spark 4.2.0/2.13.18 · 3.5.9/2.12.18 · 3.5.9/2.13.8 세 판본. 우리 코드 경고 0건 |
| SPI 배선 | **도달 확인** — 세 판본 모두 `ConnectionProviderBase.create` 에서 호출. `path_guess=SCHEMA` 3건 |
| `analyze-trace.py` | **실제 추적으로 처음 검증** — 추적 0건 → `MEASUREMENT_FAILED`(5), `SCHEMA` 만 → `NOT_PROVEN`(3). 2026-08-27 판정 오류 2건 수정 후 반례 시험 30건 통과(`g0-b1-analyzer-tests.py`) |

**아직 아닌 것.** 위 회차는 Oracle 서버 없이 도달 불가 URL 로 돌렸다. 따라서

- `TASK` 경로 커버리지 — connection 이 열리지 않아 task 가 시작되지 않았다. **미측정**
- 프리앰블 적용 여부 · fail-closed 성립 — **미측정**
- 한 회차의 물리 connection 개수 — **미측정**

즉 **§1 의 질문 세 개는 여전히 하나도 답해지지 않았다.** 답하려면 실제 Oracle 이 필요하다(계획서 S4~S6).

### 2026-08-28 병행 회차 — Maven 부분 클래스패스

같은 시기에 다른 세션이 **전체 배포판 없이 Maven 아티팩트만**으로 같은 항목을 확인했다.
위 회차(전체 배포판 + `spark-submit`)와 결론이 어긋나지 않으므로 둘 다 남긴다.

**2026-08-28 로컬 실측(Spark 4.2.0 / Scala 2.13.16 / JDK 21.0.11) — 확정된 것:**

| 항목 | 결과 | 근거 |
|---|---|---|
| SPI 시그니처 일치 | ✅ | `javap` 로 확인. 4멤버 정확히 일치. **`modifiesSecurityContext` 가 4.2.0 에서는 default 가 아니라 abstract 다** — 우리는 override 하고 있다 |
| `build.sh` 빌드 | ✅ exit 0 | jar 생성 + `META-INF/services` 등록 출력 |
| 바이트코드 타깃 | ✅ major=61(Java 17) | `--release 17` 이 실제로 적용됨 |
| ServiceLoader 발견 | ✅ | `name()=g0b1tracer`, `modifiesSecurityContext=false` |
| `canHandle` 판별 | ✅ | oracle=true / postgresql=false / mysql=false |
| provider 충돌 가능성 | 바이트코드상 확인 | Basic과 custom이 같은 Oracle URL을 handle할 수 있다. 해결 기본값은 Spark의 explicit `connectionProvider=g0b1tracer`; Basic 전역 disable은 진단 fallback |

**아직 확정되지 않은 것:**

- 런타임에서 실제로 provider 중복 예외가 나는가 — 바이트코드 논증까지만. `spark-submit` 필요
- SCHEMA·TASK 경로 커버리지, fail-closed — Oracle 서버 필요(계획 S4~S6)
- `METADATA` 경로 — 하네스가 유발하지 않는다(§7)

> 위 측정은 **Maven 아티팩트 부분 클래스패스**로 했다(전체 배포판 아님). 그래서 "Spark 내장 provider 6개 중 몇 개가 로드되는가"는 **측정하지 못했다** — 부분 클래스패스에서 `ServiceConfigurationError` 가 났고, 그때 나온 "provider 1개" 는 측정이 아니라 클래스패스 결함이다. 증거: `evidence/g0-0b1-local-s2s3.json`

`analyze-trace.py`의 합성 추적 시험은 false positive를 놓쳤다. 8차 리뷰에서 task preamble failure가
없고 결과가 `ERROR`여도 `coverage=PROVEN`, exit 0이 되는 반례를 재현했다.

병합 시점(2026-08-30)의 정리 — 위 두 회차를 합쳐도 **§1 의 질문 세 개는 여전히 미측정이다.**
답하려면 실제 Oracle 이 필요하다(계획서 S4~S6).
