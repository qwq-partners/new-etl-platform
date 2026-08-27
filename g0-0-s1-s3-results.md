# G0-0 S1~S3 실행 결과 — 첫 실측 회차

> **회차 식별자**: `RUN-2026-08-27-SBX-01` · **profile**: `SANDBOX_CONTAINER`
> **`versions_lock_digest`**: `14ba8f52d68734434d08881abcceb176142df584121d1d7867e919ae102cb160`
>
> 이 저장소의 산출물이 **처음으로 실행된 기록**이다. `etl-platform-local-poc-plan.md` 의 S0~S8 중
> **S1·S2·S3 만** 돌았다. S4 이후는 이 환경에서 실행 불가였다(§5).

---

## 0. 이 회차의 환경 — 먼저 읽어라

**이 회차는 로컬 WSL2 회차가 아니다.** 계획서가 상정한 `~/g0/`(WSL2 Ubuntu 24.04 · 24 vCPU · 31Gi)가
아니라, **Oracle 을 띄울 수 없는 일회성 원격 컨테이너**에서 돌았다.

| | 계획서의 LOCAL_WSL | **이 회차 (SANDBOX_CONTAINER)** |
|---|---|---|
| vCPU / RAM | 24 / 31Gi | 4 / 15Gi |
| Oracle 컨테이너 | 가능 | **불가** — 이미지 blob 호스트가 egress 정책에서 차단(§5) |
| 수명 | 지속 | 세션 종료 시 파기 |

그래서 증거 계약에 세 번째 profile 값 `SANDBOX_CONTAINER` 를 신설했다(`g0-evidence.schema.json`,
`g0-normalize.py`). `LOCAL_WSL` 로 찍는 것은 **거짓 라벨**이고, 라벨을 비워 두는 것은 계약 위반이다.
이 profile 로 정규화하면 증거에 다음이 박힌다.

> `profile=SANDBOX_CONTAINER` — 이 증거는 **하네스 동작 확인용**이며 설계 주장의 근거가 아니다.
> 이 환경에는 Oracle 서버가 없다. 원천에 붙는 모든 측정은 미실행이며, 여기서 확인된 것은
> 코드가 그 Spark 판본에 대해 컴파일·배선되는가 뿐이다. **LOCAL_WSL 보다 제약이 강하다.**

### 증거 등급 (계획서 §1 기준)

이 회차의 산출은 **전부 H** 다 — "하네스가 돈다 / 이 결함이 코드 안에 있다". **D 는 하나도 없다.**
D 로 올라가려면 원천에 붙어야 하는데 이 회차는 붙지 못했다.

---

## 1. 실행한 것과 결과

| # | 단계 | 성공 판정(계획서) | 결과 |
|---|---|---|---|
| S1 | Spark 설치 | `spark-submit --version` 출력 + sha 기록 | **통과** — 3판본. `versions.lock` 기록 |
| S2 | **B1 컴파일** | `build.sh` exit 0 + SPI 등록 출력 | **통과** — 3판본 모두 |
| S3 | B1 SPI 배선 | conf 있으면 tracer 도달 / 없으면 provider 중복 예외 | **통과(단, 판정문 정정 필요 — §4-1)** |

S0(작업 사본·`fs.inotify.max_user_instances`)은 **해당 없음**으로 처리했다. 그 파라미터는 k3d 를 위한
것이고 k3d 는 S8 이후이며, 이 컨테이너는 애초에 k3d 를 띄울 수 없다.

측정에 쓴 판본 (`versions.lock` 전문 참조)

```
JDK      OpenJDK 21.0.10+7-Ubuntu-124.04   (build.sh --release 17 → 바이트코드 major=61 실측)
Spark    4.2.0        / Scala 2.13.18      spark-4.2.0-bin-hadoop3            ← lock 의 pin
         3.5.9        / Scala 2.12.18      spark-3.5.9-bin-hadoop3
         3.5.9        / Scala 2.13.8       spark-3.5.9-bin-hadoop3-scala2.13
ojdbc    ojdbc11-23.9.0.25.07.jar          sha256 f52e9335…bca1da  (Maven Central 서명 일치)
python   3.11.15 · pyspark 는 spark-submit 번들 사용
```

---

## 2. S2 — B1 컴파일 (계획서가 "최속 신호"라 부른 지점)

**결과: 세 판본 모두 `exit 0`.** `javac --release 17 -Xlint:all` 에서 **우리 코드에 대한 경고 0건**.
`META-INF/services/org.apache.spark.sql.jdbc.JdbcConnectionProvider` 등록도 jar 안에서 확인됐다.

이것으로 B1 README §8 의 상태 서술이 갱신된다 — "실제 Spark jar 에 대고는 아직 빌드하지 않았다"는
더 이상 사실이 아니다.

### F1 — Scala 2.12/2.13 SPI 시그니처 분기 우려는 성립하지 않았다

`versions.lock` 이 `scala:` 항목에 달아 둔 주석은 이랬다.

> `2.12 | 2.13 — JdbcConnectionProvider SPI 시그니처가 갈린다`

**같은 Java 소스 3파일이 2.12.18·2.13.8·2.13.18 모두에 대해 무수정 컴파일됐다.** `scala.collection.
immutable.Map` / `scala.Option` / `scala.Tuple2` 의 **Java 에서 본 시그니처**는 두 계열에서 같다.
주석은 남겨 두되(런타임 바이너리 호환은 별개다) "컴파일이 갈린다"는 예상은 **실측으로 기각**한다.

### F2 — `bad path element` 경고 19건은 우리 결함이 아니다

빌드 로그에 `derby.jar` / `scala-library.jar` 등을 못 찾는다는 경고가 19건 나온다. 추적한 결과
**서드파티 jar 의 MANIFEST `Class-Path`** 에서 온 것이다.

```
derby-10.16.1.1.jar        Class-Path: derbyshared.jar derbyLocale_*.jar …
derbytools-10.16.1.1.jar   Class-Path: derby.jar derbyclient.jar derbynet.jar
scala-compiler-2.13.18.jar Class-Path: scala-reflect.jar scala-library.jar
```

배포판은 버전 붙은 이름(`derby-10.16.1.1.jar`)으로 담는데 매니페스트는 버전 없는 이름을 가리킨다.
javac 가 그 링크를 따라가 못 찾고 경고한다. **빌드 결과에 영향이 없다.** 다음 회차 운영자가 이걸
빌드 실패 징후로 오독하지 않도록 `build.sh` 에 주석으로 박았다.

---

## 3. S3 — SPI 배선 (Oracle 서버 없이)

도달 불가 URL(`jdbc:oracle:thin:@//127.0.0.1:1/G0B1_NO_SUCH_SERVICE` — 아무도 listen 하지 않는
loopback 포트)로 두 회차를 돌렸다. **어떤 실서버에도 로그온을 시도하지 않았다**(안전 규칙 §3.1-2).

| 판본 | conf 있음 → tracer 라인 | conf 없음 → tracer 라인 |
|---|---|---|
| Spark 4.2.0 (2.13.18) | **3** (전부 `SCHEMA`) | 0 |
| Spark 3.5.9 (2.12.18) | **3** (전부 `SCHEMA`) | 0 |
| Spark 3.5.9 (2.13.8) | **3** (전부 `SCHEMA`) | 0 |

`TASK` 경로가 0건인 것은 정상이다 — connection 이 열리지 않으니 task 가 시작되지 않는다.
**`SCHEMA`·`TASK` 2경로 커버리지는 이 회차에서 측정되지 않았다.**

### F3 — 배선은 성립한다. 분류기도 맞았다

tracer 가 실제로 불린 지점은 `ConnectionProviderBase.create` 였고, 그 위 스택은 계획이 예상한 그대로다.

```
ConnectionProviderBase.create
  ← JdbcDialect.$anonfun$createConnectionFactory$1
  ← JdbcUtils$.withConnection
  ← JDBCRDD$.resolveTable          ← Trace.classify 가 SCHEMA 로 판정한 근거
  ← JDBCRelation$.getSchema
  ← JdbcRelationProvider.createRelation
```

`open_error` 에는 실제 드라이버 오류가 그대로 남았다 — `ORA-12541: Cannot connect. No listener at
host 127.0.0.1 port 1`. 즉 프리앰블 이전 단계까지는 전 경로가 살아 있다.
`driver_props_passed` 는 `["password","user"]` 로, Spark 전용 키가 드라이버로 새지 않았다.

### F4 — ★ conf 를 빠뜨리면 **Spark 4.2.0 은 원인을 감춘다**

conf 없는 회차에서 세 판본 모두 job 이 죽었다. 그런데 **운영자가 보는 메시지가 판본마다 다르다.**

```
Spark 3.5.9  IllegalArgumentException: JDBC connection initiated but more than one
             connection provider was found. Use 'connectionProvider' option to select
             a specific provider. Found active providers [BasicConnectionProvider@…,
             etl.g0b1.TracingConnectionProvider@…]

Spark 4.2.0  AnalysisException: [FAILED_JDBC.CONNECTION] Failed JDBC jdbc:oracle:***
             (redacted) on the operation: Couldn't connect to the database SQLSTATE: HV000
```

**4.2.0 의 메시지는 네트워크 장애처럼 읽힌다.** 실제로는 connection 을 만들려는 시도조차 없었다.
`JdbcUtils.classifyException` 이 provider 선택 예외를 `FAILED_JDBC.CONNECTION` 으로 덮는다.

원인이 정말 그것인지는 추정으로 두지 않고 **Spark 의 선택 지점을 직접 호출해** 확인했다
(`ConnectionProvider$.MODULE$.create(driver, opts, None)`).

```
RESULT_EXC_CLASS: java.lang.IllegalArgumentException
RESULT_EXC_MSG:   JDBC connection initiated but more than one connection provider was found.
                  … Found active providers [BasicConnectionProvider@622ef26a,
                  etl.g0b1.TracingConnectionProvider@41de5768]
```

`v4.2.0` 소스도 같은 것을 말한다 — `ConnectionProviderBase.create` 는
`filteredProviders.size != 1` 이면 던진다.

**함의 두 가지.** 방향을 섞지 마라.

1. **jar 가 classpath 에 있고 conf 만 빠진 경우 → fail-closed 다.** 조용히 Basic 으로 넘어가지
   않는다. 다만 4.2.0 에서는 "DB 에 못 붙었다"로 보이므로 **conf 누락을 원천 장애로 오진**하기 쉽다.
2. **jar 자체가 안 올라간 경우 → 조용히 Basic 이 쓰인다.** 예외가 없다. 이쪽이 실제 위험이다.
   유일한 탐지 신호는 **추적 라인 0건**이고, `analyze-trace.py` 는 그것을 `MEASUREMENT_FAILED`
   (exit 5)로 낸다 — 이 판정이 왜 `NOT_PROVEN` 과 분리돼야 하는지가 여기서 실증됐다.

### F5 — ★ `disabledJdbcConnProviderList=basic` 은 Kerberos 원천에서 충분하지 않다

두 Spark 판본 모두 내장 provider 6종을 싣고 있고 그중에 **`OracleConnectionProvider`(name = `oracle`)**
가 있다. 이 회차에서 그것이 걸리지 않은 이유는 `canHandle` 이 Kerberos 를 요구하기 때문이다.

```scala
// SecureConnectionProvider (OracleConnectionProvider 의 상위)
canHandle = keytab != null && principal != null && driverClass == "oracle.jdbc.OracleDriver"
// BasicConnectionProvider
canHandle = keytab == null || principal == null
```

두 조건은 **정확히 배타적**이다. 따라서

- Kerberos **미사용**: 후보 = { Basic, ours } → `basic` 하나만 끄면 된다 ← 이 회차
- Kerberos **사용**: 후보 = { Oracle, ours } → `basic` 을 꺼도 여전히 2개다. **같은 예외가 난다.**
  그때는 `spark.sql.sources.disabledJdbcConnProviderList=basic,oracle` 이어야 한다.

사내 원천의 인증 방식이 정해지기 전에는 이 conf 값을 규범에 고정하지 마라.

### F6 — 판정기가 합성이 아닌 실제 추적에서 처음 검증됐다

`analyze-trace.py` 의 판정 로직은 그동안 합성 추적으로만 확인돼 있었다. 이번에 실제 산출로 돌렸다.

| 입력 | 판정 | exit |
|---|---|---|
| 추적 0건 (conf 없는 회차) | `MEASUREMENT_FAILED` | **5** |
| `SCHEMA` 3건 · 프리앰블 0/3 · fail-closed 회차 없음 | `NOT_PROVEN` (blocking 3건 열거) | **3** |

README §3 의 종료 코드 표와 일치한다.

**범위를 좁혀 읽어라 — 검증된 것은 두 음성 경로뿐이다.** `PROVEN` 경로는 이 회차에서
한 번도 태워지지 않았다. 7차 교차 리뷰 P0-06 은 바로 그 경로의 허점을 지적한다 —
`fail=all` 이 schema connection 에서 즉시 던져 task connection 에 도달하지 못해도
analyzer 가 fail-closed 를 `YES` 로 두고 `PROVEN` 이 가능하다는 것. **이 회차는 그 지적을
반증하지도 확증하지도 않았다.** 그 판정에는 실제 Oracle 이 필요하다.

### F7 — 증거 계약이 처음으로 산출물을 통과했다

`g0-normalize.py --profile SANDBOX_CONTAINER --b1 … --versions-lock versions.lock` 이 레코드를 쓰고
`g0-evidence.schema.json`(Draft 2020-12) 자기 검증을 **위반 0건**으로 통과했다. capability 축 7개는
전부 `UNDETERMINED` 로 남았다 — G0-0A 를 돌리지 않았으니 그게 맞는 값이다.

**"통과했다"는 도구가 도는가에 대한 사실이지 계약이 옳은가에 대한 사실이 아니다.** 7차 교차 리뷰는
그 계약 자체를 P0 로 판정한다 — P0-02(불완전·조작 산출물이 `MEASURED` 가 된다),
P0-03(증거가 대상·시각·판본에 묶이지 않는다), P0-04(`g0_evidence` 라는 한 이름이 서로 다른 두
계약을 가리킨다). 이 회차가 통과한 것은 **그 결함들을 그대로 안은 계약**이다. 이 F7 을
"증거 계약이 검증됐다"로 인용하지 마라.

### F8 — 부수 확인

- **pyspark 별도 설치 불필요.** `spark-submit` 이 `$SPARK_HOME/python` 을 PYTHONPATH 에 넣는다.
  `versions.lock` 의 `pyspark: UNSET` 은 `BUNDLED` 로 바뀐다.
- **Spark 3.5.9 가 JDK 21 에서 이 경로를 완주했다.** 3.5.x 의 공식 지원은 Java 8/11/17 이므로
  이 관측을 "3.5.9 는 JDK 21 을 지원한다"로 일반화하지 마라. **JDBC 읽기 경로 한 줄기만 봤다.**
- `run-g0-0b1.py` 는 `status: "ERROR"` 를 내면서도 **exit 0** 을 반환한다. README §3 이 "이 스크립트는
  판정하지 않는다"고 규정하므로 설계상 의도지만, `run.sh` 를 거치지 않고 직접 부르면 성공으로 보인다.
  결함으로 단정하지 않고 관찰로 남긴다.

---

## 4. 계획서·README 정정

### 4-1. S3 성공 판정문 (`etl-platform-local-poc-plan.md` §3)

> ~~conf **있으면** tracer 도달, **없으면** `more than one connection provider` 예외~~

**예외는 나지만 그 문구는 Spark 4.2.0 에서 보이지 않는다.** 판정을 예외 메시지로 하면 4.2.0 에서
S3 를 "실패"로 오판한다. 판정 기준은 **추적 라인 수**여야 한다 — conf 있으면 ≥1, 없으면 0.

### 4-2. B1 README §8 (현재 상태)

"실제 Spark jar 에 대고는 아직 빌드하지 않았다" → 3판본 빌드·배선 완료로 갱신.

### 4-3. B1 README §3 (반드시 필요한 conf)

`basic` 만으로 충분하다는 서술에 Kerberos 단서를 붙인다(F5).

---

## 5. 실행하지 못한 것 — S4 이후

**차단 사유: 컨테이너 이미지 blob 호스트가 egress 정책에서 거부된다.**

```
docker pull → https://production.cloudfront.docker.com/… → 403 (CONNECT rejected by policy)
```

registry 인증과 manifest 조회까지는 통과하고 **layer blob 내려받기에서 막힌다.** 우회 시도는 하지
않았다. 따라서 이 환경에서 다음은 **미실행**이며, "안 나왔다"가 아니라 **측정 불가**다.

| 단계 | 내용 | 상태 |
|---|---|---|
| S4 / S4.5 | Oracle Free 컨테이너 · sqlplus · fixture | **불가** (이미지 반입 차단) |
| S5 | G0-0A 86 probe + C00 | **불가** (S4 의존) |
| S6 | B1 본실행 coverage / failclosed | **불가** (S4 의존) |
| S7 | B0 stock smoke 대조군 | **불가** (S4 의존) |
| S8 | **CE01~CE09 최초 실행** | **불가** (S4 의존) |

계획서가 "로컬의 최고 가치"라 부른 S8 은 그대로 남아 있다. **S1~S3 은 그 앞의 관문이었고, 이제
그 관문은 통과했다.**

---

## 5.1. 7차 교차 리뷰(`c59bf6d`)와의 관계

이 회차와 병행해 `main` 에 7차 교차 리뷰가 올라왔다(`etl-platform-v2.0-codex-seventh-cross-review.md`).
그 리뷰는 `538ec31` 을 기준판으로 하고 **"G0-0 은 한 번도 실행되지 않았다"** 를 전제로 쓰였다.
이 회차가 그 전제를 **부분적으로** 낡게 만든다 — S1~S3 은 실행됐다. 다만 리뷰가 겨눈 P0 6건은
**하나도 해소되지 않는다.** 대조는 다음과 같다.

| 리뷰 항목 | 이 회차와의 관계 |
|---|---|
| P0-01 `derive_axes()` 승격 오류 | **무관.** G0-0A 를 안 돌렸다. 축은 전부 `UNDETERMINED` 였다 |
| P0-02 불완전 산출물이 `MEASURED` | **미해소.** 이 회차는 그 결함을 안은 계약을 통과했을 뿐이다(F7) |
| P0-03 대상·시각·판본 binding | **일부 실증.** `versions_lock_digest` 는 정규화 시점 해시이며 B1 산출물이 스스로 기록한 것이 아니다 — 리뷰 지적 그대로다 |
| P0-04 `g0_evidence` 두 계약 | **미해소.** 이 회차는 `profile` enum 만 늘렸다. 스키마 분리는 하지 않았다 |
| P0-05 overlay 축 병합 | **무관** |
| P0-06 B1 이 task-path fail-closed 없이 `PROVEN` | **미판정.** `PROVEN` 경로를 태우지 못했다(F6 단서 참조) |
| §7 "`JdbcConnectionProvider` 는 `DeveloperApi`/`Unstable` 이며 provider 선택은 pinned runtime 에서 검증해야 한다" | **이 회차가 그 검증이다.** 3판본에서 build·ServiceLoader·provider selection 을 실측했다 |
| §8 "`c2fa93b` 는 P0 를 해소하지 않는다" | 이 회차(`0b95fc2`)도 마찬가지다. **해소하지 않는다** |

리뷰 §6 의 권고 실행 순서는 4번에서 "B1 을 path-specific fail-closed 하네스로 고친 뒤 pinned
Spark 에서 build/실행한다"고 한다. 이 회차는 그중 **build/실행 절반만** 먼저 한 것이며,
**하네스 수정을 대신하지 않는다.**

---

## 6. 다음 회차(WSL2)에서 할 일

1. `versions.lock` 을 **덮어쓴다.** `profile: LOCAL_WSL` 로 바꾸고 JDK·Spark·python 을 그 호스트
   실측값으로 다시 적는다. 이 파일의 sha256 이 증거에 박히므로 그대로 두면 안 된다.
2. **S1·S2 는 재실행한다.** 이 회차의 통과는 *이 컨테이너의 JDK 21* 에 대한 것이다. 다만 이제
   "실패하면 SPI 시그니처 문제"가 아니라 "실패하면 그 호스트의 JDK 문제"로 범위가 좁아졌다.
3. **S3 는 추적 라인 수로 판정한다**(§4-1).
4. S4 부터가 새 미지수다. 계획서 §3 의 S4 분기 확인(`ALTER SESSION SET STANDBY_MAX_DATA_DELAY`
   가 PRIMARY 에서 거부되는가)을 sqlplus 한 줄로 먼저 하라.
5. 사내 Spark 판본이 정해지면 **그 판본으로 S2 를 다시 돌린다.** 이 회차가 3판본을 커버한 것은
   보험이지 대체가 아니다.

---

## 7. 이 회차가 만든 파일

| 파일 | 변경 |
|---|---|
| `versions.lock` | JDK·Spark·Scala·python·ojdbc 실측 기입, `profile: SANDBOX_CONTAINER` |
| `g0-evidence.schema.json` | `profile` enum 에 `SANDBOX_CONTAINER` 추가 |
| `g0-normalize.py` | 같은 profile 값 + 그 profile 전용 경고문 |
| `g0-0b1-connection-provider/build.sh` | `bad path element` 경고 해설 주석(F2) |
| `g0-0b1-connection-provider/README.md` | §3 Kerberos 단서(F5) · §8 상태 갱신 |
| `etl-platform-local-poc-plan.md` | S3 판정 기준 정정(§4-1) |
| **`g0-0-s1-s3-results.md`** | 이 문서 |
