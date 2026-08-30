# 로컬 WSL2 G0-0 실행 계획

> 작업 사본: `~/g0/` (**ext4**). `/mnt/c` 에서는 실행하지 않는다 — drvfs/9p 는 파일 락·fsync 의미가 달라 DB 파일과 docker 레이어를 두면 안 된다.
>
> **이 계획의 한 문장**: 로컬은 "사내 PoC 의 축소판"이 아니라 **하네스를 처음 돌려 보는 곳**이다.
>
> **2026-08-30 상태**: B1의 partial Maven classpath compile·SPI linkage만 수행됐다. 8차 리뷰에서
> M0 실행 안전성·M1 child evidence contract·M2 B1 경로 검증 전에는 S5~S8 결과를
> 신뢰 가능한 G0-0 증거로 받을 수 없다고 판정했다. 이 문서는 수정 후 목표 계획이며,
> 현재 스크립트를 사내 원천에 실행하라는 승인이 아니다.

실측 자원: WSL2 Ubuntu 24.04 · vCPU 24 · RAM 31Gi(가용 29Gi) · `/` 778G · systemd=true · 컨테이너 도구 없음.

---

## 1. 로컬이 증명하는 것 / 증명하지 못하는 것

**이 문서에서 가장 중요한 표다.** 로컬 결과를 사내 설계 근거로 오독하면 이 프로젝트의 규율이 무너진다.

| 등급 | 뜻 | 사내 이전 |
|---|---|---|
| **H** | 하네스가 돈다 / 이 결함이 코드 안에 있다 | 하네스에 대한 사실로만. **설계에 대해 아무 말도 하지 않는다** |
| **D** | Spark 판본 내부 사실 또는 SQL 의미론의 **존재 증명** | **조건부**. 조건 불충족 시 H 로 강등 |
| **X** | 현재 로컬 계획 범위 밖 | 이전 없음. `REQUIRES_CORP_ENV`·`NOT_INDUCED`·`NO_TEST_ARTIFACT`·`G1_SCOPE`·`LOCAL_DEFERRED`를 구분하며, **원리적 측정 불가라는 뜻이 아니다** |

> **2026-08-30(8차 M4-5) — X 의 사유를 왜 나누는가.** 이전 판의 X 정의는 "원리적으로 측정
> 불가"였고 8차 리뷰가 그것을 틀렸다고 판정했다. METADATA 경로는 다른 DSv2 시나리오로,
> F-13 은 반복·대기 실행으로, 규모는 적절한 인프라에서, 플랫폼 거동은 코드가 생기면,
> ADG·라이선스 거동은 사내 환경에서 **전부 측정 가능**하다. "지금 여기서 못 잰다"와
> "잴 수 없다"를 같은 칸에 넣으면 **후속 측정 계획이 서지 않는다** — 무엇이 갖춰지면 잴 수
> 있는지가 사유 이름 자체에 들어 있어야 한다.
>
> `UNMEASURABLE` 이라는 값은 **쓰지 않는다.** 정말로 operationalize 할 수 없는 주장이 나오면
> 그때 도입하고, 그때도 "무엇을 관측하면 반증되는가"를 못 적으면 그것은 주장이 아니라 신념이다.

### H — 로컬의 주된 산출

- G0-0A 87 probe 완주 — producer `exit 0` ∧ `probe_run_end` sentinel ∧ `manifest_ok=true` ∧ `emitted=87`. **넷 다여야 한다**
- `c_expected=87` 정적 일치 확인. 실제 Oracle 완주는 아직 없으므로 경험적 확정으로 부르지 않는다
- §8 이식성 21건의 파싱 가능성 — `ORA-00900`/`ORA-06550`/미정의 `&변수` 같은 실행 차단 결함 색출
- B1 컴파일 + SPI 배선
- **CE01~CE09 최초 실행** — 특히 CE08 의 `SIGKILL`, CE06 의 다중 connection, CE02 의 미commit 유지. **사내 생산라인 DB 에서는 영원히 못 돌린다. 로컬의 최고 가치다**

### D — 조건부 이전

같은 Spark 버전만으로는 부족하다. **2026-08-30(8차 M4-5) — 그 "조건"을 산문이 아니라 술어로
고정한다.** 아래가 **전부** 같아야 D 항목이 이전 후보가 되고, 하나라도 다르거나 증거에
결속돼 있지 않으면 그 항목은 **H 로 강등**된다.

```text
exact Spark distribution/build + full classpath   (partial Maven classpath 는 이 조건을 만족하지 않는다)
Scala / JDK bytecode level
OJDBC jar digest
provider jar / source / config digest
deployment mode(driver·executor topology)
datasource 경로(DSv1 / DSv2)와 query·options
profile / source identity
```

> **오늘 이 술어를 만족하는 것은 무엇인가.** 로컬 회차는 **partial Maven classpath 에서의
> compile 과 ServiceLoader 도달** 두 가지뿐이다. 아래 목록의 나머지는 **D 로 분류돼 있으나
> 현재 조건을 만족하지 못하므로 지금은 이전할 수 없다** — 목록에 있다는 것은 "조건이 갖춰지면
> 이전 후보"라는 뜻이지 "지금 이전 가능"이 아니다. 특히 **SCHEMA·TASK 커버리지 · fail-closed ·
> connection peak 는 full distribution 회차 전에는 인용 금지**다.

- **SCHEMA·TASK 2경로 커버리지** / **fail-closed 성립 여부**(어느 경로가 예외를 삼키는가)
- 한 회차의 물리 connection 개수·open/close 패턴(NEW-05/13/18)
- `sessionInitStatement` 가 단일 statement 로만 실행되고 schema 경로를 우회(NEW-03/04)
- CE02·CE04·CE05·CE06·CE07 — Oracle read consistency·SQL 의미론이라 에디션 무관. **단 "그 위험이 실재한다"는 존재 증명으로만.** "얼마나 자주 일어나는가"로는 **절대 이전 금지**
- CE01 typed successor / overflow ORA 코드 — **로컬은 26ai, 원천은 19c 계열.** 판본 병기 필수

### X — 현재 로컬 계획 범위 밖

| 항목 | 근거 |
|---|---|
| **ADG lag 거동 일체** (`ORA-03172`, `ORA-03173`, `STANDBY_MAX_DATA_DELAY` 실효) | `REQUIRES_CORP_ENV` — §2 |
| `DATABASE_ROLE='PHYSICAL STANDBY'` **양성** 분기 | `REQUIRES_CORP_ENV` — 로컬 역할은 항상 PRIMARY. Preamble의 음성 경로만 시험된다 |
| 원천의 실제 capability 값 (charset·`MAX_STRING_SIZE`·`ROWDEPENDENCIES`·실제 grant/profile) | `REQUIRES_CORP_ENV` — 로컬 계정은 우리가 만든 것이므로 권한 결과는 자기가 정한 값의 되읽기일 뿐 |
| canonical hash 벡터 V-01~V-16 | `REQUIRES_CORP_ENV` — 로컬은 AL32UTF8 고정이다. 원천이 `KO16MSWIN949`면 로컬 통과를 인용하지 않는다 |
| **METADATA 경로 커버리지** | `NOT_INDUCED` — 현재 하네스가 유발하지 않는다. 다른 DSv2 trigger/config로 후속 측정 가능 |
| F-13 유휴 정지 | `LOCAL_DEFERRED` — 하루 2~3회·최소 3일 반복이 필요한 후속 측정 |
| 규모 (10k Job / 40k Run / burst 500) | `G1_SCOPE` — 단일 호스트 범위 밖이며 G1에서 측정 |
| **플랫폼 자체** (Control Plane·Guard·lease·Commit Adjudication·FI-01~66) | `NO_TEST_ARTIFACT` — 플랫폼 코드가 없어 현재 시험 대상이 없고 구현 뒤 측정 가능 |

### 현재 오독 방지 경고는 불충분하다

`g0-normalize.py` 는 `--profile LOCAL_WSL` 이면 증거에 다음을 박는다.

> `profile=LOCAL_WSL — 이 증거는 하네스 동작 확인용이며 설계 주장의 근거가 아니다.`

**로컬 증거 정규화는 `--profile LOCAL_WSL` 로 해야 하지만 이것만으로 충분하지 않다.** profile은 caller 입력이라
`CORP_POC`로 다시 붙일 수 있다. M1에서 child가 관측한 source/runtime/profile과 consumer-side 허용 범위를 결속하고,
불일치·누락·재라벨을 fail-closed로 거부해야 한다.

> **2026-08-30 — 8차 §8.4 누수 경로 4건의 현재 상태.**
>
> | 누수 경로 | 상태 |
> |---|---|
> | normalizer 가 caller 의 `--profile` 을 믿는다 | **닫힘(M1).** child manifest 의 `profile` 과 대조하고 다르면 exit 4 다. **재라벨은 child 를 다시 만들지 않으면 불가능하다** |
> | child evidence 에 profile/run/source/lock digest 가 없다 | **닫힘(M1-2).** `source_id`·`harness_digest`·`started_at`/`ended_at` 까지 manifest 에 박힌다 |
> | normalization 당시 lock 을 사후 hash 한다 | **닫힘(M1).** child 가 **실행 시점** digest 를 적고 집계기가 그것과 대조한다 |
> | local B1 evidence 에 raw `javap`/ServiceLoader 출력과 artifact hash 가 없다 | **열려 있다.** `g0-0-s1-s3-results.md` 에 산문으로 있고 기계 판독 산출물이 아니다 — S2·S3 회차를 `g0-run-child.sh` 로 감싸야 닫힌다 |
>
> 그리고 M3-3 이 하나를 더 닫았다. **`--profile LOCAL_WSL`·`SANDBOX_CONTAINER` 회차는 모든 축의
> `effective_value` 가 floor 로 내려간다**(`PROFILE_NOT_AUTHORITATIVE`). 그 전에는 레코드가
> "설계 주장의 근거가 아니다"라고 경고하면서 동시에 확정 capability 값을 싣고 있었다 —
> 소비자가 그 값을 읽으면 경고는 아무것도 막지 못한다. 이제 읽히는 값 자체가 floor 다.
>
> **남은 것은 소비자 쪽이다.** `gate_eligible=false` 와 floor 는 기계 소비를 막지만
> **사람이 문서에 옮겨 적는 것**은 막지 못한다. 그 경로는 이 표(§1)가 유일한 방어다.

> 2026-08-27 추가 — profile 값이 하나 늘었다. `SANDBOX_CONTAINER` 는 **Oracle 을 띄울 수 없는
> 일회성 원격 컨테이너** 회차용이며, 제약이 `LOCAL_WSL` 보다 **강하다**(원천에 붙는 측정이 전부
> 미실행). WSL2 회차를 그 값으로 찍지 마라. 반대도 마찬가지다.

---

## 2. ADG 문제의 답

### 판정: 로컬에서 Data Guard 를 구성할 수 없다 — 확정

Oracle Database Licensing Information, Table 1-5 High Availability. Free 열이 전부 **N** 이다.

```
Data Guard—Redo Apply = N       Data Guard—SQL Apply = N
Data Guard—Snapshot Standby = N Real-Time Cascading Standbys = N
Active Data Guard = N           Flashback Transaction Query = N
```

XE 21c 도 같다. **evidence 에 "Data Guard 가 없다"로 뭉뚱그리지 말고 위 행을 그대로 인용한다.** 특히 `Active Data Guard=N` 이 `ORA-03172` 재현 불가의 직접 근거다.

### `environment_guard` — 끄지 않는다. 그러나 "그냥 두는 것"도 답이 아니다

소스 실측:

- `runner.py` 는 `DATABASE_ROLE != 'PRIMARY'` 면 중단한다. **로컬 단일 인스턴스가 이 조건을 정면으로 만족한다** — 완화가 불필요하다.
- `CE_STANDBY_DSN` 미설정 시 preflight 가 suite 의 expected 를 **그대로 복사**하고 `standby_verified=False` 를 찍는다.
- 그러면 `enforce_guard` 의 비교는 **자기 자신과의 비교(항등식)** 라 언제나 통과한다.

따라서 "가드가 하나도 빠짐없이 켜진 채로 성립한다"는 **과대 서술이다.** 2026-08-27 수정으로 `guard_checks` 에 다음이 남도록 했다.

```
standby_db_unique_name=NOT_CHECKED (CE_STANDBY_DSN 미설정 —
  expected 값을 복사해 자기 자신과 비교했다. 항등식이며 검증이 아니다)
```

**`CE_STANDBY_DSN` 을 설정하지 않는다.** 두 번째 Free 컨테이너를 standby 인 척 물리면 `standby_verified=true` 라는 **거짓**이 만들어진다 — 그것이 가드를 끄는 것보다 나쁘다.

---

## 3. 단계별 계획

> **실행할 때는 `g0-0-runbook.md` 를 편다.** 이 문서는 *무엇을 왜 그 순서로* 를 말하고,
> runbook 은 *어떤 명령을 어떤 순서로* 를 말한다. 2026-08-27 조치 이후 실행 순서가
> 바뀌었으므로(회차를 탐색/증거로 나눈다 — `versions.lock` 확정 시점 때문이다)
> 이 표만 보고 돌리면 집계가 거부된다.

크리티컬 패스는 **S0~S8**. 각 단계에 성공 판정과 실패의 의미를 붙인다.

| # | 무엇 | 성공 판정 | 소요(기대) |
|---|---|---|---|
| **S0** | 작업 사본 + 커널 파라미터 | `fs.inotify.max_user_instances` = 8192 (**실측 기본 128** — k3d 대표 실패 지점) | 1h |
| **S1** | Spark 설치 (Oracle·Docker·k8s 불필요) | `spark-submit --version` 이 판본·Scala 출력. sha 를 `versions.lock` 에 기록 | 1h |
| **S2** | **★ B1 컴파일** — 최속 신호 | `build.sh` exit 0 + `META-INF/services` 등록 출력 | 0.5h |
| **S3** | B1 SPI 배선 (여전히 Oracle 없이) | **추적 라인 수로 판정한다** — conf 있으면 ≥1, 없으면 0 (아래 정정). explicit `connectionProvider=g0b1tracer` 선택과 ServiceLoader 도달을 함께 본다. Basic 전역 disable 은 진단 fallback | 1h |
| **S4** | Docker + Oracle Free 컨테이너 (**k8s 아직 없음**) | 판본·charset·`memory_target`·`DB_UNIQUE_NAME` 실측 후 `versions.lock` 기록 | 3h |
| **S4.5** | 실행 전제물 3종 | ojdbc jar / sqlplus 경로 / fixture 테이블 | 2h |
| **S5** | G0-0A + C00 실행 | M0/M1 완료 후 A 네 조건 + C00 external completion contract | 2h |
| **S6** | **★★ B1 본실행** coverage + failclosed | M2 완료 후 explicit provider·schema/task/metadata 독립 회차와 주입 독립 oracle 통과 | 3h |
| **S7** | B0 stock smoke (대조군) | S1c·S2·S3 관측치 산출 | 1h |
| **S8** | **★ CE01~CE09 최초 실행** | 9개 실행 ∧ `injection_observed=true` ∧ `leftover_objects=[]` | 4h |

S9 이후(k3d + Dagster/Polaris/MinIO)는 **별도 투자**이며 **S8 종료가 하드 체크포인트**다. 여기서 시간이 새면 G0-0 이 또 밀린다 — 이 저장소가 이미 한 번 저지른 실수(측정 전 문서 개정)의 변형이다.

### S1~S3 은 이미 한 번 돌았다 (2026-08-27, profile `SANDBOX_CONTAINER`)

Oracle 을 띄울 수 없는 원격 컨테이너에서 S1·S2·S3 만 실행했다. 결과는 `g0-0-s1-s3-results.md`.
**로컬 WSL2 회차의 대체물이 아니다** — JDK·호스트가 다르므로 S1·S2 는 WSL2 에서 다시 돌린다.
다만 그 회차에서 나온 정정 두 건은 이 계획에 반영해야 한다.

**정정 1 — S3 의 성공 판정을 예외 메시지로 하면 안 된다.**
conf 없는 회차에서 예외는 세 판본 모두 났다. 그러나 **Spark 4.2.0 은 그 문구를 보여 주지 않는다** —
`JdbcUtils.classifyException` 이 `[FAILED_JDBC.CONNECTION] … Couldn't connect to the database` 로
덮는다(3.5.9 는 원문 노출). 메시지로 판정하면 4.2.0 에서 S3 를 실패로 오판한다.
판정은 **추적 라인 수**로 한다 — conf 있으면 ≥1, 없으면 0.

**정정 2 — `disabledJdbcConnProviderList=basic` 은 Kerberos 원천에서 부족하다.**
내장 `OracleConnectionProvider`(name = `oracle`)의 `canHandle` 은 keytab·principal 이 **둘 다 있을 때**
참이고, `BasicConnectionProvider` 는 **둘 중 하나라도 없을 때** 참이라 정확히 배타적이다.
Kerberos 를 쓰면 후보가 { Oracle, ours } 가 되어 `basic` 을 꺼도 여전히 2개다 → `basic,oracle`.

### S2 가 왜 최속인가

`build.sh`는 `$SPARK_HOME/jars` 외에 아무것도 요구하지 않는다. 여기서 답하는 것은
**compile/API linkage뿐**이며 full runtime·connection-path coverage·fail-closed는 남는다.

### S6 의 분기 — 반드시 먼저 확인할 것

로컬은 PRIMARY 라 `ALTER SESSION SET STANDBY_MAX_DATA_DELAY` 가 거부될 수 있다(**미확인** — S4 에서 sqlplus 한 줄로 확인하라). 거부되면 프리앰블이 던져 **coverage 회차가 통째로 failclosed 로 변질**되고 fail-closed 실험과 구분이 안 된다.

2026-08-27 수정으로 `run.sh` 6번째 인자에 `none` 을 주면 그 옵션을 아예 빼도록 했다.

```bash
./run.sh "$URL" ETL_PROBE ETL_PROBE.G0_TARGET FREE PRIMARY none
```

---

## 4. 판본 표기 — 흔한 오해

`gvenzl/oracle-free` 의 `23.26.x` / `23-*` / `latest-*` 태그는 **23ai 가 아니라 Oracle AI Database 26ai** 다(빌드 스크립트 주석: `23.26.0 and beyond calls it 26ai`). 진짜 23ai 는 `23.9` 계열을 명시 pin 해야 한다.

- **하네스 동작 검증**(CE 실행 가능성·B1 빌드·SQL 파싱) → 판본 무관. 최신 태그로 충분하다
- **19c 개연 논증에 쓰려는 SQL 의미론 축**(CE01 overflow ORA 코드 등) → 26ai 결과만으로 개연을 주장하지 마라. 필요하면 21c/23ai 로 2차 회차를 돌린다

**계정 권한은 측정이 아니라 선택이다.** 이미지 기본 계정은 `DB_DEVELOPER_ROLE` 만 받는다. 그러면 G0-0A 의 다수 probe 결과가 '에디션 사실'이 아니라 **'이 이미지의 grant 사실'** 이 된다. 의도적으로 정하고 evidence 에 `local_grant_set` 으로 라벨링하라. (CE01~CE09 는 특권 객체를 하나도 쓰지 않는다 — 위 grant 는 G0-0A 를 위한 것이다.)

---

## 5. 자원

Oracle 컨테이너 ~4Gi + Spark local[4] ~4Gi + 여유. **S0~S8 은 k8s 가 없으므로 피크가 10Gi 안쪽**이고 29Gi 대비 넉넉하다.

`.wslconfig` 는 **만들지 않는다.** Windows 호스트 총 RAM 63.6GiB 중 WSL 이 이미 기본 50% 를 받고 있다. `memory=26GB` 로 고정하면 가용치가 줄어들 뿐이고 적용에 `wsl --shutdown`(세션 파괴)이 따른다. 상한이 필요하면 그것은 k3d `--servers-memory`/`--agents-memory` 와 pod limits 로 건다 — **그쪽이 실제로 초과를 막는 지점이다**(k3d 노드는 기본적으로 메모리 limit 이 없어 kubelet 이 호스트 전체를 capacity 로 보고한다).

---

## 6. 하지 않을 것

| 안 함 | 이유 |
|---|---|
| Oracle 을 k8s 안에 넣기 | G0-0A 는 sqlplus, C 는 python-oracledb 로 **직접** 붙는다. StatefulSet·PVC·포트포워딩이 늘고 얻는 것이 0 |
| S8 이전에 k3d·Dagster·Polaris·MinIO | **어느 산출물도 k8s 를 요구하지 않는다** |
| `CE_STANDBY_DSN` 에 두 번째 컨테이너 물리기 | `standby_verified=true` 라는 거짓을 만든다 |
| `suite.local.yaml` 분리 | `artifact_hash` 가 디렉터리 전체를 순회하므로 파일 추가만으로 해시가 깨진다 |
| evidence 최상위에 새 필드 신설 | `evidence.schema.json` 이 `additionalProperties: false` 다 |
| `--jars` 로컬 경로로 Spark on K8s | Spark 문서상 동작하지 않는다. B1 추적 파일도 executor 에서 소실된다 |
| EE 이미지로 ADG 구성 | 이미지·RAM·설정 시간이 크고 라이선스 범위 문제가 있다. **크리티컬 패스에서 제외** |

---

## 7. 첫 세션에 할 일 다섯

1. **S0** — `~/g0/` 작업 사본, `fs.inotify.max_user_instances=8192`
2. **S1** — Spark 타르볼 전개, `spark-submit --version` 을 `versions.lock` 에 기록
3. **S2** — `./build.sh`. compile/API linkage만 확인하며 runtime·coverage·fail-closed는 답하지 않는다
4. **S3** — 도달 불가 URL로 explicit `connectionProvider=g0b1tracer` 선택과 SPI 도달 확인
5. `versions.lock` 의 JDK·Spark·Scala 항목을 실측값으로 채우고 sha256 을 기록

**1~4 는 Oracle 서버도 Docker 도 k8s 도 필요 없다.** — 2026-08-27 회차가 이것을 실증했다
(Oracle 없는 컨테이너에서 1~4 와 5 를 모두 완료). 다만 그 회차는 JDK 21 · 다른 호스트이므로
WSL2 에서 2~3 을 다시 돌린다.
