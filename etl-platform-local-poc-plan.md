# 로컬 WSL2 G0-0 실행 계획

> 작업 사본: `~/g0/` (**ext4**). `/mnt/c` 에서는 실행하지 않는다 — drvfs/9p 는 파일 락·fsync 의미가 달라 DB 파일과 docker 레이어를 두면 안 된다.
>
> **이 계획의 한 문장**: 로컬은 "사내 PoC 의 축소판"이 아니라 **하네스를 처음 돌려 보는 곳**이다.

실측 자원: WSL2 Ubuntu 24.04 · vCPU 24 · RAM 31Gi(가용 29Gi) · `/` 778G · systemd=true · 컨테이너 도구 없음.

---

## 1. 로컬이 증명하는 것 / 증명하지 못하는 것

**이 문서에서 가장 중요한 표다.** 로컬 결과를 사내 설계 근거로 오독하면 이 프로젝트의 규율이 무너진다.

| 등급 | 뜻 | 사내 이전 |
|---|---|---|
| **H** | 하네스가 돈다 / 이 결함이 코드 안에 있다 | 하네스에 대한 사실로만. **설계에 대해 아무 말도 하지 않는다** |
| **D** | Spark 판본 내부 사실 또는 SQL 의미론의 **존재 증명** | **조건부**. 조건 불충족 시 H 로 강등 |
| **X** | 원리적으로 측정 불가 | 이전 없음. "안 나왔다"는 부재의 증거가 아니라 **측정 불가**다 |

### H — 로컬의 주된 산출

- G0-0A 86 probe 완주 — `exit 0` ∧ `probe_run_end` sentinel ∧ `manifest_ok=true` ∧ `emitted=86`. **넷 다여야 한다**
- `c_expected=86` 의 경험적 확정 — 57→78→86 세 번 정정된 값이 처음으로 검증된다
- §8 이식성 21건의 파싱 가능성 — `ORA-00900`/`ORA-06550`/미정의 `&변수` 같은 실행 차단 결함 색출
- B1 컴파일 + SPI 배선
- **CE01~CE09 최초 실행** — 특히 CE08 의 `SIGKILL`, CE06 의 다중 connection, CE02 의 미commit 유지. **사내 생산라인 DB 에서는 영원히 못 돌린다. 로컬의 최고 가치다**

### D — 조건부 이전 (조건: 사내 Spark 가 **동일 판본 vanilla**)

- **SCHEMA·TASK 2경로 커버리지** / **fail-closed 성립 여부**(어느 경로가 예외를 삼키는가)
- 한 회차의 물리 connection 개수·open/close 패턴(NEW-05/13/18)
- `sessionInitStatement` 가 단일 statement 로만 실행되고 schema 경로를 우회(NEW-03/04)
- CE02·CE04·CE05·CE06·CE07 — Oracle read consistency·SQL 의미론이라 에디션 무관. **단 "그 위험이 실재한다"는 존재 증명으로만.** "얼마나 자주 일어나는가"로는 **절대 이전 금지**
- CE01 typed successor / overflow ORA 코드 — **로컬은 26ai, 원천은 19c 계열.** 판본 병기 필수

### X — 원리적으로 불가

| 항목 | 근거 |
|---|---|
| **ADG lag 거동 일체** (`ORA-03172`, `ORA-03173`, `STANDBY_MAX_DATA_DELAY` 실효) | §2 |
| `DATABASE_ROLE='PHYSICAL STANDBY'` **양성** 분기 | 로컬 역할은 항상 PRIMARY. Preamble 의 음성 경로만 시험된다 |
| 원천의 실제 capability 값 (charset·`MAX_STRING_SIZE`·`ROWDEPENDENCIES`·실제 grant/profile) | **로컬 계정은 우리가 만든 것이다.** 권한 결과는 자기가 정한 값의 되읽기일 뿐 |
| canonical hash 벡터 V-01~V-16 | 로컬은 **AL32UTF8 고정이고 바꿀 수 없다**(이미지에 DB 가 이미 생성돼 있다). 원천이 `KO16MSWIN949` 면 로컬 통과가 사내 통과를 시사하지 않는다 → **인용 금지** |
| **METADATA 경로 커버리지** | 하네스가 DSv1 만 쓰고 `spark.sql.catalog.*` 를 설정하지 않아 **유발조차 하지 않는다**. 로컬·사내 **공통 공백** |
| F-13 유휴 정지 | 하루 2~3회·최소 3일 반복 필요 |
| 규모 (10k Job / 40k Run / burst 500) | 단일 호스트. G1 소관 |
| **플랫폼 자체** (Control Plane·Guard·lease·Commit Adjudication·FI-01~66) | **저장소에 플랫폼 코드가 0줄이다.** 로컬이든 사내든 아직 시험 대상이 아니다 |

### 오독 방지 장치는 이미 있다

`g0-normalize.py` 는 `--profile LOCAL_WSL` 이면 증거에 다음을 박는다.

> `profile=LOCAL_WSL — 이 증거는 하네스 동작 확인용이며 설계 주장의 근거가 아니다.`

**로컬 증거 정규화는 반드시 `--profile LOCAL_WSL` 로 한다.** 새 장치를 만들 필요가 없다.

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

크리티컬 패스는 **S0~S8**. 각 단계에 성공 판정과 실패의 의미를 붙인다.

| # | 무엇 | 성공 판정 | 소요(기대) |
|---|---|---|---|
| **S0** | 작업 사본 + 커널 파라미터 | `fs.inotify.max_user_instances` = 8192 (**실측 기본 128** — k3d 대표 실패 지점) | 1h |
| **S1** | Spark 설치 (Oracle·Docker·k8s 불필요) | `spark-submit --version` 이 판본·Scala 출력. sha 를 `versions.lock` 에 기록 | 1h |
| **S2** | **★ B1 컴파일** — 최속 신호 | `build.sh` exit 0 + `META-INF/services` 등록 출력 | 0.5h |
| **S3** | B1 SPI 배선 (여전히 Oracle 없이) | conf **있으면** tracer 도달, **없으면** `more than one connection provider` 예외 | 1h |
| **S4** | Docker + Oracle Free 컨테이너 (**k8s 아직 없음**) | 판본·charset·`memory_target`·`DB_UNIQUE_NAME` 실측 후 `versions.lock` 기록 | 3h |
| **S4.5** | 실행 전제물 3종 | ojdbc jar / sqlplus 경로 / fixture 테이블 | 2h |
| **S5** | G0-0A + C00 실행 | 위 네 조건 + C00 세 값 산출 | 2h |
| **S6** | **★★ B1 본실행** coverage + failclosed | `verdict.coverage == PROVEN` ∧ failclosed 가 `EXPECTED_FAILURE_OBSERVED` | 3h |
| **S7** | B0 stock smoke (대조군) | S1c·S2·S3 관측치 산출 | 1h |
| **S8** | **★ CE01~CE09 최초 실행** | 9개 실행 ∧ `injection_observed=true` ∧ `leftover_objects=[]` | 4h |

S9 이후(k3d + Dagster/Polaris/MinIO)는 **별도 투자**이며 **S8 종료가 하드 체크포인트**다. 여기서 시간이 새면 G0-0 이 또 밀린다 — 이 저장소가 이미 한 번 저지른 실수(측정 전 문서 개정)의 변형이다.

### S2 가 왜 최속인가

`build.sh` 는 `$SPARK_HOME/jars` 외에 아무것도 요구하지 않는다. **저장소 최대 미지수의 절반이 Oracle 서버 없이, Docker 없이, k8s 없이 답해진다.** 실패해도 그것이 첫 측정 결과다.

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
3. **S2** — `./build.sh`. **여기서 저장소 최대 미지수의 절반이 답해진다**
4. **S3** — 도달 불가 URL 로 SPI 배선 확인 (conf 유/무 두 회차)
5. `versions.lock` 의 JDK·Spark·Scala 항목을 실측값으로 채우고 sha256 을 기록

**1~4 는 Oracle 서버도 Docker 도 k8s 도 필요 없다.**
