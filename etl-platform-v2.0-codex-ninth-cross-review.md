# ETL Platform v2.0 — 9차 Codex 교차 리뷰

- 리뷰일: 2026-08-31
- 대상 저장소: `qwq-partners/new-etl-platform`
- 변경 기준 커밋: `94a555dd80a192bd7ec12c861aa94ea29824abf5`
- 검토한 브랜치 tip: `d82e3d0`
- 요청서: `codex-cross-review-prompt-9th.md`
- 범위: M0~M4 폐쇄 재검증, 397개 회귀 시험 재현, M5 사내 원천 실행 가능 여부
- 중요한 한계: Oracle·Spark 클러스터·사내 원천에 대한 G0-0은 실행하지 않았다. 아래 실측은 저장소 하네스와 로컬 최소 클래스패스에 대한 것이다.

---

## 0. 최종 판정

### 0.1 한 문장

**M0~M4는 전부 닫히지 않았다. 397/397 회귀 통과는 재현되지만, 그 시험 묶음이 실물 실행 경로와 증거 결속을 충분히 검증하지 못한다. 따라서 현재 runbook 그대로 사내 원천에서 full G0-0을 실행하는 M5는 NO-GO다.**

아키텍처 방향인 **Dagster OSS + 얇은 Java Control Plane + Spark + Iceberg/Polaris**는 이 판정으로 기각되지 않는다. 이번 NO-GO는 플랫폼 방향이 아니라 **측정 하네스와 증거 계약의 실행 승인**에 대한 것이다.

### 0.2 결정표

| 결정 | 판정 | 이유 |
|---|---|---|
| 아키텍처 방향 유지 | **GO** | 이번 반례는 측정·증거 계층 결함이며 Dagster 전환 가치나 Control Plane 경계를 뒤집지 않는다 |
| M0~M4 전부 CLOSED | **NO-GO** | M0 PARTIAL, M1 OPEN, M2 OPEN, M3 PARTIAL, M4 PARTIAL |
| 397개 회귀 시험 통과 주장 | **확인** | Python 368 + Java 29를 같은 판에서 재현 |
| 397개 통과를 M5 안전성 증거로 사용 | **NO-GO** | 축약·합성 fixture와 실물 launcher 사이의 불일치가 존재 |
| 현재 runbook 그대로 full G0-0 실행 | **NO-GO** | 필수 환경변수·경로·인자 불일치로 일부 단계는 실행 자체가 불가능 |
| 생산라인 밀접 원천에 A/B0/B1/C00 일괄 실행 | **NO-GO** | 원천별 budget, fail-fast identity, query 시간·I/O 상한이 없다 |
| CE01~CE09를 사내 원천에서 실행 | **절대 NO-GO** | 쓰기 가능한 폐기용 primary 전용이며 corporate source evidence와도 분리해야 한다 |
| 제한된 사전 inventory | **조건부 GO** | 아래 §7의 M5a부터 단계별 해제 조건을 충족할 때만 가능 |
| v2.0 문서 동결 | **NO-GO** | 실행 계약과 사실 문서가 아직 같은 상태를 말하지 않는다 |

### 0.3 M별 폐쇄 판정

| M | 자체 주장 | 재판정 | 핵심 |
|---|---|---|---|
| M0 실행 안전성 | 완료 | **PARTIAL** | 종료 코드·일부 상한은 개선됐지만 원천 전체 세션 budget, 시간·I/O 상한, identity fail-fast가 없다 |
| M1 child evidence contract | 완료 | **OPEN** | A probe 집합·서버 신원·실행 profile·하네스 코드가 증거에 완전히 결속되지 않는다 |
| M2 B1 재작성 | 완료 | **OPEN** | 실제 launcher의 run label과 Python mode가 달라 `PROVEN`이 도달 불가능하다 |
| M3 normalizer | 완료 | **PARTIAL** | taxonomy·floor는 개선됐지만 입력 증거를 신뢰할 전제가 깨지고 기본 TTL·최종 gate가 fail-open이다 |
| M4 사실·규범 문서 | 완료 | **PARTIAL** | 핵심 Oracle/Spark 정정은 맞지만 과대 문구와 stale runbook/HANDOFF가 남았다 |

---

## 1. 검증 방법과 재현 결과

### 1.1 같은 판 확인

검토는 PR 브랜치 tip `d82e3d0`의 LF 보존 snapshot에서 수행했다. M0~M4 변경 기준은 요청서에 적힌 `94a555d`다. 공유 작업 폴더의 `main`을 전환하거나 사용자 변경을 덮어쓰지 않았다.

### 1.2 회귀 시험

다음 결과를 재현했다.

| 시험 | 결과 |
|---|---:|
| `g0-normalize-tests.py` | 147 PASS / 0 FAIL |
| `g0-axes-tests.py` | 127 PASS / 0 FAIL |
| `g0-b1-analyzer-tests.py` | 43 PASS / 0 FAIL |
| `g0-m0-safety-tests.py` | 51 PASS / 0 FAIL |
| `InjectionMatrix.java` | 29 PASS / 0 FAIL |
| **합계** | **397 PASS / 0 FAIL** |

Java 29건은 Spark 4.2.0 API를 흉내 낸 최소 클래스패스에서 빌드·실행했다. 이는 `Preamble.shouldFail`의 순수 매트릭스 검증이지, 실제 Spark 배포판·ServiceLoader·Oracle 연결·executor 분산 경로의 증거가 아니다.

### 1.3 왜 397 PASS와 NO-GO가 동시에 참인가

회귀 시험은 대체로 “주어진 합성 레코드를 판정기가 예상대로 분류하는가”를 잘 검사한다. 그러나 M5 승인을 위해 필요한 것은 다음까지 포함하는 **launcher → Spark/Oracle → raw artifact → manifest → normalizer** 종단 계약이다.

1. launcher가 실물 판정기가 요구하는 이름과 파일을 실제로 생성한다.
2. 산출물이 요청한 원천·테이블·profile에서 나온 것임을 서버 사실로 증명한다.
3. probe 집합이 빠지거나 새로 추가돼도 완결성 검사가 실패한다.
4. 행 수 제한이 아니라 세션·시간·I/O·재시도까지 원천 보호 한계가 선다.
5. corporate source와 폐기용 CE 환경의 증거가 섞이지 않는다.

현재 397건은 이 종단 성질을 시험하지 않는다. 특히 `g0-normalize-tests.py`의 양성 fixture와 B1 analyzer fixture가 실물 producer가 만들 수 없는 입력을 사용한다.

---

## 2. P0 — M5 실행을 막는 결함

### P0-01. A 완결성 검사가 선언된 probe 집합을 검증하지 않는다

`g0-normalize.py:357-392`의 `cov_a()`는 다음만 확인한다.

- summary 존재 여부
- end sentinel 존재 여부
- duplicate ID
- summary가 한 개인지
- `manifest_ok=false`인지

하지만 아래는 확인하지 않는다.

- 실제 parse된 probe 수가 `summary.emitted`와 같은가
- `summary.expected`가 현재 versioned manifest의 87과 같은가
- 빠진 ID가 없는가
- 알 수 없는 ID가 없는가
- 모든 ID가 정확히 한 번 존재하는가

저장소 자신의 `full_fixture()`는 이 결함을 양성 대조로 고정한다.

- `g0-normalize-tests.py:337-401`의 A log에는 실제 probe 레코드가 3개뿐이다.
- summary는 `expected=86, emitted=86, manifest_ok=true`다.
- 현재 SQL은 `g0-0a-capability-inventory.sql:58`에서 `c_expected=87`이다.
- 그런데 normalizer는 A를 `MEASURED`로 만들고 전체 fixture는 exit 0이 된다.

즉 **3개 probe + 거짓 summary**가 87개 완주로 승인된다. M1의 핵심인 “missing/unknown probe 거부”가 실물 계약에서는 닫히지 않았다.

필수 정정:

1. A probe ID 집합을 별도 versioned manifest로 둔다.
2. `set(parsed_ids) == set(required_ids)`, 실제 개수 = expected = emitted를 동시에 검사한다.
3. unknown·missing·duplicate·malformed JSON·summary 불일치 중 하나라도 있으면 집계 전에 exit 4로 거부한다.
4. 테스트 fixture는 실제 87 ID를 생성하거나 production manifest에서 자동 생성해야 한다.

### P0-02. source와 profile이 자가 신고값에 머문다

#### 원천 신원

wrapper는 `G0_SOURCE_ID`를 받아 manifest에 기록하지만, normalizer는 이를 A 서버 probe가 반환한 `DB_UNIQUE_NAME`과 비교하지 않는다.

- fixture manifest 기본값: `TESTSTBY` — `g0-normalize-tests.py:35`
- A 서버 probe 값: `ETLSTB` — `g0-normalize-tests.py:346`
- 양성 fixture는 이 불일치 상태로도 통과한다.
- `g0-normalize.py:719-746`는 서버 값을 `source`와 binding에 사용하지만 manifest source ID와의 동등성을 강제하지 않는다.

child끼리 같은 거짓 source ID를 쓰는 것은 “같은 원천에서 나왔다”는 증명이 아니다.

#### 실행 profile

`g0-run-child.sh:15`는 caller가 `CORP_POC`를 직접 선택하게 한다. 실제 WSL 로컬 실행도 `PROFILE=CORP_POC`로 manifest를 만들 수 있고, normalizer는 manifest·CLI·lock이 같은 문자열이면 authoritative profile로 취급한다. `PROFILE_NOT_AUTHORITATIVE` floor는 `LOCAL_WSL`과 `SANDBOX_CONTAINER`에만 적용된다(`g0-normalize.py:761-764`).

따라서 **로컬 증거를 CORP_POC로 재라벨링**하면 floor를 우회한다. 이는 8차의 profile relabel 반례가 그대로 살아 있다는 뜻이다.

필수 정정:

- profile은 사용자가 고르는 label이 아니라 승인된 launcher/environment attestation에서 파생한다.
- lock profile, launcher attestation, manifest profile, normalizer profile을 모두 비교한다.
- A의 `DB_UNIQUE_NAME`·`DATABASE_ROLE`·`CON_NAME`·target owner/table/column을 기대값과 동일 connection에서 fail-fast 검증한다.
- 모든 child는 자기 서버 신원을 남겨야 하고, corporate source와 CE disposable environment에는 서로 다른 scope를 부여한다.

### P0-03. B1 `PROVEN`은 현재 실물 launcher에서 도달할 수 없다

이 결함은 analyzer 단위 시험 43건이 모두 통과해도 남는다.

1. `run.sh:83-84`는 system run label을 `failclosed_schema` / `failclosed_task`로 나눈다.
2. 그러나 Python에는 두 회차 모두 `--mode failclosed`를 넘긴다.
3. Python phase 파일은 `g0-0b1-phase-{a.mode}.txt`이므로 `g0-0b1-phase-failclosed.txt`를 쓴다(`run-g0-0b1.py:128-143`).
4. provider의 Trace는 system run label을 사용해 `g0-0b1-phase-failclosed_task.txt`를 읽는다.
5. task 주입 회차에서 provider가 phase를 읽지 못해 `UNDECLARED`가 되고 `injection_applied`가 0이 된다.
6. Python terminal token도 `run: a.mode`, 즉 `failclosed`를 출력한다(`run-g0-0b1.py:275-289`).
7. analyzer는 `failclosed_schema`와 `failclosed_task` token을 요구한다(`analyze-trace.py:220-247`).

따라서 현재 wiring으로는 두 필수 run의 terminal token을 찾을 수 없으며 `PROVEN`이 될 수 없다. 반면 `g0-b1-analyzer-tests.py`는 producer가 만들지 않는 `failclosed_schema`/`failclosed_task` token을 직접 만들어 판정기에 넣는다.

추가 결함:

- `task_only`도 먼저 `reader.load().schema`를 실행한다(`run-g0-0b1.py:206-220`).
- `partitioned_count`의 `.load()` 단계에서 driver/schema connection이 실패해도 task 경로 주입처럼 보일 수 있다.
- “task 경로”라는 귀속은 실행 topology의 직접 증거가 아니라 scenario 이름에 의존한다.
- phase 파일은 driver 로컬 파일이므로 다른 노드의 executor에서는 같은 파일을 볼 수 없다.

필수 정정:

1. run identity를 단일 필드로 만들고 launcher·Python·system property·phase file·trace·terminal token이 같은 값을 사용하게 한다.
2. 실제 `run.sh`를 호출하는 통합 시험을 추가한다. analyzer fixture를 직접 합성하는 시험만으로는 안 된다.
3. task connection임을 executor/JVM/taskAttempt 정보로 증명한다.
4. phase 전파는 driver 로컬 파일이 아니라 executor에서 검증 가능한 immutable run config로 바꾼다.
5. schema 단계 실패와 task connection 실패를 별도 positive control로 분리한다.

### P0-04. 원천 보호 한계가 행 수 제한에 치우쳐 있다

`MAX_PARTITIONS=8`과 `MAX_CONCURRENT_SESSIONS=12`는 하드코딩됐지만, 현재 식에서는 session 상한 12가 사실상 작동하지 않는다.

- 추정 세션은 `num_partitions + 1`이다.
- partitions 최대가 8이므로 추정 최대는 9다.
- 따라서 12 초과 거부 분기는 정상 입력에서 도달하지 않는다.
- 여러 job이 동시에 실행될 때 source 전체 session budget을 조정하는 global/source lease가 없다.

Spark 문서도 `numPartitions`를 최대 동시 JDBC connection 수로 설명한다. 하지만 이것은 **한 Spark read의 상한**이지 source 전체의 안전 budget이 아니다. 또한 `lowerBound`/`upperBound`는 필터가 아니라 partition stride 결정용이므로 원천 읽기 범위를 제한하지 않는다. [Spark JDBC options](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

또한 `ROWNUM=1`이나 `LIMIT_ROWS`는 반환 행 수만 제한한다. empty/sparse/high-HWM 객체에서 block visit, wall-clock, physical read, undo/redo 영향의 상한은 아니다.

필수 정정:

- 기본 partitions는 1로 둔다.
- source별 승인된 `max_concurrent_sessions`·`max_active_jobs`·statement deadline·retry budget을 명시한다.
- 같은 source의 모든 probe/job이 공유하는 lease 또는 semaphore를 둔다.
- query timeout/cancel의 실제 Oracle·Spark 전파를 positive control로 검증한다.
- 대상 table touch 전 단계와 후 단계를 분리하고, 대상 touch는 access plan·예상 비용·승인 window가 있을 때만 연다.

### P0-05. A identity probe가 대상 접촉을 막지 않는다

`g0-0a-capability-inventory.sql:113-129`는 identity facts를 기록하지만 기대값 불일치 시 즉시 중단하지 않는다. 그 뒤 대상 객체를 읽는 probe가 계속 실행된다. 확인된 target touch는 최소 다섯 곳이다(`:134`, `:140-141`, `:216`, `:225`, `:264`).

즉 잘못된 TNS/service/container/table을 지정한 경우에도 “신원 불일치 증거를 남긴 뒤 잘못된 대상에 질의”할 수 있다. 생산라인 밀접 원천에서는 기록보다 차단이 먼저여야 한다.

필수 정정:

- DUAL 기반 identity preflight를 별도 script/connection으로 수행한다.
- `DB_UNIQUE_NAME`·role·container·service·user가 승인 manifest와 다르면 target SQL을 parse/execute하기 전에 종료한다.
- 본 probe connection에서도 같은 identity를 재확인한다.
- C00 `ACK_FULL_SCAN=Y`에도 같은 gate를 적용한다.

### P0-06. runbook은 현재 그대로 실행할 수 없다

`g0-0-runbook.md`는 “이 문서만 따라가면 된다”는 실행 문서지만 다음 불일치가 있다.

1. wrapper가 요구하는 `G0_SOURCE_ID`를 공통 환경 단계에서 export하지 않는다(`g0-run-child.sh:41-47`).
2. normalizer 명령은 정의되지 않은 `$DB_UNIQUE_NAME`을 사용한다(`g0-0-runbook.md:322`).
3. B0 명령에 필수 `--expect-db-unique-name`이 없다(`g0-0b0-spark-smoke.py:71`, runbook `:273-276`).
4. B1 artifact 경로 `$PWD/g0-0b1-evidence.json`에는 `RUN_ID`가 없어서 wrapper가 거부한다(`:249`).
5. CE artifact `$PWD/evidence.json`에도 `RUN_ID`가 없다(`:291-294`).
6. CE 실행에 필수인 `CE_ENV_ALLOWLIST` 설정이 없다.
7. `CE_DOC_PATH`는 현 규범이 아닌 `v1.2.3.1`을 가리킨다(`:287`).
8. runbook은 SQL 변수를 prompt 또는 앞선 wrapper `DEFINE`로 넣는다고 설명하지만, A SQL 내부의 `DEFINE`이 이를 다시 덮는다.

이 상태에서는 운영자 실수가 아니라 **문서대로 실행했기 때문에 실패**한다. 실행 전 M5의 직접 차단 사유다.

필수 정정:

- runbook에 적힌 명령을 clean shell에서 그대로 실행하는 dry-run/lint 통합 시험을 만든다.
- 모든 필수 env·argument·artifact path를 한 versioned run manifest에서 생성한다.
- 비밀번호는 argv가 아니라 stdin/env/secret mount만 사용한다.
- 문서 예제도 wrapper의 immutable path 계약을 통과해야 한다.

### P0-07. CE 증거는 corporate source evidence와 같은 source ID로 묶을 수 없다

CE01~CE09는 DDL/DML이 가능한 폐기용 primary에서만 실행해야 한다. 반면 A/B0/B1/C00은 사내 standby/source를 관측한다. 현재 top-level child 집합은 같은 `source_id`를 요구하므로 둘을 한 회차에 넣으려면 다음 둘 중 하나가 된다.

- CE가 corporate source ID를 거짓으로 신고한다.
- 서로 다른 source라며 집계가 거부된다.

외부 allowlist 파일도 “repository 밖에 있다”는 사실만 증명한다. 승인 주체·환경 소유권·서명·변경 권한의 독립성을 증명하지 않는다.

필수 정정:

- evidence contract에 `environment_scope`/`source_scope`를 도입한다.
- corporate read-only source evidence와 disposable primary counterexample evidence를 별도 record로 만든다.
- CE는 package 외부 allowlist에 더해 독립 승인 주체, expected DB identity, allowed schema/prefix, TTL, 서명 또는 변경 불가능한 배포 manifest를 요구한다.
- CE는 어떤 경우에도 production/standby runbook의 “다음 단계”로 자동 실행하지 않는다.

---

## 3. P1 — M5 후속 또는 최종 G0 전에 반드시 고칠 결함

### P1-01. `harness_digest`가 behavior surface 전체를 덮지 않는다

`g0-run-child.sh:80-89`는 11개 파일을 하드코딩한다. 다음과 같은 실행 의미를 바꾸는 파일이 빠져 있다.

- provider Java source
- ServiceLoader resource
- `build.sh`
- child JSON schemas
- `g0-final-contract.json`과 final gate
- CE `suite.yaml`·scenario definitions·scenario code

누락 파일을 바꿔도 digest가 그대로일 수 있다. 새 파일은 목록에 자동으로 들어가지 않는다.

권고:

- versioned harness manifest에 파일 path·purpose·digest를 선언한다.
- manifest 자체도 digest에 포함한다.
- 선언되지 않은 새 실행 파일/스키마가 있으면 build와 normalization을 실패시킨다.
- Git tree ID 또는 signed release artifact digest와 병행한다.

### P1-02. trace 완결성이 “하나의 trace_end”로 축약된다

`analyze-trace.py:68-84`는 전체 trace에서 `trace_end`가 하나라도 있으면 `trace_complete=true`로 둔다. driver가 정상 종료하고 executor trace가 SIGKILL로 잘려도 전체가 complete로 보일 수 있다.

권고:

- 기대 JVM/file/run 집합을 먼저 선언한다.
- 각 trace stream마다 start/end, line count, run ID, JVM ID, checksum을 검증한다.
- 하나라도 끝나지 않으면 그 경로는 `MEASUREMENT_FAILED`다.

### P1-03. 기본 TTL 30일은 측정하지 않은 정책값이다

`g0-normalize.py:73`의 `DEFAULT_TTL_DAYS=30`에는 source별 근거가 없다. runbook은 이를 operator-declared로 표현하지만 사용자가 선언하지 않아도 자동 적용된다.

권고:

- 기본값을 0 또는 `NO_TTL_DECLARED`로 둔다.
- 비zero TTL은 source owner가 근거와 함께 명시한 정책일 때만 허용한다.
- Oracle patch/role/privilege/DDL/connection provider/versions.lock 변경 시 TTL과 무관하게 즉시 무효화한다.

### P1-04. final gate는 임의의 forged record를 승인할 수 있다

`g0_final_gate.py:89-112`의 `admit()`은 다음 세 가지만 본다.

- `record_type == g0_evidence`
- `gate_eligible is True`
- contract path에 값이 존재

값의 schema·digest·stage verdict·same-lock·source binding을 확인하지 않는다. 따라서 contract 항목마다 임의의 문자열을 채운 forged record도 승인된다.

`aggregate()`가 `NotImplementedError`인 것은 정직하지만, 그와 별개로 `admit()`이 true를 낼 수 있어서는 안 된다.

권고:

- final schema와 aggregator가 생기기 전에는 `admit()`이 항상 false를 반환한다.
- 이후 G0-1~G0-5 artifact schema, signer/digest, exact version lock, source binding, stage verdict를 모두 검증한다.
- `g0-final-contract.json`의 항목 이름을 실제 P §8.1 field와 일치시킨다.

### P1-05. B0 preflight는 뒤이은 모든 physical connection을 결속하지 않는다

B0는 첫 DUAL query에서 identity를 확인하지만 이후 Spark가 여는 driver/executor connection들이 같은 database/container/service에 붙었다는 증거는 없다. connection pool 또는 TNS failover가 있으면 첫 connection의 identity로 나머지를 대표할 수 없다.

권고:

- provider가 모든 physical connection에서 identity/preamble을 검증한다.
- trace에 connection별 `DB_UNIQUE_NAME`·role·CON_NAME·service를 남긴다.
- 하나라도 다르면 해당 child 전체를 폐기한다.

### P1-06. `PROFILE_NOT_AUTHORITATIVE` floor 자체는 유지할 가치가 있다

이 floor를 추가한 방향은 타당하다. 문제는 과한 보수성이 아니라 `CORP_POC` label을 caller가 자가 선언할 수 있어 우회된다는 점이다. floor를 삭제하지 말고 profile attestation을 고쳐야 한다.

### P1-07. `covered.present=false`는 사실 표현으로 허용 가능하다

`present=false` 자체를 schema 위반으로 볼 필요는 없다. 다만 downstream에서 다음을 구분해야 한다.

- raw collection이 일부만 수행된 사실
- G0-0 measurement completeness
- capability grade
- final G0 eligibility

현재 `outcome` 분리는 좋은 방향이다. 그러나 “complete” 조건은 exact required probe/child set을 통과할 때만 참이어야 한다.

---

## 4. M3 축 모델 재판정

### 4.1 개선이 확인된 부분

다음은 8차보다 명확히 좋아졌다.

- 13개 atomic/composite axis를 분리했다.
- `watermark_commit_bound`를 lag 축과 독립적으로 복원했다.
- transient/denied/wrong-target/probe-bug를 기능 부재로 오판하지 않는 taxonomy를 도입했다.
- probe별 typed predicate를 두었다.
- raw `value`와 실행에 쓰는 `effective_value`를 분리했다.
- floor는 값을 올리지 않도록 했다.
- table-scoped axis에 binding을 요구한다.
- G0-0 record는 `gate_eligible=false`다.
- process/measurement/capability/final gate 결과를 분리했다.

### 4.2 아직 축 결과를 신뢰할 수 없는 이유

`derive_axes`의 규칙 자체보다 앞단 증거가 문제다. A의 3-probe fixture가 MEASURED가 되고, source/profile relabel이 가능하며, harness digest가 behavior surface를 덮지 않는다. 좋은 파생식도 잘못 결속된 입력을 받으면 재현 가능한 오답을 낸다.

따라서 M3 판정은 다음과 같다.

- **축 taxonomy/파생 방향:** GO
- **현재 evidence에서 나온 effective axis를 사내 설계 입력으로 사용:** NO-GO
- **M1/P0 정정 후 재검증:** 필요

---

## 5. M4 Oracle·Spark 사실 검증

### 5.1 정정 방향이 맞는 항목

1. `AS OF TIMESTAMP`의 실제 시점은 요청 시각보다 **최대 3초 이전**일 수 있다. “±3초”가 아니다. [Oracle Flashback guidance](https://docs.oracle.com/en/database/oracle/oracle-database/19/adfns/flashback.html)
2. `SCN_TO_TIMESTAMP`의 통상 정밀도 설명과 `AS OF TIMESTAMP`의 방향성 있는 매핑 설명은 같은 보장이 아니다.
3. `ORA-08180`은 timestamp→SCN mapping 실패, `ORA-08181`은 제공된 SCN이 valid range 밖인 경우다. 같은 유효 SCN 재사용 자체가 `ORA-08181`의 원인이 아니다. [Oracle 11.2 error messages](https://docs.oracle.com/cd/E11882_01/server.112/e17766.pdf)
4. Flashback query 권한은 `READ|SELECT`와 함께 object `FLASHBACK` 또는 `FLASHBACK ANY TABLE` 중 하나다. [Oracle Flashback privileges](https://docs.oracle.com/en/database/oracle/oracle-database/18/adfns/flashback.html)
5. Spark `connectionProvider` 옵션을 명시적으로 사용하는 것이 global provider disable보다 좁고 올바른 주 수단이다. [Spark JDBC options](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)
6. object privilege grant와 runtime activation/primary retention change를 분리한 것은 맞다.
7. “요청 전 실증”이라는 grant hold는 현재 측정 0건이라는 전제와 정합한다.

### 5.2 남은 과대·stale 문구

- `etl-platform-v2.0-grant-request-verdict.md`와 `HANDOFF.md`의 “dictionary row 1건”은 측정하지 않은 수치다. “일회성 metadata/redo/audit/invalidation 영향, 정확한 양은 미측정” 정도로 낮춰야 한다.
- 공통 timestamp literal은 여러 connection에 같은 anchor를 전달할 **수단**이 될 수 있다. 그러나 모든 physical connection과 모든 relevant query에 실제로 전파됐다는 실행 증거는 아직 없다. “확인된 이득”이 아니라 “검증할 잠재 이득”이다.
- `README.md`에 남은 “7축 표” 표현은 현재 13축 규격과 맞지 않는다.
- runbook의 `v1.2.3.1` pointer와 H/D/X 설명이 M4 정정 뒤에도 동기화되지 않았다.

M4의 사실 방향은 대부분 맞지만 repository 전체의 규범 동기화가 끝나지 않았으므로 **PARTIAL**이다.

---

## 6. P2 — 정리하면 좋은 항목

1. A SQL 주석의 target touch 수를 실제 5건과 맞춘다.
2. `MAX_CONCURRENT_SESSIONS=12`의 도달 불가능한 분기를 제거하거나 source policy 식으로 대체한다.
3. CE의 “PASS”가 harness 실행 완결인지 mitigation 성공인지 이름을 분리한다.
4. `g0-final-contract.json`의 `hash_vector_result (V-01~V-16)` 같은 표시 이름과 실제 field name을 분리한다.
5. `executed_at`을 임의 child의 `measured_at`으로 만족시키지 말고 authoritative final execution time을 정의한다.
6. raw field 존재와 semantic validation을 `covered`에서 분리한다.

---

## 7. M5를 여는 안전한 순서

현재 M5를 하나의 “사내 원천 실행” 단계로 두면 위험 범위가 너무 넓다. 다음처럼 분할해야 한다.

### M5a — 도구·runbook 사전 검증

대상 DB 연결 없이 수행한다.

해제 조건:

- P0-01, P0-02, P0-03, P0-06 정정
- exact 87-ID manifest 시험
- 실물 `run.sh` 기반 B1 통합 시험
- runbook 명령 dry-run 시험
- immutable path와 모든 required env 검증
- harness manifest/digest 완전성 시험

판정: **현재 NO-GO, 수정 후 GO 가능**

### M5b — DUAL·dictionary-only source inventory

target table을 전혀 읽지 않는 별도 mode다.

조건:

- source owner가 승인한 단일 connection
- DUAL identity fail-fast
- statement timeout과 retry=0 또는 명시적 작은 budget
- 실제 실행 시간·세션·SQL_ID·취소 결과 보존
- CORP_POC attestation

판정: **조건부 GO**

### M5c — 제한된 target touch

`ROWNUM=1`을 안전 상한으로 간주하지 않는다.

조건:

- table별 access plan 또는 검증된 index path
- source-specific time/I/O/session budget
- 운영 window와 즉시 중단 기준
- target identity 재검증
- 기본 partitions=1
- owner 승인

판정: **현재 NO-GO**

### M5d — Spark B0/B1 경로

조건:

- full Spark 배포판·JDK·ojdbc·provider 판본 고정
- source-global lease
- 모든 physical connection identity/preamble trace
- B1 producer/analyzer 종단 통합 통과
- executor 분산 topology에서 phase·trace 완결성 증명
- query cancellation positive control

판정: **현재 NO-GO**

### M5e — CE01~CE09

조건:

- disposable writable primary
- corporate source와 별도 evidence record/scope
- 독립 승인된 allowlist와 exact DB/schema/prefix
- 생성·삭제·복구 확인

판정: **사내 production/standby에서는 절대 NO-GO; 폐기 환경에서만 조건부 GO**

---

## 8. 최소 수정 순서

### P0 순서

1. runbook과 실제 CLI/path/env를 일치시키고 executable dry-run 시험을 추가한다.
2. profile attestation과 server identity binding을 추가한다.
3. A 87-ID exact manifest 검증을 구현한다.
4. harness digest를 versioned complete manifest/tree 기반으로 바꾼다.
5. source safety envelope를 source별 정책으로 만들고 기본 partitions=1, global lease, statement deadline을 둔다.
6. B1 run identity/phase/token wiring을 하나로 통일하고 실물 launcher 통합 시험을 추가한다.
7. corporate source와 disposable CE evidence scope를 분리한다.

### P1 순서

8. TTL 기본값을 미선언으로 바꾼다.
9. final schema/aggregator 전까지 final gate를 hard-disabled 한다.
10. trace completeness를 JVM/file/run별로 검증한다.
11. M4 stale 문구와 runbook pointer를 동기화한다.

---

## 9. 재리뷰 합격 기준

다음 반례가 모두 실물 producer를 통해 통과해야 M5a를 GO로 바꿀 수 있다.

1. A에서 required probe 하나 삭제 → 집계 전 exit 4
2. A에 unknown probe 하나 추가 → exit 4
3. A 실제 3건 + summary 87 → exit 4
4. manifest source와 서버 `DB_UNIQUE_NAME` 불일치 → target touch 전 종료
5. WSL에서 `PROFILE=CORP_POC` 재라벨 → 거부 또는 non-authoritative floor
6. behavior file 하나 추가/변경 → harness digest 변화 또는 unlisted-file 실패
7. 실제 `run.sh` 실행이 `failclosed_schema`와 `failclosed_task` token을 만들고 analyzer가 인식
8. task 회차가 schema connection에서 실패 → TASK proven 금지
9. executor trace 하나만 sentinel 없이 종료 → 전체 `MEASUREMENT_FAILED`
10. runbook의 모든 명령을 clean environment에서 dry-run → 필수 env/argument/path 누락 0건
11. 두 동시 run이 source session budget을 넘으려 함 → 둘째 run admission 거부
12. final gate에 arbitrary string forged record → 무조건 거부
13. corporate A/B record와 disposable CE record 혼합 → scope 위반 거부

---

## 10. 결론

9차의 가장 중요한 발견은 “397개가 적다”가 아니다. **397개가 잘 검증하는 경계와 M5가 요구하는 경계가 다르다**는 점이다.

M0~M4 작업은 헛되지 않았다. 종료 코드 보존, child schema, SQLCODE taxonomy, effective floor, G0-0/final G0 분리, Oracle/Spark 사실 정정은 모두 유지해야 한다. 다만 현재는 다음 세 뿌리가 남아 있다.

1. **증거 신원:** 누가·어디서·어느 원천에 대해 실행했는가가 서버 사실에 완전히 결속되지 않는다.
2. **실행 종단:** B1과 runbook의 실물 producer가 판정기가 기대하는 artifact를 만들지 못한다.
3. **원천 보호:** 반환 행 수와 단일 run partition 상한이 source 전체 시간·I/O·세션 안전을 보장하지 않는다.

따라서 다음 행동은 사내 원천 full sequence 실행이 아니라 **P0 1~7 수정 → M5a 종단 검증 → M5b DUAL/dictionary-only inventory**다. 이 순서가 끝나기 전까지 “남은 것은 실행뿐”이라는 상태 표시는 철회해야 한다.
