# Codex 3차 교차 리뷰(v1.2.2) 검토서 — v3.1

- 검토일: 2026-08-23 (v3.0) / **개정: 2026-08-23 (v3.1 — Codex 4차 확인 반영)**
- 검토 대상: `etl-platform-v1.2.2-codex-third-cross-review.md` (669줄, P0 7 / P1 13 / P2 2 + 이전 25건 재판정 + 범위 권고)
- 대조 문서: `etl-platform-target-architecture-v1.2.2.md`(A, 1,634줄), `etl-platform-poc-test-plan-v1.md` 6차(P, 552줄)
- 방법: 26개 판정 단위를 A·P 본문 인용으로 재검증 → 회의적 재검토 → **v3.1에서 Codex 4차 확인 5건을 1차 출처(Iceberg·Polaris·Dagster 소스, Oracle 19c 문서)로 재확인하고 §7을 전면 재작성**했다.
- 판정 원칙: 결함은 채택하되 처방은 축소안, 새 구성요소·새 상태·새 테이블은 두지 않는다. 보장 명칭 하향은 semantic patch로 가능하므로 기각 이력과 무관하게 판정한다.

---

## 0. v3.1에서 바뀐 것

Codex 4차 확인의 **5건 요구를 전부 수용**한다. 세 건은 내 v3.0 처방의 실제 결함이었다.

| # | Codex 요구 | v3.0 상태 | v3.1 처리 |
|---|---|---|---|
| 1 | FI-50을 4개 timing case + ambiguous로 재작성 | (iii) 한 case만 재정의 | **§7.1로 전면 재작성**(5 case + 대조군, 21회차) |
| 2 | `commit.retry.num-retries=0`을 예방책으로 쓰지 말 것 | "main 노출 0이 된다"로 서술 — **내 오류** | **철회**. 소스로 확인: `SnapshotProducer.apply()`가 매 시도(첫 시도 포함) `refresh()`하므로 A가 B의 refresh 전에 착지하면 409조차 없다 |
| 3 | FI-50 snapshot을 overlap oracle에서 allowlist하지 말 것 | allowlist 예외 1줄 제안 — **내 오류** | **철회**. §7.2에서 Control 상태로 유도되는 4-불변식 술어로 교체, 주입 로그는 제외 목록이 아니라 **대조값** |
| 4 | Dagster 기본안을 sensor `RunRequest/run_key`로 | private `create_run+submit_run`을 기본안으로 | **교체**. §7.3 — sensor 단일 채널, private API는 조건부 차선책 |
| 5 | ZERO_GAP·delete 축·canonical hash·capability evidence·G0 재정의 | 부분(축 분리는 qualifier로, hash는 문자 길이 prefix) | **§7.4~§7.7**. hash는 **내 오류** — 문자 길이 prefix로는 Oracle DB charset ↔ Spark UTF-8 **바이트열 차이**가 남는다 |

그리고 v3.1 초안 자체를 회의적으로 재검토해 **내 교정안에서 7건을 추가로 고쳤다**: (a) `commit.status-check.*`는 REST 경로가 쓰지 않는다(1.11.0+ `reconcileOnSimpleUpdate`가 대체, ≤1.10.x에는 없음), (b) `COMPOSE` 단독은 문서상 NFC가 아니다, (c) 판정 SQL의 `tstzrange`가 A:1163 `window_range **numrange**`와 타입 충돌, (d) 술어의 repair 부모 참조 불일치·도달 불가 분기, (e) sensor 지연 논증이 60초 gRPC 예산과 daemon 제출 루프를 혼동(그리고 `SC-02c`는 이미 쓰이는 id), (f) poll의 in-flight 배제 규칙과 복구 규칙이 서로를 무효화, (g) `DBMS_CRYPTO.HASH(CLOB)`의 charset이 문서에 없는데 기본 경로로 뒀다.

**한 건은 조건부로 이견을 남겼다가 철회했다** — §7.7. `KILL SESSION`이 문서상 롤백이라는 이유로 C1~C4 아래의 조건부 ENFORCED를 제안했으나, **Codex가 같은 지시를 재확인해 그 판단을 따랐다(2026-08-24)**: v1.2.3 최종본에서 `TXN_AGE_KILL_JOB`은 ENFORCED 근거에서 제외되고 `ZERO_GAP`은 commit-ordered cutoff(`STANDBY_VISIBLE_SCN`·`CDC_OFFSET`) 또는 동기식 fail-closed 장치(`SYNC_COMMIT_GUARD`)에만 허용된다. C1~C4는 폐기하지 않고 kill job Source의 overlap 보수화·상시 반증 조건으로 재배치했다.

---

## 1. 총평

**Codex 3차의 결정표(방향 GO / v1.2.2 동결 NO-GO / 기준서 6차 동결 NO-GO / Oracle ZERO_GAP NO-GO / 주차 1 조건부 GO)와 4차의 "§7 그대로 v1.2.3 제작 NO-GO"에 모두 동의한다.** 26건 중 **확인 9, 부분 확인 17, 기각 0**. 범위 권고 5건 중 1건(§9.3 "게이트를 셋으로")만 사실 오인으로 기각한다 — P는 이미 3단이며 리뷰의 G0/G1/G2 정의는 P:441·P:462·P:464와 문장 단위로 같다(§7.6에서 이름과 PASS 조건을 정식화한다).

이번 리뷰가 v1.2.2에 대해 옳게 지적한 것은 **"보장 명칭이 실제 메커니즘보다 강하다"** 한 줄로 요약된다. 세 자리에서 같은 문제가 난다: late-apply commit(예방이 아니라 탐지·복구), `bound_kind = ENFORCED`(엔진 강제가 아니라 측정된 상한), Source 절대 한도(회계 키가 유일하지 않으면 envelope가 무너짐).

**내 쪽 오류 6건을 먼저 적는다.**

- **P1-01(치명)**: 검토서 2가 채택하고 v1.2.2가 구현한 `executionMetadata.runId = uuid5(contract_id, resubmit_no)`는 **Dagster GraphQL에 존재하지 않는 입력 필드**다(`schema/inputs.py:297-311`의 `GrapheneExecutionMetadata` = `tags`·`rootRunId`·`parentRunId`). 내부 파서가 `runId`를 읽어도 graphene이 미선언 입력을 먼저 거부한다. v1.2.1의 `CON_DBID`와 같은 **실행 불가** 결함이므로 리뷰의 P1을 **P0로 상향**한다.
- **`retry=0` 예방 주장(v3.1에서 철회)**: `SnapshotProducer.apply()`는 `commit()`의 태스크 본문 **안에서** `refresh()`를 호출하므로(`SnapshotProducer.java:293-295, 480-522`) 첫 시도부터 그 시점 head를 parent로 삼고, `assert-ref-snapshot-id`도 **그 refresh한 head**를 싣는다(`UpdateRequirements.java:117-129`). Control이 기록한 H0은 프로토콜의 어떤 assertion에도 들어가지 않는다.
- **allowlist(v3.1에서 철회)**: 하네스 목록으로 판정 모집단에서 snapshot을 빼면 이중 commit 검증을 스스로 무력화한다는 Codex 지적이 옳다.
- **canonical hash(v3.1에서 교체)**: 문자 길이 prefix는 구분자 주입만 막는다. **SHA-256에 들어가는 바이트열 자체**가 Oracle(DB charset)과 Spark(UTF-8)에서 다르다.
- **P1-04**: 검토서 2 채택안에 내가 `coverage = contract.window.high`로 적었다. 부분 CAS 뒤 종결에서 과대 표시된다.
- **P2-01**: P §2.4의 `dq_basis` 중복 선언은 6차 패치에서 내가 만든 것이다.

**리뷰가 틀렸거나 과장한 곳도 분명하다.** 3차 리뷰의 P0-01 실패 interleaving("attempt 2가 FINALIZED한 뒤 attempt 1의 보관 요청이 성공해 같은 snapshot을 붙인다")은 성립하지 않는다 — 보관 요청은 A의 writer가 refresh했던 head(H0)를 assert하므로 head가 H2로 옮겨간 뒤에는 Polaris가 409로 기각한다. 따라서 **현행 P:281 FI-50 (iii)은 재현 불가능한 시나리오**이며 §7.1이 이를 대조군으로 뒤집는다. Codex 4차도 이 정정에 동의했다.

```text
Architecture direction:            GO            — 동의
v1.2.2 semantic freeze:            NO-GO         — 동의
PoC 6차 acceptance freeze:         NO-GO         — 동의
Oracle ZERO_GAP (APPLICATION_TS):  NO-GO(현 정의) — 동의. §7.7 — ENFORCED 근거는 `SYNC_COMMIT_GUARD`뿐이고 kill job Source는 BEST_EFFORT(2026-08-24 최종)
PER_CHUNK_FENCE ZERO_GAP:          조건부        — 하한을 T_lb_1로 정정 + delete 축 분리 표시
G0 실행 준비:                       조건부 GO      — 동의. §7.6 PASS 조건 충족 시
검토서 3 §7 그대로 v1.2.3 제작:       NO-GO → v3.1에서 5건 정정 완료
```

---

## 2. 1차 출처로 확정한 외부 사실

| # | 사실 | 출처 | 영향 |
|---|---|---|---|
| 1 | `SnapshotProducer.apply()`가 **매 시도(첫 시도 포함)** `refresh()` 후 그 head를 parent로 삼는다. 평범한 append는 `validate(TableMetadata, Snapshot)`가 빈 구현이라 conflict 검사가 없다 | `SnapshotProducer.java:281, 293-295, 473-476, 480-522`(main·1.9.1 동일) | **retry=0 예방 주장 철회**. "driver가 Control이 기록한 head 위에 commit한다"는 취지의 서술을 A/P에서 전부 삭제 |
| 2 | REST commit의 requirement는 `AssertTableUUID` + `AssertRefSnapshotID(main, **refresh한 head**)`이며 Polaris가 서버 metadata에 검증해 실패 시 409 | `RESTTableOperations.java:187-192`, `UpdateRequirements.java:50-58,117-129`, Polaris `CatalogHandlerUtils.java:394-517` | CAS는 **writer가 관측한 head**에 대한 것이지 Control이 지정한 head가 아니다 |
| 3 | `commit.retry.num-retries` 기본 4, `Tasks.retry(n) ⇒ maxAttempts = n+1`, `onlyRetryOn(CommitFailedException)`. 409→`CommitFailedException`, 5xx→`CommitStateUnknownException`(재시도 없음, `cleanAll` 미실행) | `TableProperties.java:89-90`, `Tasks.java:162-163,403-421`, `ErrorHandlers.java:119-137` | retry=0이 닫는 것은 **409 뒤 재적용** 하나뿐 |
| 4 | 재시도는 `snapshotId()`를 memoize해 **같은 id**를 쓰며, 첫 시도가 실제 착지했고 제3 writer가 append하면 재시도가 `setBranchSnapshot(ours, main)`으로 **main을 우리 snapshot으로 되돌린다** | `SnapshotProducer.java:496-512, 686-695` | 코드 사실로는 성립하나 **정상 REST 경로에서 도달 불가**(409는 미착지·504는 `CommitStateUnknownException`이라 재시도 loop 밖) — 금지된 429/`Retry-After` 503 재전송에서만 도달 가능하므로 retry=0의 유지 근거가 아니다(§7.1 (vii), 2026-08-24 정정) |
| 5 | `HTTPClient`의 `ExponentialHttpRequestRetryStrategy`는 **429는 메서드 무관, 503+`Retry-After`는 조건부**로 같은 POST를 재전송한다(POST는 비멱등이라 IOException·500·502·504에는 재전송 없음) | `HTTPClient.java:128`, 전략 L87-155 | Spark↔Polaris 사이에 **rate-limiting 프록시/LB 금지**를 운영 제약으로 |
| 6 | REST 경로는 `commit.status-check.*`(=`BaseMetastoreTableOperations.checkCommitStatus`)를 쓰지 않는다. unknown-state 조정은 `RESTTableOperations.reconcileOnSimpleUpdate`이며 **1.11.0 신설**(1.9.x·1.10.x에 없음) | Iceberg 소스 | 504 case의 기대값이 **pinned 버전에 따라 갈린다** |
| 7 | Dagster `RunRequest.run_key`: sensor는 **across all sensor evaluations**, schedule은 **per tick**. dedup 권위는 run storage의 `dagster/run_key` 태그이며 scope = (sensor 이름, repository selector). tick retention은 이를 지우지 않으나 **run wipe/delete는 지운다** | `run_request.py:63-98`, `_daemon/sensor.py`(`fetch_existing_runs` 직렬 조회) | §7.3 기본안의 근거 |
| 8 | `sensors.use_threads` 기본 **False**, `num_submit_workers` 기본값 없음. 제출 루프는 요청마다 code location 획득 + `get_execution_plan` gRPC + `create_run` + `submit_run` | instance config, `_daemon/sensor.py` | 500 burst는 **설정하지 않으면 1건씩 직렬 제출** |
| 9 | 중단된 tick은 `reserved_run_ids`로 최대 24h 재개(`MAX_TIME_TO_RESUME_TICK_SECONDS=86400`), FAILURE tick은 1회 재시도 | Dagster 소스 | `run_submission` 테이블·exact recovery journal 기각의 1차 근거 |
| 10 | Oracle: `COMPOSE`는 **단독으로 NFC가 아니다** — "To get a string in the NFC form, first call DECOMPOSE with the CANONICAL setting and then COMPOSE". 인자가 Unicode charset이 아니면 무동작 | 19c SQL Reference `COMPOSE` | hash 규격을 `COMPOSE(DECOMPOSE(TO_NCHAR(v),'CANONICAL'))`로 |
| 11 | `DBMS_CRYPTO`는 VARCHAR2를 직접 받지 않으며 "convert it to the uniform database character set **AL32UTF8**, and then to RAW"를 문서가 지시한다. CLOB overload가 어떤 charset 바이트를 해시하는지는 **문서에 없음(미확인)** | 19c PL/SQL Packages | CLOB도 명시적 AL32UTF8 → **BLOB overload** 사용 |
| 12 | profile 세션 자원 제한 위반 후 "the only operations the user can perform are COMMIT, ROLLBACK, or disconnect (**in this case, the current transaction is committed**)". `CONNECT_TIME`만 롤백+세션 종료이며 "checks every few minutes … can exceed this limit slightly (for example, by 5 minutes)" | 19c DB Security Guide | **profile은 ZERO_GAP 근거에서 삭제**(Codex 수용). `IDLE_TIME`은 트랜잭션 나이와 무관 |
| 13 | `KILL SESSION`은 문서상 "roll back ongoing transactions"이며 ORA-00031(marked for kill)은 "as soon as possible after its current uninterruptable operation is done" — **지연 상한 수치는 없음** | 19c 문서 | §7.7 — kill은 안전 방향이나 `L`은 미보장 |

미확인으로 남긴 것: Polaris 서버측 commit 처리 horizon, `STANDARD_HASH`의 RAW 인자 허용(배제 규칙에 의한 추론), `UTL_I18N`·`UTL_RAW`·`DBMS_CRYPTO`의 SQL 직접 호출 가능성, Oracle ↔ JVM 정규화 테이블의 Unicode 버전 일치, marked-for-kill 세션의 이후 COMMIT 성공 가능성 — 전부 **G0 실증 대상**으로 이관한다.

---

## 3. P0 7건 재판정

| ID | 판정 | 우리 등급 | freeze 전 | 채택 방식 |
|---|---|---|---|---|
| P0-01 late target commit terminal 부재 | **부분** | P1(데이터) | **필수** | 예방 불가를 본문에 명시하고 `DETECT_AND_REPAIR`로 명명, No-Go 7 재정의, A:1088 사실 정정, FI-50 5-case 재작성(§7.1·§7.2) |
| P0-02 application timestamp bound 미강제 | **부분** | P1 | **필수** | profile 근거 삭제, `bound_evidence` + C1·C4(자격·유지)/C2·C3(kill job overlap 보수화) — ENFORCED 근거는 `SYNC_COMMIT_GUARD`뿐, 미충족 시 `BEST_EFFORT`(§7.7) |
| P0-03 비동기 `PK_RECONCILE`은 ZERO_GAP 아님 | **부분** | P2 | 아니오 | 축 분리 표시로 해소(§7.4), enum 신설은 불필요 |
| P0-04 `zero_gap_evidence` fail-open | **부분** | P1 | **필수** | 무효화 트랜잭션에서 **조건부** `HOLD_NEW(ZERO_GAP_INVALIDATED)` — 새 capability 값으로 7.2 5번 rule 재평가가 거부일 때만 |
| P0-05 PER_CHUNK_FENCE sweep 하한 | **부분** | P1 | **필수** | sweep low를 `T_lb_1 − overlap`으로(‘fence_ts’는 wall-clock) + INITIAL_LOAD 구간 `delete_consistency` 상향 표시 |
| P0-06 session tag 충돌 | **부분** | P1 | **필수** | 회계 join key를 `{contract_id}/a{attempt_no}`로 고정, MODULE/ACTION은 보조 |
| P0-07 PoC oracle 자기모순(A~E) | **확인**(A·B) / **부분**(C·D·E) | P1(기준서) | **필수** | A→§7.2(allowlist 없는 술어), B→`from_state <> to_state` 필터, C→§7.5, D→집계 창+클라이언트 식별, E→권위 표본 3건 추가 |

---

## 4. P1 13건 + P2 2건 판정

| ID | 판정 | 등급 | 채택 방식 |
|---|---|---|---|
| P1-01 Dagster 제출 멱등성 | **확인** | **P0 상향** | §7.3 — shard sensor 단일 채널 + `run_key`, private API는 조건부 차선책 |
| P1-02 release activation intent | 부분 | P2 | `deployed_at` 선기록 + operation `completed_step` 재개(A:308 패턴), 9.3에 release별 cron 전개 1문장, FI-24c crash 2지점 |
| P1-03 cross-type lease DB 제약 | 부분 | P2 | A:362 보장 문구 하향 + `target_lease` 행 트리거(matrix 재검사) + DML 권한 한정 + FI-51 raw-session 변형 |
| P1-04 Guard SHARE→UPDATE deadlock | **확인** | P1 | Source `FOR UPDATE`를 credential breaker로 한정, 나머지 자동 Hold는 제약 (7) insert-or-get |
| P1-05 maintenance reclaim 뒤 착지 | 부분 | P1 | `RECLAIMED` 전이에서 head-settle로 `reclaimed_head_snapshot_id` 기록 → ancestor-or-self 판정(시각 기준 금지) |
| P1-06 Outbox transition key | **확인** | P1 | 기존 `rebind_count`를 transition key에 포함 |
| P1-07 RESYNC history 유일성 | 부분 | P2 | AFTER INSERT 트리거 + `reason=RECONSTRUCTED`, P:223 (b) 집계 기준 정정 |
| P1-08 freshness coverage | **확인** | P2 | `max(ledger.window_high WHERE cas_applied)` |
| P1-09 DB hard-cap evidence gate | 부분 | P2 | Source 저장 validator에 `SOURCE_SESSION_CAP_INVARIANT` |
| P1-10 credential fan-out 시험 | 부분 | P2 | FI-16(C) fan-out 변형 + `{password_rollover_time_seconds, verified_at}` |
| P1-11 fence origin provenance | 부분 | P2 | ACTIVE 전이의 모니터 세션 선수립-교체 규범(트랜잭션 밖 수립 → 안에서 포인터 교체) |
| P1-12 ORA-01722 오분류 | 부분 | P1 | 재판정 일치 시 `SPARK_FAILED`, LONG/LONG RAW는 publish 422 |
| P1-13 Gap Recovery owner failover | **확인** | P2 | IN_PROGRESS operation 재수행 1문장 + FI-02(g) |
| P2-01 ledger `dq_basis` 중복 | **확인** | P2 | P:113 중복 제거 + Merge 외 NULL + 열 순서 A:1103 정렬 |
| P2-02 FI-51 `lease_grant` 증거 | **확인** | P2 | `lease_grant`에 `stage ∈ {SOURCE_TOKEN, TARGET_LEASE}` 판별자 |

---

## 5. 이전 25건 폐쇄 매트릭스 재판정

리뷰: CLOSED 9 / PARTIAL 14 / OPEN 2. **우리 판정: CLOSED 9 / PARTIAL 15 / OPEN 1** (P0-01 OPEN→PARTIAL — 채택안 3종은 구현됐고 잔여는 명칭·oracle; P1-01은 OPEN 유지 + 등급 P0 상향).

CLOSED 9: P0-02·P0-04, P1-02·03·05·07·11·14·15.
PARTIAL 15: P0-01·03·05·06·07·08·09·10, P1-04·06·08·09·10·12·13.
OPEN 1: P1-01.

반박한 리뷰 논거: "release별 expected expansion 부재"(A:1354·A:688·P:247에 이미 있음), "Outbox key가 DB 강제 아님"(제약 (6) unique 실재), "delete 표본 부재"(FI-41f), "REATTACH가 오탐"(이번 P0-07B로 승격), "hard-cap이 Guard gate여야"(설계 의도 — validator로 이동).

---

## 6. 기각하거나 축소한 처방

1. **catalog gateway + `commit_intent_id`/`fencing_generation`** — 외부 commit 경계 확장. 3회 연속 제출됐고 3회 모두 기각. 단 §7.1이 **예방이 불가능함을 본문에 명시**하므로 이 기각은 "위험 없음"이 아니라 "명칭을 낮춘다"와 짝이다.
2. **`NO_COMMIT` 자동 확정 금지** — RTO 30분과 양립 불가(리뷰 자인).
3. **`ZERO_GAP` 등급 자체의 하향** — `ZERO_GAP`은 A:935 정의상 upsert 축이며 이중 commit과 무관하다. 하향 대상은 No-Go 7번·§5.1 (3)(b)다.
4. **`BOUNDED_DELETE_LAG`·`DETECT_AND_REPAIR` enum 신설** — §7.4의 파생 표시로 같은 정직성을 얻는다(JobSpec 입력 증가 0).
5. **`full_semantics_digest`(버전·topology·TNS·wallet)** — 대부분 `capability_digest`·`versions.lock`·런타임 identity 대조·`SCHEMA_DRIFT`가 덮는다. 실제 공백(trigger DDL·heartbeat job DDL·timestamp 형식·DB charset·`MAX_STRING_SIZE`)만 digest 입력에 추가한다.
6. **`ZERO_GAP` 초기 적재 `EXTRACT_ONCE` 제한** — LOB old version 보존 요구가 오히려 길어진다.
7. **final SCN full exact reconciliation phase** — 새 실행 단계이며 `PK_RECONCILE`이 같은 일을 한다.
8. **`activation_intent` 테이블 / `transition_version` 컬럼 / RESYNC unique key / `target_lease_conflict` evidence / fence provenance 5필드** — 기존 필드·트리거·jsonb로 닫힌다.
9. **Guard마다 hard-cap fail-closed / Source ACTIVE gate** — 핫패스 재검사이고 Source ACTIVE 상태는 A에 없다.
10. **게이트 3분할 신설** — P는 이미 3단. §7.6에서 이름(G0)과 PASS·증거·만료 조건만 정식화한다.
11. **(v3.1 신설) `commit.retry.num-retries = 0`을 예방책으로 쓰는 것** — 내 v3.0 처방을 철회한다. §7.1 (vii)의 이유로 **설정은 유지하되 근거를 바꾼다**.
12. **(v3.1 신설) FI-50 snapshot allowlist** — 철회. §7.2로 대체.

---

## 7. v1.2.3 범위 (semantic errata — 기능 추가 0)

### 7.1 FI-50 재작성 — 5 timing case

**확정 전제(버전 고정 필수)**: T1 = `apply()`가 매 시도 refresh(외부 사실 1), T2 = `AssertRefSnapshotID`는 **writer가 refresh한 head**를 assert(외부 사실 2), T3 = `num-retries` 기본 4·`onlyRetryOn(CommitFailedException)`(외부 사실 3). 또한 Polaris `polaris.config.rollback.compaction.on-conflicts.enabled`는 기본 false이며 **true면 assert 실패를 롤백으로 통과시킬 수 있으므로 PoC 설정 단언 항목**으로 둔다.

**주입 훅**: 프록시 `hold_then_apply(release_anchor)` — client에 즉시 504를 주고 보관한 `updateTable`을 앵커에서 전달한다. 앵커는 **클라이언트 신원(Run Pod IP 또는 Template이 REST catalog `header.*`로 붙인 `X-Contract-Id`)** 으로 분류한 카탈로그 호출로 정의한다. 사후에 프록시 로그 순서로 **case 귀속을 확정**하고, 기대와 다르면 실제로 밟은 case로 **재분류**해 그 case의 합격식으로 판정한다(§8.2에 재분류 기록).

| case | 앵커 | 기대 동작 | 합격 조건(요지) |
|---|---|---|---|
| **(i)** attempt 2 pod의 **첫 카탈로그 호출 이전** (3회) | SA 생성 K8s 이벤트 직후 | A가 H0→H1′ 착지. Run Pod가 읽는 base = H1′ ≠ Guard `last_committed_snapshot_id` → `chunks:begin(1)`이 **SA 쓰기 전** `RECONCILIATION_REQUIRED` | attempt 2 snapshot **0** ∧ 그 window `cas_applied` row 0 ∧ `RECONCILIATION_REQUIRED(EXTERNAL_SNAPSHOT)` 정확히 1 ∧ 전이 시각 < SA 쓰기 ∧ repair REPLAY FINALIZED 뒤 비교 A 차이 0 |
| **(ii)** `chunks:begin(1)` 200 후 ∧ B의 commit-전 `loadTable` 전 (설정 2종 × 3회) | 마지막 `loadTable`로부터 `t_gap` 경과 | T1에 의해 B가 H1′를 head로 보고 **첫 시도로 append** → **409·retry 없음** → 같은 논리 window snapshot 2개. `chunks/{1}:commit`의 lineage가 탐지 | **`num-retries` 값과 무관하게** 409 **0** ∧ 재시도 0 ∧ 같은 window ingest snapshot 정확히 2 ∧ **둘 다 미채택** ∧ `RECONCILIATION_REQUIRED` 1 ∧ watermark 전진 0 ∧ repair 뒤 비교 A 차이 0 |
| **(iii)** B의 commit-전 `loadTable` 후 ∧ `updateTable` 도착 전 (설정 2종 × 3회) | 프록시가 B의 `updateTable`을 붙잡음 | 409 발생. ⑴ 기본: refresh 후 재시도 → 중복 2개(=(ii) 최종 상태). ⑵ `=0`: `CommitFailedException` → `SPARK_FAILED` → Adjudication → late snapshot 1개만 | ⑴ 409 정확히 1 ∧ 재시도 1 ∧ (ii) 술어. ⑵ 409 1 ∧ 재시도 **0** ∧ attempt 2 snapshot 0 ∧ `RECONCILIATION_REQUIRED` 1 ∧ 양쪽 모두 repair 뒤 비교 A 차이 0 |
| **(iv)** B의 `updateTable` 200 후 (3회, 대조군) | — | T2에 의해 보관 요청의 assert가 현재 head와 불일치 → **409, 착지 실패**. **현행 P:281 (iii)이 재현 불가임을 증명하는 행** | 409 정확히 1 ∧ 새 snapshot **0** ∧ `RECONCILIATION_REQUIRED` **0** ∧ 계약 정상 FINALIZED |
| **(v)** ambiguous — `forward_then_504`(FI-14와 같은 훅) (3회: a/b/c) | 응답만 504 | **버전 의존**: pinned ≤1.10.x → `CommitStateUnknownException` 즉시 실패(`cleanAll` 미실행 — manifest 잔존) → receipt `committed=null` → Adjudication. ≥1.11.0 → `reconcileOnSimpleUpdate`가 **1회 refresh로 조용히 성공 처리**(단 `AddSnapshot`+동일 snapshot `SetSnapshotRef`만인 update set에서만) | 분기별로 기대값을 나누어 판정하고 **pinned 버전과 실제로 밟은 분기를 §8.2에 기록**. 버전을 명시하지 않은 합격식은 쓰지 않는다 |

**추가 규칙 2건.**
- **(vi) 프록시가 만드는 5번째 창(설정 단언)**: `HTTPClient`가 **429는 메서드 무관, 503+`Retry-After`는 조건부**로 같은 POST를 재전송한다(외부 사실 5). FI 프록시는 **429와 `Retry-After` 503을 절대 반환하지 않는다**를 하네스 불변식으로 두고, 운영 배포에도 "Spark↔Polaris 사이 rate-limiting 프록시/LB 금지"를 넣는다.
- **(vii) `num-retries = 0`의 근거 — 철회(2026-08-24 정정)**: 재시도가 snapshot id를 memoize해 같은 id를 쓰고, 첫 시도가 착지한 뒤 제3 writer C가 append하면 재시도의 `setBranchSnapshot(ours, main)`이 **main을 우리 snapshot으로 되돌려 C를 lineage에서 떨어뜨린다**는 것은 코드 사실이다(외부 사실 4). 그러나 이 경로는 **정상 REST 경로에서 도달 불가**다 — 409를 받은 시도는 애초에 착지하지 않았고(외부 사실 2), 504는 `CommitStateUnknownException`이라 `onlyRetryOn(CommitFailedException)` loop에 들어가지 않는다(외부 사실 3). 도달 가능한 유일한 조건은 HTTP 계층이 429·`Retry-After` 503에 비멱등 POST commit을 재전송하는 경우이며(외부 사실 5) 그것은 (vi)의 금지 제약이 이미 배제한다. 따라서 `=0`은 조용한 손실 차단의 근거가 **아니고** 이중 commit 예방도 아니다 — 값 선택은 FI-50의 설정 2종 대조 측정에 위임한다(A 13.2 v1.2.3.1).
- **(viii) WAP**: `fast_forward`는 client측 조상 검사와 서버측 `AssertRefSnapshotID` 양쪽으로 fail-closed이므로 case (ii)의 조용한 append를 명시적 실패로 바꾼다. 단 fast-forward commit은 `SetSnapshotRef`만이라 1.11 reconcile 대상이 아니다 → H-06에 "504 on fast_forward" 변형 1건 추가.
- **(ix) P:234 FI-14 문구 정정**: "`forward_then_504`(client 재시도 후 정상 FINALIZED)"는 틀렸다 — `CommitStateUnknownException`은 재시도되지 않는다. "(≥1.11) 단발 reconcile 성공 / (≤1.10) 즉시 실패 후 Adjudication"으로 교체.

**측정 항목**(합격식과 별도로 기록): 탐지 지연(late `committed_at` → `RECONCILIATION_REQUIRED` 전이, case별 p50/p99 — 경로마다 차수가 다르므로 단일 임계값을 두지 않고 v1.2.3 SLO 선언의 입력으로 올린다), 노출 기간(→ repair REPLAY `finalized_at`, p99를 선언 SLO와 대조), 노출 중 비교 A 1회 추가 실행(관측), case별 409·재시도 수.

### 7.2 이중 commit oracle 재정의 — allowlist 철회

검토서 3 §7이 P:358·P:375에 넣으려던 `snapshot_id` allowlist 예외를 **철회**한다. 현행 P의 `allowlist`는 'Snapshot metadata 100%'(메타데이터 부재 판정)에만 남으며 이중 commit 판정과 분리된다.

**정의**: 모집단 P = main ancestry 중 `summary['etl.writer_kind'] = 'ingest'`인 snapshot. **채택** = 그 id를 `committed_snapshot_id`로 갖는 `cas_applied = true` ledger row 존재. **U** = P 중 미채택(정상 soak 기대값 ∅). **겹치는 쌍** = 같은 `(job_id, lane)`에서 `[etl.logical_window_low, high)`가 겹치는 두 원소. **window 값의 단위는 `window_kind`가 결정한다**(SCN 정수 | UTC epoch µs — A:1163 `window_range **numrange**`). repair/backfill writer는 `etl.writer_kind = 'repair'`로 stamp되어 **P에 들어오지 않는다**(§5.1 (3)(b)의 lane 문구는 `operation_class`가 아니라 `writer_kind`가 권위임을 밝힌다).

- **불변식 1(예외 없음)**: 겹치는 쌍 중 **양쪽 다 채택된** 쌍 = **0**. FI-50 회차 포함 어떤 예외도 없다. FI-50이 만드는 중복은 **양쪽 다 미채택**이므로 이 술어를 무력화하지 않는다 — allowlist가 불필요한 이유다.
- **불변식 2(설명 책임)**: U의 **모든** 원소 m이 (a) 미채택 ∧ (b) 그 attempt가 Control에 미채택 확정(`ADJUDICATED ∧ verdict='NO_COMMIT'`, 또는 그 ledger row가 `dq_result='EXTERNAL_SNAPSHOT' ∧ cas_applied=false`, 또는 **다음 attempt**가 `FENCED ∧ verdict IS NULL ∧ adjudicated_head_snapshot_id = m`) ∧ (c) m의 계약 **또는 m을 lineage에 실어 탐지한 계약**이 `RECONCILIATION_REQUIRED(EXTERNAL_SNAPSHOT)`로 전이 ∧ (d) **(c)에서 확정한 계약**을 부모로 하는 repair REPLAY가 FINALIZED이고 그 `cas_applied` window 합집합이 m의 window를 포함. 하나라도 아니면 No-Go.
  > 불변식 1은 "이중 commit이 **채택까지 된**" 최악만 잡는다. **탐지 실패형**(한쪽만 채택)은 불변식 2가 잡는다 — (c)가 없으면 즉시 불합격이다.
- **불변식 3(교차 검증)**: `|U|`와 겹치는 쌍 수가 §8.2 주입 기록에서 계산한 기대값과 **정확히 일치**해야 한다(초과 = 설명되지 않은 외부 commit, 미달 = 주입 미재현 — 둘 다 불합격). 하네스 로그는 **제외 목록이 아니라 대조값**이다.
- **불변식 4(노출)**: `repair_finalized_at − committed_at`의 p99 ≤ 선언 SLO, 미해소 원소 1건이라도 있으면 No-Go.

판정 SQL은 `numrange`/`range_agg`(PostgreSQL 14+, `versions.lock`에 명시)를 쓰고, Q0의 ancestry·U·pairs는 **판정용 뷰 3개**(`v_poc_p`, `v_poc_u`, `v_poc_pairs` — 하네스 산출물이며 운영 테이블이 아니다)로 만들어 4개 쿼리가 공유한다. 참조 열은 전부 A 6.1·13.1에 실재하는 것만 쓴다(`commit_evidence_ledger`, `contract_state_history`, `attempt_state_history`, `execution_attempt.adjudicated_head_snapshot_id`, `execution_contract.parent_contract_id`). AuditEvent의 개입 snapshot id 목록은 payload 스키마를 §8.1 8번에서 고정하고 `jsonb_array_elements_text`로 평탄화한다.

### 7.3 Dagster 제출 단일 경로 — shard sensor + `run_key`

**기본안**: shard schedule은 **occurrence 생성 전용**(tick은 `occurrences:batch-create-or-get`만 호출하고 `SkipReason`, RunRequest 0)으로 남기고 — cron 전개·timezone/DST·`scheduled_execution_time`의 권위를 Dagster에 유지하기 위해서다 — **shard sensor가 유일한 제출 채널**이 된다. 평가마다 Control `POST /v1/shards/{shard}/submissions:poll {limit}` → 항목마다 `RunRequest(run_key="{contract_id}:{resubmit_no}", asset_selection=[…], tags={contract_id, resubmit_no, dagster/priority, dagster/max_runtime})`. 5개 재제출 경로(stale·Gap Recovery·Hold 해제·수동 NORMAL·RETRY)가 이 한 큐로 합류한다.

- **원칙 5는 깨지지 않는다**: occurrence 생성 권위는 tick에 남고 sensor는 **이미 생성된 계약의 제출**만 맡는다(Control이 due Job을 계산하지 않는다). 따라서 A:701("missing/duplicate 1건이면 cursor sensor로 승격")은 **삭제가 아니라 재작성** — cursor는 채택하지 않으며(상태 권위는 Control), 승격 대상은 private adapter 차선책이다.
- **인터페이스는 Control API 폴링**(DB 직접 읽기 불채택 — 경계 유지). definition 로드는 Control에 의존하지 않는다(SC-01 유지). 호출 실패는 `SkipReason` + tick FAILURE로 fail-closed.
- **poll 선정 조건(내부 모순 해소)**: `submission_in_flight IS NULL` **또는** (`now() − requested_at > submission_recheck_after` **∧** tag `dagster/run_key = "{contract_id}:{resubmit_no}"`인 run이 없음). 후자에서는 **`resubmit_no`를 올리지 않고 같은 run_key로** 다시 싣는다 — run_key 멱등이 "요청이 닿았는지 모름"을 흡수한다. 확정 조회는 `runsOrError(filter:{tags:[{key:"dagster/run_key", …}]})`(daemon이 반드시 붙이는 태그).
- **`submission_in_flight` 재정의**: `{resubmit_no, run_id, at}` → **`{resubmit_no, requested_at}`**. `last_submitted_run_id`는 사후 기록.
- **"계약당 non-terminal run ≤ 1"의 권위**: run_key는 `resubmit_no`를 포함하므로 재제출은 의도적으로 새 run을 만든다. 권위는 ① poll의 in-flight 제외 ② Guard binding CAS(`ATTEMPT_ALREADY_BOUND`) 둘이며, run_key는 "같은 `(contract, resubmit_no)`의 재평가 중복"만 막는다. 보장 명칭을 그대로 낮춘다.
- **따라오는 운영 제약 3건**: sensor 이름 고정, code location 이름·repository selector 고정(bundle digest를 이름에 넣지 않는다), **run row 삭제·wipe 금지**(dedup 권위가 run storage이므로).
- **지연**: 60초 gRPC 예산은 **sensor 평가 RPC**에만 걸린다. 실제 병목은 daemon 제출 루프이며 `sensors: {use_threads: true, num_workers: N, num_submit_workers: M}`을 `dagster.yaml`에 **명시하지 않으면 500건이 1건씩 제출된다**. burst 지연 상한 = `minimum_interval + poll + (limit × 직렬 제출비용 / num_submit_workers)`. 시험은 **`SC-02d` 신설**(`SC-02c`는 이미 H-11에 배정) — (a) tick→마지막 run 제출 end-to-end, (b) `fetch_existing_runs` 구간, (c) `num_submit_workers` 1/4/16 대조.
- **RTO 30분은 오히려 개선된다**: 중단된 tick은 `reserved_run_ids`로 최대 24h 재개, FAILURE tick은 1회 재시도(외부 사실 9) — 이것이 `run_submission` 테이블·exact recovery journal 기각의 1차 근거다.

**차선책(private `create_run + submit_run`)** 조건: (a) `versions.lock`에 Dagster 완전 고정(patch 자동 상향 금지), (b) `dagster._core.instance.methods.*` import를 버전 게이트로 감쌈(불일치 시 기동 거부), (c) **G0-4에서 실증**(왕복·중복 run_id 거부·`NOT_STARTED` 잔존 run 회수 3건), (d) 사용은 sensor가 SC-02d에서 SLO 미달일 때에 한하며 그때도 **채널 수를 늘리지 않고 sensor의 제출 구현만 교체**한다.

### 7.4 보증 축 분리 — JobSpec 입력 증가 0

`upsert_consistency`는 `load.cutoff.guarantee_grade`의 **표시 이름**(값 집합 동일)이고, `delete_consistency`·`delete_lag_slo_seconds`는 `delete_semantics`에서 **전순 결정되는 파생값**이다. 따라서 JobSpec 입력은 1개로 유지되고 validator rule 이름도 그대로다. 17장 `422` 응답의 `field`는 **JobSpec 키(`load.cutoff.guarantee_grade`)** 를 쓴다.

| `delete_semantics.kind` | `delete_consistency` | `delete_lag_slo_seconds` |
|---|---|---|
| `SOFT_DELETE` | `SYNC` | `0` (추가 지연 없음; 절대 반영 지연은 그 Job의 `freshness_slo`와 같다) |
| `PK_RECONCILE {interval}` | `BOUNDED_LAG` | `duration(interval) + reconcile Job의 freshness_slo` |
| `CDC_LATER` / `NONE_DECLARED` | `NONE` | `null` |

- `ZERO_GAP`은 `delete_consistency = NONE`과 양립하지 않는다(= 기존 A:511 rule). **새 rule 없음.**
- **INITIAL_LOAD `PER_CHUNK_FENCE` 구간은 선언값과 무관하게 `BOUNDED_LAG`로 상향 표시**한다(적재 중 hard delete는 sweep이 만들지 않는다). `SOFT_DELETE`는 12.2 final sweep로 잔여 0, `PK_RECONCILE`은 잔여 = `interval + freshness_slo`. `initial_load_in_progress`(계약 상태에서 파생)를 함께 노출한다.
- `last_reconcile_at` = 같은 pinned **target table identity**를 대상으로 하는 `PK_RECONCILE` 계약의 마지막 `finalized_at`(새 링크 컬럼을 두지 않는다).
- P §3.2 비교 B의 "설명된 차이"를 수치화: `target − Source` 각 PK에 대해 `(비교 시각 − 마지막 Source 변경 관측 시각) ≤ delete_lag_slo_seconds`면 설명된 차이, 초과면 **설명되지 않은 차이**.

### 7.5 canonical hash — NFC → AL32UTF8 바이트 → 길이-prefix → 2단 SHA-256

```
column_bytes(i) = TAG(1B) ‖ BYTELEN(4B BE) ‖ BYTES
d_i = SHA256(column_bytes(i))
g_j = SHA256(0xFE ‖ COUNT_j(4B BE) ‖ d_{j,1} ‖ …)      -- 매핑표 순서 32개씩
h   = SHA256(0xFF ‖ COLCOUNT(4B BE) ‖ g_1 ‖ … ‖ g_m)
```

- **항상 2단 고정**(상한 분기 자체가 drift 원인). 컬럼 수 **> 1024는 publish validator 거부**.
- 타입 태그: `0x00 NULL`(BYTELEN 0 — sentinel 폐기) / `0x01 STRING` / `0x02 NUMBER`(고정 십진 텍스트, `-0` 금지) / `0x03 TIMESTAMP`(UTC ISO-8601 µs) / `0x04 BINARY`(원바이트, `RAWTOHEX` 폐기) / `0x05 LOB_DIGEST`(BYTES 자리에 SHA-256 32B).
- **STRING(Oracle)**: `UTL_I18N.STRING_TO_RAW( COMPOSE( DECOMPOSE( TO_NCHAR(<col>), 'CANONICAL' ) ), 'AL32UTF8' )` — `COMPOSE` 단독은 NFC가 아니며(외부 사실 10), `TO_NCHAR`는 선택이 아니라 **필수**(비-Unicode charset이면 무동작). `DECOMPOSE` 결과가 최대 길이를 넘으면 **오류 없이 조용히 절삭**되므로 `LENGTHB` 사전 가드를 두고 초과 컬럼은 `0x05` 경로로 강제한다.
- **NULL vs 빈 문자열**: Oracle에서 `''` = NULL이므로 **Spark도 `''`를 NULL(`0x00`)로 정규화**한다.
- **CLOB은 `DBMS_CRYPTO.HASH`의 CLOB overload를 쓰지 않는다** — charset이 문서에 없다(외부 사실 11). 명시적 AL32UTF8 변환 후 **BLOB overload**로 넘긴다. 결과적으로 엔진은 좁은 행 = `STANDARD_HASH(RAW,'SHA256')`, 광폭·LOB = `DBMS_CRYPTO.HASH(BLOB, HASH_SH256)` 두 경로이며 **두 경로의 동일성은 가정하지 않고 G0-3에서 교차검증**한다.
- **미확인 표기**: `STANDARD_HASH`의 RAW 허용은 배제 규칙에 의한 **추론**(G0-3 실증), `UTL_I18N`·`UTL_RAW`·`DBMS_CRYPTO`의 SQL 직접 호출 가능성 미확인 → **기본안은 PL/SQL 함수로 감싼다**(physical standby는 읽기 전용이므로 **객체 생성·EXECUTE 권한은 primary 선행 작업** — 22장 DBA 항목), Oracle ↔ JVM Unicode 버전 일치 미확인 → V-03~V-05 픽스처로 실측.
- **`MAX_STRING_SIZE`**: 규격은 `STANDARD`(SQL RAW 2000B)를 가정한다. `EXTENDED` 전환은 비가역이며 뷰 무효화·함수기반 인덱스 UNUSABLE 부작용이 있어 **운영 standby에 요구하지 않는다**. 소스별 값은 `SourceCapability.max_string_size`로 등록한다.
- **Spark**: `sha2(binary,256)`는 hex 문자열이므로 중간 단계는 `unhex()`로 되돌린다. `normalize(str,'NFC')` built-in은 4.0~4.2 registry에 **없다** → `java.text.Normalizer` 결정론 UDF를 Template에 고정하고 **UDF 소스 digest를 hash spec digest에 포함**한다.
- 검증 벡터 V-01~V-16(ASCII / 비-UTF8 charset 한글 / NFD·NFC / 결합문자 / BMP 밖 / NULL·`''`·`'<<NULL>>'` / 구분자 주입 2종 / CHAR trailing space / NUMBER canonical / 시간대 4종 / 선행 0x00 RAW / 32·33·64·65 컬럼 / RAW 상한 경계 / CLOB·BLOB 동일 내용 교차 / 1바이트 음성 대조).

### 7.6 G0 — Executable Spec Go 정식화

**PASS 조건**: **G0-1** DDL이 빈 PostgreSQL에 오류 0으로 적용 + **제약 10종 각각의 위반 거부 양성 증거 1건**(존재만으로는 PASS 아님) / **G0-2** §5.1 4층 + 보조 불변식 + §6 검출 쿼리 전부 dry-run, 미존재 열·enum 참조 0, 쿼리별 양성·음성 각 1건, EXPLAIN 통과 / **G0-3** canonical hash 벡터 V-01~V-16 양측 일치·불일치가 기대대로, `hash_participation = EXCLUDED` 컬럼 목록 기록 / **G0-4** pinned Dagster에서 sensor `run_key` tick-경계 멱등·scope 확인, `fetch_existing_runs` 500-key 소요 계측, GraphQL `ExecutionMetadata`에 `runId` 부재 introspection / **G0-5** 넷이 **모두 같은 `versions.lock` 아래**에서 실행.

**증거 `g0_evidence`**(`zero_gap_evidence`와 같은 형식): `g0_report_id`, `executed_at`, `versions_lock_digest`, `ddl_digest`, `verdict_sql_digest`, `canonical_hash_spec_digest`, `hash_vector_result`, `submission_path_result`, `source_kind ∈ {ORACLE_TEST_INSTANCE, ORACLE_COMPATIBLE_STUB}`, `oracle_env{nls_characterset, max_string_size}`.

**무효화**(→ `g0_pass := false` + Outbox `g0 invalidated`, 그 시점부터 G1·G2 판정은 "미판정"): versions.lock 변경(patch 포함) / DDL digest 변경 / 판정 SQL digest 변경 / hash spec digest 변경(매핑표·태그·정규화·Spark UDF 교체 포함) / Source `NLS_CHARACTERSET`·`MAX_STRING_SIZE` 변경 / sensor 이름 규칙·repository selector 변경 / stub → 실 Oracle 대체는 무효화가 아니라 **재실증 후 새 report id, 이전 id는 `supersedes`**.

`SourceCapability.zero_gap_evidence`에 `g0_report_id`를 추가하고, 무효화 규칙에 한 줄 추가한다 — **무효화된 `g0_report_id`에 의존하는 모든 Source의 `zero_gap_verified`가 같은 트랜잭션 규칙으로 false가 되고**, 그 Source의 ACTIVE `ZERO_GAP` Job에는 기존 `HOLD_NEW(ZERO_GAP_INVALIDATED)` 규칙이 적용된다(2026-08-24 — "무효화 뒤 **설정된** true만 되돌린다"는 v3.1 초판 문장은 이미 유효하던 G2를 남겨 fail-open이었다). 과거 보고서 자체는 보존하고 `supersedes`로 연결한다.

### 7.7 ZERO_GAP과 트랜잭션 나이 — ENFORCED 근거는 `SYNC_COMMIT_GUARD`뿐(2026-08-24 최종)

**산술.** W = 트리거가 쓴 `UPDATE_DT`, C = 그 트랜잭션의 commit 시각, B ≡ sup(C − W), O = overlap. 회차 n의 추출 하한은 `low_n − O`이고 `low_n = T_{n−1} − safety_lag`이다. 문제 row는 C 이후 첫 fence 회차 n(T_n ≥ C)에 보이며, 잡히려면 `T_{n−1} − safety_lag − O ≤ W`. n의 정의상 **T_{n−1} < C**이므로 `O ≥ B − safety_lag`이면 충분하다. ⇒ **(a) A:544의 `O ≥ B + safety_lag + clock_skew`는 이미 `2·safety_lag + skew`만큼 보수적이고, (b) ETL 주기는 상한식에 들어가지 않는다**(내 v3.0의 `period`는 ETL 주기가 아니라 kill job의 `repeat_interval`이어야 한다).

**B의 상한.** kill이 성립하면 트랜잭션은 **롤백**되므로 그 row는 애초에 없다(gap이 아니다). 남는 것은 "커밋에 성공한 트랜잭션의 최대 나이"이며,

> **B ≤ T_threshold + G + P_kill + Q + L**
> G = `V$TRANSACTION.START_TIME` 초 절삭(≤ 1s, 유계) · P_kill = kill job `repeat_interval`(설정값) · Q = Scheduler 정시성(**문서 미보장**) · L = kill 지연(**문서 미보장** — ORA-00031은 "as soon as possible after its current uninterruptable operation is done", 수치 상한 없음)
> ⇒ `O ≥ T_threshold + 1s + P_kill + (Q+L)ₘₐₓ,ₒᵦₛ + safety_lag + clock_skew`

**판정(2026-08-24 최종).** Q·L에 문서상 상한이 없으므로 "문서 보증"으로서의 ENFORCED는 성립하지 않는다. 나는 아래 C1~C4 아래의 조건부 ENFORCED를 제안했으나 **Codex가 같은 지시를 재확인해 그 결정을 따랐다** — v1.2.3 최종본에서 `TXN_AGE_KILL_JOB`은 ENFORCED 근거에서 제외되고, `ZERO_GAP`은 commit-ordered cutoff(`STANDBY_VISIBLE_SCN`·`CDC_OFFSET`) 또는 동기식 fail-closed 장치(`SYNC_COMMIT_GUARD`)에만 허용한다. C1~C4는 폐기하지 않고 kill job Source의 overlap 보수화·상시 반증 조건으로 재배치했다. 아래 산술은 그 재배치의 근거로 남긴다. 그러나 Codex가 든 **이유는 kill job에 적용되지 않는다**: 자원 제한 위반 후 COMMIT 허용은 **profile 축의 성질**이고(외부 사실 12), `KILL SESSION`은 문서상 롤백이다(외부 사실 13). 게다가 ORA-00031 경로의 결과는 "커밋된 채 누락"이 아니라 "**미커밋 상태로 관측 가능**" — 안전 방향의 실패다. 따라서 이 반론은 **kill job Source를 무조건 `DETECT_AND_REPAIR`로 보내는 근거가 약하다**는 데까지만 유효하고 등급 결정을 뒤집지 못한다. **최종 결론(2026-08-24, v1.2.3 / v1.2.3.1): ENFORCED 근거는 `SYNC_COMMIT_GUARD`뿐이며 `TXN_AGE_KILL_JOB` Source는 `BEST_EFFORT` + Audit이고, C1~C4는 그 Source의 overlap 보수화(C3)·상시 반증(C4) 조건으로만 남는다.** 위 산술과 C1~C4는 그 재배치의 근거로 유지한다.

- **C1 분산 트랜잭션 부재**: dblink/XA 없음 ∧ `DBA_2PC_PENDING` 상시 0. in-doubt 트랜잭션은 kill로 해소되지 않고 `COMMIT FORCE`로 **롤백이 아니라 커밋될 수 있어** B를 무계로 만든다 — 위반 Source는 `upsert_consistency = BEST_EFFORT`로 강등.
- **C2 kill job liveness**: `JOB_QUEUE_PROCESSES > 0` ∧ PDB Scheduler 활성 ∧ 마지막 성공 실행이 `P_kill + margin` 이내 ∧ RAC이면 `GV$TRANSACTION` + `instance_id` 전 인스턴스 커버. **이 liveness가 관측되지 않는 회차의 효과는 Guard fail-closed가 아니라 C3 overlap 보수화 근거 상실 → 그 회차를 AuditEvent로 기록하는 것이다**(그 Source에 이미 필수인 12.3 Audit의 window가 그 회차를 덮으므로 회차 단위 표시를 새로 두지 않는다)(2026-08-24 최종 재배치 — kill job Source에는 `ZERO_GAP` 계약이 존재하지 않는다; A 11.3 C2·22장 4번과 동기).
- **C3 overlap 부등식**: 위 식을 publish validator가 검증(`(Q+L)`은 SLO가 아니라 **관측량** — PoC 실측 최대값 + margin).
- **C4 상시 반증**: 최대 트랜잭션 나이·kill 지연 분포를 상시 관측해 `(Q+L)ₘₐₓ,ₒᵦₛ` 초과가 1건이라도 나오면 `zero_gap_verified := false` + 기존 Outbox `zero gap verification invalidated`.

**용어**: `bound_kind = ENFORCED`의 정의를 "DBA가 DB 장치로 **보증**"에서 "**DBA 장치 + 측정된 상한 + 미충족 시 탐지**"로 하향한다(A:1575가 이미 "Oracle에는 트랜잭션 지속시간을 직접 강제하는 파라미터가 없다"고 적고 있으므로 문구 정정이지 새 필드가 아니다). **`TXN_AGE_KILL_JOB` 단독 Source와 네 조건 미충족 Source의 기본값은 모두 `BEST_EFFORT`**이며(2026-08-24 최종 — ENFORCED 근거는 `SYNC_COMMIT_GUARD`뿐), `CONNECT_TIME`은 백스톱으로만 남기되 "checks every few minutes … can exceed by 5 minutes"를 상한식의 명시 항으로 넣는다. `IDLE_TIME`·`MAX_IDLE_BLOCKER_TIME`은 근거에서 제거한다. "marked for kill 세션이 이후 COMMIT에 성공할 수 없다"는 **미확인**이므로 L을 상한식에서 빼지 않는다.

### 7.8 절별 편집 목록

**A** — 6.1(A:328·340·341·350·362·364·365·367: `bound_evidence`+digest 입력, `password_rollover` 2항, `fence_ts` 정의, `lease_grant.stage`, 보장 문구 하향, Source lock 한정, `target_lease` 트리거, 자동 Hold reason 1종, `submission_in_flight` 재정의, 제약 (5) 근거 교체) · 5.1(A:252 `sensors.*`·run wipe 금지) · 5.4(A:308·312 RESYNC 트리거·release operation 재개) · 6.2(A:388·401·403) · 7.2(A:511·512·519) · 7.3(A:545·553·556) · **9.1~9.3(A:616~630·672·693·695·701 — sensor 단일 채널 전면 재작성)** · 10.2(A:807·809 `T_lb_k`) · 11.2(A:905·908·909 회계 키) · 11.3(A:925·935·939 등급 3항·증거 기반 명문화) · 11.4(A:950 LOB RETENTION) · 12.1·12.2(A:985·1007 sweep 하한·delete 축) · 12.3(A:1029·1041) · **13.1·13.2(A:1088 Append OCC 사실 정정·A:1129 착지 5갈래·`DETECT_AND_REPAIR`)** · 14.2·14.3 · 16.2·16.4(coverage·transition key·`main_exposed`·조건부 Hold) · 17·18·19(A:1373·1435·1443·**1457·1462 No-Go 7 재정의**) · 22장(LOB RETENTION·txn-age 장치·C1~C4·PL/SQL 객체 primary 선행·권한).

**P(7차)** — §2.1(`MAX_STRING_SIZE`·ORA-02391 창·`bound_evidence`) · §2.3 · §2.4(`dq_basis` 중복·`lease_grant.stage`) · §3.1(CLOB Merge Job 1개) · **§3.2(canonical hash 전면 교체·LOB 미비교)** · §3.3(**SC-02d 신설**) · §3.4(FI-02(g)·FI-06(c)·**FI-14 문구 정정**·FI-16(C)·FI-24c(c)·FI-27(d)·FI-41g·FI-42 deadlock·FI-44e (b)(c)·FI-45d·**FI-50 전면 재작성**·FI-51 (iv)) · §5·§5.1(REATTACH 필터·**4-불변식 술어**·delete lag·coverage) · §6(**No-Go 7 재정의**·ORA-02391 모집단·출처 v1.2.2) · §8.1(**G0 5항**) · §8.3(G0 게이트 문단·G2에 Oracle probe) · §8.6.

**순서**: v1.2.3 + 기준서 7차 → **G0** → G1(BEST_EFFORT) → Source별 G2 → 해당 Source만 강한 보장 활성화.

---

## 8. 결론

3차 리뷰는 앞선 두 리뷰와 성격이 다르다. 1·2차는 "빠진 불변식"을 찾았고, 3차는 **"있는 불변식이 실제로 그만큼을 보장하지 못한다"**를 찾았다. 4차 확인은 여기서 한 걸음 더 나아가, **내가 제안한 교정 수단 자체가 그만큼을 보장하지 못한다**는 것을 짚었고 그 지적은 세 건 모두 소스 수준에서 사실이었다.

가장 값진 소득은 하나의 문장으로 요약된다. **Iceberg REST commit의 CAS는 "writer가 방금 refresh한 head"에 대한 것이지 "Control이 지정한 head"에 대한 것이 아니다.** 이 사실이 (a) `retry=0`이 예방책이 아닌 이유, (b) 현행 FI-50 (iii)이 재현 불가인 이유, (c) WAP/branch + fast-forward가 실제로 예방 수단이 되는 이유를 동시에 설명한다. v1.2.3은 이 사실 위에서 보장 명칭을 다시 쓰는 개정이어야 한다.

§7.7의 이견은 Codex의 재확인으로 철회했고(2026-08-24 — `TXN_AGE_KILL_JOB`은 ENFORCED 근거가 아니다), v1.2.3 최종본이 그 결정을 반영한다. 5건 정정이 이 문서에 반영됐으므로, 다음 단계는 v1.2.3 + 기준서 7차 제작이다.
