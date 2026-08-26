# 신규 ETL Platform 목표 아키텍처 v1.2

- 문서 상태: PoC 승인 후보안 — v1.2 (PoC 기준서 §8.4 반영 개정)
- 작성일: 2026-08-22 (v1) · 개정일: 2026-08-23 (v1.1) · 2026-08-23 (v1.2)
- 변경 근거: PoC 시험·합격 기준서 v1(`etl-platform-poc-test-plan-v1.md`) §8.4 “v1.2 반영 후보” 14건, §2.4 계측 테이블, §7 운영 파라미터 시작값
- 기준 규모: Job 약 10,000개, Run 약 40,000건/일, 정시 Burst 약 500건
- 핵심 적재: Oracle → Spark → Iceberg/Polaris

## v1.1 → v1.2 변경 이력

개정 원칙: PoC 시험·합격 기준서 v1(`etl-platform-poc-test-plan-v1.md`, 이하 기준서)이 v1.1 밖에서 **전제해야 했던 동작**(§8.4 14건)과 그 문서가 도입·고정한 계측 테이블(§2.4)·파라미터 시작값(§7)만 본문에 정의한다. 실행 모델(Occurrence ⊃ Contract ⊃ Attempt, Guard 1~9번, Commit Adjudication 13.2, lease 13.3/11.2, Hold 14.1/14.2, 6.2 상태 집합)은 바꾸지 않으며 기존 상태·사유·엔드포인트 이름을 재사용한다 — 새 이름은 기존에 없을 때만 만들고 본문에 “신설”로 표시한다. 19장은 기준서로 이관하고 판정 구조 요약만 남긴다. PoC 결과로 확정할 값(22장 1·3·6·7·8·11·20·22번)은 “잠정”으로 표시한다. 아래 표에 없는 절은 v1.1 그대로다.

| ID | 절 | 변경 요지 | 근거 |
|---|---|---|---|
| A1 | 6.1 / 6.2 / 10 / 10.2 / 11.3 / 13.1 / 13.4 / 16.4 | `DEGRADED_CONFIDENCE` = Guard 6번에서 `BEST_EFFORT` 계약에만 기록하는 contract 속성 `confidence = DEGRADED`(`confidence_reason = DATUM_STALE \| LAG_QUERY_FAILED`). lag threshold 초과·fence 미조회는 등급 무관 거부 유지. **attempt 실행 중 lag 재조회·CAS 거부·`ADJUDICATION_PENDING` 전이 없음** — 모든 chunk가 `AS OF visible_scn`으로 읽으므로 Guard 이후 lag 상승은 row 집합을 바꾸지 못한다 | 기준서 §5 apply lag 행, FI-11, §8.4 |
| A2 | 5.4 / 6.2 / 10.2 / 13.2 / 14.1 / 16.4 / 17 | Guard 5번 끝 **target health check**(`target_health_timeout_seconds` 5초) 실패 → Guard 사유 `TARGET_UNAVAILABLE`(200 거부, `PLANNED` + backoff, Source 미조회); breaker key(Polaris catalog·AIStor)별 연속 `platform_breaker_failures`(3)회면 같은 트랜잭션에서 자동 `HOLD_NEW(PLATFORM_BREAKER(key))`(Job 목록/Global scope). Guard 뒤 `base_snapshot_id` 조회 실패는 `attempt-failure {TARGET_UNAVAILABLE}` → SA 미생성이므로 구간 판정 없이 즉시 `NO_COMMIT`. Adjudication의 Polaris 조회 실패는 보류(`RECONCILIATION_REQUIRED` 아님) | FI-28, §8.4 |
| A3 | 6.1 / 10.1 / 10.2 / 17 | Run Pod 호출 수락 조건을 **소유권 검사** 두 조건으로 확정 — binding 일치 ∧ `attempt.terminal_ingested_at IS NULL`(신설 컬럼). 어긋나면 기존 `412 ATTEMPT_FENCED` + `fence_reason: REBOUND \| RUN_TERMINAL`. 반입 전 zombie 호출은 정당한 소유자 호출로 수락(불변식: 소유권 통과 Run Pod ≤ 1). 반입↔commit 경합은 contract row lock 직렬화, driver `proceed`·Control `chunks:begin`은 chunk_no 멱등 | FI-09(A), §8.4 |
| A4 | 6.2 / 10 / 10.2 / 13.1 / 13.2 / 16.4 / 17 | 탐지는 Run Pod(receipt k의 ancestor `lineage`를 `chunks/{n}:commit`에 실음), 판정은 Control(13.2 3번과 같은 분류 함수). lease 기록 없는 개입 snapshot이면 ledger row(`dq_result = EXTERNAL_SNAPSHOT`, CAS 없음) + `RECONCILIATION_REQUIRED(EXTERNAL_SNAPSHOT)` 전이(새 전이 — Adjudication·verdict 없음), Run Pod는 `DQ_FAILED`와 같은 stop → `finalize {outcome: RECONCILIATION_REQUIRED}` 프로토콜, Run FAILURE. OCC 실패 경로는 13.2로 같은 상태에 수렴 | FI-31(a), §8.4 |
| B1 | 6.2 / 9.3 / 13.4 / 14.2 / 16.4 | CATCHUP을 `expires_at`(= `hold_release_at + period`)에서 **면제하지 않음** — 만료는 데이터 손실이 아니라 설명 주체 교체(window는 watermark·fence에서만 파생). `PLANNED` CATCHUP과 `PLANNED` NORMAL은 공존(coalesce 주체·대상은 window를 가진 계약뿐, Job당 `PLANNED` ≤ 2), 만료 알림은 `hold_id` 단위 집계, 해제 202에 `expires_before_completion` | SC-06, §8.4 |
| B2 | 10.2 / 14.3 / 17 | RETRY 응답 확정 — `ATTEMPT_ACTIVE`·`COMMIT_OBSERVED`·verdict 미확정 `ADJUDICATION_PENDING`은 `409 ATTEMPT_IN_PROGRESS`(+`required_action`), `PLANNED`는 `409 CONTRACT_NOT_STARTED`, 미소비 authorization 재요청은 `200 ALREADY_SUBMITTED` 또는 9.3 단일 경로 재제출(`resubmit_no`↑, `retry_no` 유지), `CONTRACT_CLOSED`는 종결 계약 전용 | FI-20, §8.4 |
| B3 | 6.1 / 9.2 / 9.3 / 14.3 / 17 | 수동 NORMAL은 create-or-get 뒤 `PLANNED` ∧ non-terminal run 없음일 때만 제출(`launch_result: SUBMITTED \| ALREADY_SUBMITTED \| NOT_LAUNCHABLE`, `next_eligible_at` 대기 없음); 재제출은 stale 루프·Gap Recovery·Hold 해제·수동 NORMAL·RETRY 재요청이 공유하는 단일 코드 경로(`resubmit_no` 계약 row 채번) | FI-20(b), §8.4 |
| B4 | 9.3 / 16.4 / 17 / 22 | `POST /v1/schedule-gap-recoveries {scope?, range?, dry_run?}` → 202 + operation_id — 자동(heartbeat gap) 경로와 같은 operation·같은 코드(actor·range만 다름), 절차 자체가 멱등(재기동 시 새 row·새 Run 0), 겹치는 진행 중 operation은 200 기존 id, range 상한 `gap_recovery_max_range_seconds`(7일), 완료 이벤트 `schedule gap recovery completed` | FI-02, §8.4 |
| C1 | 6.1 / 6.2 / 10 / 10.2 / 14.1 / 16.4 / 17 / 22 | credential breaker = Source 단위·ACTIVE revision 기준, **attempt당 1건** 계수(입력: 새 reason `CREDENTIAL_FAILURE` — driver precheck 세션을 executor보다 먼저 열어 attempt당 실패 로그인 ≤ 1). `credential_breaker_failures`(1) 도달 시 같은 트랜잭션에서 Source `HOLD_NEW(CREDENTIAL_FAILURE)`; 해제는 새 CredentialRevision ACTIVE 전이 트랜잭션에서만(수동 release는 `412 CREDENTIAL_REVOKED`). validator 상한식 `credential_breaker_failures + 최대 동시 ETL Job ≤ FAILED_LOGIN_ATTEMPTS − 1`(`SourceCapability.failed_login_attempts`) | FI-16, §8.4 |
| C2 | 6.2 / 7.1 / 10.2 / 14.1 / 17 | ConnectionRevision REVOKED는 운영자 명령(`…/connection-revisions/{rid}/revoke {reason, force}`) — 살아 있는 attempt가 참조하면 기본 `412 OPEN_CONTRACTS_RUNNING` 거부(OPEN_CONTRACT_CHECK 본문 재사용), `force`면 FORCE_STOP 프로토콜로 `CANCELLED(CONNECTION_REVOKED)`; ACTIVE 없으면 Guard `CONNECTION_REVOKED`(`CREDENTIAL_REVOKED` 행과 동일 처리, 모니터 세션도 ACTIVE descriptor) | FI-27, §8.4 |
| C3 | 6.2 / 8.2 / 17 | `POST /v1/releases/{id}/rollback {reason}` → 202 {operation_id, release_id} — 동기 단계에서 `409 RELEASE_NOT_ACTIVE`·OPEN_CONTRACT_CHECK 읽기 사전검사(412), rollback release는 VALIDATED에서 시작해 DEPLOYED→VERIFIED→ACTIVE, ACTIVE 트랜잭션에서 `FOR UPDATE` 재검사 + 문제 release ROLLED_BACK + 문제 release를 pin한 `PLANNED` 전부 VOID(`interface_changed` 무관), `rollback_of` unique로 멱등 재개 | FI-24a, §8.4 |
| C4 | 7.2 / 11.3 / 17 | validate·publish 동일 validator — 거부 규칙 위반은 둘 다 `422 VALIDATION_FAILED {violations[{rule_id, field, actual, computed_minimum, inputs, message}], warnings[]}`(409는 상태 충돌 전용). `OVERLAP_BELOW_MINIMUM`: `computed_minimum = max_open_txn_seconds + safety_lag + clock_skew`(11.3 공식을 `safety_lag`로 통일 — `apply_lag`는 fence에 흡수), ZERO_GAP 거부·BEST_EFFORT 경고 | FI-12 대조군, §8.4 |
| C5 | 2 / 6.1 / 16.4 / 22 | Control **lateness sensor**(stale 루프와 같은 스케줄러, `planned_scan_interval` 주기, 읽기 전용)가 `freshness breach`·`expected occurrence missing`의 단일 계산 주체. lateness = `contract.finalized_at − logical_scheduled_at`(새 contract 컬럼 `first_guard_ok_at`·`last_cas_at`·`finalized_at`), `(job, logical_at)`당 1건 dedup, missing grace = `planned_stale_after`·커서 ≤ 1 period·schedule 단위 집계. Dagster freshness policy는 알림 원천에서 제외(22장 20번 확정) | 기준서 §2.2·§5, §8.4 |
| D1 | 6.1 / 5.3 / 5.4 / 16.1 / 16.2 | 기준서 계측 테이블 5종을 운영 1급 테이블로 채택 — `contract_state_history`·`attempt_state_history`·`lease_state_history`(전이와 같은 트랜잭션, 트리거 강제, append-only), `guard_result`(거부의 양성 증거·보호 지연), `attempt_timeline`(lateness 분해 원천). 쓰기 주체·보관 기간·재동기화 시 `actor=RESYNC`·화면/지표 참조 | 기준서 §2.4, §8.4 P-05 |
| — | 19 | **19장 축약** — 19.1~19.5를 기준서 참조로 대체하고 판정 등급(Go/Conditional Go/No-Go), Phase 1 게이트/Phase 2 진입 게이트, 즉시 No-Go 14건, 가설 13건 id·제목만 요약. 9.3·11.2의 19장 인용을 기준서 §5.1/FI-17로 정정 | 기준서 §1.2·§6·§4·§8.3 |
| D2 | 20 | Phase 1에 기준서 §2.2 구현 범위(Phase 2 항목의 API-only 선행 구현)·이력 테이블·시험 훅·Phase 1 게이트 판정 추가, Phase 2에 진입 게이트(실 DR shadow 전) 추가 | 기준서 §2.2·§8.3 |
| D3 | 22 / 5.1 / 9.1 | 22번을 기준서 §7 시작값의 잠정 기본값 + 규칙으로 재서술 — `run_monitoring.poll_interval_seconds` 60, `chunk_proceed_timeout_seconds = reattach_grace_seconds + 120`(Control 기동·설정 검증), Job class별 `freshness_slo` 잠정값, `credential_breaker_failures` 1·`platform_breaker_failures` 3, `daemon_heartbeat_gap_seconds = 2 × 최소 period`, `run_monitoring.max_runtime_seconds` 규칙(Job별 tag `dagster/max_runtime`). 5.1 run_monitoring 행·9.1 batch 응답에 반영 | 기준서 §7 |

신설 이름(v1.2): 상태·사유 `RECONCILIATION_REQUIRED(EXTERNAL_SNAPSHOT)`, `dq_result = EXTERNAL_SNAPSHOT`, verdict/attempt-failure reason `TARGET_UNAVAILABLE`·`CREDENTIAL_FAILURE`·`CONNECTION_REVOKED`, Guard 사유 `TARGET_UNAVAILABLE`·`CONNECTION_REVOKED`, Hold reason `CREDENTIAL_FAILURE`·`PLATFORM_BREAKER(key)`, contract 속성 `confidence`·`confidence_reason`·`first_guard_ok_at`·`last_cas_at`·`finalized_at`·`resubmit_no`, attempt 속성 `terminal_ingested_at`, `ATTEMPT_FENCED.fence_reason`, API 코드 `409 ATTEMPT_IN_PROGRESS`·`409 CONTRACT_NOT_STARTED`·`409 RELEASE_NOT_ACTIVE`·`422 VALIDATION_FAILED`, 응답 필드 `launch_result`·`expires_before_completion`·`lineage`, 엔드포인트 `POST /v1/schedule-gap-recoveries`·`POST /v1/releases/{id}/rollback`·`POST /v1/sources/{id}/connection-revisions/{rid}/revoke`, 이벤트 `degraded confidence`·`target unavailable`·`schedule gap recovery completed`, 파라미터 `target_health_timeout_seconds`·`platform_breaker_failures`·`credential_breaker_failures`·`gap_recovery_max_range_seconds`, 이력 테이블 5종(6.1)과 그 값 `contract_state_history.actor ∈ {RELEASE_CHECK, EXPIRY, GAP_RECOVERY, RESYNC}`·`attempt_state_history.reason = REATTACH`·lease state `GRANTED`·`RELEASED`, validator rule `OVERLAP_BELOW_MINIMUM`·`ZERO_GAP_REQUIRES_MAX_OPEN_TXN`(17), batch 응답 필드 `max_runtime_seconds`·RunRequest tag `dagster/max_runtime`(9.1), release 속성 `rollback_of`(6.2), `source.credential_breaker`(6.2), `SourceCapability.failed_login_attempts`.

## v1 → v1.1 변경 이력

개정 원칙: 리뷰(`etl-platform-target-architecture-v1-review.md` v1.1)의 P0 2건과 §1.4 “① v1.1 선반영” 항목이 지적한 **의미론 결함만** 고친다. PoC에서 판정할 가설(500 RunRequest batch의 성능, WAP 적용 범위, compaction 동시성, snapshot 보존 용량, Run Pod 자원 등)은 본문에 결정으로 쓰지 않고 “PoC 시험·합격 기준서(별도 문서)”로 보낸다. 아래 표에 없는 절은 v1 그대로다. v1.1 초안에 대한 4관점 검증(커버리지·내부 정합·기술 정확성·구현 충분성, 86건)과 재빌드본에 대한 2차 검증(86건 회귀·정합성·8개 시나리오 구현 추적, 51건)을 반영해 실행 모델을 확정했다. 2차 검증에서 확정한 결정: source token은 SparkApplication 종료 확인(`finalize`)에서만 반환, 자동 재시도 사유는 `RUN_WORKER_LOST`뿐이며 그 외 새 attempt는 RETRY API가 발급한 `retry_authorization`을 Guard가 소비, Full 0건은 driver가 쓰지 않고 receipt만 보고, `DQ_FAILED`/`RECONCILIATION_REQUIRED`는 repair REPLAY가 window·lease를 인수해 `RESOLVED`로 닫음, `ADJUDICATION_PENDING`은 `expires_at`에 만료(부분 commit 보존), Run Pod 사망 시 SparkApplication이 살아 있으면 `reattach_grace_seconds` 동안 fencing 보류.

| 절 | 변경 요지 | 리뷰 근거 |
|---|---|---|
| 1 / 4 | “만들지 않는 것” 목록을 실행 계약·evidence·lease 소유와 정합하게 재서술, Iceberg snapshot = 1차 증거(만료 후 증거는 ledger) | 검증 I-07 |
| 2 | 원칙 3(fencing 선행)·4(release 고정 규칙)·5(admission 범위와 예외 3종)·6(정각 논리 시각 유지) 재서술, 원칙 9(visibility fence) 추가 | DOC-01, CON-02, ORA-01 |
| 3 | 논리 아키텍처에 Guard→Control, code location→Control, Spark→Control, Control→Operator/Oracle 경로와 Bundle 저장소 추가 | 검증 I-08 |
| 5.1 / 5.3 / 5.4(신설) | 복구 의미 정정(동일 Run resume 비전제), Webserver read-only 분리, dagster.yaml·daemon 환경변수 기준값, 증거/snapshot 보관 분리, Control PostgreSQL HA/DR | DAG-04, DAG-08, DAG-10, OPS-01, OPS-09, ICE-01 |
| 6.1 / 6.2 | Occurrence ⊃ Contract ⊃ Attempt 3계층. Guard 1 트랜잭션 원자성에 맞춰 `PLANNED → ATTEMPT_ACTIVE → ADJUDICATION_PENDING …`로 계약 상태 단순화, 미실행 종결 `VOID(reason)`, disposition, CredentialRevision(attempt 단위 고정), CommitEvidenceLedger(chunk 단위), DefinitionRelease 전이 주체·`effective_from`·ROLLED_BACK | CON-01, CON-03, CON-06, CON-12, CRT-07, CRT-01, CRT-09, 검증 M1/M2/M4/M10 |
| 7.1 ~ 7.3 | pinned revision의 REVOKED 시 재해석, Wizard 단계(cutoff·초기 적재·delete semantics·타입 경고·용량 Gate), JobSpec 필드 추가 | CRT-05, CRT-06, ORA-02, ORA-09, ICE-02, ORA-06 |
| 8.2 | release 절차에 OPEN_CONTRACT_CHECK·`effective_from`·ROLLED_BACK 반영 | 검증 I-09 |
| 9.1 ~ 9.3 | occurrence 정체성 키(`client_request_id` 포함), release 고정 규칙(Job 단위·ROLLED_BACK 제외), schedule 이름/default_status, tick 정합성(batch 응답 형식·fail-closed·`max_tick_retries`), catch-up 문구 정정, Schedule Gap Recovery 절차, PLANNED stale 검사 파라미터 | CON-01, DOC-03, DAG-06, DAG-02, DAG-01, DOC-13 |
| 10 ~ 10.2 | 시퀀스 수정(fence는 Control Source 모니터 세션이 Guard 시점에 읽음), 실패 경로 3종, attempt별 SparkApplication, Guard 단일 트랜잭션 검사 순서(window → target → source), binding CAS·복구 절차, chunk 루프 소유자 = Run Pod, 거부 사유별 결과 표, `ATTEMPT_FENCED`, Dagster 종료 사실 반입 표 | CON-02, DAG-03, DAG-04, ICE-06, CRT-04, 검증 COV-01/I-01/M1~M9 |
| 11.1 ~ 11.5 | DB-enforced hard limit 명시, Source 모니터 세션, weight 정의·heartbeat 주체·2단계 회수·관측 fence(길이 제한), SourceVisibilityFence(cutoff 3종·보증 등급), 역할 검증, SCN 출처·ORA-01555, contract-scoped staging | CON-05, ORA-03, ORA-01, ORA-10, ORA-05, 검증 M5/I-11 |
| 12.1 ~ 12.4 | Full 모드·overwriteMode 고정·0건 정책·source_scn, Append dedup 범위·0-row·단일 INITIAL_LOAD contract, Merge dedup 규칙·delete semantics enum·lease 종류·literal 규칙, 파티션 역할·타입 매핑 부록 | ICE-02, CRT-05, CRT-08, ICE-09, ORA-09, ORA-07, CRT-06 |
| 13.1 ~ 13.4 | chunk 단위 commit evidence ledger·commit wrapper·DQ 정의(모드별 row 대조)·DQ accept, Commit Adjudication(WRITER_FENCED → writer_kind 필터 구간 판정·접두 chunk·0-row receipt), lease 3단계·isolation 설정, window 계산(high ≤ low → NO_DATA, chunk 생성, exclusion constraint 구현)·RETRY stale·overrun coalesce | ICE-04, CON-08, OPS-05, ICE-05, CON-04, 검증 COV-02/M6/S3/S4/S12 |
| 14.1 ~ 14.4 | HOLD_NEW/DRAIN/FORCE_STOP 의미와 timeout, Hold 중 occurrence disposition, CATCHUP 생성 조건, 모드 표 갱신, INITIAL_LOAD는 BackfillPlan 밖 | CON-06, CRT-07, DOC-03, 검증 I-03/I-10/S10 |
| 16.4 / 17 / 18 | 이벤트·엔드포인트 추가(batch occurrence, guard, chunks:begin, dagster-terminal-event, attempt-failure, watermark:seed, adjudicate, dq:accept, contracts/{id}/retry), maintenance lease 체계 정합 | DAG-03, OPS-06, ORA-10, CRT-09, 검증 I-17 |
| 19 ~ 22 | PoC matrix·fault injection·SLO·No-Go를 v1.1 의미론에 맞춤, Phase 1/2 산출물과 MVP 목록 일치(maintenance Job·purge 포함), 확정 항목 11~22번 추가·10번 교체 | OPS-13, OPS-04, SCL-03, 검증 COV-08/COV-10/I-12~I-16 |

용어 변경(DOC-13): 9.3 “Reconciler” → **Schedule Gap Recovery**(누락 tick 복원 전용, commit 판정은 하지 않음), 13.2 상태 판정 → **Commit Adjudication**(결과 상태는 `RECONCILIATION_REQUIRED` 유지), 11.4/12.3의 정기 대조 → **Data Reconciliation Audit**. “ambiguous commit”은 현상 이름으로만 남긴다.

## 1. 결론

신규 플랫폼은 **Dagster OSS-first + 얇은 ETL Control Plane** 구조를 권고한다.

- Dagster는 Asset 정의, 스케줄 평가, Run Queue, 재실행, 실행 이력과 운영 UI를 담당한다.
- 별도 Control Plane은 Dagster가 알 수 없는 Source/TNS, Job 등록 Wizard, Source 보호, 전역 실행 멱등성, Hold, Template Release, 그리고 Dagster가 보장하지 못하는 실행 계약(Occurrence/Contract/Attempt)·commit evidence·watermark·lease·DQ 판정만 담당한다.
- Spark Operator, Iceberg, Polaris, AIStor, DataHub, Prometheus/Grafana, OpenSearch, Kafka는 유지한다.
- `1 Job/1 Target Table = 1 Dagster Asset`으로 표현하되, **Job마다 Python 파일을 만들지 않는다.** JobSpec과 공용 Asset Factory/Component가 10,000개의 Asset 정의를 생성한다.
- Airbyte는 핵심 오케스트레이터로 도입하지 않는다. 향후 표준 Connector가 필요한 일부 Source에만 선택적으로 검토한다.

이 구조는 Airflow와 HAflow의 역할을 그대로 이름만 바꾸는 것이 아니다. 특히 다음 기능은 새로 만들지 않는다.

- Dagster Run Queue의 대체와 Dagster run 상태(queued/running/failure)의 복제 — Control은 데이터 계약·attempt 종결 사실만 기록하고, 이미 만든 occurrence의 재제출(9.3)은 Dagster Adapter를 통한 bounded 재제출로만 한다
- 자체 Task/step retry 엔진 — Spark 실패의 자동 재시도는 없다. Control RETRY는 운영자 명령이고, 새 attempt 생성은 Guard의 fencing 규칙(13.2)을 따른다
- Job별 DAG/Python 파일
- 범용 DAG 편집기
- 자체 로그/라인리지/인프라 모니터링 화면

다만 Source 보호와 Hold, 모든 실행 채널에 걸친 멱등성은 Dagster 단독으로 충족되지 않으므로 Control Plane이 필요하다. 따라서 “아무 UI도 개발하지 않는 순수 Dagster”는 현재 요구사항과 맞지 않는다.

## 2. 핵심 설계 원칙

1. **의도·실행·데이터 Commit의 권위 저장소를 분리한다.**
2. **LLM 추천보다 Source 보호 정책이 항상 우선한다.**
3. **Retry 전에 이전 writer를 fencing하고, 그 다음 Iceberg commit 여부를 확인한다.** fencing 없이 “commit 증거 없음”만으로 재실행하지 않는다.
4. **모든 실행은 불변 JobSpec·Template·Image digest를 고정한다.** 어떤 버전을 고정할지는 “먼저 요청한 쪽”이 아니라 **논리 실행 시각에 유효했던 Definition Release**로 결정한다.
5. **Control Plane은 due Job을 계산하지 않는다.** Dagster tick이 제시한 (job, logical_time)에 대해 admission(Hold·멱등성·버전 고정·fence)만 판정한다. 예외는 세 가지뿐이며 모두 이미 정의된 cron의 bounded 재해석이다 — 수동 NORMAL의 최근 cron 경계 계산(9.2), Schedule Gap Recovery의 장애 구간 한정 cron 전개(9.3), PLANNED stale 재제출(9.3). CATCHUP의 논리 시각은 cron이 아니라 Hold 해제 시각이다(14.2). 16.4의 lateness sensor는 기대 tick을 전개하지만 occurrence를 만들지 않는 알림 전용 검사이므로 이 예외 목록에 들지 않는다.
6. **Hold 해제 시 누락된 모든 cron을 재생하지 않고, 데이터 의미에 맞게 coalesce한다.** 정각 논리 시각은 유지하며, 실제 실행만 Source admission과 우선순위 queue에서 대기한다.
7. **원천 DB를 다시 읽지 않고 Target 단계만 재시도할 수 있는 경로를 제공한다.**
8. **안전하지 않은 자동화보다 명시적 `RECONCILIATION_REQUIRED` 상태를 허용한다.**
9. **Incremental window의 상한은 호스트 시각이 아니라 Source에서 관측한 가시성 fence에서만 파생한다.** fence를 읽지 못하면 Spark를 제출하지도, watermark를 전진시키지도 않는다.

## 3. 논리 아키텍처

```mermaid
flowchart LR
    U[플랫폼 운영자] --> W[ETL 관리 UI]
    W --> C[Control API]
    X[CLI / 사내 자동화] --> C

    subgraph CP[ETL Control Plane - Modular Monolith]
      C --> JR[(Job / Source Registry)]
      C --> EC[(Occurrence / Contract / Attempt / Lease / Commit Evidence Ledger)]
      C --> TP[(Template / Definition Release)]
      C --> HO[(Hold / Backfill)]
      C --> AD[LLM Advisor]
    end

    TP --> BS[(AIStor Definition Bundles + ACTIVE pointer)]
    BS --> DS[Dagster Code Locations]
    DS -- tick batch create-or-get / run_status_sensor --> C
    C --> DA[Dagster Adapter]
    DA --> DW[Dagster Webserver x2]
    DD[Dagster Daemon x1] --> DPG[(Dagster PostgreSQL HA)]
    DW --> DPG
    DS --> DW
    DD --> RP[Kubernetes Run Pod]

    RP --> G[Execution Contract Guard]
    G -- guard / chunks:begin / evidence / CAS --> C
    G --> SP[SparkApplication Client]
    SP --> SO[Spark Operator]
    SO --> SJ[Spark Driver / Executors]

    SJ --> ORA[(Oracle DR)]
    SJ -- lease heartbeat --> C
    C -- Source 모니터 세션: fence / role / GV$SESSION --> ORA
    C -- FORCE_STOP / lease 회수 SA delete --> SO
    SJ --> STG[(AIStor Run Staging)]
    SJ --> ICE[(Iceberg Tables)]
    ICE --> POL[Polaris Catalog]

    SJ --> LOG[AIStor Logs / Spark Event Logs]
    DPG --> DU[Dagster UI]
    LOG --> DU
    ICE --> DH[DataHub]
    JR --> DH

    C --> OB[(Transactional Outbox)]
    OB --> KF[Kafka Alert Topic]
    SJ --> PM[Prometheus / Grafana]
    SJ --> OS[OpenSearch]
```

## 4. 시스템별 책임과 권위

| 대상 | 소유하는 사실 | 소유하지 않는 것 |
|---|---|---|
| Job/Source Registry | 사용자가 승인한 Source, JobSpec, 정책과 Release | 실행 중 상태 |
| Occurrence/Execution Contract | 논리 실행 시점·범위, 불변 버전, commit 증거, watermark CAS | Dagster의 queued/running/failure 이력 |
| Dagster PostgreSQL | Schedule/Sensor tick, Run Queue, Run/Step/Retry/Event 이력 | Iceberg commit 진실, Source 정책 원본 |
| SparkApplication CR | Spark 계산의 현재 상태 | 데이터 commit 성공 판정 |
| Iceberg Snapshot | 데이터가 실제로 commit되었다는 1차 증거(만료 전까지 — 만료 후 증거는 Control DB CommitEvidenceLedger, 13.1) | Job 설정과 Run Queue |
| Polaris | Iceberg REST Catalog와 접근 제어 | 오케스트레이션 상태 |
| DataHub | Discovery, ownership, table/column lineage | 실시간 Run 상태의 권위 원본 |
| Prometheus/Grafana | SLO와 시계열 지표/Alert | Job 상세 로그 |
| OpenSearch | 검색 가능한 상세 로그 | Job 설정 원본 |
| Kafka Outbox | 사내 메신저로 전달할 정규화 이벤트 | Run 상태 저장소 |

이 경계가 무너지면 새 Control Plane이 HAflow 2.0으로 커진다. 특히 Control DB에 Dagster의 `QUEUED/RUNNING/FAILURE`를 별도 상태 머신으로 복제하지 않는다. Dagster run 종료 사실(FAILURE/CANCELED/SUCCESS)은 Commit Adjudication의 트리거 이벤트로만 반입하고(10.2) 상태로 저장하지 않는다. 화면에 필요한 통합 상태는 Dagster에서 재생성 가능한 read model로만 유지한다.

## 5. Kubernetes 배포 기준안

### 5.1 Dagster

- Webserver: 2 replicas. 일반 사용자용 `dagster-webserver --read-only` 인스턴스와 플랫폼팀 전용 write 인스턴스를 분리하고, write 인스턴스는 SSO reverse proxy 뒤에 둔다(22장 10번). Dagster OSS에는 인증·RBAC·감사 로그가 없으므로 proxy access log가 감사 기록이다.
- Daemon: 1 replica, Kubernetes가 자동 재시작
- Dagster PostgreSQL: HA 구성, PITR/backup 포함
- Code Location: Source domain 또는 업무 domain 기준으로 shard
- Run Launcher: `K8sRunLauncher`
- Run executor: Run Pod 안에서는 우선 in-process
- Run Pod가 `SparkApplication` CR을 생성·감시·취소하고 chunk 루프를 소유한다(10.2). **Run Pod가 죽은 뒤 같은 Dagster Run이 재개(resume)되지는 않는다.** in-process executor에서는 run monitoring이 해당 run을 FAILED로 표시할 뿐이며, 재접속은 새 Dagster Run이 같은 ExecutionContract를 받아 기존 ExecutionAttempt와 SparkApplication에 재결합하는 방식으로만 이루어진다(10.2 복구 절차).

Dagster OSS는 Webserver 다중 replica는 지원하지만 Daemon active-active와 동일 Code Location의 다중 replica는 지원하지 않는다. 이 replica 제약은 공식 OSS 배포 구조와 일치한다. [Dagster OSS deployment architecture](https://docs.dagster.io/deployment/oss/oss-deployment-architecture) 따라서 목표는 무중단이 아니라 **30분 이내 자동 복구**, 내부 운영 목표는 5분으로 둔다. 30분/5분은 기존 플랫폼 운영 SLA를 잠정 인용한 값이며 22장에서 확정한다.

#### dagster.yaml·daemon 기준값 (PoC 배포안)

정확한 키 이름은 고정 버전 문서에서 재확인한다. 값은 PoC 측정으로 조정하되 아래 항목은 반드시 명시한다.

| 영역 | 설정 | 이유 |
|---|---|---|
| run_coordinator | `max_concurrent_runs` = 최대 동시 Spark 수 + 여유(기본값 10은 정시 burst를 50 wave로 직렬화함), `dequeue_use_threads: true`, `dequeue_num_workers`, `dequeue_interval_seconds` | 10.2, 기준서 §3 |
| run_monitoring | `enabled: true`, `start_timeout_seconds`(burst 고려, 시작값 300), `cancel_timeout_seconds`, `free_slots_after_run_end_seconds`, **`poll_interval_seconds: 60`**(기본 120 — Run Pod 사망 탐지 지연의 상한이며 `chunk_proceed_timeout_seconds` 여유 120초의 근거), `max_runtime_seconds`(instance 기본값 = 최대 period + 여유. Job별 상한은 Control이 내리는 RunRequest tag `dagster/max_runtime` — 실행 중 계약의 유일한 시간 상한, 9.3) | 10.2, 11.2, 22장 22번 |
| run_retries | `enabled: true`, `retry_on_asset_or_op_failure: false` — run worker crash 자동 재시도만. Spark 실패의 retry는 Control RETRY 경유 | 10.2 |
| scheduler (`DagsterDaemonScheduler` config) | `max_tick_retries ≥ 1`(기본 0). `max_catchup_runs`는 파티션 schedule 전용이라 미사용 | 9.1, 9.3 |
| schedules | `use_threads: true`, `num_workers`, `num_submit_workers` | 9.1 |
| daemon 환경변수 (dagster.yaml 아님) | `DAGSTER_SCHEDULE_GRPC_TIMEOUT_SECONDS`(미설정 시 `DAGSTER_GRPC_TIMEOUT_SECONDS`, 둘 다 없으면 60초) | 9.1 |
| concurrency | `pools.granularity: run`, Source별 pool은 느슨한 상한 | 11.2 |
| schedule 정의 | `default_status=RUNNING`, 결정론적 이름 | 9.1 |
| retention | run/event log purge 도구는 OSS 미제공 — 자체 purge(21장) | 5.3 |

### 5.2 Code Location shard

초기값을 고정하지 않고 PoC 결과로 정한다. 시작 후보는 8~16개이며 다음 기준을 동시에 만족해야 한다.

- 10,000 Asset cold load 시간
- 단일 Job publish 후 해당 shard reload 시간
- Code Location 장애 blast radius
- Webserver와 Daemon의 metadata 조회 부하
- 운영자가 이해할 수 있는 Source/domain 경계

Job 1건 변경마다 전체 10,000개를 다시 읽는 구조는 피한다. 변경된 shard의 불변 Definition Bundle만 새로 생성·검증·승격한다.

### 5.3 객체와 이력 보관

하루 40,000 Run이면 연간 약 1,460만 Run이다. Run Pod와 SparkApplication까지 합치면 Kubernetes object churn도 크다.

- 완료된 Run Pod와 SparkApplication에 TTL/GC 적용
- Dagster PostgreSQL의 Run/Event 용량, index, vacuum, backup/restore를 7일 soak test로 측정
- 온라인 상세 이력, 장기 요약 이력, AIStor 로그 보관 기간을 분리
- CommitEvidenceLedger(13.1) 보관 기간 > Commit Adjudication(13.2) 조사 기간. Iceberg snapshot 보관은 ledger와 분리해 테이블 등급별(Full은 짧게, Append/Merge는 time-travel 요구별)로 두되, Adjudication이 진행 중인 contract의 `base_snapshot` 이후 snapshot은 만료 대상에서 제외
- Dagster 버전별 지원 범위를 확인한 뒤 Run/Event archive 또는 purge 절차를 운영 Runbook으로 고정
- v1.2 이력·계측 테이블(6.1): `contract_state_history`·`attempt_state_history`·`lease_state_history`·`guard_result`는 CommitEvidenceLedger와 같은 보관 기간(> Commit Adjudication 조사 기간)을 적용한다 — Adjudication과 사후 감사가 ledger와 이력을 함께 읽기 때문이다. `attempt_timeline`은 온라인 90일(잠정) 후 Job·일 단위 lateness 집계만 남긴다. 월 단위 파티션, append-only(UPDATE/DELETE 금지), purge는 파티션 drop. 규모는 Run당 약 10~12 row(계약 3·attempt 3~4·lease 4) ≈ 50만 row/일, `guard_result`는 거부 재시도를 포함해 5~10만 row/일. 수치는 22장 7번에서 확정

### 5.4 Control PostgreSQL과 플랫폼 장애 동작

Control PostgreSQL은 watermark·ExecutionContract·lease·commit evidence ledger·Outbox의 유일한 권위 저장소다(4장). 오래된 백업으로 복구되면 watermark가 과거로 돌아가 Append 이중 commit, lease/fencing token 소실로 동시 쓰기가 생긴다.

- HA: 동기 복제, PITR. 잠정 RPO ≤ 1분, RTO ≤ 30분. 분기 1회 복구 리허설
- 복구 후 재동기화 Runbook: Iceberg snapshot summary(`etl.*` 키)와 Dagster run tag로 contract·attempt·watermark를 재구성하고, 재구성 완료 전까지 Global Hold. 이력 테이블(6.1)은 재구성하지 않으며 재구성된 contract·attempt·lease row마다 `actor=RESYNC` history row 1개만 남긴다 — 복구 이전 이력의 공백은 감사상 “설명된 공백”이다
- Polaris/AIStor 장애: Guard 5번 끝의 **target health check**(Control이 `target_health_timeout_seconds` 기본 5초 안에 Polaris `GET /v1/config` + pinned target table `loadTable`, AIStor payload/staging bucket `HEAD`)가 실패·timeout이면 `TARGET_UNAVAILABLE`로 거부한다 — 6번(Source 모니터 세션) 앞이므로 Source를 읽지 않고 fence도 소비하지 않으며, 계약은 `PLANNED` 유지 + backoff(10.2 표). breaker key는 Polaris catalog(`target.catalog`) 또는 AIStor이고, key별 연속 `TARGET_UNAVAILABLE` 거부가 `platform_breaker_failures`(기본 3)회에 도달하면 그 Guard 트랜잭션이 자동 `HOLD_NEW`를 만든다(reason `PLATFORM_BREAKER(key)`; Polaris catalog면 그 catalog를 target으로 가진 Job 목록 scope, AIStor면 Global). 카운터는 같은 key의 health check 성공에서 0으로 돌아간다. 해제는 14.1 규칙대로 운영자가 회복을 확인한 뒤 수행하며 CATCHUP이 놓친 회차를 coalesce한다 — 11.1의 circuit breaker는 Source 전용이므로 별도 규정. 실행 중 attempt는 `HOLD_NEW`와 무관하게 끝까지 수행하며 Polaris 오류는 receipt(`committed=null`·`exception_class`)로 보고돼 `attempt-failure` → Commit Adjudication(13.2)으로 간다(staging 보존, 새 attempt는 11.5 재사용). **Adjudication 서비스가 Polaris를 읽지 못하면 verdict를 내리지 않고 `adjudication_delay_seconds` backoff로 재시도한다** — 조회 실패는 13.2의 ‘증거 만료·상충’이 아니며 `RECONCILIATION_REQUIRED`로 보내지 않는다. Guard 성공 뒤 Run Pod가 `base_snapshot_id`를 읽지 못하면(`guard_retry_budget_seconds` 동안 재시도 후) `attempt-failure {reason: TARGET_UNAVAILABLE}`를 호출한다(10.2 표) — 이때 SA는 생성된 적이 없고(UID 없음) `attempt.base_snapshot_id`도 기록되지 않았으므로 Control은 13.2의 구간 판정 없이 즉시 `WRITER_FENCED` + verdict `NO_COMMIT(TARGET_UNAVAILABLE)`로 둔다(writer가 존재한 적 없음). health check 결과는 breaker key별로 수 초(`target_health_timeout_seconds` 이내) 캐시해 정시 burst 500건의 Guard가 각자 Polaris를 두드리지 않게 하며, 한 Guard의 probe 총 예산은 `target_health_timeout_seconds` 1회다(row lock 보유 중 외부 I/O 상한)
- Source 모니터 세션(11.2)이 끊기면 해당 Source의 Guard는 `FENCE_UNAVAILABLE`로 거부한다(Spark 미제출)
- 플랫폼 전체 DR(다른 클러스터/사이트) 범위 내/외는 22장에서 확정

## 6. Control Plane 구조

초기에는 Microservice가 아니라 **Java 기반 Modular Monolith + PostgreSQL**을 권고한다. 현재 중앙 플랫폼팀만 Job을 관리하므로 서비스 분산의 이점보다 트랜잭션 일관성과 운영 단순성이 더 크다.

### 6.1 핵심 Aggregate

- `SourceSystem`
  - `ConnectionRevision`
  - `CredentialRevision` (신설 — 자격증명 교체를 descriptor와 분리해 버전 관리. attempt 단위로 고정)
  - `SecretRef` (외부 Secret 저장소 경로)
  - `SourceSafetyEnvelopeVersion` (`max_chunk_span`, `safety_lag`, `clock_skew`, `datum_stale_seconds`, lag threshold 포함)
  - `SourceCapability` (lag 신호 종류, Flashback/UNDO retention, `max_open_txn_seconds`, hard delete, NLS_CHARACTERSET·DB 시간대, `failed_login_attempts` — ETL 계정 profile의 `FAILED_LOGIN_ATTEMPTS` 값, 6.2 credential breaker 상한 검증의 입력)
  - `MetadataSnapshot`
  - Source 모니터 세션 상태(11.2)
- `Job`
  - 변경 가능한 `JobDraft`
  - 불변 `JobSpecVersion`
- `Template`
  - 불변 `TemplateVersion`
- `DefinitionRelease`
  - `Bundle`, `Manifest`, 검증 결과, `effective_from`
- **실행 3계층**
  - `ExecutionOccurrence` — 논리 실행 시점. unique key와 종결 disposition을 가진다.
    - `ExecutionContract` — occurrence당 활성 1개. pinned release/digest, cutoff 종류와 fence 값(`visible_scn`, `fence_ts` — contract당 1회 고정), `confidence`(`FULL` | `DEGRADED`, `confidence_reason` — Guard 6번에서 1회 결정, 11.3), window(`window_range`, `window_kind`, `low`는 chunk CAS마다 전진), `current_attempt`, `retry_authorization`, `next_eligible_at`, `expires_at`(NORMAL·CATCHUP만), `resubmit_no`(Control 채번 단조 증가 — 9.3 재제출 단일 경로), `first_guard_ok_at`·`last_cas_at`·`finalized_at`(lateness 분해의 권위 원천, 16.4 — 각 전이 트랜잭션이 기록), 계약 상태와 종결 사유.
      - `ExecutionAttempt` 1..N — `attempt_no`, Dagster run binding 이력(`dagster_run_ids`, 현재 `bound_dagster_run_id`, `rebind_count`, `terminal_ingested_at` — 현재 binding run의 terminal 사실을 반입한 시각, 재결합으로 binding이 바뀌면 NULL로 초기화; 10.2 소유권 검사), SparkApplication 이름·UID·`last_observed_sa_status`, lease 집합, `base_snapshot_id`, chunk 목록과 `expected_chunk_count`(attempt마다 `low`부터 재산출), `credential_revision_id`, `reattach_deadline`, payload URI·digest, attempt 상태와 verdict(reason 포함).
- `BackfillPlan` / `BackfillItem`
- `MaintenanceHold`
- `TargetLease` (`EXCLUSIVE_TABLE` / `PARTITION_OR_FILESET` / `APPEND`), `SourceLease` (weighted token, `EXPIRING`/`RECLAIMED`)
- `CommitEvidenceLedger` — contract·attempt·chunk 단위 snapshot 증거. Iceberg snapshot 만료와 무관하게 영속
- **상태 이력·계측 테이블**(v1.2 신설 — PoC 기준서 §2.4의 계측 테이블 5종을 운영 1급 테이블로 채택. 테이블명은 기준서 그대로. 모두 append-only이며 **권위 저장소가 아니다** — 권위는 `ExecutionContract`·`ExecutionAttempt`·lease row의 현재 상태)
  - `contract_state_history` — `contract_id, attempt_no|null, from_state, to_state, reason, actor, dagster_run_id|null, at`. actor ∈ {`GUARD`, `RUNPOD`, `ADJUDICATION`, `SENSOR`, `OPERATOR`, `STALE_LOOP`, `HOLD_RELEASE`, `RELEASE_CHECK`(OPEN_CONTRACT_CHECK, 6.2), `EXPIRY`(`expires_at` 만료, 9.3), `GAP_RECOVERY`(9.3), `RESYNC`(5.4)} — 뒤 4개는 신설 actor 값
  - `attempt_state_history` — `attempt_id, from_state, to_state, reason, bound_dagster_run_id, rebind_count, last_observed_sa_status|null, at`. `reason`은 6.2 verdict reason·Guard 거부 사유를 그대로 쓴다. 재결합은 6.2대로 상태 전이가 아니지만 `from_state = to_state`, `reason = REATTACH`(신설 reason) row를 남긴다
  - `lease_state_history` — `lease_id, kind(SOURCE|TARGET), lease_type(TARGET: EXCLUSIVE_TABLE|PARTITION_OR_FILESET|APPEND, SOURCE: null), from_state, to_state, token_weight, granted_to_contract, attempt_no, at`. state는 11.2의 `EXPIRING`·`RECLAIMED`에 부여 `GRANTED`·반환/해제 `RELEASED`(신설 2개)를 더한 집합
  - `guard_result` — `contract_id, dagster_run_id, attempt_no|null, result(OK | FINALIZED_NO_DATA | 10.2 표의 거부 사유), contract_state_after, next_eligible_at|null, run_started_at, at`. Guard 호출마다 1 row. state history에 접지 않고 별도로 두는 이유: `LEASE_BUSY`·`SOURCE_LAG_EXCEEDED`·`FENCE_UNAVAILABLE`·`CREDENTIAL_REVOKED`·`ATTEMPT_ALREADY_BOUND`·`CONTRACT_CLOSED` 등 대부분의 거부는 계약 상태를 바꾸지 않아 전이 row가 없는데, 거부 횟수·보호 지연과 “0건 조건의 양성 증거”(Hold 중 제출 시도 ≥ 1 등)는 바로 이 row에서 나온다
  - `attempt_timeline` — attempt당 1 row: `attempt_id, t0_queued_at, t1_launch_at, t2_pod_started_at, t3_guard_ok_at, t4_sa_created_at, t5_driver_running_at, t6_first_receipt_at, t7_finalized_at, resubmit_t0[]`. `t7_finalized_at`은 `contract.finalized_at`의 복사이며 lateness의 권위 원천은 contract 컬럼이다(16.4 `freshness breach`, 22장 22번 `freshness_slo`) — 이 테이블은 attempt 단위 분해(t0~t6) 전용. 플랫폼 지연 = `t2 − t0`, 제출 지연 = `t5 − t4`, 실행 = `t6..t7`, 보호 지연 = `guard_result.result ∈ {LEASE_BUSY, SOURCE_LAG_EXCEEDED, FENCE_UNAVAILABLE}`인 row에 대해 Σ(`at − run_started_at`) + Σ(`next_eligible_at − at`) — 16.4와 같은 정의(플랫폼 사유 `TARGET_UNAVAILABLE`·`CONTROL_API_UNAVAILABLE`은 플랫폼 지연)
  - 쓰기 규칙: history 3종은 상태를 바꾸는 **같은 Control 트랜잭션**에서 INSERT한다. 구현은 `execution_contract`·`execution_attempt`·lease 테이블의 상태 컬럼 UPDATE 트리거로 강제하고 reason·actor·dagster_run_id는 트랜잭션 로컬 설정(`SET LOCAL etl.actor …`)으로 넘긴다 — 응용 코드 경로 하나가 빠져 이력이 끊기는 일을 막는다. `guard_result`는 Guard 트랜잭션의 “결과는 commit” 부분(10장 실패 경로 1)에 속해 savepoint 롤백 뒤에도 기록된다. `attempt_timeline`은 t0~t3을 Guard 트랜잭션이(Run Pod가 `guard` 본문에 자기 Dagster run stats의 enqueued/launch/start 시각을 실음 — 거부 시에는 `guard_result.run_started_at`에만 남음), t4~t6을 Run Pod가 `chunks/{1}:commit` 본문으로, t7을 `finalize`(Adjudication의 finalize 대행 포함)가 채운다. 재결합·재제출 run의 t0는 `resubmit_t0[]`에 append한다. attempt 없이 종결된 계약(`PLANNED → FINALIZED_NO_DATA`)은 `attempt_timeline` row가 없지만 `contract.finalized_at`이 같은 전이 트랜잭션에서 기록되므로 lateness는 동일하게 contract 컬럼으로 계산한다(`VOID`는 t7이 없어 16.4 (a)로만 잡힌다)
  - 용도: 13.2 `WRITER_FENCED`·verdict 증빙(`attempt_state_history`의 `TERMINAL_OBSERVED → FENCED → ADJUDICATED`), 11.2 “`RECLAIMED` 확인 전 재부여 금지” 감사(`lease_state_history`), 16.1 contract 상세 화면, 16.2 지표 원천, PoC 기준서 §5.1 판정 쿼리의 운영 재사용
- `AdvisorAnalysis`
- `NotificationOutbox`
- 공통 `AuditEvent` (actor, auth_method, source_ip, idempotency_key 필수), `IdempotencyRecord`

Retry는 같은 Contract와 같은 window를 사용하고 실제 Spark 재실행만 새 `attempt_no`를 가진다. Replay와 Backfill만 별도 Contract를 만든다. Dagster run binding은 Attempt의 속성이며 Contract는 `current_attempt`만 가진다.

### 6.2 버전 상태

`ConnectionRevision`:

```text
CANDIDATE → VERIFIED → ACTIVE → SUPERSEDED 또는 REVOKED
```

- ConnectionRevision은 descriptor hash로 **contract**에 pin된다(7.1). `REVOKED`는 운영자 명령(`POST /v1/sources/{id}/connection-revisions/{rid}/revoke {reason, force}`, 17장)이며 시계가 없다. 그 revision을 참조하는 attempt가 `BOUND`/`SUBMITTED`이고 `WRITER_FENCED` 미확정이면(계약 `ATTEMPT_ACTIVE`·`COMMIT_OBSERVED`·`DQ_FAILED`·`RECONCILIATION_REQUIRED(EXTERNAL_SNAPSHOT)`(finalize 전, 13.1)·`ADJUDICATION_PENDING(RUN_WORKER_LOST, reattach 대기)`) 기본값은 **거부** — `412 OPEN_CONTRACTS_RUNNING` + `{contract_id, attempt_no, state, required_action: DRAIN Hold}` 목록(OPEN_CONTRACT_CHECK와 같은 본문), 상태 변경 없음. `force: true`면 같은 트랜잭션에서 `REVOKED` 전이 후 해당 attempt마다 Source scope Hold의 `FORCE_STOP` 프로토콜(14.1)을 기동하고 결과 reason은 `CONNECTION_REVOKED`다(`CANCELLED(CONNECTION_REVOKED)` 또는 `ABORTED_NO_COMMIT`, 부분 commit은 Adjudication이 CAS 반영). 살아 있는 attempt가 없으면(PLANNED, fenced `ADJUDICATION_PENDING`만) 즉시 `REVOKED`. `REVOKED` 뒤 새 attempt는 Guard가 현재 ACTIVE revision으로 재해석해 attempt에 기록하고(7.1, 10.2 6번), ACTIVE가 없으면 `CONNECTION_REVOKED`로 거부한다(10.2 표 — `CREDENTIAL_REVOKED` 행과 같은 처리, 새 ACTIVE 등록 시 대기 계약 `next_eligible_at` 리셋). `SUPERSEDED`는 pin된 계약의 새 attempt에서 그대로 쓴다.

`CredentialRevision`:

```text
CANDIDATE → VERIFIED(실제 Spark namespace에서 로그인 테스트) → ACTIVE → SUPERSEDED(grace 기간) → REVOKED
```

- `credential_grace_seconds`(Source별, 기본 `run_monitoring.max_runtime_seconds`)는 SUPERSEDED 전이 시각부터 센다.
- credential은 contract가 아니라 **attempt**에 고정된다. 새 attempt 생성 시 Guard는 현재 ACTIVE revision을 선택해 `attempt.credential_revision_id`에 기록한다. contract의 `pinned_credential_revision`은 최초 attempt 값의 기록일 뿐이다.
- REVOKED는 ACTIVE revision이 없을 때 신규 attempt를 `CREDENTIAL_REVOKED`로 거부하고(Guard 거부 — 200 응답·`PLANNED` 유지 + backoff, 10.2 표; `412 CREDENTIAL_REVOKED`는 breaker가 열린 `CREDENTIAL_FAILURE` Hold의 수동 release 거부에만 쓴다)(ACTIVE가 있으면 그것을 선택), 실행 중 attempt에 대해서는 Source scope Hold의 `FORCE_STOP` 프로토콜(14.1)을 자동 기동한다.
- **credential breaker**(Source 단위, 현재 ACTIVE revision 기준). 입력: attempt의 `attempt-failure {reason: CREDENTIAL_FAILURE}`(driver precheck 세션 로그인 실패 `precheck_failure{reason: CREDENTIAL_FAILURE, ora_code}` 또는 receipt `exception_class ∈ {ORA-01017, ORA-28000, ORA-28001, ORA-28002}` — 10장 인터페이스)와, 같은 revision으로 로그인하는 경우에 한해 Source 모니터 세션(11.2)의 재접속 실패. **attempt당 1건**으로 센다(세션 수·Spark task retry와 무관). SUPERSEDED revision(grace 중)에 고정된 attempt의 실패는 ACTIVE breaker에 세지 않고 그 attempt만 아래 규칙대로 `ADJUDICATION_PENDING`에 둔다. 누적이 `credential_breaker_failures`(Source별, 기본 1)에 도달하면 그 `attempt-failure` 트랜잭션에서 Source scope `HOLD_NEW` Hold(`reason = CREDENTIAL_FAILURE`, `credential_revision_id` 기록)를 만들고 `source.credential_breaker = {revision_id, failures, opened_at, hold_id}`를 기록하며 Outbox에 `source credential failure` 1건(`event_id = hold_id`)을 넣는다. 실행 중 attempt는 중단하지 않는다(`HOLD_NEW` 의미 — 이미 로그인한 세션은 비밀번호 변경에 영향받지 않고, 아직 로그인 전인 attempt는 실패를 1건씩만 더한다). 실패한 attempt는 `WRITER_FENCED` 확정 후 verdict `NO_COMMIT(reason=CREDENTIAL_FAILURE)`로 `ADJUDICATION_PENDING`에 남고 자동 재시도는 없다(운영자 RETRY — Guard가 새 ACTIVE revision 선택 — 또는 `abort`·`expires_at`, 10.2). breaker가 열린 동안 Control은 같은 revision으로 모니터 세션을 재접속하지 않는다(그 Source의 Guard는 어차피 Hold로 먼저 거부된다). **해제 경로는 하나뿐이다**: 그 Source의 새 CredentialRevision이 `ACTIVE`로 전이하는 트랜잭션에서 breaker를 닫고(`failures = 0`) 같은 Hold를 해제한다(actor = revision을 승격한 운영자, catch-up은 14.2 규칙, 대기 계약 `next_eligible_at` 리셋은 10.2 표의 `CREDENTIAL_REVOKED` 행). `POST /v1/holds/{id}/release`로 `CREDENTIAL_FAILURE` Hold를 직접 해제하려 하면 breaker가 열려 있는 한 `412 CREDENTIAL_REVOKED`다 — 자격증명이 다시 통하는지 증명하는 유일한 수단이 새 revision의 VERIFIED 로그인 테스트이기 때문이다.
- Oracle `FAILED_LOGIN_ATTEMPTS`와의 관계: breaker는 계정이 잠기기(ORA-28000) 전에 열려야 한다. in-flight attempt는 driver precheck 세션(11.3 role 재검증 세션)을 executor보다 먼저 열고 실패 시 executor 세션 없이 종료하므로(10장 인터페이스) attempt당 실패 로그인은 최대 1회다 — 단 precheck 통과 후 비밀번호가 바뀐 경합에서는 executor/JDBC 재접속이 Spark task retry마다 실패할 수 있으므로 Template은 ORA-01017/28000에서 task retry를 끊는 fail-fast를 강제하고(22장 9번), SUPERSEDED grace 동안 구 비밀번호가 유효한지(gradual password rollover)는 DBA 확인 항목이다. 따라서 Source 저장 시 validator가 `credential_breaker_failures + SourceSafetyEnvelope 최대 동시 ETL Job ≤ SourceCapability.failed_login_attempts − 1`을 강제한다(위반은 17장 validate 형식의 422). 예: profile 10, 최대 동시 ETL Job 4 → 상한 5, 기본값 1.

`DefinitionRelease`:

```text
DRAFT → COMPILED → VALIDATED → DEPLOYED → VERIFIED → ACTIVE → SUPERSEDED | ROLLED_BACK
                                                ↘ FAILED
```

| 전이 | 수행 주체 | 판정 근거 | 실패 시 |
|---|---|---|---|
| COMPILED | Control compiler | JobSpec → compiled plan 생성. manifest에 Job별 `interface_changed: bool`(asset key·partition spec·schedule group diff) 기록 | DRAFT 유지, 오류 목록 |
| VALIDATED | Python compile service(Dagster 고정 버전) | Bundle을 실제 Definitions로 로드 성공 + 예상 schedule 집합 산출 | FAILED |
| DEPLOYED | Dagster Adapter | Bundle을 불변 객체(AIStor, `bundle_digest` 키)로 게시하고 shard ACTIVE 포인터 변경 후 reload 호출 | 포인터 롤백 |
| VERIFIED | Dagster Adapter | code location이 노출한 manifest digest == 기대 digest, 기대 schedule 집합이 instance에서 RUNNING | 직전 ACTIVE bundle로 자동 fallback + 알림 |
| ACTIVE | Control | **OPEN_CONTRACT_CHECK**(한 Control 트랜잭션): `interface_changed = true`인 Job의 계약 row를 `FOR UPDATE`로 잠근 뒤 (a) `PLANNED` → `VOID(SUPERSEDED_BY_RELEASE)` + occurrence `SUPERSEDED_BY(RELEASE)`(occurrence는 재생성하지 않으며 다음 정규 tick이 새 release로 실행), (b) 그 외 비종결 상태(`ATTEMPT_ACTIVE`·`COMMIT_OBSERVED`·`ADJUDICATION_PENDING`·`DQ_FAILED`·`RECONCILIATION_REQUIRED`)가 하나라도 있으면 `412 OPEN_CONTRACTS_RUNNING`과 `{contract_id, state, required_action}` 목록 반환 — required_action: ATTEMPT_ACTIVE/COMMIT_OBSERVED → DRAIN Hold, ADJUDICATION_PENDING → `retry` 완료 또는 `abort`, DQ_FAILED → `dq:accept` 또는 repair REPLAY, RECONCILIATION_REQUIRED → 수동 reconcile 후 `resolve`. `interface_changed = false`인 Job의 계약은 상태와 무관하게 건드리지 않으며 pinned release의 plan으로 계속 실행된다. Guard도 contract row `FOR UPDATE`를 잡으므로 두 트랜잭션은 row lock으로 직렬화된다. 자동으로 실행 중 계약을 마감하는 경로는 없다. 통과 시 `effective_from = max(now(), 요청 시 지정한 미래 시각)` 확정(이후 불변) | — |
| SUPERSEDED / ROLLED_BACK | Control | 다음 release ACTIVE 시 SUPERSEDED. rollback(`POST /v1/releases/{id}/rollback`, 17장)은 직전 ACTIVE bundle을 **새 release**(`rollback_of = id`; bundle 객체가 불변이고 이미 VALIDATED를 통과했으므로 VALIDATED에서 시작)로 DEPLOYED → VERIFIED → ACTIVE 승격하고, 그 ACTIVE 트랜잭션에서 문제 release를 ROLLED_BACK으로 표시한다 | rollback release의 ACTIVE 단계 OPEN_CONTRACT_CHECK 실패 → operation FAILED(412 본문), 새 release VERIFIED 유지, 문제 release ACTIVE 유지 |

Bundle 전달 방식(이미지 내장 vs 외부 저장소 pull)의 최종 선택은 PoC 기준서 항목이다. 어느 쪽이든 code location 기동이 Control API에 의존하지 않도록 ACTIVE 포인터는 AIStor 객체 또는 ConfigMap에 둔다.

`ExecutionOccurrence.disposition` — 실행 없이 끝난 회차도 반드시 설명된다.

```text
PENDING → EXECUTED                           # contract가 ATTEMPT_ACTIVE에 처음 진입할 때, 또는 PLANNED에서 FINALIZED_NO_DATA로 직행할 때
        → COALESCED_INTO(contract_id)        # 열린 window/활성 contract에 흡수
        → SKIPPED_BY_HOLD(hold_id)
        → SUPERSEDED_BY(RELEASE | INITIAL_LOAD)
        → REJECTED_AT_GUARD(reason)          # SCHEMA_DRIFT | SOURCE_ROLE_MISMATCH — Spark 미제출, contract VOID(reason)
        → EXPIRED_UNLAUNCHED                 # PLANNED가 expires_at 도달 — NORMAL·CATCHUP 공통, 다음 NORMAL이 [current_watermark, fence)를 덮음 (9.3)
        → CANCELLED(actor, reason)           # 실행 전 운영자 취소 (contract VOID(OPERATOR))
```

`ExecutionContract`는 계산 상태가 아니라 데이터 계약 상태만 가진다. Dagster의 queued/running/failure는 복제하지 않고, 아래 표의 입력 이벤트로만 전이한다. Guard(10.2)는 binding·window·lease를 **한 트랜잭션**으로 처리하므로 중간 상태를 두지 않는다.

`DEGRADED_CONFIDENCE`(11.3)는 상태가 아니라 contract 속성 `confidence = DEGRADED`다. Guard 6번에서 한 번 결정되며 이후의 전이·chunk CAS·finalize 규칙은 `FULL`과 동일하다 — `ZERO_GAP` 계약은 같은 조건에서 Guard가 `SOURCE_LAG_EXCEEDED`로 거부하므로 `DEGRADED`를 가진 채 `ATTEMPT_ACTIVE`에 들어오는 경로가 없다. attempt 실행 중 lag로 인한 전이는 없다.

```text
PLANNED → ATTEMPT_ACTIVE → COMMIT_OBSERVED → FINALIZED            # COMMIT_OBSERVED = committed=true인 첫 chunk CAS 성공(0-row chunk의 CAS는 상태를 바꾸지 않음), FINALIZED = Spark 종료 확인 후 finalize
ATTEMPT_ACTIVE | COMMIT_OBSERVED → DQ_FAILED → FINALIZED | RESOLVED # commit 뒤 DQ 실패(13.1 검사 1·5): CAS 없이 ledger row만 기록 → dq:accept(FINALIZED) 또는 repair REPLAY/resolve(RESOLVED)
ATTEMPT_ACTIVE | COMMIT_OBSERVED → RECONCILIATION_REQUIRED → RESOLVED  # chunks/{n}:commit의 lineage 검증이 lease 기록 없는 외부 snapshot 발견(13.1 chunk 검증 규칙, 사유 EXTERNAL_SNAPSHOT): CAS 없이 ledger row만 기록 → repair REPLAY/resolve(RESOLVED). Adjudication 없음
PLANNED → FINALIZED_NO_DATA                                         # high ≤ low (13.4): Spark 미제출
ATTEMPT_ACTIVE → FINALIZED_NO_DATA                                  # 0-row receipt 후 finalize (12.1/12.2)
ATTEMPT_ACTIVE | COMMIT_OBSERVED → ADJUDICATION_PENDING             # Dagster terminal 반입(RUN_WORKER_LOST · FINALIZE_MISSING) · attempt-failure · lease 만료 · FORCE_STOP
ADJUDICATION_PENDING → ATTEMPT_ACTIVE | COMMIT_OBSERVED             # RUN_WORKER_LOST 재결합(같은 attempt, 직전 상태 복귀)
ADJUDICATION_PENDING → ATTEMPT_ACTIVE                               # 판정 NO_COMMIT/PARTIAL_COMMIT 후 새 attempt (자동 재시도 사유 또는 retry_authorization)
ADJUDICATION_PENDING → COMMIT_OBSERVED → FINALIZED                  # 판정 COMMIT (Control이 finalize 대행)
ADJUDICATION_PENDING → FINALIZED_NO_DATA                            # 판정 S′ = ∅ + 0-row receipt (13.2; Full allow_empty_full=false 제외), 또는 운영자 accept-empty (EMPTY_FULL, 12.1)
ADJUDICATION_PENDING → ABORTED_NO_COMMIT                            # 판정 NO_COMMIT + 운영자 abort, 또는 FORCE_STOP 프로토콜(14.1)의 NO_COMMIT 결과 (종결)
ADJUDICATION_PENDING → RECONCILIATION_REQUIRED                      # 판정 불가 (13.2)
RECONCILIATION_REQUIRED → RESOLVED(resolution)                      # resolution: REPAIR_CONTRACT(replay_contract_id) | WATERMARK_SEED | OPERATOR_ACCEPT
PLANNED → VOID(reason)                                              # 실행 없이 종결: SKIPPED_BY_HOLD | COALESCED | EXPIRED_UNLAUNCHED | SUPERSEDED_BY_RELEASE | SUPERSEDED_BY_INITIAL_LOAD | SCHEMA_DRIFT | SOURCE_ROLE_MISMATCH | OPERATOR
ATTEMPT_ACTIVE | COMMIT_OBSERVED | ADJUDICATION_PENDING → CANCELLED_AT_SAFEPOINT | CANCELLED(reason)
                                                                    # reason: FORCE_STOP | HOLD | EXPIRED | STALE_WINDOW | CREDENTIAL_REVOKED | CONNECTION_REVOKED | OPERATOR | SCHEMA_DRIFT | SOURCE_ROLE_MISMATCH | SUPERSEDED_BY_RELEASE
```

| 전이 | 입력 이벤트 | 출처 |
|---|---|---|
| PLANNED → ATTEMPT_ACTIVE | Guard 트랜잭션 성공: attempt 생성·binding, window 예약(low·high 확정)과 chunk 목록 산출, target lease → source token 획득 | Run Pod Guard → Control |
| PLANNED → VOID / FINALIZED_NO_DATA | Guard 거부(10.2 표) · OPEN_CONTRACT_CHECK · PLANNED stale 만료 · `high ≤ low`(이때 2번에서 만든 attempt row·binding은 같은 트랜잭션에서 롤백, `current_attempt` NULL) | Control |
| ATTEMPT_ACTIVE → COMMIT_OBSERVED | `committed_snapshot_id`가 있는 첫 `chunks/{n}:commit` 성공(ledger row + watermark CAS 한 트랜잭션). 0-row chunk의 ledger row + CAS는 `ATTEMPT_ACTIVE`를 유지하고, 이후 chunk commit은 상태를 바꾸지 않는다 | Run Pod |
| COMMIT_OBSERVED → FINALIZED | Run Pod가 SA terminal(`COMPLETED`)을 watch로 확인한 뒤 `POST /v1/contracts/{id}/finalize {outcome: FINALIZED, sa_status}` 호출. Control은 ledger row 1..expected_chunk_count가 모두 CAS됐는지 검증한 뒤 마지막 row에 `finalized=true`, 계약 FINALIZED, window·target lease·source token 해제를 한 트랜잭션으로 수행. 누락 row가 있으면 `409 CHUNKS_INCOMPLETE` | Run Pod → Control |
| ATTEMPT_ACTIVE → FINALIZED_NO_DATA | 0-row receipt 후 SA 종료 확인 → `finalize {outcome: FINALIZED_NO_DATA}`(12.1/12.2). window·lease 해제 | Run Pod → Control |
| ATTEMPT_ACTIVE / COMMIT_OBSERVED → ADJUDICATION_PENDING | `dagster-terminal-event`(10.2 표) · Run Pod `attempt-failure` · source lease EXPIRING · FORCE_STOP | Control |
| ADJUDICATION_PENDING → (판정) | Commit Adjudication(13.2): `WRITER_FENCED` 확정 → 구간 판정. 수행 주체는 Control의 Adjudication 서비스뿐 | Control |
| ADJUDICATION_PENDING → ATTEMPT_ACTIVE \| COMMIT_OBSERVED (재결합) | `RUN_WORKER_LOST`이고 SA가 살아 있으며 `reattach_deadline` 이전에 새 Run의 Guard가 재결합 — 같은 attempt, 직전 상태로 복귀 | Run Pod Guard |
| ADJUDICATION_PENDING → ATTEMPT_ACTIVE (새 attempt) | 판정 NO_COMMIT/PARTIAL_COMMIT 후 새 Dagster Run의 Guard가 `attempt_no + 1` 생성 — 자동 재시도 사유(`RUN_WORKER_LOST`, `max_auto_attempts` 내) 또는 `retry_authorization` 소비 | Run Pod Guard |
| ATTEMPT_ACTIVE / COMMIT_OBSERVED → DQ_FAILED | Run Pod의 `chunks/{n}:commit`에 검사 1·5 실패가 실리면 Control은 ledger row(`dq_result=FAILED`)만 기록하고 CAS 없이 전이. Run Pod는 driver에 `stop`을 보내고 SA terminal 확인 후 `finalize {outcome: DQ_FAILED, sa_status}`를 호출하며 Control은 그 트랜잭션에서 source token만 반환한다(window·target lease 유지). `drain_timeout_seconds` 안에 finalize가 없으면 fencing 단계로 token 회수 | Run Pod → Control |
| ATTEMPT_ACTIVE / COMMIT_OBSERVED → RECONCILIATION_REQUIRED | Run Pod의 `chunks/{n}:commit` 본문 `lineage`에서 Control이 lease 기록 없는 개입 snapshot을 확인(13.1 chunk 검증 규칙 — 13.2 3번과 같은 분류). ledger row(`committed_snapshot_id` 포함, `dq_result=EXTERNAL_SNAPSHOT`)만 기록하고 CAS 없이 전이(종결 사유 `EXTERNAL_SNAPSHOT`). Run Pod는 driver에 `stop`을 보내고 SA terminal 확인 후 `finalize {outcome: RECONCILIATION_REQUIRED, sa_status}`를 호출하며 Control은 그 트랜잭션에서 source token만 반환한다(window·target lease 유지). `drain_timeout_seconds` 안에 finalize가 없으면 fencing 단계로 token 회수. Adjudication·`WRITER_FENCED`·verdict 없음 | Run Pod → Control |
| DQ_FAILED → FINALIZED | 운영자 `dq:accept`(사유·승인자 — Full row 급감 등 확인된 정상 변동). watermark는 `window.high`로 전진 | Control |
| DQ_FAILED / RECONCILIATION_REQUIRED → RESOLVED | repair REPLAY 계약의 FINALIZED 트랜잭션(`parent_contract_id`가 가리키는 원 계약을 같은 트랜잭션에서 닫고 watermark 전진), 또는 운영자 `POST /v1/contracts/{id}/resolve`(사유·승인자 필수, `watermark:seed` 동반 가능) | Control |
| ADJUDICATION_PENDING → ABORTED_NO_COMMIT / CANCELLED(OPERATOR) | FORCE_STOP 프로토콜(14.1, 부분 commit 없음) 또는 운영자 `POST /v1/contracts/{id}/abort {reason}` — verdict NO_COMMIT → ABORTED_NO_COMMIT, PARTIAL_COMMIT → CANCELLED(OPERATOR), 둘 다 window·lease 해제 후 다음 회차가 `[current_watermark, fence)`를 덮음 | Control |
| → CANCELLED_AT_SAFEPOINT / CANCELLED | Hold DRAIN(`CANCELLED_AT_SAFEPOINT`는 Run Pod `finalize` 호출) / FORCE_STOP 프로토콜(14.1), `expires_at` 도달(`ADJUDICATION_PENDING`만, `WRITER_FENCED` 확정 후 — 9.3), STALE_WINDOW RETRY 거부, CredentialRevision REVOKED(프로토콜 결과의 reason은 `CREDENTIAL_REVOKED`), ConnectionRevision 강제 REVOKED(`force: true`, reason `CONNECTION_REVOKED`) | Control |

`ExecutionAttempt`:

```text
CREATED → BOUND(bound_dagster_run_id) → SUBMITTED(SparkApplication 생성) → COMPLETED(finalize 성공 · SA 종료 확인)
                                                                       ↘ TERMINAL_OBSERVED → FENCED → ADJUDICATED(verdict = COMMIT | PARTIAL_COMMIT | NO_COMMIT, reason)
```

재결합(reattach)은 BOUND/SUBMITTED attempt의 `bound_dagster_run_id`를 새 run으로 교체하는 것이며(`rebind_count` 증가) 상태 전이가 아니다. SparkApplication의 running 여부는 `last_observed_sa_status` 캐시(10.1)로만 두고 상태 전이가 아니다. verdict reason: `RUN_WORKER_LOST` | `FINALIZE_MISSING` | `SPARK_FAILED` | `SPARK_TERMINATED_WITHOUT_RECEIPT` | `COMMIT_STATE_UNKNOWN` | `CHUNK_DQ_FAILED` | `EMPTY_FULL` | `SOURCE_ROLE_MISMATCH` | `FORCE_STOP` | `LEASE_EXPIRED` | `CREDENTIAL_REVOKED` | `TARGET_UNAVAILABLE`(Run Pod가 `base_snapshot_id`를 읽지 못해 SA 미생성 — 구간 판정 없이 즉시 `NO_COMMIT`, 5.4) | `CREDENTIAL_FAILURE`(로그인 실패 — 6.2 credential breaker) | `CONNECTION_REVOKED`(ConnectionRevision 강제 REVOKED — 6.2).

window·lease 해제 불변식:

- `FINALIZED`, `FINALIZED_NO_DATA`, `RESOLVED`, `ABORTED_NO_COMMIT`, `VOID`, `CANCELLED*` 진입 트랜잭션에서 window 예약과 target lease를 해제한다. **source token은 11.2 규칙대로 SparkApplication 종료 확인 시점에만 반환한다** — `VOID`·`ABORTED_NO_COMMIT`·`CANCELLED(FORCE_STOP | EXPIRED | STALE_WINDOW | CREDENTIAL_REVOKED | CONNECTION_REVOKED | OPERATOR | HOLD | SCHEMA_DRIFT | SOURCE_ROLE_MISMATCH | SUPERSEDED_BY_RELEASE)`는 SA가 없거나 `WRITER_FENCED`가 확정된 뒤의 전이(복구 경로 4~8번 거부는 3.3 fencing 이후이며 새 source token은 아직 획득 전)이므로 같은 트랜잭션에서 반환하고, `FINALIZED`·`FINALIZED_NO_DATA`·`CANCELLED_AT_SAFEPOINT`(DRAIN)·`DQ_FAILED`·`RECONCILIATION_REQUIRED(EXTERNAL_SNAPSHOT, 13.1)`는 Run Pod가 SA 종료(COMPLETED 관측 또는 `RECLAIMED`)를 확인한 `finalize` 호출에서 반환한다.
- `ADJUDICATION_PENDING` 동안 window와 target lease는 유지하고, source token은 `WRITER_FENCED`(= source lease `RECLAIMED`, 11.2) 확정 시 반환한다.
- `RECONCILIATION_REQUIRED`와 `DQ_FAILED`는 target lease와 window를 유지한다(source token은 `DQ_FAILED`·`RECONCILIATION_REQUIRED(EXTERNAL_SNAPSHOT)` 진입 직후의 `finalize {outcome: DQ_FAILED | RECONCILIATION_REQUIRED}` 또는 `WRITER_FENCED`에서 이미 반환됨). 이 두 상태에 도착하는 `attempt-failure`·lease EXPIRING·terminal event는 감사 로그만 남기고 무시한다. 두 상태는 commit이 존재하나 watermark가 전진하지 않은 상태이므로, 원 계약이 `RESOLVED`(repair REPLAY FINALIZED 또는 `resolve` 호출) 또는 `FINALIZED`(`dq:accept`)로 전이할 때까지 같은 job의 새 window 예약은 `OPEN_WINDOW`로 막힌다(후속 NORMAL occurrence는 `COALESCED_INTO`). repair REPLAY는 parent의 window를 인수하고 target lease는 repair 쓰기 방식에 맞게 새로 획득한다(13.4). 알림은 계약 단위 1건으로 집계한다.
- 상태 전이와 해제가 같은 트랜잭션이 아니면 14.2의 catch-up이 “열린 window 1개” 규칙에 막힌다.

## 7. Job 생성 UX

### 7.1 Source System 관리

필수 화면과 기능:

- Source 이름, 소유 부서, 중요도, Primary/DR 구분
- Oracle service 정보와 기본 Schema
- Secret 저장소의 Credential reference
- TNS 연결 방식
  - Easy Connect
  - `tnsnames.ora` Alias
  - Raw Connect Descriptor
- Wallet/인증서는 일반 설정이 아니라 Secret으로 분리
- 실제 Spark 실행 namespace/network path에서 연결 테스트
- metadata 조회 권한 테스트
- Flashback SCN, Data Guard lag, hard delete, `update_dt` 신뢰성 등 capability 등록
- Source 보호 정책과 변경 이력

Job은 TNS revision을 직접 참조하지 않고 `source_system_id`만 참조한다. 실행 계약 생성 시점에 ACTIVE revision을 실제 descriptor hash로 해석해 고정한다. 그래서 TNS를 승격해도 10,000 Job을 다시 publish할 필요가 없다. descriptor hash는 계약 생성 시 pin하고, credential revision은 계약이 아니라 Guard가 attempt를 만들 때 현재 ACTIVE revision을 선택해 attempt에 기록한다(6.2). ACTIVE revision이 없고 REVOKED만 있으면 `CREDENTIAL_REVOKED`로 거부한다. `SUPERSEDED`는 grace 기간 내 그대로 사용한다. descriptor의 ConnectionRevision이 `REVOKED`이면 Guard가 현재 ACTIVE revision으로 재해석해 attempt에 기록한다(갱신 이력 보존). 실행 중 attempt가 참조하는 ConnectionRevision은 기본적으로 `REVOKED`할 수 없고(`412 OPEN_CONTRACTS_RUNNING` 목록 — 먼저 DRAIN Hold로 비운다, 6.2), `force`로 REVOKED하면 그 attempt는 `FORCE_STOP` 프로토콜로 `CANCELLED(CONNECTION_REVOKED)`가 된다. ACTIVE ConnectionRevision이 없으면 Guard는 `CONNECTION_REVOKED`로 거부한다(10.2 표).

Raw Descriptor는 허용 host/port/protocol을 검증하고 `IFILE`, 외부 경로, 허용되지 않은 protocol을 차단한다.

### 7.2 Job Wizard

운영자가 한 화면 흐름에서 다음을 완료한다.

1. Source System 선택
2. Schema/Table 탐색과 컬럼·PK·Index·통계 확인 — 위험 타입(정밀도 미지정 NUMBER, LONG/RAW, CLOB, VARCHAR2 BYTE 길이, DATE vs TIMESTAMP) 경고 표시
3. LLM/Rule Advisor 추천 확인 (선택 단계 — Advisor 장애 시 건너뜀)
4. Full / Append / Merge 선택. Full은 `FULL_STATIC_REPLACE` / `PARTITION_REPLACE` 중 택일
5. Watermark 컬럼과 **cutoff 종류** 선택: `APPLICATION_TIMESTAMP_WITH_OVERLAP`(기본) / `STANDBY_VISIBLE_SCN`(PoC 한정) / `CDC_OFFSET`(향후). APPLICATION_TIMESTAMP는 Source capability의 `max_open_txn_seconds`가 등록된 경우에만 `ZERO_GAP` 등급 허용
6. 초기 적재 모드(`initial_load`)와 delete semantics(`NONE_DECLARED` / `SOFT_DELETE` / `PK_RECONCILE` / `CDC_LATER`) 선택. Critical 테이블은 `NONE_DECLARED` publish 거부
7. Source → Target 컬럼 mapping — 플랫폼 표준 타입 매핑표(12.4 부록) 기본 적용, 등록자가 수정
8. 표준화/단순 가공식/업무 WHERE 입력
9. `dt/wt/mt/yt` 파생 partition 선택 — 물리 partition은 원천 timestamp 컬럼의 hidden transform만 사용하고 dt/wt/mt/yt는 `partition_granularity` 메타데이터로 유지(12.4)
10. Target namespace/schema/table 확인
11. 1/2/4/6/12/24시간 또는 cron 설정 — validator가 최소 주기 1시간을 강제(1시간 미만은 22장 확정 전 금지)
12. Spark profile과 Source read profile 확인
13. Preview SQL, 실행계획 점검, **Source 용량 Gate**(해당 Source·주기 band의 utilization ρ > 0.7 경고, > 1.0 거부 또는 DBA 승인), overlap ≥ `max_open_txn_seconds + safety_lag + clock_skew` 검증(`ZERO_GAP`은 거부 — `422 VALIDATION_FAILED`, rule `OVERLAP_BELOW_MINIMUM`, 계산된 최소값 동봉; `BEST_EFFORT`는 경고만. validate와 publish가 같은 validator·같은 응답 형식, 17장), 초기 적재 예상 row·시간·부하 표시 후 publish

컬럼은 기본적으로 1:1 자동 생성하고 등록자가 이름·타입·표준화·가공식을 직접 수정한다. 복잡한 사용자 코드는 일반 JobSpec에 문자열로 넣지 않고 별도 고급 Template로 승격한다.

### 7.3 JobSpec 예시

```yaml
job_id: oracle_eqp_event_001
source_system_id: EQP_DR_A
ownership:
  team: MES-DATA
  contact: mes-data@corp
  severity_class: CRITICAL
source:
  schema: MES
  table: EQP_EVENT
  consistency_mode: FLASHBACK_SCN        # AS OF <visible_scn>, 11.4
load:
  mode: MERGE
  merge_mode: COPY_ON_WRITE              # | MERGE_ON_READ (PoC 비교)
  primary_key: [EVENT_ID]
  cutoff:
    kind: APPLICATION_TIMESTAMP_WITH_OVERLAP   # | STANDBY_VISIBLE_SCN | CDC_OFFSET
    columns: [INSERT_DT, UPDATE_DT]
    predicate_strategy: UNION_DEDUP
    overlap: PT30M                        # ≥ source.max_open_txn_seconds + safety_lag + clock_skew (publish 검증)
    guarantee_grade: ZERO_GAP             # | BEST_EFFORT — lag 신호 없는 Source는 BEST_EFFORT만 허용
  dedup:
    order_by: [UPDATE_DT DESC NULLS LAST, ROW_HASH DESC]
  delete_semantics:
    kind: SOFT_DELETE                     # | NONE_DECLARED | PK_RECONCILE | CDC_LATER
    column: DEL_YN
    value: "Y"
    target_action: FLAG                   # | DELETE
    # PK_RECONCILE이면: {kind: PK_RECONCILE, interval: P1D, window: P7D}
  initial_load:
    mode: FULL_SNAPSHOT_THEN_INCREMENTAL  # | FROM_TIMESTAMP | NONE
    chunk: P1D
    cooldown: PT2M
target:
  catalog: polaris
  namespace: raw_mes
  table: eqp_event
  partition_transform: day(event_dt)
  partition_granularity: DAY              # dt/wt/mt/yt 메타데이터 (12.4)
  timestamp_type: TIMESTAMP_NTZ           # 잠정 기본 NTZ — 전사 확정은 22장 12번
schedule:
  cron: "0 * * * *"
  timezone: Asia/Seoul
freshness_slo: PT20M                      # lateness = FINALIZED − logical_scheduled_at
template:
  channel: approved
read_profile:
  num_partitions: 1
  query_timeout_seconds: 1800             # ≤ source lease TTL − margin
  extract_once: true
```

Full Job의 경우 `load.mode: FULL`, `load.full_mode: FULL_STATIC_REPLACE | PARTITION_REPLACE`, `load.allow_empty_full: false`, 선택적 `load.change_detection: NONE | MAX_UPDATE_DT | TAB_MODIFICATIONS`를 가진다. `PARTITION_REPLACE`는 `load.partition_replace: {column, lookback: P3D}`로 범위를 지정한다.

실제 publish 결과에는 alias 대신 `definition_release_id`, `job_spec_digest`, `template_digest`, `definition_bundle_digest`, `Spark image digest`, compiler version, `connection_revision_id`, `credential_revision_id`가 들어간다. 이 값들은 9.2 규칙에 따라 ExecutionContract(credential은 attempt)에 고정된다.

## 8. Template와 Definition 배포

### 8.1 소스 파일 생성 방식

- Job마다 Python/DAG 파일 생성: 금지
- 상위 10개 유형: 공용 Component/Asset Factory로 구현
- 나머지 유형: 검토 후 재사용 가능한 TemplateVersion으로 추가
- Job Registry의 불변 JobSpec을 compile하여 Asset 정의를 생성

Dagster는 설정에서 여러 Asset을 생성하는 Asset Factory 패턴을 공식 지원한다. [Creating asset factories](https://docs.dagster.io/guides/build/assets/creating-asset-factories)

외부 Registry 상태를 실행 시마다 live 조회하지 않는다. publish 시 검증된 **Definition Bundle**로 freeze하고 Code Location은 그 Bundle만 읽는다. Dagster의 state-backed component를 사용할 경우에도 OSS reload가 최신 state를 읽는 특성을 고려해, 별도 Manifest digest 검증으로 의도치 않은 최신 버전 유입을 막는다. [State-backed components](https://docs.dagster.io/guides/build/components/state-backed-components)

### 8.2 공용 Template 일괄 변경

기존처럼 Template 하나의 변경을 다수 Job에 반영할 수 있다. 단 즉시 전역 반영 대신 다음 Release 절차를 적용한다.

1. 새 `TemplateVersion` 생성
2. 영향 Job 목록과 변경된 compiled plan 산출
3. Unit/contract test
4. 대표 Job canary
5. Definition Bundle 생성
6. 특정 shard 배포와 load 검증
7. OPEN_CONTRACT_CHECK(6.2, 9.2) 통과 후 approved channel 승격(ACTIVE), `effective_from` 기록
8. 이상 시 rollback: `POST /v1/releases/{id}/rollback {reason}`(17장)으로 직전 bundle을 **새 release**로 ACTIVE 승격(`effective_from = rollback release의 ACTIVE 전이 시각`, 미래 시각 지정 불가)하고 문제 release를 `ROLLED_BACK`으로 표시. 같은 ACTIVE 트랜잭션에서 문제 release를 pin한 `PLANNED` contract는 Job의 `interface_changed` 여부와 무관하게 `VOID(SUPERSEDED_BY_RELEASE)`로 닫고(다음 tick이 새 release로 실행), 실패 contract의 RETRY는 `PINNED_RELEASE_INACTIVE`로 거부되어 REPLAY로 안내한다(14.3). `interface_changed = true`인 Job에 비종결 계약이 있으면 6.2 규칙대로 `412 OPEN_CONTRACTS_RUNNING`이며 실행 중 계약을 자동으로 마감하는 경로는 rollback에도 없다

Rollback은 **미래 Run의 코드 버전만** 되돌린다. 이미 Iceberg에 commit된 잘못된 데이터는 snapshot rollback, repair backfill 등 별도 데이터 복구 절차가 필요하다.

`latest` image tag는 사용하지 않는다. Queue 대기, Retry, Backfill에서도 이전 image digest를 실행할 수 있도록 Artifact 보관 기간을 데이터 재처리 기간보다 길게 둔다.

## 9. 스케줄과 전역 멱등성

### 9.1 정상 경로

정상 스케줄링의 권위는 Dagster이다.

- Definition Bundle이 동일 `(cron, timezone, shard)` Job을 묶은 `ScheduleDefinition`을 생성한다. 이름은 `sched__{shard}__{cron_slug}__{tz}`로 결정론적이며 digest를 포함하지 않는다(이름이 바뀌면 Dagster의 schedule 상태·tick 이력이 끊긴다).
- 모든 생성 schedule은 `default_status=RUNNING`이다. 새 `(cron, tz)` 조합이 생겨도 STOPPED로 배포되지 않는다. Release VERIFIED 단계에서 기대 schedule 집합이 instance에서 RUNNING인지 대조하고, Schedule Gap Recovery가 주기적으로 STOPPED를 알림한다. Dagster UI에서 schedule을 끄는 것은 운영 절차로 금지하며 정지는 Hold로만 한다.
- Schedule tick은 대상 Job에 대해 Control API를 호출해 occurrence를 create-or-get하고, 반환된 `contract_id`를 tag로 하는 `RunRequest`를 Job별로 만든다. Job별 10,000 Schedule을 만들지 않으면서 각 Job은 독립 Run을 유지한다.
  - 호출은 **batch 1회**(`POST /v1/occurrences:batch-create-or-get`, 한 트랜잭션)를 기본안으로 한다. schedule 평가는 code server gRPC 호출 안에서 실행되며 기본 timeout이 60초이므로(5.1 환경변수), 500건이 그 안에 드는지는 PoC 기준서에서 실측으로 판정한다.
  - 응답은 Job별 `{job_id, occurrence_id, disposition, contract_id|null, contract_state, priority, max_runtime_seconds, launch: true|false}`이며 schedule은 `launch=true`인 항목만 RunRequest로 만든다(`launch`는 disposition=PENDING이고 contract_state=PLANNED이며 binding이 없을 때만 true). 항목 하나라도 처리 실패면 전체 트랜잭션을 rollback하고 5xx를 반환한다.
  - 응답 항목에 `max_runtime_seconds`(Job별 `run_monitoring.max_runtime_seconds` 값, 22장 22번 규칙)를 함께 내리고 schedule은 이를 RunRequest tag `dagster/max_runtime`으로 쓴다(`priority` → `dagster/priority`와 같은 경로). 9.3 stale 루프·14.2 CATCHUP·14.3 RETRY의 Adapter `launchRun`도 같은 tag를 붙인다.
  - Control API timeout/5xx → 해당 tick은 **fail-closed**: RunRequest를 내지 않고 tick FAILURE로 기록한다. `scheduler.config.max_tick_retries ≥ 1`(DagsterDaemonScheduler)로 재시도한다. create-or-get은 멱등이므로 재시도는 안전하다.
  - 부분 성공(occurrence는 PLANNED로 생성됐지만 RunRequest가 나가지 못함)은 9.3의 PLANNED stale 검사가 회수한다.
  - Hold 중인 Job은 occurrence를 `SKIPPED_BY_HOLD(hold_id)`, contract를 `VOID(SKIPPED_BY_HOLD)`로 생성하고 RunRequest를 내지 않는다.
  - `INITIAL_LOAD` contract가 비종결인 Job의 NORMAL tick은 occurrence를 `SUPERSEDED_BY(INITIAL_LOAD)`, contract를 `VOID(SUPERSEDED_BY_INITIAL_LOAD)`로 생성하고 RunRequest를 내지 않는다(12.2).
- RunRequest의 `run_key`는 `contract_id`로 둔다. 동일 tick/복구 범위의 중복 억제에만 쓰고 전역 멱등성의 권위로 쓰지 않는다. `dagster/priority` 태그는 Control이 응답에 내려준 `priority` 값을 그대로 쓴다(계산은 Control: `floor(100 × (now − logical_at)/period) + critical_bonus + Source round-robin offset`).

Dagster 공식 API도 sensor의 `run_key`는 해당 sensor 평가 전체, schedule의 `run_key`는 해당 tick/복구 범위에서 중복 Run 생성을 막는다고 설명한다. [Dagster schedules and sensors API](https://docs.dagster.io/api/dagster/schedules-sensors)

### 9.2 ExecutionOccurrence 정체성과 버전 고정

occurrence의 정체성은 **데이터 의미**로만 구성한다. 전역 중복 방지는 Control PostgreSQL unique constraint가 담당한다.

```text
NORMAL / CATCHUP / INITIAL_LOAD:
  UNIQUE (job_id, operation_class, logical_scheduled_at_utc)

BACKFILL:
  UNIQUE (backfill_plan_id, job_id, logical_window)

REPLAY:
  UNIQUE (job_id, 'REPLAY', parent_contract_id, client_request_id)

RERUN_LATEST:
  UNIQUE (job_id, 'RERUN_LATEST', client_request_id)      -- parent_contract_id는 선택 속성
```

`schedule_revision_id`, `job_spec_digest`, `template_digest`, `definition_bundle_digest`, image digest, connection descriptor hash는 **정체성이 아니라 ExecutionContract의 `pinned_*` 속성**이다(credential revision은 attempt 속성, 6.2). 템플릿을 재배포해도 같은 논리 시각의 Job이 두 번 만들어지지 않는다.

`schedule_revision_id`는 Job의 `(cron, timezone, shard)` 조합이 바뀔 때마다 1 증가하며 release에 기록된다. 그룹 이동 release가 ACTIVE가 되면 이전 revision으로 생성된 `PLANNED` occurrence는 OPEN_CONTRACT_CHECK에서 `SUPERSEDED_BY(RELEASE)`로 닫고 새 그룹의 다음 tick부터 정상 키로 생성한다. 같은 logical_at에 두 revision의 tick이 도달해도 키에 revision이 없으므로 occurrence는 1개다.

버전 고정 규칙:

- NORMAL: `logical_scheduled_at`에 유효했던 Definition Release를 고정한다. 판정은 shard 단위가 아니라 **Job 단위**다 — 해당 Job의 manifest entry(job_spec_digest)가 포함된 ACTIVE·SUPERSEDED release 중 `effective_from ≤ logical_at`인 최신 것. `ROLLED_BACK`·FAILED release는 후보에서 제외한다(rollback은 새 release를 ACTIVE로 만들므로 문제 release가 유효했던 구간은 직전 정상 release가 덮는다). 조건을 만족하는 release가 없으면(첫 release 이전 tick) occurrence를 `SUPERSEDED_BY(RELEASE)`로 마감하고 실행하지 않는다. 배포와 정각 tick이 경합해도 결과가 결정론적이다.
- CATCHUP / INITIAL_LOAD / REPLAY / RERUN_LATEST / BACKFILL: 생성 시점의 현재 ACTIVE release를 고정한다(논리 시각이 과거라도 버그 수정 release를 받는다).
- 이미 만들어진 contract는 republish로 바뀌지 않는다. 실행 인터페이스(asset key, partition spec, schedule group)가 바뀌는 release만 OPEN_CONTRACT_CHECK(6.2)에서 영향 Job의 `PLANNED` contract를 `VOID(SUPERSEDED_BY_RELEASE)`로 마감한다. 예외는 rollback(8.2 8번)으로, 문제 release를 pin한 `PLANNED` contract는 `interface_changed`와 무관하게 같은 사유로 마감한다(pinned release가 ROLLED_BACK이면 어차피 Guard `PINNED_RELEASE_INACTIVE`). 실행 중 contract는 어느 경우에도 자동으로 마감하지 않는다.

모드 × 키 × 충돌 시 동작:

| 모드 | operation_class | 정체성 키 | 이미 있으면 | 고정 버전 | Watermark |
|---|---|---|---|---|---|
| NORMAL (schedule·수동) | NORMAL | job + logical_at | 기존 contract 반환 | 논리 시각 기준 release | 성공 시 이동 |
| RETRY | (contract 재사용) | contract_id | `attempt_no` 증가 | 기존 contract 값 | 기존 규칙 (13.4 stale 규칙 적용) |
| CATCHUP (Hold 해제) | CATCHUP | job + hold_release_at | 기존 반환 | 현재 ACTIVE | 이동 |
| INITIAL_LOAD | INITIAL_LOAD | job + publish_at | 기존 반환 | 현재 ACTIVE | 초기값 설정 |
| REPLAY | REPLAY | job + parent_contract + client_request_id | 기존 반환 | 현재 ACTIVE | 기본 미이동 |
| BACKFILL | BACKFILL | plan + job + window | 기존 반환 | 현재 ACTIVE | 기본 미이동 |
| RERUN_LATEST | RERUN_LATEST | job + client_request_id | 기존 반환 | 현재 ACTIVE | 모드별 |

- 수동 NORMAL(`POST /v1/jobs/{id}/runs {mode: NORMAL}`, Custom UI·자동화)은 Control API가 `logical_scheduled_at`(가장 최근 cron 경계)을 계산해 같은 키로 create-or-get한다. 그래서 UI에서 같은 시간의 NORMAL 실행을 눌러도 이미 schedule occurrence가 있으면 같은 계약을 반환한다. 반환 뒤의 제출 규칙: (a) contract가 `PLANNED`가 아니면(이미 `ATTEMPT_ACTIVE` 이상이거나 종결) Run을 만들지 않고 `200 {launch_result: NOT_LAUNCHABLE, contract_state}`; (b) `PLANNED`이고 Adapter가 tag `contract_id`인 non-terminal Dagster run(queued·starting·running)을 확인하면 Run을 추가 제출하지 않고 `200 {launch_result: ALREADY_SUBMITTED, dagster_run_id}` — 같은 계약에 Run을 하나 더 넣어도 Guard binding CAS에서 `ATTEMPT_ALREADY_BOUND`로 끝날 뿐 Run Pod만 낭비하기 때문이다; (c) `PLANNED`이고 non-terminal run이 없으면(tick의 RunRequest 유실·queue에서 terminal·stale 대기·신규 생성) 9.3 재제출 경로와 같은 Adapter `launchRun`(tag `contract_id`·`resubmit_no`·`dagster/priority`)으로 제출하고 `202 {launch_result: SUBMITTED, operation_id}` — 이때 `next_eligible_at` backoff는 기다리지 않는다(운영자 의도 우선, admission은 어차피 Guard가 다시 판정한다). Hold 중이면 create-or-get 전에 423이며 occurrence를 만들지 않는다(17장). 최종 중복 차단은 어느 경우든 Guard의 binding CAS다. Dagster UI 직접 실행(origin=DAGSTER_UI, 10.2)은 Run Pod 자신이 이미 Run이므로 이 제출 규칙을 타지 않고 곧바로 `guard`로 간다.
- HTTP `Idempotency-Key` 헤더는 호출 중복 억제용 `IdempotencyRecord`에만 쓴다. REPLAY/RERUN_LATEST는 요청 본문의 `client_request_id`(운영자·자동화가 부여, 필수)가 occurrence 정체성의 일부다 — 헤더와 별개 값이다.
- Replay는 의도적으로 새 계약을 만들며 `parent_contract_id`, 사유, 승인자를 요구한다.
- 모든 write는 추가로 target lease(13.3)를 획득하고, Incremental은 job당 열린 window 1개 규칙(13.4)을 따른다.

### 9.3 장애 복구 — Schedule Gap Recovery

Dagster scheduler는 **비파티션 schedule의 놓친 tick 중 가장 최근 1개만** 평가한다. `max_catchup_runs`는 파티션 schedule에만 적용되므로 grouped schedule에는 조정할 catch-up 설정이 없다. [Dagster scheduler 구현](https://github.com/dagster-io/dagster/blob/1.13.18/python_modules/dagster/dagster/_scheduler/scheduler.py)

최소 주기 1시간·RTO 30분 조건에서는 보통 tick 1개 이상 놓치지 않으며, 놓친 tick 1개는 데이터 의미상 안전하다 — Full은 최신 1회로 coalesce되고 Incremental은 다음 회차가 `[last_watermark, fence)`로 덮는다.

복구 의미:

- 정기 복구: 최신 logical tick 1건
- Incremental: 마지막 production watermark부터 최신 safe cutoff까지 논리적 1회(물리적으로 여러 chunk 가능)
- Full: 최신 상태 1회
- 1 cron 주기 이상 장애: **Schedule Gap Recovery**가 expected cron 전개와 occurrence(disposition 포함)를 비교해 명시적 disposition을 만든다. 트리거는 daemon heartbeat gap 감지(`daemon_heartbeat_gap_seconds`, 기본 2 × 최소 period) 또는 운영자 요청이며, 검사 범위는 장애 구간 + 1 주기로 bounded. 복원 절차: (1) 최신 expected tick에 대해 `NORMAL` occurrence를 create-or-get(키 job + logical_at, release는 9.2 NORMAL 규칙 — Dagster 자체 catch-up tick과 같은 row로 수렴), (2) 그 contract_id를 받아 중간 누락 tick을 `COALESCED_INTO(contract_id)`로 생성, (3) (1)의 contract가 PLANNED이고 binding이 없으면 PLANNED stale 경로와 같은 Adapter launch로 제출. Gap Recovery는 Hold·lease·window·commit 판정을 하지 않는다 — 그것은 Guard와 Commit Adjudication의 몫이다.
  - 수동 기동: `POST /v1/schedule-gap-recoveries {scope?: {job_ids | shard | source_id}, range?: {from, to}, dry_run?: false}` → `202 {operation_id}`, 결과는 `GET /v1/operations/{operation_id}`(expected tick 수, disposition별 생성 건수, launch 건수, Hold로 건너뛴 건수). 기본값은 `to = now()`, `from = scope의 마지막 완료 Gap Recovery의 to(없으면 now() − daemon_heartbeat_gap_seconds) − 1 period`이며 `to − from > gap_recovery_max_range_seconds`(기본 7일, 22장 22번)면 400. 자동 경로(heartbeat gap 감지, actor `SYSTEM`, range = 마지막 heartbeat − 1 period ~ now)와 수동 경로는 **같은 operation을 만드는 같은 코드**이며 차이는 actor·range뿐이다. 수동 기동의 용도는 `daemon_heartbeat_gap_seconds` 미만의 장애(자동 기동 없음), 금지된 schedule STOPPED의 복원 뒤(9.1), `expected occurrence missing` 알림 후속(16.4), Control PostgreSQL PITR 재동기화(5.4)다. 멱등성: `Idempotency-Key`(IdempotencyRecord)와 별개로 절차 자체가 멱등이다 — (1)은 unique key create-or-get, (2)는 row가 없는 tick에만 생성, (3)은 `PLANNED` ∧ binding 없음 ∧ Adapter non-terminal run 없음일 때만 제출(9.3 재제출 단일 경로)하므로 같은 범위를 다시 기동해도 새 row·새 Run이 0이다. scope가 겹치는 operation이 진행 중이면 새로 만들지 않고 그 `operation_id`를 200으로 반환한다. `dry_run: true`는 expected − 설명된 누락 목록만 돌려주고 쓰지 않는다. 완료 시 `schedule gap recovery completed`(16.4)를 Outbox에 남긴다. 어느 경로든 Hold·lease·window·commit 판정은 하지 않는다(Hold 중 Job은 create-or-get이 `SKIPPED_BY_HOLD`로 설명한다, 9.1).
- `SKIPPED_BY_HOLD`, `COALESCED_INTO`, `SUPERSEDED_BY`는 “설명된 누락”이다. PoC 기준서 §5.1 “정상 Run 중복/누락” 판정 쿼리(19장 즉시 No-Go 2번)의 기대 집합 = cron 전개 − 설명된 누락.
- 정각 논리 시각은 유지한다. 복구 직후 동시 제출은 cron 분산이 아니라 Source admission과 우선순위 queue(10.2)가 제한한다.

PLANNED stale 검사와 재제출 루프: `state = PLANNED AND created_at < now() − planned_stale_after AND next_eligible_at IS NOT NULL AND next_eligible_at ≤ now() AND NOT held`를 `planned_scan_interval`(기본 60초) 주기로 스캔한다(`operation_class = CATCHUP`는 `planned_stale_after` 면제). `planned_stale_after` 기본값은 `max(5분, run_monitoring.start_timeout_seconds)`로 Dagster queue 대기를 포함한다. 재제출 전에 Dagster에 `tag contract_id = ?`인 non-terminal run이 **없음**을 Adapter가 확인하고(`run_key`는 tick 범위 억제일 뿐이다) GraphQL `launchRun`에 tag `contract_id`·`resubmit_no`·`dagster/priority`를 붙여 제출한다. 최종 중복 차단은 Guard의 binding CAS(10.2)다. 같은 루프가 `ADJUDICATION_PENDING`이고 (verdict NO_COMMIT/PARTIAL_COMMIT이며 reason이 자동 재시도 사유 `RUN_WORKER_LOST`이고 attempt 수 < `max_auto_attempts`) 또는 (`retry_authorization.consumed = false`)인 계약도 `next_eligible_at` 이후 재제출한다.

이 재제출 로직 — Adapter의 “tag `contract_id` non-terminal run 없음” 확인 → `launchRun`(tag `contract_id`·`resubmit_no`·`dagster/priority`·`dagster/max_runtime`) — 는 stale 루프, Schedule Gap Recovery (3), Hold 해제 핸들러(14.2), 수동 NORMAL(9.2 (c)), RETRY 재요청(14.3)이 공유하는 **단일 코드 경로**다. `resubmit_no`는 계약 row(`FOR UPDATE`)에서 Control이 채번하는 단조 증가 값이며(tick의 RunRequest는 0), 경로마다 다른 것은 호출 주체·actor와 `next_eligible_at` 대기 여부뿐이다. 두 주체가 거의 동시에 확인·제출해 Run이 둘 생기는 창은 남지만 Guard binding CAS가 attempt를 하나로 막고 진 쪽은 `ATTEMPT_ALREADY_BOUND`로 조용히 끝난다(알림 없음).

`expires_at`은 cron 기반 occurrence(NORMAL·CATCHUP)에만 두며 `logical_scheduled_at + period`다(INITIAL_LOAD·REPLAY·BACKFILL·RERUN_LATEST는 `expires_at = NULL`, 운영자 취소만). 재제출 대기 중인 `PLANNED` 또는 `ADJUDICATION_PENDING` 계약이 `expires_at`을 넘기면 재제출하지 않고 각각 occurrence `EXPIRED_UNLAUNCHED` + contract `VOID(EXPIRED_UNLAUNCHED)` / contract `CANCELLED(EXPIRED)`(이미 CAS된 부분 commit은 보존, window·lease 해제)로 마감하고 알림한다. 실행 중(`ATTEMPT_ACTIVE`·`COMMIT_OBSERVED`) 계약은 `expires_at`로 취소하지 않으며 `run_monitoring.max_runtime_seconds`(Job별 RunRequest tag `dagster/max_runtime`, 22장 22번)·Hold만 중단시킨다. `ADJUDICATION_PENDING`의 만료는 `WRITER_FENCED`가 확정된 계약에만 즉시 적용한다 — `RUN_WORKER_LOST`로 SA가 살아 있고 `reattach_deadline` 미경과인 계약은 `reattach_deadline`까지 기다렸다가 10.2 표의 fencing → Commit Adjudication(부분 commit CAS 반영)을 먼저 수행한 뒤 `CANCELLED(EXPIRED)`로 마감하며, 그 전에는 window·lease·source token을 해제하지 않는다. Incremental은 다음 회차가 `[last_watermark, fence)`로 덮으므로 데이터 손실이 없고, Full은 최신 회차가 대체한다. 이는 due 계산이 아니라 Control이 이미 만든 row의 stale 검사이므로 원칙 5와 충돌하지 않는다.

CATCHUP의 `expires_at`도 같은 규칙이다 — `logical_scheduled_at = hold_release_at`이므로 `hold_release_at + period`이며 면제하지 않는다. catch-up 대기열이 period를 넘는 경우(ΣD/C > period) lease를 얻지 못한 CATCHUP은 `EXPIRED_UNLAUNCHED` / `VOID(EXPIRED_UNLAUNCHED)`로 마감되고, 그 뒤 Guard에 도달하는 NORMAL이 `[current_watermark, safe_cutoff(fence))`를 덮는다. 이것이 데이터 손실이 아닌 이유는 window가 논리 시각이 아니라 **watermark와 Guard 시점 fence**에서만 파생하기 때문이다(13.4) — `PLANNED` CATCHUP은 window·fence·lease를 갖지 않으므로 NORMAL이 계산하는 window와 정확히 같은 구간을 계산했을 것이고, pinned release도 NORMAL(`effective_from ≤ logical_at`인 최신, `logical_at ≥ hold_release_at`)이 CATCHUP(해제 시점 ACTIVE)보다 같거나 새롭다. CATCHUP을 `expires_at`에서 면제하면 포화 Source에서 CATCHUP 하나가 이후 모든 NORMAL을 `COALESCED_INTO`로 흡수한 채 영원히 `PLANNED`에 머무는 **보이지 않는 기아**가 생기므로 채택하지 않는다. 만료 알림은 `operation_class = CATCHUP`이면 Job 단위가 아니라 `hold_id` 단위 집계 1건이다(10.2 표의 `HOLD` 거부와 같은 집계 단위, 16.4). 만료 전까지 같은 Job의 `PLANNED` CATCHUP과 `PLANNED` NORMAL은 공존할 수 있으며(최대 1 period, Job당 `PLANNED` 계약 ≤ 2 — Hold 이전부터 남은 `DQ_FAILED`·`RECONCILIATION_REQUIRED`는 별도), 둘 중 Guard 7번에서 먼저 window를 예약한 쪽이 실행하고 다른 쪽은 `operation_class`와 무관하게 `OPEN_WINDOW` → `VOID(COALESCED)` / `COALESCED_INTO`다. `PLANNED`끼리는 coalesce하지 않는다(13.4).

PoC에서 한 건이라도 missing/duplicate가 나오면 precomputed occurrence + cursor sensor 방식으로 승격한다. 이 승격안은 원칙 5의 명시적 예외다.

## 10. 실행 흐름

```mermaid
sequenceDiagram
    participant S as Dagster Schedule/UI/API
    participant C as Control Plane
    participant M as Source 모니터 세션 (Control → Oracle)
    participant D as Dagster Run Pod (Attempt)
    participant K as SparkApplication
    participant O as Oracle DR
    participant I as Iceberg/Polaris

    S->>C: occurrences batch create-or-get
    C->>C: Hold / 멱등성 / release 고정
    C-->>S: {contract_id, launch, priority} 목록
    S->>D: RunRequest(run_key=contract_id, tags)
    D->>C: guard (contract_id, dagster_run_id)
    C->>C: Guard 트랜잭션 시작: contract 조회 → binding CAS → Hold·digest 확인
    C->>M: role / lag / visible_scn / schema digest
    M-->>C: fence
    C->>C: window(low, high) + chunk 목록 → target lease → source token → commit
    C-->>D: attempt_no + pinned plan + fence + window + chunk 목록
    D->>I: target 현재 snapshot 조회 (base_snapshot_id)
    D->>C: chunks:begin(1, base_snapshot_id, payload digest)
    D->>K: create-or-get sa-{contract}-a{attempt}
    loop chunk k = 1..expected (Run Pod 소유)
        D->>C: chunks:begin(k) — Hold·lease·binding 재확인 (k ≥ 2)
        C-->>D: OK | DRAIN | ATTEMPT_FENCED
        D->>K: proceed{k, low_k, high_k} [Pipes] (DRAIN·FENCED면 stop)
        K->>O: role 재검증 → AS OF visible_scn → bounded extract
        K->>I: commit (summary: contract/attempt/chunk)
        K-->>D: chunk_receipt{k, rows, committed, snapshot_id, dq} [Pipes]
        D->>I: base_snapshot 이후 snapshot/refs 조회
        D->>C: chunks/{k}:commit — ledger 기록 + watermark CAS (한 트랜잭션)
    end
    D->>K: stop [Pipes]
    K-->>D: Spark 종료 (SA terminal)
    D->>C: finalize {outcome, sa_status}
    C-->>D: FINALIZED (window·lease·source token 해제)
    D-->>S: Asset materialization
    Note over S,C: Dagster run의 FAILURE/CANCELED/SUCCESS 사실은 run_status_sensor가 C로 보고한다
```

실패 경로 3종:

1. **Guard 거부** — Guard는 한 Control 트랜잭션이며 검사 1번(contract 조회·`FOR UPDATE`) 직후에 `SAVEPOINT reserve`를 둔다. 거부가 확정되면 savepoint 이후의 쓰기(2번·3.4의 attempt row·`current_attempt`·`bound_dagster_run_id`/`rebind_count`·`retry_authorization.consumed`, 7번 window 예약, 8번 lease)만 `ROLLBACK TO SAVEPOINT reserve`로 되돌리고, 거부 결과(계약 상태 마감·occurrence disposition·`next_eligible_at`·감사 이벤트·Outbox)는 savepoint 롤백 뒤 같은 트랜잭션에서 기록해 commit한다 — “예약은 rollback, 결과는 commit”이 항상 원자적이다. 단 2번에서 같은 트랜잭션에 반입한 Dagster terminal 사실과 3.3의 `WRITER_FENCED`·verdict는 거부 결과에 속하므로 savepoint 롤백 후 다시 기록한다. Spark는 제출되지 않는다.
2. **Run Pod 사망(RUN_WORKER_LOST)** — `dagster-terminal-event` FAILURE 반입 → `ADJUDICATION_PENDING(RUN_WORKER_LOST)`. SparkApplication이 살아 있으면 Control은 fencing하지 않고 `reattach_deadline`을 기록한다. `run_retries`가 띄운 새 Run의 Guard가 deadline 전에 도착하면 **재결합**(같은 attempt, Pipes 경로 재생으로 누락 receipt 회수, 직전 상태 복귀). deadline까지 재결합이 없거나 SA가 이미 terminal이면 `WRITER_FENCED` → Commit Adjudication(13.2) → `COMMIT`이면 Control이 finalize를 대행, `NO_COMMIT`/`PARTIAL_COMMIT`이면 자동 재시도 예산 내에서 새 attempt(재개 지점 = 마지막 CAS 값).
3. **Hold FORCE_STOP / lease 만료 / attempt-failure** — Control이 SparkApplication을 삭제하고 driver/executor pod 부재를 확인(`WRITER_FENCED`) → Commit Adjudication → 부분 commit이 있으면 반영해 CAS 후 `CANCELLED`(또는 운영자 RETRY 대기), 없으면 `ABORTED_NO_COMMIT`/`ADJUDICATION_PENDING` → window·lease 해제 규칙(6.2).

인터페이스:

- RunRequest tag에는 `contract_id`만(재제출은 `resubmit_no`, RETRY는 `retry_no` 추가).
- Guard 응답: `attempt_no`, pinned compiled plan URI+digest, fence(`visible_scn`, `fence_ts`), `confidence`·`confidence_reason`(11.3), window, chunk 목록·`expected_chunk_count`, descriptor hash, secret ref, `credential_revision_id`, lease id. 재결합 응답(10.2 3.2)은 여기에 `last_committed_snapshot_id`(가장 최근 non-null `committed_snapshot_id` — 13.1 lineage base)를 더한다.
- contract payload는 Run Pod가 Guard 응답에서 조립해 `contracts/{contract_id}/a{attempt_no}/payload.json`으로 AIStor에 쓰고(내용: 위 응답 전체 + `attempt_no`·`dagster_run_id`), 그 URI와 sha256을 SparkApplication spec과 chunk 1의 `chunks:begin` 본문에 싣는다. Control은 digest를 attempt에 기록만 하고 AIStor에 쓰지 않는다. 재결합 시 새 Run Pod는 같은 경로의 기존 객체를 재사용한다(덮어쓰기 금지). CR에 descriptor·자격증명 평문 금지.
- Spark → Run Pod Pipes message 세 종류: `chunk_receipt{chunk_no, extracted_rows, written_rows, merge_metrics: {inserted, updated, deleted}|null, committed: bool|null, snapshot_id|null, dq: {pk_unique, not_null, watermark_max_le_high}, dq_failed: bool, exception_class|null}` — 모든 chunk 종료마다 1건(`committed=true`가 commit 성공, `CommitStateUnknownException`은 `committed=null` + `exception_class`, 0-row는 `committed=false, dq_failed=false`, driver DQ 실패는 `committed=false, dq_failed=true`, 그 외 쓰기 실패는 `committed=false, exception_class` → `attempt-failure {reason: SPARK_FAILED}`); `precheck_failure{reason}` — role 재검증 실패 등 쓰기 전 중단; `proceed_timeout{last_chunk_no}` — `chunk_proceed_timeout_seconds` 초과 종료(10.1). Pipes 경로는 Dagster run이 아니라 attempt에 귀속한다: `pipes/{contract_id}/a{attempt_no}/messages`.
- Run Pod → Spark Pipes message: `proceed{chunk_no, low, high}`, `stop`.
- credential 실패 매핑: driver는 추출 직전 role 재검증 세션(11.3)을 **executor보다 먼저** 열며, 이 로그인이 ORA-01017/28000/28001/28002로 실패하면 executor 세션을 열지 않고 `precheck_failure{reason: CREDENTIAL_FAILURE, ora_code}`를 남기고 exit code 1로 종료한다(Template 요구사항). Run Pod는 이것과, receipt `exception_class`가 위 코드인 경우(precheck 이후 비밀번호가 바뀐 경합)를 `attempt-failure {reason: CREDENTIAL_FAILURE, ora_code, credential_revision_id}`로 attempt당 1회 보고한다. Control은 6.2 credential breaker 규칙을 같은 트랜잭션에서 적용한다.
- Spark → Iceberg는 snapshot summary `etl.*` 키. extract-once 경로의 staging manifest는 commit receipt가 아니라 extract 증거(13.1 DQ)다.
- Run Pod는 Oracle에 접속하지 않는다. Run Pod의 Iceberg 조회는 target 현재 snapshot(`base_snapshot_id`)과 chunk 검증(13.1)에 한정한다.

### 10.1 SparkApplication adapter

Dagster Pipes의 일반 Kubernetes Pod 실행 기능만으로 `SparkApplication` CR의 create/watch/reconnect 의미가 자동 해결되지는 않는다. 얇은 전용 client를 둔다. Pipes 프로토콜(`open_pipes_session` + AIStor message reader)은 driver ↔ Run Pod 메시지 채널로 재사용한다.

- 이름: `sa-{contract_short}-a{attempt_no}` — attempt마다 다른 이름. DNS-1123 subdomain이지만 Operator가 이름을 pod label 값으로 쓰므로 **실질 상한은 63자**(소문자·숫자·`-`)
- chunk는 같은 attempt의 SparkApplication 안에서 driver가 순차 실행한다. chunk마다 SA를 새로 만들지 않는다
- `create-or-get`: 같은 attempt에 대해서만 멱등. API timeout 후 중복 생성 방지
- spec 불변: 기존 CR의 spec을 업데이트하지 않는다(Operator의 “업데이트 → 재제출” 의미 회피)
- attempt에 CR UID, `status.sparkApplicationId`, `last_observed_sa_status`를 기록. get 결과 “없음”은 ledger·attempt 기록과 대조해 TTL GC인지 미생성인지 판정
- `timeToLiveSeconds ≥ Commit Adjudication 조사 기간`
- watch reconnect: resourceVersion 만료와 API disconnect 처리
- cancel: Dagster cancel과 SparkApplication delete/terminate 연결
- Pipes 메시지 경로는 attempt 단위(`pipes/{contract_id}/a{attempt_no}/messages`)다. 재결합한 Run Pod는 같은 경로를 **처음부터 재생**해 `chunk_receipt` 중 ledger row가 없는 chunk는 snapshot 검증 → `chunks/{n}:commit`을 대신 수행한 뒤 그 다음 chunk부터 `chunks:begin` → `proceed`를 이어간다. driver가 마지막으로 보낸 receipt의 chunk_no와 ledger 최대 chunk_no가 같을 때만 `proceed(next)`를 보낸다
- driver는 `proceed{chunk_no}`를 **chunk_no 기준으로 멱등** 처리한다 — 이미 시작했거나 완료한 chunk_no의 `proceed`는 무시한다. terminal 반입 직전에 `chunks:begin(k+1)` OK를 받은 zombie Run Pod와 재결합 Run Pod가 같은 `proceed(k+1)`을 보낼 수 있기 때문이다(10.2 소유권 검사). `stop`은 소유권 검사를 통과한 Run Pod 또는 Control(FORCE_STOP·lease 회수)만 보낸다
- Control의 `chunks:begin`도 같은 `(attempt_no, chunk_no)`에 대해 멱등이다 — 재결합 Run Pod가 zombie의 OK 이후 같은 chunk의 `chunks:begin`을 다시 호출해도 ledger·상태는 바뀌지 않고 같은 응답(`OK`/`DRAIN`)을 받는다
- driver는 `proceed` 대기가 `chunk_proceed_timeout_seconds`를 넘기면 Pipes에 `proceed_timeout{last_chunk_no}`를 남기고 exit code 1로 종료한다 — SA는 `FAILED`가 되어 Adjudication의 `WRITER_FENCED` 조건을 만족한다
- Wallet/`TNS_ADMIN`·`sessionInitStatement`는 driver와 executor pod 모두에 동일하게 마운트·적용한다(executor가 JDBC 세션을 연다)
- owner/reference와 label: Job, contract, attempt, Dagster run, template digest
- Spark Operator restart policy: 기본 `Never`; Application-level retry는 Dagster/Control 정책이 결정

### 10.2 Execution Contract Guard

Custom UI뿐 아니라 Dagster UI에서도 수동 실행·Retry·Backfill이 가능해야 하므로 **모든 Asset의 Spark submit 직전** Guard를 강제하고, chunk마다 재확인한다. Guard(`POST /v1/contracts/{id}/guard`, 입력 `(contract_id, dagster_run_id)` + 선택 필드 `run_stats {enqueued_at, launch_at, pod_started_at}` — Run Pod가 자기 Dagster run의 시각을 실으며 Guard 트랜잭션이 `attempt_timeline` t0~t2와 `guard_result.run_started_at`에 기록한다, 6.1)는 Control의 **한 트랜잭션**이며 거부 시 savepoint 규칙(10장 실패 경로 1 — savepoint는 1번 직후)으로 예약은 rollback되고 결과는 commit된다. Guard의 거부는 HTTP 오류가 아니라 **정상 응답(200)** `{result: <사유>, contract_state, next_eligible_at?}`이다(17장). Guard의 Oracle 조회는 모두 Control의 Source 모니터 세션(11.2)에서 트랜잭션 안(6번 시점, contract row lock 보유 중 조회 1회, `monitor_query_timeout_seconds` 기본 5초 초과 시 `FENCE_UNAVAILABLE`)에 수행하고 Run Pod는 Oracle에 접속하지 않는다.

contract tag가 없는 Run(Dagster UI 직접 실행)은 Run Pod가 먼저 `POST /v1/occurrences:batch-create-or-get`을 단일 항목 `{job_id, operation_class: NORMAL, origin: DAGSTER_UI, dagster_run_id}`로 호출한다. Control이 9.2 규칙으로 `logical_scheduled_at`(가장 최근 cron 경계)을 계산해 같은 키로 create-or-get하고 `{contract_id, contract_state}`를 돌려주며 occurrence에 `origin=DAGSTER_UI`를 감사 기록한다. Critical Job(`severity_class = CRITICAL`)은 `412 DIRECT_LAUNCH_FORBIDDEN`을 반환하고 Run Pod는 Run을 FAILURE로 끝낸다(사용자에게 보이도록, op failure 예외 — `run_retries` 비대상). 반환된 contract_id로 `guard`를 호출해 같은 순서를 따른다.

검사 순서:

1. contract 조회. 종결 상태(`FINALIZED*`, `RESOLVED`, `ABORTED_NO_COMMIT`, `VOID`, `CANCELLED*`, `DQ_FAILED`, `RECONCILIATION_REQUIRED`)면 `CONTRACT_CLOSED`로 거부. 재실행 의도는 RETRY(`ADJUDICATION_PENDING` 계약만)·REPLAY·RERUN_LATEST를 Control API로 명시해야 한다. `COMMIT_OBSERVED`·`ADJUDICATION_PENDING`은 2~3번 복구 절차로 재진입한다
2. **binding CAS** — contract row를 `FOR UPDATE`로 읽고 `current_attempt`(없으면 NULL)와 그 attempt의 `bound_dagster_run_id`를 확인한다.
   - `current_attempt IS NULL`: attempt 1을 CREATED→BOUND로 생성, `UPDATE contract SET current_attempt = 1 WHERE id = ? AND current_attempt IS NULL`
   - binding run ≠ 호출 run이면 Control은 먼저 자신이 반입한 terminal 사실을 보고, 없으면 **동기적으로 Dagster Adapter(`runOrError(runId).status`)를 조회**한다(`adapter_sync_timeout_seconds` 기본 5초, 실패 시 `CONTROL_API_UNAVAILABLE`과 같은 client 재시도). terminal(FAILURE/CANCELED/SUCCESS)이면 그 사실을 같은 트랜잭션에서 반입(아래 반입 표 적용)하고 3번 복구 절차로 간다. SUCCESS인데 계약이 `ATTEMPT_ACTIVE`/`COMMIT_OBSERVED`/`ADJUDICATION_PENDING`이면 `FINALIZE_MISSING` 경우로 같은 절차를 따른다
   - non-terminal이면 `ATTEMPT_ALREADY_BOUND`로 거부(거부된 Run은 materialization 없이 SUCCESS로 종료, 계약 변경 없음)
   `attempt_no`는 Guard가 계산하며 Run은 제시하지 않는다
3. **복구 절차**(기존 attempt가 있는 경우):
   1. 기존 `ExecutionAttempt`와 SparkApplication 존재 여부를 확인한다
   2. SparkApplication이 살아 있고 계약이 `ATTEMPT_ACTIVE`·`COMMIT_OBSERVED` 또는 `ADJUDICATION_PENDING(RUN_WORKER_LOST, reattach_deadline 미경과)`이면 **재결합**: `bound_dagster_run_id` 교체(`rebind_count` 증가), `ADJUDICATION_PENDING`이었으면 같은 트랜잭션에서 직전 상태(`ATTEMPT_ACTIVE`/`COMMIT_OBSERVED`)로 복귀, 새 attempt 없음. 재결합은 여기서 Guard 트랜잭션을 commit하고 4~9번을 **수행하지 않는다** — 응답은 기존 attempt의 `attempt_no`·pinned plan·fence·window(`low` = 마지막 CAS 값)·남은 chunk 목록·lease id·`credential_revision_id`이고, Hold는 다음 `chunks:begin`에서 재확인한다(`DRAIN`만 중단). 단 SA가 살아 있어도 pinned release가 ROLLED_BACK/FAILED이면 재결합하지 않고 3.3(fencing)으로 간다. Run Pod는 Pipes 경로를 재생해 누락 receipt를 회수한 뒤 watch와 chunk 루프를 이어받는다(10.1)
   3. 종료됐으면 `WRITER_FENCED`(SA terminal 확인, 또는 SA 삭제 + driver/executor pod 부재 확인)를 먼저 확정하고 Commit Adjudication(13.2)을 수행한다
   4. verdict `COMMIT`이면 Spark를 재제출하지 않고 finalize 단계로 직행. `NO_COMMIT`/`PARTIAL_COMMIT`이고 writer가 확실히 fence됐으며 **다음 중 하나**일 때만 `UPDATE contract SET current_attempt = N + 1 WHERE id = ? AND current_attempt = N`으로 새 attempt를 만들고 4번 이후를 계속한다: (i) verdict reason이 `RUN_WORKER_LOST`이고 기존 attempt 수 < `max_auto_attempts`, (ii) `contract.retry_authorization.consumed == false`(같은 트랜잭션에서 `consumed = true`). 둘 다 아니면 `RETRY_REQUIRED`로 거부(계약 `ADJUDICATION_PENDING` 유지, Run SUCCESS 종료). 새 attempt는 window·fence를 **재계산하지 않는다**: `contract.window`(low = 마지막 CAS 값, high·`visible_scn` = 최초 Guard 값)를 그대로 쓰고 `low == current_watermark`만 검증하며(아니면 `STALE_WINDOW`), `now() − contract.fence_ts > source_capability.undo_retention_seconds × 0.5`이면 `FENCE_EXPIRED`로 거부한다(운영자는 `abort` 후 다음 회차 또는 Full은 `RERUN_LATEST`). window 예약과 target lease는 계약이 이미 보유한 것을 재사용하고 8번에서 source token만 새로 획득한다. 6번의 모니터 세션 조회는 role·lag·schema 검사용이며 contract의 `visible_scn`·`fence_ts`는 갱신하지 않는다(11.3 “contract당 1회”)
4. Hold 재확인 — `PLANNED` 계약은 `VOID(SKIPPED_BY_HOLD)`, attempt가 있던 계약은 `CANCELLED(HOLD)`
5. pinned digest 확인 — 실행 내용(plan·template·image)은 contract의 pinned 값을 항상 우선하고 Launchpad 입력은 무시. 실행 인터페이스 불일치(asset key/partition spec)만 `INTERFACE_MISMATCH`(재시도 금지). `pinned_release`가 ROLLED_BACK/FAILED면 `PINNED_RELEASE_INACTIVE`. 이어서 **target health check**(5.4): Polaris `GET /v1/config` + pinned target table `loadTable`, AIStor payload/staging bucket `HEAD`를 `target_health_timeout_seconds`(기본 5초) 안에 수행하고 실패·timeout이면 `TARGET_UNAVAILABLE`로 거부한다 — 6번 앞이므로 Source를 읽지 않는다. 같은 트랜잭션에서 breaker key(Polaris catalog / AIStor)의 연속 실패 카운터를 증가시키고 `platform_breaker_failures`에 도달하면 자동 `HOLD_NEW`(scope는 5.4)를 같은 트랜잭션에서 생성한다(이번 계약은 `guard_result.result = TARGET_UNAVAILABLE`로 기록하되 Hold를 만드는 같은 트랜잭션에서 `VOID(SKIPPED_BY_HOLD)` / `SKIPPED_BY_HOLD(hold_id)`로 마감해 stale 루프 대상에서 제외한다 — 14.1의 Guard `HOLD` 거부와 같은 처리; 이후 계약은 4번에서 `HOLD`). 성공이면 카운터를 0으로. probe 결과는 key별 수 초 캐시·Guard당 총 예산 `target_health_timeout_seconds` 1회(5.4)
6. Source precheck(모니터 세션): `DATABASE_ROLE`/`OPEN_MODE` 검증 → `SOURCE_ROLE_MISMATCH`; fence 읽기 — `visible_scn`·`fence_ts`를 읽지 못하면 등급과 무관하게 `FENCE_UNAVAILABLE`(high를 계산할 수 없음); `apply_lag`가 envelope threshold 초과면 등급과 무관하게 `SOURCE_LAG_EXCEEDED`; lag 신호가 불확실(`DATUM_TIME`이 `datum_stale_seconds` 초과 stale, 또는 lag 조회만 실패/timeout)이면 `ZERO_GAP`은 `SOURCE_LAG_EXCEEDED`로 거부하고 `BEST_EFFORT`는 통과하되 같은 트랜잭션에서 `contract.confidence = DEGRADED`·`confidence_reason = DATUM_STALE | LAG_QUERY_FAILED`를 기록한다(11.3 `DEGRADED_CONFIDENCE`); `ALL_TAB_COLUMNS` digest 비교 → `SCHEMA_DRIFT`. credential: 현재 ACTIVE revision 선택(REVOKED만 있으면 `CREDENTIAL_REVOKED`). descriptor: contract의 pinned ConnectionRevision이 `REVOKED`면 현재 ACTIVE revision으로 재해석해 attempt에 기록(7.1), ACTIVE가 없으면 `CONNECTION_REVOKED`(모니터 세션 조회 전에 판정 — 모니터 세션 자체도 ACTIVE revision의 descriptor로 접속한다). 이것이 contract의 **유일한** lag 조회다 — `chunks:begin`·`chunks/{n}:commit`·`finalize`는 Oracle을 조회하지 않는다(11.3)
7. Incremental window 예약(13.4): watermark row `FOR UPDATE`, `low = current_watermark`, `high = safe_cutoff(fence)`. `high ≤ low`이면 `FINALIZED_NO_DATA`로 마감(Spark 미제출, 2번의 attempt row·binding은 롤백). 열린 window가 있으면 lease를 잡기 전에 `VOID(COALESCED)` + occurrence `COALESCED_INTO`로 마감하고 Skip(열린 contract가 `INITIAL_LOAD`면 `VOID(SUPERSEDED_BY_INITIAL_LOAD)` / `SUPERSEDED_BY(INITIAL_LOAD)`; 열린 contract가 이 계약의 `parent_contract_id`인 repair REPLAY는 13.4 인수 규칙). Full은 job당 활성 contract 존재 여부로 같은 판정. **chunk 목록 `[low_k, min(low_k + max_chunk_span, high))`과 `expected_chunk_count = ceil((high − low) / max_chunk_span)`를 Guard가 산출해 attempt에 기록**한다 — Run Pod는 이 목록을 순서대로 소비할 뿐 chunk를 새로 만들거나 합치지 않는다
8. lease 획득 — 전역 고정 순서 **window 예약 → target lease → source weighted token**(13.3). bounded try-lock(`lease_try_timeout_seconds`), 실패 시 역순 해제 후 `LEASE_BUSY`
9. 성공: contract `ATTEMPT_ACTIVE`, occurrence `EXECUTED`. 응답에 `attempt_no`·pinned plan·fence·window·chunk 목록·lease id·`credential_revision_id`

chunk 루프의 소유자는 **Run Pod**다. Run Pod는 **모든 chunk k ≥ 1 직전**에 `POST /v1/contracts/{id}/chunks:begin`을 호출한다. chunk 1의 `chunks:begin`은 SparkApplication 생성 **전**에 호출하며 본문에 Run Pod가 Polaris에서 읽은 target table의 현재 snapshot id(`base_snapshot_id`, 테이블이 비어 있으면 null)와 payload digest를 싣고, Control은 이를 `attempt.base_snapshot_id`·payload digest에 기록한다(이미 기록된 attempt에 다른 값이 오면 `412 BASE_SNAPSHOT_MISMATCH`). chunk k ≥ 2의 ledger row `base_snapshot_id`는 가장 최근의 non-null `committed_snapshot_id`(없으면 `attempt.base_snapshot_id`)다. `chunks:begin`이 `OK`를 반환한 뒤에만 SA create-or-get(k=1) 또는 driver `proceed{chunk_no, low, high}` 전송을 한다. driver는 chunk 커밋 후 `chunk_receipt`를 보내고 다음 `proceed`를 대기한다. Run Pod는 receipt마다 snapshot 검증 → `chunks/{n}:commit`(ledger 기록 + watermark CAS 한 트랜잭션)을 수행한다. `chunks:begin` 응답이 `DRAIN`이면 `stop`을 보내고 SA 종료를 기다려 14.1 (3)의 `finalize`를 호출한다. 응답이 `412 ATTEMPT_FENCED`이면 이 Run Pod는 더 이상 attempt의 소유자가 아니므로 driver에 **어떤 Pipes 메시지도 보내지 않고**(`stop` 포함 — SA는 재결합한 Run Pod 또는 Control이 소유) `finalize`도 호출하지 않은 채 tag `guard_result=attempt_fenced`로 SUCCESS 종료한다. `HOLD_NEW`는 진행 중 attempt의 후속 chunk를 막지 않는다. `chunks/{n}:commit` 응답이 `DQ_FAILED`(검사 1 summary 불일치·검사 5, 13.1)이면 Control은 그 chunk의 ledger row를 `dq_result=FAILED`·CAS 없이 기록하고 계약을 `DQ_FAILED`로 전이한다. Run Pod는 `stop`을 보내고 SA terminal을 watch로 확인한 뒤 `finalize {outcome: DQ_FAILED, sa_status}`를 호출하며, Control은 상태를 바꾸지 않고(`DQ_FAILED` 유지, window·target lease 유지) source token만 반환한다. Run은 materialization 없이 FAILURE(op failure 예외 — `run_retries` 비대상)로 끝내 운영자에게 보이게 한다. `drain_timeout_seconds` 안에 finalize가 없으면 FORCE_STOP 프로토콜의 fencing 단계로 token을 회수한다. driver는 추출 직전 role을 재검증하며(fence는 다시 읽지 않고 contract의 `visible_scn`으로 `AS OF SCN`만 적용), 불일치면 쓰기 없이 종료하고 `precheck_failure`를 남긴다. receipt 없이 SA가 terminal(FAILED/COMPLETED)이 되면 Run Pod는 `POST /v1/contracts/{id}/attempt-failure {reason: SPARK_TERMINATED_WITHOUT_RECEIPT, sa_status}`를 호출한 뒤 Run을 FAILURE로 끝낸다. `committed=null` receipt는 `attempt-failure {reason: COMMIT_STATE_UNKNOWN}`, `committed=false`이고 `exception_class`가 있는 receipt(쓰기 전 실패, commit 없음 확실)는 `attempt-failure {reason: SPARK_FAILED, exception_class}`, `precheck_failure`는 `attempt-failure {reason}` 경로다. Control은 SA terminal 확인(`WRITER_FENCED`) 후 판정한다.

`chunks/{n}:commit` 응답이 `RECONCILIATION_REQUIRED`이면(본문 `lineage`의 개입 snapshot 중 lease 기록 없는 외부 writer가 있음 — 13.1 chunk 검증 규칙) Control은 그 chunk의 ledger row를 `committed_snapshot_id` 포함·`dq_result=EXTERNAL_SNAPSHOT`·CAS 없이 기록하고 계약을 `RECONCILIATION_REQUIRED(EXTERNAL_SNAPSHOT)`로 전이한다(6.2). Run Pod는 `DQ_FAILED`와 같은 절차로 driver에 `stop`을 보내고 SA terminal을 watch로 확인한 뒤 `finalize {outcome: RECONCILIATION_REQUIRED, sa_status}`를 호출하며(Control은 source token만 반환, window·target lease 유지, `drain_timeout_seconds` 초과 시 fencing 단계로 회수), Run을 materialization 없이 FAILURE(op failure 예외 — `run_retries` 비대상)로 끝낸다. 이 경로는 Commit Adjudication이 아니다(writer는 자기 attempt이고 살아 있음). RETRY는 `CONTRACT_CLOSED`(14.3)이며 복구는 repair REPLAY(parent window 인수, 13.4) 또는 `resolve`뿐이다.

마지막 chunk의 CAS 뒤 Run Pod는 `stop`을 보내고 SA terminal(`COMPLETED`)을 watch로 확인한 뒤 `POST /v1/contracts/{id}/finalize {outcome: FINALIZED | FINALIZED_NO_DATA | CANCELLED_AT_SAFEPOINT | DQ_FAILED | RECONCILIATION_REQUIRED, sa_status, hold_id?}`를 호출한다. Control은 ledger row 1..expected_chunk_count(DRAIN은 `window.low == 마지막 CAS 값`)를 검증한 뒤 상태 전이와 window·target lease·source token 해제를 한 트랜잭션으로 수행한다. Run Pod가 finalize 전에 죽으면 Commit Adjudication이 verdict `COMMIT`으로 같은 finalize 로직을 대행한다.

Run Pod → Control의 모든 contract 호출(`guard` 이후의 `chunks:begin`, `chunks/{n}:commit`, `finalize`, `attempt-failure`)은 `(contract_id, attempt_no, dagster_run_id)`를 필수로 실으며, Control은 contract row `FOR UPDATE` 아래에서 **소유권 검사** 두 조건을 확인한다: (1) `contract.current_attempt == attempt_no AND attempt.bound_dagster_run_id == dagster_run_id`, (2) `attempt.terminal_ingested_at IS NULL` — 그 binding run에 대한 Dagster terminal 사실(FAILURE/CANCELED/SUCCESS)이 아직 반입되지 않음. `terminal_ingested_at`은 `dagster-terminal-event` 수신 또는 Guard 2번의 동기 Adapter 조회가 binding run의 terminal을 반입할 때 계약 상태와 무관하게(아래 반입 표에서 ‘무시’로 처리되는 경우 포함) 같은 트랜잭션에서 기록하고, 재결합으로 binding이 바뀌면 NULL로 초기화한다. 둘 중 하나라도 어긋나면 `412 ATTEMPT_FENCED {fence_reason: REBOUND | RUN_TERMINAL}`로 거부한다(`fence_reason`은 진단·지표용이며 Run Pod 동작은 같다). 따라서 run_monitoring이 FAILED로 표시했으나 프로세스가 살아 있는 Run Pod(zombie)는 **재결합 전이라도** terminal 반입 직후부터 거부된다. 재결합은 terminal 반입 뒤에만 일어나므로 어느 시점에도 소유권 검사를 통과하는 Run Pod는 최대 1개다. 반입 이전의 zombie 호출은 정당한 소유자의 호출이므로 정상 처리한다 — SA는 아직 그 Run Pod가 쥐고 있고 다른 Run은 binding될 수 없다. 거부받은 Run Pod는 driver에 `proceed`·`stop`을 포함한 어떤 Pipes 메시지도 보내지 않고 `finalize`도 호출하지 않은 채 종료한다(Run이 이미 terminal이면 종료 자체는 무의미하다).

terminal 반입과 경합한 chunk commit: `dagster-terminal-event` 반입 트랜잭션과 `chunks/{n}:commit` 트랜잭션은 같은 contract row lock으로 직렬화된다. (a) commit이 먼저면 ledger row·CAS는 그대로 유효하고(재결합 Run은 `low = 마지막 CAS 값`부터 이어감) zombie의 다음 `chunks:begin`이 `RUN_TERMINAL`로 거부된다 — `proceed`는 `chunks:begin` OK 이후에만 보내므로 driver에 추가 proceed가 가지 않는다. (b) 반입이 먼저면 commit은 `ATTEMPT_FENCED(RUN_TERMINAL)`로 거부되고 그 chunk의 receipt는 Pipes 경로에 남아 재결합 Run Pod의 재생(10.1) 또는 Commit Adjudication의 접두 구간 판정(13.2)이 ledger·CAS를 대행한다 — 어느 쪽이든 chunk당 ledger row는 1개다. (c) zombie가 `chunks:begin(k+1)` OK 직후 반입이 일어나 `proceed(k+1)`을 이미 보낸 경우 재결합 Run Pod가 `proceed(k+1)`을 다시 보낼 수 있으므로 driver는 `proceed`를 chunk_no 기준으로 멱등 처리한다(10.1). 거부는 감사 이벤트와 지표(`attempt_fenced_total{fence_reason}`)로만 남기고 Kafka 알림은 내지 않는다. `DQ_FAILED`(finalize 전) 계약의 binding run terminal도 반입·기록되므로 그 Run Pod의 정당한 `finalize {outcome: DQ_FAILED}`가 `RUN_TERMINAL`로 막힐 수 있다 — 이 경우 `drain_timeout_seconds` 초과 시 fencing 단계가 source token을 회수한다(6.2, 상태는 `DQ_FAILED` 유지).

거부된 Run의 종료 규칙: `INTERFACE_MISMATCH`와 `DIRECT_LAUNCH_FORBIDDEN`을 제외한 모든 Guard 거부는 Run Pod가 예외를 던지지 않고 tag `guard_result=<사유>`를 남기며 **materialization 없이 SUCCESS**로 종료한다(`run_retries`·실패 알림 비대상). Guard가 `FINALIZED_NO_DATA`로 마감한 경우 Run Pod는 metadata `{rows: 0, no_data: true, contract_id}`를 가진 materialization을 내고 SUCCESS로 종료한다(freshness는 정상 갱신).

Guard 거부 사유별 결과(attempt가 없던 계약 기준):

| 사유 | Run 결과 | 계약 / occurrence | 재제출 | 알림 |
|---|---|---|---|---|
| `CONTRACT_CLOSED` | SUCCESS(tag `contract_closed`) | 변경 없음 | 없음 | 없음(지표만) |
| `ATTEMPT_ALREADY_BOUND` | SUCCESS | 변경 없음 | 없음 | 없음 |
| `RETRY_REQUIRED` | SUCCESS | `ADJUDICATION_PENDING` 유지 | 없음(운영자 RETRY/ABORT) | `adjudication_pending_alert_after` 규칙 |
| `HOLD` | SUCCESS(tag `skipped_by_hold`) | `VOID(SKIPPED_BY_HOLD)` / `SKIPPED_BY_HOLD` | 없음(CATCHUP이 대체) | Hold 단위 집계 1건 |
| `OPEN_WINDOW` | SUCCESS | `VOID(COALESCED)` / `COALESCED_INTO` | 없음 | 없음 |
| `INTERFACE_MISMATCH` | FAILURE, 재시도 금지 | `VOID(SUPERSEDED_BY_RELEASE)` / `SUPERSEDED_BY(RELEASE)` | 없음 | 운영자 |
| `PINNED_RELEASE_INACTIVE` | SUCCESS | `VOID(SUPERSEDED_BY_RELEASE)` / `SUPERSEDED_BY(RELEASE)` | 없음(REPLAY 안내) | 운영자 |
| `SOURCE_ROLE_MISMATCH` (Guard 감지) | SUCCESS | `VOID(SOURCE_ROLE_MISMATCH)` / `REJECTED_AT_GUARD(SOURCE_ROLE_MISMATCH)` + Source 자동 `HOLD_NEW` | 없음 | 즉시 |
| `SOURCE_ROLE_MISMATCH` (driver 재검증, `attempt-failure`) | FAILURE | `ADJUDICATION_PENDING` → fencing 후 `CANCELLED(SOURCE_ROLE_MISMATCH)` + Source 자동 `HOLD_NEW` | 없음 | 즉시 |
| `SCHEMA_DRIFT` | SUCCESS | `VOID(SCHEMA_DRIFT)` / `REJECTED_AT_GUARD(SCHEMA_DRIFT)` + 해당 Job 자동 `HOLD_NEW`(schema 승인 시 해제, 이후 회차는 SKIPPED_BY_HOLD) | 없음 | 운영자 + `schema drift detected` |
| `SOURCE_LAG_EXCEEDED` / `FENCE_UNAVAILABLE` | SUCCESS | `PLANNED` 유지, `next_eligible_at = now() + backoff` | stale 루프(9.3) | Source 단위 집계 1건 |
| `TARGET_UNAVAILABLE` (Guard 5번 health check) | SUCCESS | `PLANNED` 유지, `next_eligible_at = now() + backoff`; breaker key 연속 `platform_breaker_failures`회면 같은 트랜잭션에서 자동 `HOLD_NEW` 생성(5.4) | stale 루프(9.3) — breaker 도달로 Hold를 만든 트랜잭션에서는 이 계약도 `VOID(SKIPPED_BY_HOLD)`로 마감(stale 루프는 `NOT held`라 재제출하지 않음), 이후 회차는 `SKIPPED_BY_HOLD`, 해제 시 CATCHUP이 대체(14.2) | breaker key 단위 집계 1건(`target unavailable`), Hold 생성 시 Hold 이벤트 동반 |
| `TARGET_UNAVAILABLE` (Run Pod `base_snapshot_id` 조회 실패, `attempt-failure`) | FAILURE | `ADJUDICATION_PENDING` → SA 미생성 확인 = `WRITER_FENCED` → verdict `NO_COMMIT(TARGET_UNAVAILABLE)` 유지, 운영자 RETRY/abort 대기 | 없음(자동 재시도 사유 아님) | `adjudication_pending_alert_after` 규칙 |
| `LEASE_BUSY` | SUCCESS | `PLANNED` 유지, `next_eligible_at = now() + backoff` | stale 루프(9.3) | 없음(대기 지표) |
| `CREDENTIAL_REVOKED` | SUCCESS | `PLANNED` 유지, `next_eligible_at = now() + backoff`(상한 period/2) | 새 ACTIVE credential 등록 시 Control이 해당 Source의 대기 계약 `next_eligible_at = now()`로 리셋 → stale 루프 | 즉시 |
| `CONNECTION_REVOKED` | SUCCESS | `PLANNED` 유지, `next_eligible_at = now() + backoff`(상한 period/2) | 새 ACTIVE ConnectionRevision 등록 시 Control이 해당 Source의 대기 계약 `next_eligible_at = now()`로 리셋 → stale 루프 | 즉시 |
| `CONTROL_API_UNAVAILABLE` (client) | Run Pod가 `guard_retry_budget_seconds`(기본 120) 동안 재시도 후 SUCCESS(tag) 종료 | `PLANNED` 유지 | stale 루프(9.3) | 집계 1건 |
| `FENCE_EXPIRED` (복구 경로) | SUCCESS | `ADJUDICATION_PENDING` 유지 | 없음(운영자 abort) | 운영자 |
| `STALE_WINDOW` / `PINNED_RELEASE_INACTIVE` (RETRY API) | Run 미생성(Control API 409) | `CANCELLED(STALE_WINDOW)` / 변경 없음 + REPLAY 링크 | 없음 | 운영자 |
| `ATTEMPT_IN_PROGRESS` / `CONTRACT_NOT_STARTED` (RETRY API, 14.3) | Run 미생성(Control API 409) | 변경 없음(`required_action` 안내) | 없음(진행 중 attempt 완료 대기 / 9.2 재제출) | 없음(감사 이벤트만) |
| `ATTEMPT_FENCED` (chunk·finalize·attempt-failure 호출 412, `fence_reason: REBOUND \| RUN_TERMINAL`) | 해당 Run은 이미 terminal이거나 binding을 잃음 — Run Pod는 driver에 어떤 Pipes 메시지도 보내지 않고 `finalize` 없이 종료(tag `guard_result=attempt_fenced`) | 변경 없음(소유권은 재결합 Run 또는 Adjudication에 있음) | 없음 | 없음(지표 `attempt_fenced_total{fence_reason}` + 감사 이벤트) |

복구 경로(기존 attempt가 있어 3번을 거친 경우)의 거부 결과: 새 attempt row·binding은 savepoint 롤백되고(3.4에서 소비한 `retry_authorization.consumed`도 `false`로 돌아가 다음 재제출이 같은 권한을 소비한다), 계약은 (a) `LEASE_BUSY`·`SOURCE_LAG_EXCEEDED`·`FENCE_UNAVAILABLE`·`TARGET_UNAVAILABLE`·`CREDENTIAL_REVOKED`·`CONNECTION_REVOKED`·`CONTROL_API_UNAVAILABLE`이면 `ADJUDICATION_PENDING`을 유지하고 `next_eligible_at = now() + backoff`로 stale 루프(9.3) 대상이 되며(자동 재시도 사유 또는 미소비 `retry_authorization`이 있을 때만 재제출; `TARGET_UNAVAILABLE`의 breaker 카운터·자동 `HOLD_NEW`는 복구 경로에서도 동일), (b) `HOLD`·`SCHEMA_DRIFT`·`SOURCE_ROLE_MISMATCH`·`STALE_WINDOW`(3.4, 13.4)는 같은 reason의 `CANCELLED(reason)`, `INTERFACE_MISMATCH`·`PINNED_RELEASE_INACTIVE`는 `CANCELLED(SUPERSEDED_BY_RELEASE)`(REPLAY 안내)로 마감하고 `retry_authorization`을 무효화한다, (c) `FENCE_EXPIRED`는 `ADJUDICATION_PENDING`을 유지하되 `next_eligible_at = NULL`(재제출 금지)로 두고 `retry_authorization`을 무효화한다 — 운영자 `abort` 또는 `expires_at` 만료만 남는다. occurrence disposition은 이미 `EXECUTED`이므로 바꾸지 않는다.

backoff: 초기 `guard_backoff_initial_seconds`(기본 30), 2배 증가, 상한 period/2. `PLANNED`·`ADJUDICATION_PENDING`이 `expires_at`에 도달하면 9.3 규칙으로 만료된다(`ADJUDICATION_PENDING`은 fencing 확정 후). **자동 재시도가 허용되는 verdict reason은 `RUN_WORKER_LOST`뿐**이며 횟수는 `max_auto_attempts`(기본 3)로 제한한다. `SOURCE_ROLE_MISMATCH`·`EMPTY_FULL`은 재시도하지 않고 표/12.1대로 마감하거나 운영자 대기하며, `FINALIZE_MISSING`(Adjudication이 COMMIT이 아닐 때)·`SPARK_FAILED`·`SPARK_TERMINATED_WITHOUT_RECEIPT`·`COMMIT_STATE_UNKNOWN`·`CHUNK_DQ_FAILED`·`TARGET_UNAVAILABLE`(attempt-failure — SA 미생성이면 구간 판정 없이 즉시 `NO_COMMIT`)·`LEASE_EXPIRED`·`CREDENTIAL_FAILURE`(6.2 breaker가 Source Hold를 만들고, 새 ACTIVE revision 등록으로 Hold가 풀린 뒤 운영자 RETRY의 Guard가 그 revision을 선택한다)는 운영자가 RETRY 또는 ABORT를 선택하거나 `expires_at`(9.3)에 도달할 때까지 `ADJUDICATION_PENDING`을 유지하고 `adjudication_pending_alert_after`(기본 period/2)를 넘으면 알림한다. `FENCE_UNAVAILABLE`·`SOURCE_LAG_EXCEEDED`·`TARGET_UNAVAILABLE`(Guard)·`LEASE_BUSY`는 attempt 생성 전 Guard 거부이므로 이 예산과 무관하다.

우선순위: Control이 응답에 내려준 `priority`를 `dagster/priority` 태그로 쓴다(9.1). Dagster concurrency pool은 `granularity: run`의 느슨한 상한이고, 대기 지점은 Control lease 하나다.

Dagster → Control 사실 반입: 상태별 `run_status_sensor` 3개(`run_status = FAILURE | CANCELED | SUCCESS`, 각 `monitor_all_code_locations=True` 또는 shard `monitored_jobs`)가 `POST /v1/contracts/{id}/dagster-terminal-event`(Idempotency-Key = run_id + status)를 호출하고, Control은 상태 변경과 Outbox insert를 한 트랜잭션으로 수행한다. 보조로 `ATTEMPT_ACTIVE`·`COMMIT_OBSERVED` 계약의 binding run에 대한 주기적 run 상태 polling을 둔다. `run_monitoring.enabled: true`가 전제다.

| 이벤트 | binding run과 일치 | contract 상태 | 동작 |
|---|---|---|---|
| FAILURE / CANCELED | 일치 | ATTEMPT_ACTIVE / COMMIT_OBSERVED | `ADJUDICATION_PENDING(reason=RUN_WORKER_LOST)` 전이. Control은 SA 상태를 조회해 **SA가 running이면 fencing하지 않고** `attempt.reattach_deadline = now() + reattach_grace_seconds`(기본 `run_monitoring.start_timeout_seconds + 60`)를 기록한다. deadline까지 재결합이 없으면 Control이 SparkApplication 삭제 + driver/executor pod 부재 확인(14.1 FORCE_STOP 프로토콜의 **fencing 단계만** 차용)으로 `WRITER_FENCED`를 확정하고 Adjudication을 수행한다. verdict reason은 `RUN_WORKER_LOST`를 유지하며 계약은 `ADJUDICATION_PENDING`에 남아 자동 재시도 예산(`max_auto_attempts`) 안에서 새 attempt를 만든다 — `CANCELLED(FORCE_STOP)`/`ABORTED_NO_COMMIT`로 종결하지 않는다. SA가 이미 terminal이면 즉시 `WRITER_FENCED` → Adjudication(`adjudication_delay_seconds` 기본 0) |
| FAILURE / CANCELED / SUCCESS | 일치 | ADJUDICATION_PENDING | 무시(이미 판정 중, 감사 로그만) |
| SUCCESS | 일치 | ATTEMPT_ACTIVE / COMMIT_OBSERVED(finalize 미호출) | `ADJUDICATION_PENDING(reason=FINALIZE_MISSING)` 전이 + 알림 `FINALIZE_MISSING`. Run이 SUCCESS로 끝나 재결합할 Run이 없으므로 `reattach_deadline` 없이 즉시 SA 상태를 조회해 terminal이면 `WRITER_FENCED` → Adjudication(verdict `COMMIT`이면 finalize 대행 → `FINALIZED`), running이면 fencing 단계로 fence → Adjudication. `NO_COMMIT`/`PARTIAL_COMMIT`이면 자동 재시도 없이 운영자 RETRY/ABORT 대기 |
| FAILURE / CANCELED / SUCCESS / lease EXPIRING / attempt-failure | 일치 | DQ_FAILED / RECONCILIATION_REQUIRED | 무시(운영자 대기 상태, 감사 로그만) |
| 모든 이벤트 | 일치 | FINALIZED* / RESOLVED / CANCELLED* / VOID / DQ_FAILED / RECONCILIATION_REQUIRED | 무시(감사 로그만) |
| 모든 이벤트 | 불일치(이전 binding) | — | 무시 + 감사 로그 |

Commit Adjudication의 수행 주체는 Control의 단일 Adjudication 서비스이며 호출 경로는 다섯 개뿐이다: (a) Run Pod 복구 진입(Guard 3번), (b) `dagster-terminal-event` 수신, (c) FORCE_STOP 또는 source lease `RECLAIMED` 직후, (d) `POST /v1/contracts/{id}/adjudicate`, (e) Run Pod `attempt-failure`. 어느 경로든 `WRITER_FENCED` 확정 → 구간 판정 순서는 동일하다(13.2).

그래서 GraphQL이나 Dagster UI를 직접 사용해도 **Spark submit과 Iceberg write**는 Source 보호와 Hold를 우회할 수 없다. schedule 정지·run terminate/delete·concurrency 편집 같은 Dagster UI mutation은 Guard 범위 밖이므로 5.1의 read-only 분리와 proxy 감사로 통제한다.

## 11. Oracle 읽기와 Source 보호

### 11.1 정책 계층

우선순위는 다음과 같다.

```text
DB-enforced hard limit (ETL 전용 user Profile / Resource Manager / standby 전용 service — 11.2, 22장 9번)
  > DBA/플랫폼이 승인한 SourceSafetyEnvelope
    > JobReadProfile override
      > LLM recommendation
```

LLM은 Source 전체의 동시 세션/CPU 한도를 변경하지 못한다. LLM이 추천하는 것은 그 절대 상한 안의 Job별 read profile뿐이다.

`SourceSafetyEnvelope` 예:

- 최대 동시 ETL Job
- 최대 총 JDBC connection weight
- Job당 최대 `numPartitions`
- query timeout, fetch size
- 허용 실행 시간대
- catch-up chunk 크기, cooldown
- DR apply/transport lag threshold
- circuit breaker와 자동 Source Hold 조건
- Primary 자동 fallback 금지
- `max_chunk_span`, `safety_lag`, `clock_skew`, `datum_stale_seconds`(13.4·11.3에서 참조)
- Source 모니터 세션 1개(11.2) — envelope 한도와 별도로 예약

Spark의 `numPartitions`는 최대 동시 JDBC connection 수도 결정하므로 Source quota의 핵심 변수이다. [Spark JDBC options](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

### 11.2 Connection 예산

PoC/MVP의 Critical Source 기본값은 `numPartitions=1`이다. `numPartitions`는 병렬 JDBC connection의 최대치이지 전체 과정의 Oracle session 수가 아니다 — driver의 schema 해석 세션(짧음), Spark task retry의 재연결이 더해진다.

- **Source 모니터 세션**: Control이 Source별로 읽기 전용 세션 1개를 상시 유지한다(DBA 승인 뷰만: `V$DATABASE`, `V$DATAGUARD_STATS`, `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER`, `GV$SESSION`, `ALL_TAB_COLUMNS`). Guard의 fence·role·schema 사전검사와 관측 fence가 모두 이 세션에서 수행되며, envelope 한도와 별도로 예약한다. 세션이 끊기면 해당 Source의 Guard는 `FENCE_UNAVAILABLE`로 거부한다
- weight = `numPartitions + driver_sessions`(기본 1). Phase 0에서 `V$SESSION` 실측으로 확정한다. Registry의 MetadataSnapshot으로 `customSchema`를 주입해 driver schema 조회를 줄이는 옵션을 검토한다
- lease heartbeat 주체는 Run Pod가 아니라 **Spark driver(또는 sidecar)**다 — 세션을 실제로 쥔 프로세스의 생존과 lease를 묶는다
- 2단계 회수: 만료 → `EXPIRING`(token 미반환) → Control이 SparkApplication 삭제 + driver/executor pod 부재 확인 → `RECLAIMED` → token 반환. **반환 전 재부여 금지.** lease 회수는 회계상 회수일 뿐 Oracle 세션을 끝내지 못하므로 이 순서가 없으면 절대 한도를 초과한다. `RECLAIMED` 확정이 곧 13.2의 `WRITER_FENCED`다
- 관측 fence: JDBC 세션의 `MODULE`(≤48B, `DBMS_APPLICATION_INFO.SET_MODULE`) / `ACTION`(≤32B) / `CLIENT_IDENTIFIER`(≤64B, `DBMS_SESSION.SET_IDENTIFIER`; 초과분 절단)에 job·contract_short·attempt를 길이 제한에 맞춰 기록한다(`sessionInitStatement` 또는 드라이버 `OCSID.*` clientInfo). token 부여 직전 모니터 세션이 `GV$SESSION` 집계를 pool 회계와 대조해 초과 시 거부·알림한다. 이것이 Oracle에서 집행 가능한 유일한 fencing이다
- `read_profile.query_timeout_seconds ≤ lease TTL − margin` — 같은 정책 테이블에서 파생
- pool 한도 < 절대 한도(회수 지연 여유)
- lease 해제 조건은 SparkApplication 종료 확인이다. Dagster concurrency pool은 `granularity: run`의 보조 상한으로만 쓰고 `run_monitoring.free_slots_after_run_end_seconds`를 설정한다. [Dagster concurrency pools](https://docs.dagster.io/guides/operate/managing-concurrency/concurrency-pools)
- 가변 `numPartitions` weighted lease는 MVP 이후이며, 위 회수 프로토콜은 PoC 기준서 FI-17(lease 만료/회수)의 합격 조건이자 19장 즉시 No-Go 9번의 검출 근거(`lease_state_history`, 6.1)다

병렬 읽기가 필요한 Job은 Source별 고정 profile 또는 weighted lease 중 하나를 선택한다.

DB 측 강제(ETL 전용 user Profile `SESSIONS_PER_USER`, Resource Manager consumer group, standby 전용 role-based service)는 Control lease와 별개의 두 번째 겹이며 22장에서 DBA와 확정한다. 11.1 계층 최상위 “DB-enforced hard limit”의 실체가 이것이다.

### 11.3 Source Visibility Fence와 DR lag

Incremental window의 상한(high)은 호스트 시각에서 파생하지 않는다. **fence는 Guard 시점에 Control의 Source 모니터 세션(11.2)이 읽으며**, contract에 `visible_scn`, `fence_ts`, `apply_lag`, `DATUM_TIME`을 기록한다. fence는 contract당 1회 읽고 chunk마다 다시 읽지 않는다. Spark driver는 fence를 다시 읽지 않으며 contract의 `visible_scn`으로 `AS OF SCN`만 적용하고 추출 직전 role만 재검증한다. 모니터 세션이 읽은 SCN은 뒤에 열리는 driver 세션에서도 유효하다(SCN은 전역이며 undo retention 내 `AS OF SCN` 조회 가능).

cutoff 종류(`load.cutoff.kind`):

- `APPLICATION_TIMESTAMP_WITH_OVERLAP`(기본): `high = min(SCN_TO_TIMESTAMP(visible_scn), SYSTIMESTAMP_standby) − safety_lag`. `SCN_TO_TIMESTAMP`는 3초 granularity의 근사(실제 시각이 최대 3초 이를 수 있음)이므로 `clock_skew` 항에 3초를 포함하며, 보존 기간(최소 120시간)을 넘은 SCN은 오류를 낸다. 애플리케이션 시각 컬럼(`UPDATE_DT`)은 commit 시각이 아니므로 **overlap ≥ `max_open_txn_seconds` + `safety_lag` + `clock_skew`**(7.2 13번과 동일 — publish 시점에 계산 가능한 capability·envelope 상수만 쓴다; 실행 시 `apply_lag`는 fence(`visible_scn`)에 이미 반영되고 threshold 초과는 `SOURCE_LAG_EXCEEDED`로 거부되므로 별도 항이 아니다)를 publish 검증에서 강제하고(`ZERO_GAP` 미달은 `422 VALIDATION_FAILED` rule `OVERLAP_BELOW_MINIMUM`, `BEST_EFFORT`는 경고 — 17장) PK 기반 Merge/Dedup을 적용한다. 그 상한을 보장할 수 없으면 Data Reconciliation Audit(12.3)이 필수다.
- `STANDBY_VISIBLE_SCN`(PoC 한정): snapshot을 `AS OF visible_scn`으로 고정하고 window도 SCN 범위로 둔다. SCN 범위의 row 선택에는 `VERSIONS BETWEEN SCN`(undo retention 내 짧은 window만) 또는 `ORA_ROWSCN`(block 단위·false positive, 11.4 제약)이 필요하므로 운영 기본값이 아니라 PoC 비교군이다.
- `CDC_OFFSET`(향후): 외부 CDC의 commit 기반 offset.

visible SCN의 출처는 `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER`(세션 가시 SCN)를 기본으로 하고, 보조로 `V$RECOVERY_PROGRESS`(ITEM='Last Applied Redo'의 `COMMENTS` 문자열에서 SCN 파싱 — 형식 비보증) 또는 `V$DATAGUARD_STATS`의 apply lag를 쓴다. ADG(READ ONLY WITH APPLY)에서 세 값의 정확한 관계는 22장 DBA 확정. **`V$DATABASE.CURRENT_SCN`은 standby에서 checkpoint SCN(공식 문서상 mounted·media recovery 기준)으로 마지막 적용 SCN보다 작으므로 쓰지 않는다.**

lag 신호 capability:

1. `V$DATAGUARD_STATS` 등 DB 내부 lag 조회 가능 — Guard 6번은 `visible_scn`과 lag 신호를 따로 판정한다. (a) `visible_scn`을 읽지 못하면 등급과 무관하게 `FENCE_UNAVAILABLE`. (b) apply lag가 envelope threshold를 넘으면 등급과 무관하게 `SOURCE_LAG_EXCEEDED`(lag threshold는 SourceSafetyEnvelope의 Source 보호 항목이다). (c) lag 신호가 **불확실**한 경우 — `DATUM_TIME`이 `datum_stale_seconds`(기본 30)보다 오래됐거나 lag 조회만 실패/timeout — `ZERO_GAP`은 `SOURCE_LAG_EXCEEDED`로 거부하고, `BEST_EFFORT`는 window를 예약하되 contract를 `DEGRADED_CONFIDENCE`로 표시한다(아래). 거부 시 Guard가 window를 예약하지 않는다(Spark 미제출, 계약은 PLANNED로 backoff 재제출, `expires_at` 도달 시 다음 회차에 흡수). ‘이전 high 재사용’ 경로는 두지 않는다. [Oracle Data Guard lag](https://docs.oracle.com/en/database/oracle/oracle-database/19/sbydb/managing-oracle-data-guard-physical-standby-databases.html)
2. 외부 Data Guard monitoring API/metric 사용 가능 — 동일 규칙
3. 신호 없음 — incremental을 금지하지 않되 `guarantee_grade: BEST_EFFORT`로만 허용한다. overlap·Merge·Data Reconciliation Audit을 필수로 걸고, 등급을 데이터 계약(freshness/gap grade)에 노출해 소비자가 알 수 있게 한다. 조용히 등급만 낮추지 않는다.

`DEGRADED_CONFIDENCE`는 계약 상태가 아니라 **contract 속성 `confidence = DEGRADED`**(기본 `FULL`, 사유 `confidence_reason = DATUM_STALE | LAG_QUERY_FAILED`)다. 발생 조건은 하나뿐이다: **Guard 6번**에서 모니터 세션이 `visible_scn`은 읽었으나 lag 신호가 불확실하고(위 1번 (c)) 계약의 `guarantee_grade = BEST_EFFORT`인 경우. 판정 주체는 Guard이며 Guard 트랜잭션에서 기록한다. `ZERO_GAP`은 같은 조건에서 `SOURCE_LAG_EXCEEDED`로 거부되므로 window 자체가 예약되지 않는다 — “ZERO_GAP에서 CAS를 막는다”의 실체는 Guard 거부이지 chunk 단계의 CAS 거부가 아니며, `DEGRADED`를 가진 ZERO_GAP 계약은 존재하지 않는다. capability 3(신호 없음) Source는 등급 자체가 BEST_EFFORT이고 Audit이 항상 필수이므로 표시 대상이 아니다(`confidence = FULL`, 등급이 불확실성을 이미 노출).

**attempt 실행 중에는 lag를 다시 읽지 않는다.** fence는 contract당 1회이고 모든 chunk가 `AS OF visible_scn`으로 읽으므로, Guard 이후 lag가 올라가도 `visible_scn` 이하에서 이미 적용된 row는 무효가 되지 않는다 — 이번 window의 row 집합은 Guard에서 고정됐고, 늦게 적용되는 redo는 다음 회차 Guard의 fence·overlap과 `SOURCE_LAG_EXCEEDED` 거부가 다룬다(13.4). 따라서 `chunks/{n}:commit`에 Oracle 조회를 두지 않고 CAS를 lag로 거부하지 않으며, 실행 중 lag로 `ADJUDICATION_PENDING`에 가는 경로도 없다. `DEGRADED` 계약은 CAS를 정상 수행하고 ledger row마다 `confidence`를 복사하며(13.1), `FINALIZED` 시 Data Reconciliation Audit(12.3) 우선 대상으로 표시하고, 알림은 Source 단위 `degraded confidence` 1건으로 집계한다(16.4). `confidence`는 freshness/gap grade와 함께 데이터 계약에 노출한다.

Critical Source는 `sessionInitStatement`로 `ALTER SESSION SET STANDBY_MAX_DATA_DELAY=<초>`를 설정해 apply lag 초과 시 DB가 `ORA-03172`로 거부하는 fail-fast를 기본값으로 검토한다. 이 파라미터는 real-time query 모드의 **비관리(non-administrative) 계정 세션에만** 적용되므로 ETL 전용 일반 계정이 전제다.

Primary로 자동 fallback하지 않는다. Guard(모니터 세션)와 driver의 추출 직전, 두 지점에서 `SELECT database_role, open_mode FROM V$DATABASE`로 `PHYSICAL STANDBY` / `READ ONLY WITH APPLY`를 검증하고, 불일치(switchover·failover로 같은 descriptor가 primary가 된 경우)면 즉시 중단 + Source 자동 `HOLD_NEW` + `source role mismatch` 알림(10.2 표). DBA와 standby 전용 role-based service를 만들어 ConnectionRevision이 그 service만 가리키게 한다. Raw descriptor 검증은 ADDRESS_LIST의 모든 ADDRESS에 allowlist를 적용한다.

### 11.4 읽기 일관성

여러 JDBC partition query는 동일 시점의 데이터를 읽는다는 보장이 없다.

- 가능하면 실행 시작 시 공통 SCN(11.3의 `visible_scn`)을 모든 partition query에 동일 `AS OF SCN`으로 적용. staging manifest의 SCN도 이 값이다
- 물리 standby의 undo는 primary에서 전달된 것이므로 flashback 가능 범위는 primary의 `UNDO_RETENTION` / `RETENTION GUARANTEE`에 종속된다. Source capability에 retention을 등록하고, Job의 예상 추출 시간이 retention의 50%를 넘으면 `FLASHBACK_SCN` 모드를 거부하고 `numPartitions=1` + window chunk로 강등한다
- `ORA-01555`는 “재시도 금지 + chunk 축소” 전용 오류 분류다
- Flashback 권한/UNDO 보존/ADG standby에서의 Flashback Query 지원 범위는 Source capability와 DBA 검증으로 확인한다
- 불가능한 Critical Source는 `numPartitions=1`
- 병렬도가 꼭 필요하면 bounded window + overlap + PK dedup + Data Reconciliation Audit
- `ORA_ROWSCN`은 일반 해법이 아니다: commit SCN 이상(상한)을 반환하고 `ROWDEPENDENCIES` 없이는 block 단위이며, Flashback Query(`AS OF`)에서는 지원되지 않고, pseudocolumn이라 인덱스 대상이 아니어서 `WHERE ORA_ROWSCN > :x`는 full scan이다. 특수 케이스 PoC 후보로만 둔다

Oracle은 Flashback Query의 `AS OF`와 시간보다 정확한 SCN 사용을 공식 제공한다. 12.1/14.4의 “Flashback/Archive”는 undo retention 범위 내 Flashback Query와 primary에 별도 구성하는 Flashback Data Archive를 구분해 읽는다. [Oracle Flashback Query](https://docs.oracle.com/en/database/oracle/oracle-database/19/adfns/flashback.html)

### 11.5 extract-once

Critical 또는 대형 Source는 선택적으로 다음 경로를 사용한다.

```text
Oracle → contract-scoped AIStor staging (`staging/{contract_id}/a{attempt_no}/c{chunk_no}/`) → validation/transform → Iceberg commit
```

- staging manifest에 contract ID, attempt_no, chunk_no, window, SCN(= 11.3 `visible_scn`), schema hash, file checksum 기록
- 새 attempt(13.2 `NO_COMMIT`/`PARTIAL_COMMIT` 이후)는 같은 contract·window·`visible_scn`의 staging manifest를 그대로 재사용한다(fence는 contract당 1회이므로 SCN이 attempt 간에 바뀌지 않는다). 재사용할 manifest가 없는 chunk만 재추출한다
- Target/Polaris/Iceberg 단계 실패 시 Oracle을 다시 읽지 않고 staging 재사용
- staging TTL은 retry/reconcile SLA보다 길게 설정
- 소형 Master Full load는 direct 경로 허용

S3 I/O와 저장량이 늘지만 생산 장비 DB의 재조회 위험을 줄이는 편이 우선이다.

## 12. 적재 방식별 의미

### 12.1 Full — 약 60%

- 모드를 분리한다: `FULL_STATIC_REPLACE`(테이블 전체 교체, 기본) / `PARTITION_REPLACE`(지정 partition 범위만 dynamic overwrite). Dynamic overwrite를 Full 대체 수단으로 쓰지 않는다 — 0건 결과에서 테이블이 이전 상태로 조용히 남는다. Template은 `FULL_STATIC_REPLACE`에 `spark.sql.sources.partitionOverwriteMode=static` + 테이블 전체 `INSERT OVERWRITE`/`replace`, `PARTITION_REPLACE`에 `dynamic`을 고정하며 JobSpec이 이를 바꿀 수 없다
- **0건 정책**: driver는 추출 row 수가 0이면 `allow_empty_full` 값과 무관하게 **Iceberg에 쓰지 않고** `chunk_receipt{extracted_rows: 0, written_rows: 0, committed: false}`를 보낸 뒤 정상 종료(SA `COMPLETED`)한다. Run Pod는 `allow_empty_full = true`면 SA terminal 확인 후 `finalize {outcome: FINALIZED_NO_DATA}`(target은 이전 snapshot 유지), `false`면 `attempt-failure {reason: EMPTY_FULL}`를 호출하고 Run을 FAILURE로 끝낸다. Control은 SA terminal(`WRITER_FENCED`) 확인 후 13.2 규칙으로 verdict `NO_COMMIT(EMPTY_FULL)`을 내리고 `ADJUDICATION_PENDING`에 둔다 — 자동 재시도 없음. 운영자는 `abort`(→ `ABORTED_NO_COMMIT`, 다음 회차가 대체)하거나 `POST /v1/contracts/{id}/accept-empty {reason}`(사유·승인자 필수)로 이번 회차만 빈 결과를 승인한다 — Control이 Spark 재제출 없이 `FINALIZED_NO_DATA`로 전이하고 window·lease를 해제한다. 영구 승인은 JobSpec의 `allow_empty_full: true`를 새 release로 publish해 다음 계약부터 적용한다(RETRY는 pinned JobSpec을 다시 읽으므로 같은 결과를 반복한다). 13.2와 Run Pod가 보는 `allow_empty_full`은 계약의 pinned JobSpec 값이다. 빈 테이블을 실제로 반영(교체)하는 것은 `allow_empty_full`이 아니라 운영자의 명시 `RERUN_LATEST {replace_with_empty: true}`만 허용한다. 따라서 −50% 급감 DQ는 0-row 경로와 만나지 않는다
- 계약에 `visible_scn` / `fence_ts`를 기록하고 snapshot summary에 `etl.source_scn`을 남겨 snapshot이 어느 Source 시점을 대표하는지 설명한다
- Hold 중 놓친 여러 회차는 모두 버리고 최신 1회로 coalesce. 이전 occurrence의 contract가 아직 활성이면 이번 회차도 `COALESCED_INTO`(13.4)
- commit 후 새 snapshot ID, row/file 통계, Job/contract metadata 확인 — 이전 snapshot 대비 row 급감(−50%)은 `DQ_FAILED`(13.1, 운영자 accept로 FINALIZED 가능)
- Target lease는 `EXCLUSIVE_TABLE`(13.3)
- 선택 `change_detection`: `MAX_UPDATE_DT`(인덱스 있을 때) 또는 `TAB_MODIFICATIONS`(flush 지연 확인 필요)를 Guard의 모니터 세션이 조회해 변경 없음이면 Spark 미제출로 `FINALIZED_NO_DATA`(no-op, 13.4의 `high ≤ low`와 같은 경로). 큰 테이블의 `COUNT(*)`는 DBA 승인 테이블만
- Full 테이블의 snapshot 보관은 짧은 retention 등급으로 두고 증거는 ledger(13.1)에 보관한다. 수치는 PoC 기준서
- Source가 매우 큰 경우에도 “historical backfill”로 과거 상태를 재현할 수는 없다. Flashback/Archive가 있을 때만 가능

### 12.2 Append — 전체의 약 20%

전제:

- 단조 증가하고 변경되지 않는 watermark
- 안정적인 PK/unique key
- hard delete 없음 또는 별도 처리(`delete_semantics`)
- watermark 컬럼이 partition 컬럼이거나 partition 컬럼으로 변환 가능해야 한다. 아니면 Wizard validator가 Merge로 강제한다

기본 흐름:

1. Guard에서 `[low, high)` window 예약 — `high`는 11.3 fence 기반. Target lease는 `APPEND`(13.3)
2. fixed window extract (chunk 단위, Run Pod가 `proceed`로 제어)
3. dedup: batch 내부(동일 PK 다중 row → JobSpec `dedup` 규칙) + overlap 구간은 target의 `[low − overlap, low)` partition만 anti-join(전체 스캔 금지)
4. append commit에 contract/attempt/chunk metadata 기록
5. snapshot 검증 → ledger 기록 + watermark CAS(chunk마다)
6. 0 row: `chunk_receipt{extracted_rows: 0, written_rows: 0, committed: false}`이면 snapshot 없이 해당 chunk의 ledger row + watermark CAS(`high_k`로 전진)를 허용하고, 이 계약의 ledger에 `committed_snapshot_id`가 하나도 없으면(이전 attempt의 PARTIAL_COMMIT 포함) SA 종료 확인 후 `finalize {outcome: FINALIZED_NO_DATA}`, 하나라도 있으면 `FINALIZED`. Commit Adjudication은 receipt의 0-row 보고가 있을 때만 “snapshot 0개”를 정상으로 본다. Merge도 동일

초기 적재: `initial_load` 절에 따라 **단일 `INITIAL_LOAD` occurrence/contract**(키 `job + publish_at`, 9.2)로 실행한다. 한 attempt 안에서 `initial_load.chunk`/`cooldown`과 SourceSafetyEnvelope에 따라 순차 chunk로 수행하며(BackfillPlan의 chunk 스케줄러 코드를 재사용하되 BACKFILL occurrence·BackfillItem은 만들지 않는다), chunk마다 watermark CAS로 전진하고 마지막 chunk에서 production watermark 초기값이 확정된다. 13.4의 “닫힌 구간” 규칙은 INITIAL_LOAD에 적용하지 않는다. 완료 전 도착하는 NORMAL tick의 occurrence는 `SUPERSEDED_BY(INITIAL_LOAD)`로 닫는다. `FROM_TIMESTAMP`는 과거를 의도적으로 버리는 선택이며 Wizard에서 명시 확인을 요구한다.

Append Replay/Backfill은 무조건 단순 `INSERT INTO`하지 않는다. 다음 중 정책을 선택한다.

- PK 기반 `MERGE repair`
- 영향 partition replace
- Run-specific branch에 write 후 검증·publish(cherry-pick 가능 — append 한정)

### 12.3 Merge — 전체의 약 20%

- `INSERT_DT OR UPDATE_DT` 유형은 Merge가 기본
- dedup 규칙을 JobSpec 표준으로 고정: `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY UPDATE_DT DESC NULLS LAST, ROW_HASH DESC) = 1`. UNION ALL 전략은 양쪽 window에 드는 row를 항상 2번 반환하므로 dedup은 예외가 아닌 상시 경로다
- “같은 Target row에 Source 여러 row가 match하지 않도록”은 별도 스캔이 아니라 `dedup 결과 행수 = distinct pk 수` assert로 검증한다
- fixed window와 stable PK로 retry를 결정론적으로 수행
- Target lease는 `PARTITION_OR_FILESET`(window가 닿는 partition 집합, 13.3). partition pruning이 불가능한 Merge(“broad Merge”, 예: partition 컬럼이 갱신되는 테이블)는 `EXCLUSIVE_TABLE`
- `merge_mode`(copy-on-write 기본 / merge-on-read)는 Merge-large에서 PoC 비교. merge-on-read는 compaction 동시성(13.3)이 전제
- hard delete는 자동 처리되지 않으므로 `delete_semantics`를 enum으로 확정한다:
  - `NONE_DECLARED` — 검증 안 됨. Critical 테이블은 publish 거부
  - `SOFT_DELETE(column, value, target_action=FLAG|DELETE)`
  - `PK_RECONCILE(interval, window)` — PK(+필요 시 ORA_ROWSCN)만 인덱스 스캔으로 추출 → Iceberg PK 집합과 anti-join → 삭제 반영. first-class Asset으로 두고 Source 용량 모델과 target lease를 공유한다
  - `CDC_LATER`
  - 등록 이후 purge 배치 도입 같은 변화는 통계 기준 row count drift로 경고하고 확정은 `PK_RECONCILE`로 한다
- **Data Reconciliation Audit**: 넓은 window(24~72h)의 PK + UPDATE_DT만 추출해 Iceberg와 비교하고 차이를 repair MERGE하는 정기 Job. `BEST_EFFORT` 등급과 `APPLICATION_TIMESTAMP` cutoff에서는 필수이며 Source 용량 모델에 포함한다

`INSERT_DT OR UPDATE_DT`가 Oracle index를 비효율적으로 사용할 수 있으므로 두 전략을 실행계획과 실제 부하로 비교한다.

```sql
-- A. 단일 OR predicate
WHERE INSERT_DT >= :low AND INSERT_DT < :high
   OR UPDATE_DT >= :low AND UPDATE_DT < :high

-- B. 두 bounded query를 UNION ALL 후 PK dedup (동일 AS OF SCN 필수)
```

Spark JDBC에는 바인드 파라미터가 없고 literal push-down만 있다. Template이 `query` 옵션으로 완성 SQL을 생성하되 window 값은 컬럼 타입에 맞춘 `TO_DATE` / `TO_TIMESTAMP` 고정 형식으로 넣어 DATE 컬럼의 암시 변환으로 인덱스·partition pruning이 무력화되는 것을 막는다(PoC에서 `DBMS_XPLAN`으로 확인). Wizard는 `UPDATE_DT` 인덱스/파티션 여부를 확인해 없으면 UNION_DEDUP을 경고하고 인덱스 요청 또는 Full 강등을 선택하게 한다.

Iceberg `MERGE INTO`는 Source의 여러 row가 동일 Target row를 갱신하는 입력을 허용하지 않으므로 dedup이 필수다. [Iceberg Spark writes](https://iceberg.apache.org/docs/latest/spark-writes/)

### 12.4 파티션

`dt/wt/mt/yt`는 원천 컬럼을 그대로 복사하는 단순 컬럼이 아니라 JobSpec의 표준 derived column으로 관리한다.

- 데이터량과 주 사용 query를 기준으로 day/week/month/year 추천
- 사용자가 최종 승인
- 향후 Iceberg partition evolution을 고려해 사용자 노출 컬럼과 physical transform의 역할을 구분
- partition 변경은 일반 Job edit가 아니라 schema/release change로 취급

물리 partition은 원천 timestamp 컬럼의 hidden transform(`day/month/year`)만 사용하고, `dt/wt/mt/yt`는 `partition_granularity` 메타데이터로만 유지한다(물리 중복 컬럼 없음). Hive 호환으로 물리 `dt` 컬럼이 꼭 필요한 테이블은 identity partition(dt)만 쓰고 hidden transform과 병행하지 않는다. Preview SQL 단계에서 대표 소비 쿼리의 pruning(scan files)을 검증한다.

#### 부록 — 플랫폼 표준 타입 매핑

Spark JDBC Oracle dialect 기본값을 그대로 쓰지 않고 Template 수준에서 고정한다. [Spark JDBC data source](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

| Oracle | 기본 매핑 | 규칙 |
|---|---|---|
| NUMBER(p,s) | decimal(p,s) | p > 38은 publish 경고 + 범위 검증 DQ |
| NUMBER (정밀도 미지정) | decimal(38,10) | dictionary 통계 기반 추천, 드라이버 버전별 보고값 확인 |
| DATE | Iceberg `timestamp`(NTZ) | `spark.sql.session.timeZone=Asia/Seoul`, JVM TZ, `preferTimestampNTZ`를 Runner 이미지에 고정. 전사 NTZ/timestamptz 선택은 22장 12번 |
| TIMESTAMP / TIMESTAMP WITH TZ | timestamp / timestamptz | 위와 동일 |
| VARCHAR2(n BYTE / CHAR) | Spark `varchar(n)` → Iceberg `string` | Spark 읽기는 `VarcharType(n)`(길이 검사 적용), Iceberg 저장은 string. KO16MSWIN949 → UTF-8 변환 시 길이 여유, NLS_CHARACTERSET은 Source capability |
| CLOB | string | 크기 경고 |
| LONG / RAW / LONG RAW | binary | Wizard 경고 |
| sentinel 날짜(9999-12-31 등) | 그대로 | partition 전환 규칙(null 또는 별도 partition) 명시 |

## 13. Commit 증명, Retry, Reconciliation

### 13.1 성공의 정의

SparkApplication의 `COMPLETED`만으로 성공 처리하지 않는다.

```text
Guard: visibility fence 읽기(Control 모니터 세션) → Window 예약(low, high) → lease
→ Spark 실행 (attempt_no), chunk마다:
   → chunk commit → base(k) 이후 Iceberg snapshot/refs 확인 (lineage — lease 기록 없는 외부 snapshot이면 chunks/{n}:commit에서 RECONCILIATION_REQUIRED, 아래 chunk 검증 규칙)
   → summary의 contract/attempt/chunk metadata 확인
   → chunk DQ
   → commit evidence ledger 기록 + watermark CAS (한 트랜잭션)
→ 마지막 chunk CAS 후 stop → Spark 종료 확인 → finalize에서 FINALIZED (window·target lease·source token 해제)
→ Dagster Asset materialization
```

**chunk 검증(lineage) 규칙** — 탐지는 Run Pod, 판정은 Control이며 13.2 3번과 같은 분류를 쓴다. Run Pod는 receipt k를 받으면 Polaris에서 `committed_snapshot_id`부터 parent를 따라 base(k)까지의 ancestor 목록과 각 snapshot의 `etl.*` summary·operation을 `chunks/{n}:commit` 본문 `lineage`에 싣는다. base(k)는 k=1이면 `attempt.base_snapshot_id`, k ≥ 2면 직전 non-null `committed_snapshot_id`이고, 재결합 Run Pod는 Guard 응답의 `last_committed_snapshot_id`(재결합 응답에 추가)를 쓴다. 0-row receipt(`committed=false, dq_failed=false`)는 base(k)부터 현재 snapshot까지를 같은 형식으로 싣는다. Control은 `chunks/{n}:commit` 트랜잭션에서 (1) 목록의 시작이 ledger의 base(k)와 일치하는지 검증하고(아니면 `412 BASE_SNAPSHOT_MISMATCH`), (2) 개입 snapshot 중 `etl.writer_kind ∈ {maintenance, repair}`이고 ledger에 유효한 `PARTITION_OR_FILESET`/repair lease 기록이 있는 것은 제외하며(18장 compaction과 APPEND ingestion의 정상 병행), (3) 남는 개입 snapshot이 없으면 정상 ledger row + CAS, 하나라도 있으면(`etl.*` 키 없음, 다른 contract/attempt의 ingest, lease 기록 없는 maintenance/repair) ledger row를 `committed_snapshot_id` 포함·`dq_result = EXTERNAL_SNAPSHOT`·CAS 없이 기록하고 계약을 `RECONCILIATION_REQUIRED(EXTERNAL_SNAPSHOT)`로 전이한다. 이 전이는 Commit Adjudication이 아니다 — writer는 자기 attempt이고 살아 있으므로 `WRITER_FENCED`도 verdict도 없다. Run Pod는 `DQ_FAILED`와 같은 프로토콜로 driver에 `stop`을 보내고 SA terminal을 확인한 뒤 `finalize {outcome: RECONCILIATION_REQUIRED, sa_status}`를 호출하며 Control은 source token만 반환한다(window·target lease 유지, `drain_timeout_seconds` 초과 시 fencing 단계로 회수). Run은 materialization 없이 FAILURE(op failure 예외)로 끝낸다. 자동 retry는 없다 — lease를 우회한 writer가 같은 partition을 건드렸을 수 있어 같은 window 재적재는 이중 commit 또는 덮어쓰기 위험이며, 복구는 repair REPLAY(parent window 인수, 13.4) 또는 운영자 `resolve`뿐이다. 이미 commit된 chunk k의 데이터는 ledger row가 증거다. 같은 `chunks/{n}:commit`에 외부 snapshot과 검사 1·5의 DQ 실패가 동시에 실리면 `EXTERNAL_SNAPSHOT`이 우선한다(둘 다 CAS 없음, 복구 경로는 repair REPLAY로 동일). chunk 자체 commit이 OCC(`validate-from-snapshot-id`)로 실패한 경우는 `committed=false, exception_class` → `attempt-failure {reason: SPARK_FAILED}` → Adjudication이 같은 개입 snapshot을 보고 13.2로 `RECONCILIATION_REQUIRED`에 이른다 — 두 경로의 종착 상태가 같다.

Snapshot summary에 최소 다음을 기록한다.

- `etl.job_id`
- `etl.execution_contract_id`
- `etl.attempt_no`
- `etl.chunk_no` / `etl.expected_chunk_count`
- `etl.writer_kind` (`ingest` | `maintenance` | `repair`)
- `etl.logical_window_low/high` 또는 `etl.scn_low/high`
- `etl.source_scn`
- `etl.job_spec_digest`, `etl.template_digest`
- `etl.dagster_run_id`(SparkApplication을 제출한 run), `etl.spark_application_id`

commit evidence는 snapshot summary만으로 부족하다. snapshot이 만료되면 Iceberg 안의 증거만으로는 과거 commit을 판정할 수 없으므로, **chunk별 watermark CAS와 같은 트랜잭션에서** chunk 단위 ledger row(`contract_id, attempt_no, chunk_no, expected_chunk_count, base_snapshot_id, committed_snapshot_id, sequence_number, summary, metadata_location, dq_result, confidence`)를 **Control DB의 CommitEvidenceLedger에 영속화**한다(`confidence`는 contract 값의 복사, 11.3 — row마다 재판정하지 않는다). FINALIZED는 마지막 chunk row에 `finalized=true`를 표시하는 것이며 별도 일괄 기록은 없다(이 표시는 `finalize` 호출 트랜잭션에서만 일어난다). snapshot 보관 기간은 이 ledger와 무관하게 테이블 등급별로 둔다.

Spark 런타임의 모든 Iceberg commit은 단일 commit-wrapper를 통과한다. DataFrameWriterV2는 `snapshot-property.*`를, SQL `MERGE` 등은 `CommitMetadata.withCommitProperties`를 쓰는데 후자는 driver ThreadLocal·Callable 범위이므로 wrapper 밖(다른 thread/async)의 SQL은 metadata가 누락된다. maintenance Job도 같은 wrapper로 `writer_kind=maintenance`를 남긴다. 실제 사용 Spark/Iceberg 버전 조합에서 반드시 검증한다. [Iceberg Spark configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)

MVP Data Quality Check — Source 재조회 없는 검사로 한정한다:

1. row 수 대조 — Append: `extract_rows == added-records`. Full(static replace): `extract_rows == total-records`. Merge: driver가 chunk의 MERGE 결과를 `chunk_receipt.merge_metrics`에 기록하고 `extract_rows(dedup 후) == merge_metrics.inserted + merge_metrics.updated`로 대조하며 summary의 `added-records`는 참고값. Iceberg는 `added-records`/`deleted-records`를 0일 때 기록하지 않으므로 키 부재는 0으로 해석하고, `etl.*` 키 부재만 metadata 누락으로 판정한다
2. 대상 snapshot/partition 내 PK 유일성(Append/Merge 필수)
3. PK·watermark 컬럼 NOT NULL
4. window 내 watermark max ≤ high
5. Full: 이전 snapshot 대비 row 급감(−50%) → `DQ_FAILED`
6. 선택: Source `COUNT(*)`는 `dq_source_count_allowed` 플래그가 있는 비Critical Source만, lease 1 token 소비로 모델링

실행 위치: 검사 2·3·4는 **driver가 chunk마다 commit 직전** 해당 chunk DataFrame에 대해 수행하고 결과를 `chunk_receipt.dq`에 싣는다(하나라도 실패하면 그 chunk를 commit하지 않고 `committed=false, dq_failed=true`와 `dq` 객체의 해당 항목 `false`로 보고 → Run Pod는 `attempt-failure {reason: CHUNK_DQ_FAILED}`, 계약은 `ADJUDICATION_PENDING` → 접두 chunk만 반영. Run Pod는 `extracted_rows = 0 ∧ committed=false ∧ dq_failed=false`를 0-row로 해석한다). 검사 1·5는 Run Pod가 receipt와 snapshot summary로 chunk마다 수행한다. Run Pod는 두 결과를 합쳐 `chunks/{n}:commit`에 싣고 Control이 ledger `dq_result`와 CAS를 한 트랜잭션으로 기록한다. commit 뒤에만 알 수 있는 실패(검사 1의 summary 불일치, 검사 5)만 `DQ_FAILED` 전이 대상이다. `DQ_FAILED`에서는 **RETRY를 금지**한다 — commit은 이미 되어 있으므로 같은 window를 다시 append하면 이중 commit이다. 복구는 repair REPLAY(MERGE repair / partition replace — parent의 window·target lease를 인수, 13.4) 또는 운영자 `dq:accept`(사유·승인자 기록 — Full row 급감 등 확인된 정상 변동)만 허용한다. `DQ_FAILED` 동안 window와 target lease는 유지된다(6.2). DQ 결과를 Dagster `AssetCheckResult`로도 내보낼지는 22장 결정.

### 13.2 Commit Adjudication (ambiguous commit)

예: Iceberg commit은 성공했지만 Spark Driver가 응답 전에 종료된 경우. 대표 원인은 Polaris REST catalog의 500/502/504(commit state unknown)와 Run Pod/driver 사망이다.

전제: 한 attempt 안에서 chunk는 `chunk_no` 오름차순으로 **직렬** commit되며 chunk k+1은 chunk k의 watermark CAS 성공 후에만 시작한다(10.2 proceed). 따라서 base 이후 자기 contract/attempt의 snapshot 집합은 항상 chunk 1..m의 접두 구간이다.

판정 순서:

1. **`WRITER_FENCED` 확정이 먼저다.** SparkApplication이 terminal이거나, 삭제 후 driver/executor pod 부재를 확인해야 한다(= source lease `RECLAIMED`, 11.2). fencing 전에는 어떤 판정도 내리지 않는다 — “지금까지 commit이 없다”는 관측이 “앞으로도 없다”를 보장하지 않기 때문이다. 단 `RUN_WORKER_LOST`로 SA가 살아 있는 경우는 `reattach_deadline`까지 fencing을 보류한다(10.2). 예외: SA가 생성된 적이 없고(UID 없음) `attempt.base_snapshot_id`가 기록되지 않은 attempt(`attempt-failure {TARGET_UNAVAILABLE}`, 5.4)는 writer가 존재한 적이 없으므로 2~3번 없이 즉시 `WRITER_FENCED` + verdict `NO_COMMIT(TARGET_UNAVAILABLE)`다.
2. 증거 검색은 **구간 기반**이다. attempt의 `base_snapshot_id` 이후 main(또는 attempt branch)에 생긴 모든 snapshot과 refs를 조회해 집합 S를 만든다.
3. S에서 `etl.writer_kind ∈ {maintenance, repair}`이고 ledger에 유효한 `PARTITION_OR_FILESET`/repair lease 기록이 있는 snapshot은 제외한 S′로 판정한다. 판정 대상 chunk 집합 = S′ ∪ {receipt에 `extracted_rows = 0, committed = false`로 보고된 chunk} — 0-row chunk는 snapshot 없이도 “존재”로 세며 ledger row + CAS만 대행한다:
   - S′ = ∅ → receipt에 `extracted_rows = 0, committed = false` 보고가 있으면: `load.mode ∈ {APPEND, MERGE}`이거나 `FULL ∧ allow_empty_full = true`이면 `FINALIZED_NO_DATA`, `FULL ∧ allow_empty_full = false`이면 verdict `NO_COMMIT(reason=EMPTY_FULL)`(자동 재시도 없음, 운영자 알림). receipt가 없으면 verdict `NO_COMMIT`(reason은 terminal 사유)
   - S′가 chunk 1..expected 각각 정확히 1개이고 summary의 contract/attempt가 자기 것 → verdict `COMMIT` → chunk DQ(검사 1·5)·ledger·CAS가 빠진 chunk를 대신 수행 → `COMMIT_OBSERVED` → finalize 대행 → `FINALIZED`
   - 접두 구간 1..m(m < expected)만 있고 각 chunk가 정확히 1개이며 summary 키가 자기 것 → 아직 CAS되지 않은 chunk에 대해 chunk DQ → ledger 기록 → watermark CAS를 대신 수행하고 verdict `PARTIAL_COMMIT`, contract는 `ADJUDICATION_PENDING`에 남아 새 attempt가 `window.low = 마지막 CAS 값`부터 재개한다
   - 접두 구간이 아니거나(chunk 누락 후 후속 chunk 존재), 같은 chunk_no에 snapshot 2개 이상, `etl.*` 키가 없는 snapshot, 다른 contract/attempt의 ingest snapshot, lease 기록이 없는 maintenance snapshot → `RECONCILIATION_REQUIRED`(자동 retry 금지 — lease 우회 writer 또는 metadata 누락)
   - 증거 만료(base_snapshot 이후가 expire됨)·상충 → `RECONCILIATION_REQUIRED`
4. Spark Job은 예외 종류(`CommitStateUnknownException` 등)를 `chunk_receipt.exception_class`로 보고하고, orphan 파일은 `remove_orphan_files`가 회수한다.

Commit Adjudication은 Control의 단일 Adjudication 서비스만 수행한다(10.2). Run Pod는 복구 진입(Guard 3번)과 `attempt-failure` 호출로 트리거할 뿐 Iceberg 증거를 직접 판정하지 않는다(Run Pod의 Iceberg 조회는 `base_snapshot_id` 읽기와 13.1 chunk 검증에 한정). Schedule Gap Recovery(9.3)는 adjudication을 수행하지 않는다.

진행 중 attempt의 chunk 검증(13.1)이 외부 snapshot을 발견한 경우는 Adjudication 경로가 아니다: Control이 `chunks/{n}:commit` 트랜잭션에서 위 3번과 같은 분류(`etl.writer_kind`·ledger lease 기록)로 판정해 곧바로 `RECONCILIATION_REQUIRED(EXTERNAL_SNAPSHOT)`로 전이한다. 두 경로는 같은 분류 함수를 공유한다(판정 규칙의 단일 원본). 그 계약이 뒤에 `ADJUDICATION_PENDING`으로 가는 경로는 없으며(`RECONCILIATION_REQUIRED`에 도착하는 terminal event·attempt-failure는 무시, 6.2), `POST /v1/contracts/{id}/adjudicate`는 `ADJUDICATION_PENDING` 계약에만 유효하고 그 외는 409 `CONTRACT_CLOSED`다.

branch/WAP는 모든 Job의 기본 경로가 아니다. Critical Merge/Full에 한해 “attempt branch → DQ → `fast_forward`” 경로를 PoC 기준서에서 비교한다. 제약: `cherrypick_snapshot` / `publish_changes`는 append·dynamic overwrite snapshot만 publish할 수 있으므로 Full(static)·MERGE 결과는 `fast_forward`만 가능하고, `fast_forward`는 새 snapshot을 만들지 않으므로 판정은 refs도 봐야 한다. branch는 사전 생성(RETAIN 지정)이 필요하며(쓰기는 branch를 만들지 않음), `spark.wap.branch` 경로는 테이블 속성 `write.wap.enabled=true`가 전제다. 명시 branch 식별자(`branch_x`) 쓰기는 WAP 설정 없이 가능하고 두 방식은 동시 지정할 수 없다. [Iceberg procedures](https://iceberg.apache.org/docs/latest/spark-procedures/)

### 13.3 Writer lease

target lease를 세 단계로 나눈다. 하나의 장기 table lease로 모든 writer를 묶으면 대형 테이블의 compaction(수십 분)과 1시간 ingestion이 서로 밀어내 freshness 위반 또는 compaction 기아가 생기고 10,000 테이블에서 수동 조정은 불가능하다.

| lease | 대상 | 배타 |
|---|---|---|
| `EXCLUSIVE_TABLE` | Full static replace, schema/partition 변경, broad Merge(12.3), StarRocks → Iceberg write(동일 fencing·evidence 지원 확인 전까지) | 테이블 전체 |
| `PARTITION_OR_FILESET` | 일반 Merge(window가 닿는 partition), compaction / data file rewrite, manifest rewrite | 대상 partition·파일 집합. Iceberg OCC(`use-starting-sequence-number`, partial-progress)와 병행 |
| `APPEND` | Append ingestion | Iceberg OCC + commit evidence만. 같은 Job의 직렬화는 13.4 단일 window 규칙 |

- DataFrame overwrite(`overwritePartitions`/`overwrite`)는 write option `isolation-level=serializable`(기본 null은 검사 없음)과 `validate-from-snapshot-id=<base_snapshot_id>`를 Template 기본값으로 둔다. SQL `MERGE INTO`/`UPDATE`/`DELETE`는 테이블 속성 `write.merge.isolation-level` / `write.update.isolation-level` / `write.delete.isolation-level`(기본 `serializable`)을 따르며 테이블 생성 시 `serializable`로 명시 고정한다. [Iceberg configuration](https://iceberg.apache.org/docs/latest/configuration/)
- Full 테이블은 data compaction 대상에서 제외하고 expire/orphan만 수행한다
- StarRocks 경로가 동일 contract/attempt ID를 commit evidence로 남기고 동일 lease를 사용할 수 없으면 v1 신규 경로에서 제외하고 기존 플랫폼에 남긴다. **기존 Airflow/HAflow writer도 같은 규칙**이다 — 이관 기간에 같은 target에 병행 쓰기를 하려면 동일 lease를 쓰거나 순차 시간대로 격리한다
- lease 획득 순서는 전역 고정: job window 예약 → target lease → source weighted token(10.2 7~8번과 동일). bounded try-lock, 실패 시 역순 해제

### 13.4 Watermark

- Window는 반개구간 `[low, high)`. `high`는 11.3 fence에서만 파생한다. attempt 실행 중의 apply lag 변동은 이미 예약한 window와 chunk CAS에 영향을 주지 않는다 — 모든 chunk가 contract의 `visible_scn`으로 읽으므로 row 집합은 Guard에서 고정됐고, 이후 적용되는 redo는 다음 회차의 fence(와 overlap)가 다룬다. CAS를 lag로 거부하는 경로는 없으며 ledger row는 contract의 `confidence`(11.3)를 복사할 뿐이다
- window 계산 시점은 **Guard**다(fence는 Control 모니터 세션이 같은 Guard 트랜잭션의 6번 단계, 즉 window 예약 직전에 읽음): job 단위 watermark row를 `SELECT … FOR UPDATE` 후 `low = current_watermark`, `high = safe_cutoff(fence)`. `high ≤ low`이면 Spark를 제출하지 않고 contract를 `FINALIZED_NO_DATA`로 마감한다(occurrence EXECUTED; 10.2 2번에서 만든 attempt row와 binding은 같은 트랜잭션에서 롤백해 `current_attempt`는 NULL로 남기고, 그 Dagster run의 SUCCESS 반입은 ‘binding 없음’으로 무시). chunk 목록 `[low_k, min(low_k + max_chunk_span, high))`과 `expected_chunk_count = ceil((high − low) / max_chunk_span)`는 **Guard가 산출해 attempt에 기록**하고 응답으로 내려준다(10.2 7·9번). Run Pod는 그 목록을 순서대로 `chunks:begin` → `proceed`로 실행할 뿐 chunk를 새로 정의하지 않는다. 재개 attempt는 `low = 마지막 CAS 값`으로 다시 산출하므로 attempt마다 값이 다를 수 있다(ledger·snapshot summary의 `expected_chunk_count`는 해당 attempt 값). window 크기를 거부하지 않고 chunk 수만 늘어난다
- 불변식 “열린 incremental window는 job당 최대 1개”. 구현: `execution_contract.window_range numrange NOT NULL`(`STANDBY_VISIBLE_SCN`은 SCN 정수, `APPLICATION_TIMESTAMP`는 UTC epoch 마이크로초로 정규화, `window_kind` 병기), `EXCLUDE USING gist (job_id WITH =, window_range WITH &&) WHERE (state IN ('ATTEMPT_ACTIVE','ADJUDICATION_PENDING','COMMIT_OBSERVED','DQ_FAILED','RECONCILIATION_REQUIRED'))`. `PLANNED`(예약 전) 계약은 window_range가 비어 있어 제약 대상이 아니다. Full은 window 대신 `EXCLUDE (job_id WITH =) WHERE (state IN (위 집합) AND load_mode = 'FULL')`로 같은 불변식을 건다. 다음 회차 Guard가 열린 window/활성 contract를 만나면 Run을 실패시키지 않고 occurrence를 `COALESCED_INTO(open_contract_id)`, contract를 `VOID(COALESCED)`로 마감하고 Skip한다 — queue 누적을 막는다
- chunk별 snapshot 검증 후 ledger 기록 + watermark CAS(`chunks/{n}:commit`, 한 트랜잭션). CAS 성공마다 `contract.window.low`를 전진시켜 재개 지점이 contract에 남는다. 중간 chunk 실패 시 성공한 마지막 watermark부터 재개(13.2 `PARTIAL_COMMIT`)
- RETRY·복구 재진입(10.2 3번에서 새 attempt를 만든 경우)은 window와 fence를 **재계산하지 않는다**. `contract.window.low == current_watermark`만 검증하고 아니면 Control API 409 `STALE_WINDOW` + 계약 `CANCELLED(STALE_WINDOW)`(window·lease 해제). `ADJUDICATION_PENDING` 계약이 `expires_at`으로 만료되면 `CANCELLED(EXPIRED)`로 window가 풀리고 다음 회차가 `[current_watermark, fence)`를 덮으므로, `STALE_WINDOW`는 운영자가 `watermark:seed` 또는 이동 플래그 REPLAY로 watermark를 명시 조정한 경우에만 생긴다. 종결 계약(`ABORTED_NO_COMMIT`·`CANCELLED*`·`FINALIZED*`·`RESOLVED`·`VOID`)에 대한 RETRY는 상태 변경 없이 409 `CONTRACT_CLOSED`. Full은 window 검사 없음
- REPLAY/BACKFILL은 production watermark를 이동하지 않으며 `window.high ≤ current_watermark`(완전히 닫힌 구간)일 때만 허용한다. 열린 구간과 겹치면 409 `OVERLAPS_OPEN_RANGE`. **예외 — repair REPLAY**: `parent_contract_id`가 `DQ_FAILED` 또는 `RECONCILIATION_REQUIRED` 계약이면 parent가 유지 중인 window를 repair contract가 인수한다. 인수는 repair 계약의 **Guard 7번**에서 한다: 열린 contract가 이 계약의 `parent_contract_id`이고 parent가 `DQ_FAILED`/`RECONCILIATION_REQUIRED`이면 `COALESCED`가 아니라 parent row `FOR UPDATE` → parent `window_range := 'empty'` → repair `window_range := parent 값` → parent target lease 해제 → 8번에서 repair 쓰기 방식에 맞는 target lease(`PARTITION_OR_FILESET` 또는 `EXCLUSIVE_TABLE`, 13.3)와 source token 획득을 한 트랜잭션으로 수행한다. 생성 시점에는 parent를 건드리지 않으며, parent의 window는 exclusion constraint로 계속 다른 계약을 막는다. repair contract가 FINALIZED되면 같은 트랜잭션에서 parent를 `RESOLVED(REPAIR_CONTRACT)`로 닫고 watermark를 `window.high`로 전진시킨다. 그 외 이동이 필요하면 명시 플래그와 승인. INITIAL_LOAD는 예외(12.2)
- coalesce의 주체와 대상은 모두 **window를 가진 계약**이다. `PLANNED` 계약은 window_range가 비어 있어 다른 계약을 흡수하지도, 흡수되지도 않는다 — 같은 job의 `PLANNED` 계약(예: Hold 해제 후 `LEASE_BUSY` 대기 중인 CATCHUP과 다음 정규 NORMAL)은 공존하며, 각자의 Run이 Guard 7번에 도달해 watermark row `FOR UPDATE`를 먼저 잡고 예약한 쪽이 실행하고 나머지는 `operation_class`와 무관하게 `OPEN_WINDOW` → `VOID(COALESCED)` / `COALESCED_INTO`다. 어느 쪽이 이기든 window는 `[current_watermark, safe_cutoff(fence))`로 같으므로 데이터 의미는 동일하고, 이긴 쪽의 occurrence가 실행을 설명한다. `PLANNED` 공존의 상한은 `expires_at`(9.3)이 준다

- Catch-up이 논리적으로 한 건이어도 물리적으로 여러 chunk일 수 있음

## 14. Hold, Backfill, 수동 운영

### 14.1 Maintenance Hold

Scope:

- Global
- Source System
- Domain/Code Location
- Job 목록

Mode:

- `HOLD_NEW`: 신규 Spark submit(새 attempt 포함) 차단. 진행 중 attempt는 chunk를 포함해 끝까지 수행
- `DRAIN`: 신규 실행 차단 후 active work를 **안전 지점(chunk 경계 = watermark CAS 직후)**까지만 진행하고 중단 — 기본값. DRAIN 프로토콜: (1) DRAIN 중에도 진행 중 chunk의 `chunks/{n}:commit`(snapshot 검증·ledger·CAS)은 허용한다 — 이것이 안전 지점을 만든다. (2) 다음 `chunks:begin`이 `DRAIN`을 반환하면 계약 상태는 바뀌지 않고 Run Pod가 driver에 `stop`을 보낸다. (3) Run Pod는 SA terminal을 watch로 확인한 뒤 `finalize {outcome: CANCELLED_AT_SAFEPOINT, sa_status, hold_id}`를 호출하고, Control은 `window.low == ledger 마지막 CAS 값`을 검증한 뒤 `CANCELLED_AT_SAFEPOINT` 전이 + window·target lease·source token 해제를 한 트랜잭션으로 수행한다. (4) Run은 materialization 없이 SUCCESS로 종료한다(tag `drained`). (5) (3)이 `drain_timeout_seconds`(Hold 생성 시 지정, 기본 예상 chunk 시간 × 2) 안에 오지 않으면 Control이 운영자 확인 없이 FORCE_STOP 프로토콜로 승격하고 알림한다
- `FORCE_STOP`: 명시적 승인 후 active Run 중지. 프로토콜: SparkApplication delete → driver/executor pod 부재 확인(`WRITER_FENCED`) → Commit Adjudication → 부분 commit이 있으면 반영해 CAS 후 `CANCELLED(FORCE_STOP)`, 없으면 `ABORTED_NO_COMMIT`. 판정을 건너뛰고 바로 `CANCELLED`로 가는 경로는 없다. reattach 만료(10.2 표)와 lease 회수(11.2), DQ_FAILED의 finalize 지연은 이 프로토콜의 **fencing 단계만** 쓰고 종결 상태는 각자의 규칙(`RUN_WORKER_LOST` 자동 재시도 / `LEASE_EXPIRED` 운영자 대기 / `DQ_FAILED` 유지)을 따른다. CredentialRevision REVOKED로 기동된 경우 reason은 `CREDENTIAL_REVOKED`, ConnectionRevision 강제 REVOKED(`force: true`, 6.2)로 기동된 경우 reason은 `CONNECTION_REVOKED`로 기록한다

Hold는 **네 지점**에서 검사한다: schedule 평가, Control API, Spark submit 직전 Guard, 그리고 **chunk 제출마다**(`chunks:begin`, `DRAIN`만 중단). 해제는 명시적 사용자 동작이며 자동 timer 해제를 기본으로 하지 않는다. Control이 자동으로 만드는 Hold는 네 종류이며 모두 mode `HOLD_NEW`, `reason`과 원인 식별자를 가지고 해제 조건이 각각 고정된다 — `SOURCE_ROLE_MISMATCH`(Source scope, DBA 확인 후 수동 release, 11.3), `SCHEMA_DRIFT`(Job scope, schema 승인 시 해제, 10.2 표), `CREDENTIAL_FAILURE`(Source scope, `credential_revision_id` 기록 — 6.2 credential breaker; 새 CredentialRevision의 `ACTIVE` 전이 트랜잭션에서만 해제되며 수동 release는 breaker가 열려 있는 한 `412 CREDENTIAL_REVOKED`), `PLATFORM_BREAKER(key)`(Polaris catalog면 그 catalog를 target으로 가진 Job 목록 scope, AIStor면 Global — `platform_breaker_failures` 연속 `TARGET_UNAVAILABLE`, 5.4; 운영자가 회복을 확인한 뒤 수동 release). 네 경우 모두 해제 시 catch-up은 14.2 규칙을 따른다. 운영 주석: `CREDENTIAL_FAILURE` Hold는 일시 장애로 같은 비밀번호가 멀쩡한 경우에도 **같은 값의 새 CredentialRevision**을 등록해 VERIFIED 로그인 테스트를 통과시키는 것이 유일한 해제 수단이다.

Hold 중 schedule tick은 Source를 읽지 않으며 occurrence를 `SKIPPED_BY_HOLD(hold_id)`(contract `VOID(SKIPPED_BY_HOLD)`)로 생성한다. Guard에서 HOLD로 거부된 기존 `PLANNED` 계약도 같은 트랜잭션에서 마감해 stale 루프 대상에서 제외한다(10장 실패 경로 1의 savepoint 규칙). Schedule Gap Recovery는 이를 설명된 누락으로 인식한다.

DRAIN 완료 판정: 해당 scope의 target/source lease가 모두 해제된 시점 — source token은 SA 종료 확인(`finalize`) 이후에 반환되므로 그 이후다.

### 14.2 Hold 해제

- Full: 놓친 회차를 모두 버리고 최신 1회
- Incremental: 마지막 production watermark부터 Hold 종료 시점의 safe cutoff(11.3 fence)까지 논리적 1회 catch-up
- catch-up은 `CATCHUP` occurrence(`logical_scheduled_at = hold_release_at`, 현재 ACTIVE release 고정)로 생성한다. scope 내 Job 중 Hold 구간에 `SKIPPED_BY_HOLD`·`CANCELLED_AT_SAFEPOINT`·`CANCELLED(HOLD)`·`CANCELLED(FORCE_STOP)`·FORCE_STOP 프로토콜에 의한 `ABORTED_NO_COMMIT` 기록이 1건 이상인 Job에 대해서만 생성하고, 기록이 없는 Job은 다음 정규 tick에 맡긴다. 그 뒤 도착하는 정규 NORMAL tick은 13.4 규칙으로 자연스럽게 `COALESCED_INTO`된다
- CATCHUP contract의 제출 주체는 **Hold 해제 핸들러**다. 해제 트랜잭션 commit 직후 9.3 (3)과 같은 Adapter launch(`launchRun`, tag `contract_id`·`resubmit_no=0`·`dagster/priority`)로 Job별 Run을 제출하고, 제출 실패분은 `next_eligible_at = now()`로 두어 stale 루프가 회수한다(CATCHUP은 `planned_stale_after` 면제)
- 대용량 Incremental: Source 정책에 따라 내부적으로 순차 chunk + cooldown. chunk마다 `chunks:begin` 재검사
- DR lag 신호가 없으면 보수적 safety lag와 `BEST_EFFORT` 등급 규칙 적용
- 해제 API는 예상 catch-up 소요(`N_hold × D / C`)를 202 응답과 함께 반환하고, 그 값이 scope 내 최소 period를 넘으면 응답에 `expires_before_completion: true`를 표시한다 — 일부 CATCHUP이 `expires_at`(= `hold_release_at + period`, 9.3)에 `EXPIRED_UNLAUNCHED`로 마감되고 후속 NORMAL이 `[current_watermark, fence)`를 덮는다는 뜻이다. CATCHUP은 `expires_at`에서 면제되지 않는다
- catch-up 중 도착하는 NORMAL tick의 처리는 CATCHUP 계약 상태에 따라 갈린다: CATCHUP이 `ATTEMPT_ACTIVE`·`COMMIT_OBSERVED`·`ADJUDICATION_PENDING`(열린 window)이면 NORMAL은 Guard 7번에서 `COALESCED_INTO(catchup_contract_id)`; CATCHUP이 아직 `PLANNED`(`LEASE_BUSY`·`SOURCE_LAG_EXCEEDED` backoff 등)이면 NORMAL도 `PLANNED`로 공존하고 먼저 window를 예약한 쪽이 실행한다(13.4). `PLANNED`끼리는 coalesce하지 않는다
- Hold 중 schedule tick은 Source를 읽지 않으며, 해제 때 500개가 한꺼번에 Source gate를 뚫지 못한다. 정각 논리 시각은 유지되고 실제 실행만 admission과 우선순위 queue에서 대기한다

### 14.3 수동 실행 의미

| Mode | 의미 | 버전/Window | Watermark | 제약 |
|---|---|---|---|---|
| `NORMAL` | 예정 실행과 동일. Control이 logical_at을 계산해 schedule occurrence와 같은 키 | 논리 시각에 유효한 release, 계산된 window | 성공 시 이동 | 열린 window 있으면 COALESCED. 같은 키의 계약이 이미 있으면 그것을 반환하고 Run은 `PLANNED` ∧ non-terminal run 없음일 때만 추가 제출(`SUBMITTED` 202 / `ALREADY_SUBMITTED`·`NOT_LAUNCHABLE` 200 — 9.2) |
| `RETRY` | 동일 계약 재시도, 새 attempt(`POST /v1/contracts/{id}/retry`) | 기존 digest/window/fence 고정 | 기존 계약 규칙 | `ADJUDICATION_PENDING` ∧ verdict ∈ {NO_COMMIT, PARTIAL_COMMIT} 계약만. `window.low == current_watermark`(Full 제외), pinned release ∉ {ROLLED_BACK, FAILED}(아니면 409 `PINNED_RELEASE_INACTIVE` + REPLAY 생성 링크, 자동 변환 없음), Hold 없음(아니면 423), `FENCE_EXPIRED` 아님(아니면 409 `FENCE_EXPIRED`). `DQ_FAILED`·`FINALIZED*`·`RECONCILIATION_REQUIRED`·종결 계약은 409 `CONTRACT_CLOSED`; `ATTEMPT_ACTIVE`·`COMMIT_OBSERVED`·verdict 미확정 `ADJUDICATION_PENDING`은 409 `ATTEMPT_IN_PROGRESS`; `PLANNED`는 409 `CONTRACT_NOT_STARTED`; `retry_authorization`이 이미 발급돼 미소비면 200 `ALREADY_SUBMITTED`(아래 절차) |
| `REPLAY` | 같은 범위를 의도적으로 재처리 | 명시 범위, parent 계약 기록, 현재 release | 기본 미이동(repair REPLAY는 parent 닫을 때 전진) | `window.high ≤ current_watermark` 또는 repair REPLAY(parent가 `DQ_FAILED`/`RECONCILIATION_REQUIRED`, 13.4), `client_request_id` 필수 |
| `BACKFILL` | 과거 범위 복구 계획 | plan/item별 고정, 현재 release | 기본 미이동 | 14.4 |
| `RERUN_LATEST` | Full 등의 최신 상태 재적재 | 새 계약, 현재 release | 모드별 | `client_request_id` 필수. `replace_with_empty: true`는 빈 원천을 의도적으로 반영할 때만(12.1) |

RETRY 절차: `POST /v1/contracts/{id}/retry`는 한 트랜잭션에서 (a) 위 제약을 검증하고, (b) `contract.retry_authorization = {actor, reason, issued_at, consumed: false}`와 `next_eligible_at = now()`를 기록한 뒤, (c) commit 후 Adapter `launchRun`(tag `contract_id`·`retry_no`·`dagster/priority`)으로 Run을 제출하고 `202 + operation_id`를 반환한다. attempt는 만들지 않는다 — 새 attempt는 그 Run의 Guard(10.2 3번)가 `retry_authorization`을 소비하며 만든다.

RETRY 거부와 멱등성(계약 상태 변경 없음, 감사 이벤트만): (1) `ATTEMPT_ACTIVE`·`COMMIT_OBSERVED`, 그리고 `ADJUDICATION_PENDING`이지만 verdict가 아직 없는 계약(`WRITER_FENCED` 전 — `reattach_deadline` 대기 중이거나 Adjudication 진행 중)은 `409 ATTEMPT_IN_PROGRESS` `{contract_state, current_attempt, bound_dagster_run_id, reattach_deadline?, required_action}` — required_action은 `ATTEMPT_ACTIVE`/`COMMIT_OBSERVED`면 “완료 대기 또는 DRAIN/FORCE_STOP Hold(14.1)”, verdict 미확정이면 “판정 대기(`RUN_WORKER_LOST` 재결합 창 — `reattach_deadline` 전 — 밖에서만 `POST /v1/contracts/{id}/adjudicate`로 즉시 트리거 가능, 13.2 1번)”. `CONTRACT_CLOSED`와 구분하는 이유는 안내가 반대이기 때문이다 — CLOSED는 새 계약(REPLAY·RERUN_LATEST)을, IN_PROGRESS는 기다림을 요구한다. (2) `PLANNED`는 attempt가 없어 재시도할 것이 없으므로 `409 CONTRACT_NOT_STARTED` `{required_action: RESUBMIT}` — 재제출은 `POST /v1/jobs/{id}/runs {mode: NORMAL}`(9.2, 같은 키로 create-or-get 후 재제출) 또는 stale 루프(9.3)가 한다. (3) `ADJUDICATION_PENDING` ∧ verdict ∈ {NO_COMMIT, PARTIAL_COMMIT}인데 `retry_authorization.consumed = false`인 계약(RETRY가 이미 발급됨)에 대한 두 번째 RETRY는 새 authorization을 만들지 않는다: Adapter가 tag `contract_id`인 non-terminal run을 확인해 있으면 `200 {launch_result: ALREADY_SUBMITTED, dagster_run_id, operation_id}`(기존 operation), 없으면(launch 실패·run terminal 후 Guard 미도달) 같은 authorization(`retry_no` 유지 — `retry_no`는 authorization 발급 횟수)으로 9.3 재제출 단일 경로를 타며 `resubmit_no`만 증가해 다시 `launchRun`하고 202를 반환한다. 경합으로 Run이 둘 생겨도 Guard 3.4의 `consumed = true` CAS가 attempt를 하나로 막는다. (4) `FENCE_EXPIRED`로 `next_eligible_at = NULL`인 계약은 `409 FENCE_EXPIRED`(운영자 `abort`만). `ATTEMPT_IN_PROGRESS`·`CONTRACT_NOT_STARTED`는 v1.2 신규 코드다.

운영자 종결: `POST /v1/contracts/{id}/abort {reason}` — `ADJUDICATION_PENDING` ∧ verdict NO_COMMIT → `ABORTED_NO_COMMIT`, PARTIAL_COMMIT → `CANCELLED(OPERATOR)`. 둘 다 window·lease를 해제하고 다음 회차가 `[current_watermark, fence)`를 덮는다. `POST /v1/contracts/{id}/resolve {resolution, reason}` — `RECONCILIATION_REQUIRED`/`DQ_FAILED` → `RESOLVED`(사유·승인자 필수, `watermark:seed` 동반 가능).

Custom UI와 Dagster UI 양쪽에서 제공하되, 실제 실행 규칙은 공통 Control API와 Guard를 통과한다. Dagster UI에서 contract 없이 시작된 실행은 Run Pod가 `occurrences:batch-create-or-get`(origin=DAGSTER_UI)으로 NORMAL 계약을 create-or-get하며, Critical Job은 `DIRECT_LAUNCH_FORBIDDEN`으로 거부한다(22장 10번은 이 결정으로 확정). 종결된 계약에 대한 Dagster UI 재실행은 `CONTRACT_CLOSED`로 무시된다.

### 14.4 Backfill

- 단일 Job과 다중 Job 모두 지원
- `BackfillPlan`이 Job × time window를 `BackfillItem`으로 분해
- SourceSafetyEnvelope와 target lease를 공유
- 실행 전 예상 Source query 수, JDBC connection weight, Spark resource, 대상 partition을 Preview
- 승인/일시정지/재개/취소 지원
- Full table의 과거 시점 backfill은 Source Flashback/Archive가 없으면 지원 불가로 표시

- 신규 Job 초기 적재는 BackfillPlan이 아니라 `INITIAL_LOAD` contract(12.2)로 수행한다. BackfillPlan은 watermark가 이미 있는 Job의 닫힌 과거 구간에만 쓴다

## 15. LLM 기반 Job Advisor

### 15.1 목표

Job Wizard를 자동 입력하되 운영자가 검토·수정·publish한다.

추천 대상:

- Full / Append / Merge
- Watermark 후보와 `INSERT_DT + UPDATE_DT` 전략
- PK/unique key 후보
- `dt/wt/mt/yt`
- Template
- Spark/read profile
- Source protection 범위 안의 JobReadProfile
- 단순 컬럼 표준화와 mapping
- 확인이 필요한 위험 항목

### 15.2 입력 데이터

낮은 부하의 metadata만 기본 사용한다.

- Oracle dictionary의 row estimate, size, last analyzed, columns, index/PK
- DataHub description/ownership/domain
- 기존 유사 Job의 실제 처리량, 수행시간, 실패·Source 부하 결과
- 운영자가 입력한 table 성격과 delete/update 의미

큰 테이블에 `COUNT(*)`를 실행하거나 통계 수집을 자동 수행하지 않는다. 실제 row sample은 기본적으로 LLM에 보내지 않는다.

### 15.3 안전 구조

```text
Deterministic safety rules
→ LLM structured recommendation
→ 정책 validator
→ 운영자 확인/수정
→ publish
```

- JSON Schema output 강제
- confidence와 근거 metadata ID 제공
- model/prompt/rule/metadata 버전 저장
- 추천값과 운영자 수정값을 모두 audit
- column/table comment는 prompt injection 가능 입력으로 분리·표시
- LLM 장애 시 수동 Wizard는 정상 동작
- LLM은 SQL을 실행하거나 Job을 자동 publish하지 않음
- “Master/Event”, hard delete, `UPDATE_DT` 신뢰성은 컬럼명만으로 확정하지 않음

### 15.4 모델 도입 순서

초기부터 여러 모델 router를 만들지 않는다.

1. Rule engine + 사내 허용 모델 1개로 Shadow 평가
2. 안전하지 않은 추천률과 운영자 수정률 측정
3. 통과 후 UI auto-prefill
4. 이후 모호·고위험·Critical Source만 `gpt-5.6-sol` 같은 상위 reviewer로 escalation

기존 10,000 Job은 그대로 정답으로 학습하지 않는다. 장애, 데이터 품질, Source 부하 결과가 양호한 Job만 Golden Set으로 선별한다.

Advisor는 실행 경로의 필수 구성요소가 아니며 MVP 안정화 후 도입한다.

## 16. Observability, Lineage, Alert

### 16.1 화면 역할

- Custom UI: Source, Job Wizard/CRUD, Template/Release, Hold, Backfill plan, Advisor, **contract 상세**(상태 이력·Guard 결과·attempt timeline — 6.1의 `contract_state_history`·`attempt_state_history`·`lease_state_history`·`guard_result`·`attempt_timeline`. `ADJUDICATION_PENDING`·`RECONCILIATION_REQUIRED`·`DQ_FAILED`에서 운영자가 RETRY/abort/resolve/dq:accept를 고르는 근거 화면이며 Dagster UI의 run 이력을 복제하지 않는다)
- Dagster UI: Asset/Run/Retry/Step log, execution history
- DataHub: table/column lineage, ownership, discovery
- Grafana: 플랫폼/Source/Spark SLO, Job class별 lateness 분해(`attempt_timeline` 집계)와 Guard 거부 사유별 건수·보호 지연(`guard_result` 집계)
- OpenSearch: 상세 로그 검색
- Spark History Server: executor/stage 분석

### 16.2 로그

- Run Pod와 Spark Driver/Executor log를 AIStor에 영속화
- Spark event log를 AIStor에 저장
- Dagster 화면에서 contract ID, SparkApplication, Spark History, OpenSearch query로 연결
- Prometheus label에 `run_id` 같은 고 cardinality 값은 넣지 않음
- contract·attempt 단위 감사·지표의 원천은 Control DB의 이력·계측 테이블(6.1)이다. Prometheus/Grafana로는 Job class·Source·Guard 사유 단위로 집계한 값만 내보내고(위 label 규칙 유지), contract 단위 조회는 Custom UI가 Control DB(read replica)를 읽는다. PoC 기준서 §5.1 판정 쿼리는 운영에서도 같은 테이블에 대해 실행한다

### 16.3 Lineage

- DataHub Iceberg source: table schema/ownership
- Spark lineage agent: 실제 table/column lineage
- Dagster integration/sensor: orchestration Job/Run 관계
- 동일 Spark 실행에서 OpenLineage event를 두 번 보내지 않도록 producer를 하나로 지정

### 16.4 Kafka 알림

Control DB transaction과 함께 `NotificationOutbox`에 기록하고 별도 publisher가 Kafka로 전송한다.

Event 예:

- Job failed/recovered
- freshness breach (Job별 `freshness_slo` 초과 — 아래 lateness sensor가 유일한 계산 주체)
- Source circuit breaker — `source credential failure`(6.2 credential breaker: `credential_breaker_failures` 도달로 Source `HOLD_NEW`를 만드는 트랜잭션에서 1건, `event_id = hold_id`; 개별 attempt의 `CREDENTIAL_FAILURE`는 이 Source 이벤트로 집계하고 Job 단위 failed 이벤트를 내지 않는다. 해제는 `Hold released` 이벤트), `source lag exceeded`, `source role mismatch` / platform breaker — `target unavailable`(Guard `TARGET_UNAVAILABLE` 거부를 breaker key(Polaris catalog·AIStor) 단위로 집계, `platform_breaker_failures` 도달로 자동 `HOLD_NEW`가 생기면 Hold created 이벤트 동반, 5.4)
- Hold created/released/drained
- 만료: `EXPIRED_UNLAUNCHED`·`CANCELLED(EXPIRED)`(9.3 — `operation_class = CATCHUP`은 `hold_id` 단위 집계, 아래 집계 단위 규칙)
- Commit Adjudication 결과(`RECONCILIATION_REQUIRED`), 실행 중 chunk 검증의 외부 snapshot 발견(`RECONCILIATION_REQUIRED(EXTERNAL_SNAPSHOT)`, 13.1 — 즉시, lease 우회 writer 의심이므로 운영자 알림), DQ 판정(`DQ_FAILED`), `ADJUDICATION_PENDING` 장기화(`adjudication_pending_alert_after` 초과), `FINALIZE_MISSING`(SUCCESS 반입인데 finalize 미호출, 10.2)
- Definition release failed/rolled back

At-least-once 전송을 전제로 `event_id` dedup, outage 중 집계, 동일 Source 폭주 억제와 rate limit을 적용한다.

집계 단위 추가(v1.2): `EXPIRED_UNLAUNCHED` 만료 알림은 `operation_class = CATCHUP`이면 Job 단위가 아니라 `hold_id` 단위 1건이다(9.3) — Hold 해제 직후 포화 Source에서 수백 건이 한꺼번에 만료되는 경우를 위한 규칙이다.

v1.1 추가 이벤트: `source role mismatch`(11.3), `source credential failure`(ORA-01017/28000/28001/28002 — Source-level circuit breaker + 자동 `HOLD_NEW`), `schema drift detected`, `source lag exceeded`(Source 단위 집계), `contract stalled`(Guard 이후 heartbeat/finalize가 SLA 안에 없음 — Control 트랜잭션 내부 생성), `expected occurrence missing`(기대 시각에 occurrence조차 없음 — 아래 lateness sensor). Dagster만 아는 실패(Run Pod 사망·launch 실패)는 10.2의 `run_status_sensor` 반입 경로로 Outbox에 들어온다. Source 장애로 다수 Job이 동시에 실패하면 Job 단위 이벤트를 억제하고 Source 단위 1건으로 집계한다.

v1.2 추가 이벤트: `degraded confidence`(11.3 — `BEST_EFFORT` 계약이 lag 신호 불확실 상태로 Guard 6번을 통과해 `confidence = DEGRADED`로 기록됨; Guard 트랜잭션의 Outbox에 쓰고 Source 단위 1건으로 집계, `confidence_reason` 포함. `ZERO_GAP`은 같은 조건에서 `source lag exceeded`로 집계되므로 이 이벤트를 내지 않는다), `target unavailable`(5.4 platform breaker, breaker key 단위 집계), `schedule gap recovery completed`(`operation_id`, actor `SYSTEM` | 운영자, scope·range, expected tick 수, disposition별 생성 건수, launch 건수, `dry_run` 여부 — 자동·수동 공통, operation 1건당 1건, 9.3). 기존 `Definition release failed/rolled back`의 rolled back은 rollback release의 ACTIVE 트랜잭션 Outbox insert로 생성된다(17장, 이름은 v1.1 그대로).

**lateness sensor**(v1.2 확정): `freshness breach`와 `expected occurrence missing`의 계산 주체는 Control의 lateness sensor 하나다 — PLANNED stale 루프(9.3)와 같은 스케줄러에서 `planned_scan_interval`(기본 60초) 주기로 도는 읽기 전용 검사이며 occurrence·contract를 만들거나 바꾸지 않는다(복원은 Schedule Gap Recovery, 트리거는 9.3 그대로). Dagster freshness policy는 판정·알림에 쓰지 않는다(22장 20번).

- 모집단: `operation_class ∈ {NORMAL, CATCHUP}` occurrence(cron 기반, `expires_at` 보유). INITIAL_LOAD·REPLAY·BACKFILL·RERUN_LATEST는 제외.
- lateness(o) = `t7 − o.logical_scheduled_at`. t7 = o가 가리키는 계약(`EXECUTED`면 자기 contract, `COALESCED_INTO(c)`면 c)이 `FINALIZED`·`FINALIZED_NO_DATA`·`RESOLVED`에 진입한 시각(`contract.finalized_at`; `RESOLVED`는 repair REPLAY의 FINALIZED 시각). `SKIPPED_BY_HOLD`·`SUPERSEDED_BY(*)`는 Hold·release 이벤트가 설명하므로 제외한다. `EXPIRED_UNLAUNCHED`·`REJECTED_AT_GUARD`·`CANCELLED*`·`ABORTED_NO_COMMIT`·그 외 `VOID`는 t7이 영원히 없으므로 아래 (a)로만 잡힌다.
- breach 조건(매 scan): (a) `t7 IS NULL AND now() − logical_scheduled_at > freshness_slo` 또는 (b) `t7 − logical_scheduled_at > freshness_slo`. 임계값은 JobSpec `freshness_slo`(7.3), 없으면 Job class(주기)별 기본값(22장 22번). 이벤트는 `(job_id, logical_scheduled_at)`당 1건 — `event_id = hash(job_id, logical_scheduled_at, 'FRESHNESS_BREACH')`를 Outbox unique key로 dedup하며 상태가 바뀌어도 재발행하지 않는다. breach 뒤 t7이 생기면 기존 `Job recovered` 이벤트를 같은 키로 1건 낸다. 탐지 지연 ≤ `planned_scan_interval` + Outbox 전송.
- lateness 분해(이벤트 payload와 Control read model): 대기 = `first_guard_ok_at − occurrence.created_at`(Guard 거부 backoff 포함; 그중 보호 지연은 6.1 `guard_result` 정의와 동일 — `LEASE_BUSY`·`SOURCE_LAG_EXCEEDED`·`FENCE_UNAVAILABLE` 거부의 `(at − run_started_at) + (next_eligible_at − at)` 합), 적재 = `last_cas_at − first_guard_ok_at`, finalize = `finalized_at − last_cas_at`. attempt 단위 분해(플랫폼 지연·제출 지연·실행 `t6..t7`)는 6.1 `attempt_timeline`이다. 세 timestamp는 contract 컬럼(6.1)이며 각 전이 트랜잭션이 기록한다. Prometheus에는 Job label 없이 source/shard/주기 band 집계만 올리고 Job별 값은 Control read model에서 읽는다(16.2).
- `expected occurrence missing`: Job별 커서 `last_expected_checked_at`를 두고, T에 유효한 release(9.2 NORMAL 규칙)의 manifest에 기록된 `(cron, timezone)`을 `(last_expected_checked_at, now() − planned_stale_after]` 구간에 전개한다(grace = `planned_stale_after` — tick fail-closed 재시도 `max_tick_retries`와 queue 대기를 덮는다). 전개된 tick T에 `(job_id, NORMAL, T)` occurrence row가 disposition과 무관하게 없으면(Hold 중 tick도 `SKIPPED_BY_HOLD` row를 남기므로 Hold는 면제 사유가 아니다) `(job_id, T)`당 1건 — `event_id = hash(job_id, T, 'OCCURRENCE_MISSING')`. 같은 schedule 이름(`sched__{shard}__{cron_slug}__{tz}`, 9.1)의 Job 전부가 같은 T를 놓치면 Job 단위 이벤트를 억제하고 `(schedule, T)` 1건에 Job 수를 싣는다(schedule STOPPED·daemon 정지 신호). pin할 release가 없는 T(첫 release 이전)는 기대 집합에서 뺀다. 커서는 최대 1 period만 뒤로 본다 — 그보다 긴 구간은 Schedule Gap Recovery(9.3)가 disposition을 만들어 설명한다. 이벤트는 알림일 뿐이며 복원은 운영자가 Gap Recovery를 기동하거나 schedule을 RUNNING으로 복원해 처리한다.

## 17. Control API

외부 API는 Dagster GraphQL을 직접 노출하지 않는다. Control API의 stable endpoint 뒤에 version-pinned Dagster Adapter를 둔다. Dagster는 GraphQL API가 evolving이며 breaking change 가능하다고 명시한다. [Dagster GraphQL API](https://docs.dagster.io/api/graphql)

대표 API:

```text
POST   /v1/sources
POST   /v1/sources/{id}/connection-revisions
POST   /v1/sources/{id}/verify
POST   /v1/sources/{id}/connection-revisions/{rid}/revoke   # {reason, force: false} — 살아 있는 attempt가 참조하면 412 OPEN_CONTRACTS_RUNNING + {contract_id, attempt_no, state, required_action}; force=true면 REVOKED + attempt별 FORCE_STOP → CANCELLED(CONNECTION_REVOKED) (6.2)
GET    /v1/sources/{id}/schemas/{schema}/tables

POST   /v1/jobs/drafts
POST   /v1/jobs/{id}/advisor-analyses
POST   /v1/jobs/{id}/validate                    # 200 {valid: true, warnings[]} | 422 VALIDATION_FAILED {violations[], warnings[]} (아래 형식)
POST   /v1/jobs/{id}/publish                     # 같은 validator 선행 → 거부 시 422 동일 본문(JobSpecVersion·release 미생성), 통과 시 202 + operation_id (+ warnings[])
GET    /v1/jobs/{id}/releases
POST   /v1/releases/{id}/rollback                # {reason} → 202 {operation_id, release_id} | 409 RELEASE_NOT_ACTIVE | 412 OPEN_CONTRACTS_RUNNING (8.2, 아래 절차)

POST   /v1/jobs/{id}/runs                        # {mode, client_request_id?, …}; NORMAL은 create-or-get 후 {launch_result} (9.2)
POST   /v1/contracts/{id}/retry
POST   /v1/backfill-plans
POST   /v1/backfill-plans/{id}/start

POST   /v1/holds
POST   /v1/holds/{id}/release
GET    /v1/operations/{operation_id}
```

Mutation은 `Idempotency-Key`를 받고 오래 걸리는 작업은 `202 Accepted + operation_id`를 반환한다. Adapter에는 Dagster 고정 버전별 contract test와 업그레이드 사전 검증이 필요하다.

`POST /v1/jobs/{id}/runs {mode: NORMAL}` 응답은 `{occurrence_id, contract_id, contract_state, disposition, launch_result, dagster_run_id?, resubmit_no?, operation_id?}`다. `launch_result`: `SUBMITTED`(이번 호출이 Run을 제출 — 202 + `operation_id`) | `ALREADY_SUBMITTED`(`PLANNED`이고 tag `contract_id`인 non-terminal run이 이미 있음 — 200, 그 `dagster_run_id`) | `NOT_LAUNCHABLE`(계약이 `PLANNED`가 아님 — 200, `contract_state`로 설명). 어느 경우도 오류가 아니며 계약 상태를 바꾸지 않는다(9.2). 같은 어휘를 14.3의 RETRY 재요청(`ALREADY_SUBMITTED`)이 쓴다. `launch_result`는 v1.2 신규 필드다.

`POST /v1/releases/{id}/rollback {reason}` 절차: (1) `{id}`가 현재 ACTIVE release가 아니면(`ROLLED_BACK`·`SUPERSEDED`·`FAILED`·미승격) `409 RELEASE_NOT_ACTIVE`. (2) 한 Control 트랜잭션에서 직전 ACTIVE bundle의 manifest와 diff해 Job별 `interface_changed`를 계산하고 OPEN_CONTRACT_CHECK **사전검사**(row lock 없이 읽기): `interface_changed = true`인 Job에 비종결 계약(6.2 (b) 집합)이 있으면 `412 OPEN_CONTRACTS_RUNNING` + `{contract_id, state, required_action}` 목록(6.2와 같은 본문)으로 끝내고 아무것도 만들지 않는다. 통과하면 새 release row(`rollback_of = id`, `bundle_digest` = 직전 ACTIVE 값, 상태 VALIDATED)를 만들고 `202 {operation_id, release_id}`를 반환한다. (3) 비동기 operation이 DEPLOYED → VERIFIED → ACTIVE를 수행한다(6.2 표의 같은 주체·판정). ACTIVE 트랜잭션은 OPEN_CONTRACT_CHECK를 `FOR UPDATE`로 재수행하고 통과 시 새 release ACTIVE(`effective_from = now()`), 문제 release `ROLLED_BACK`, 문제 release를 pin한 모든 `PLANNED` 계약 `VOID(SUPERSEDED_BY_RELEASE)` + occurrence `SUPERSEDED_BY(RELEASE)`, Outbox `Definition release rolled back`을 한 번에 commit한다. (4) 실패: VERIFIED 실패는 6.2 규칙(직전 ACTIVE bundle — 이 경우 문제 release의 bundle — 로 포인터 복귀 + 알림, operation FAILED); ACTIVE 재검사 실패(사전검사 이후 Guard와의 경합)는 operation `FAILED`에 같은 412 본문을 싣고 새 release는 VERIFIED, 문제 release는 ACTIVE로 남는다 — 코드 포인터는 이미 직전 bundle이지만 계약은 pinned plan으로 실행되므로 정상 release의 DEPLOYED~ACTIVE 구간과 같은 상태다. (5) 멱등성: `Idempotency-Key`는 `IdempotencyRecord`로 같은 응답을 재생하고, 키와 무관하게 같은 `{id}`에 대해 VALIDATED~VERIFIED인 rollback release가 이미 있으면(`rollback_of` unique, 종결 = ACTIVE·FAILED) 새 release를 만들지 않고 그 release의 다음 단계부터 재개하는 operation을 `202`로 반환한다. `GET /v1/operations/{operation_id}`가 단계와 412 본문을 노출한다.

v1.1 추가 엔드포인트:

```text
POST   /v1/occurrences:batch-create-or-get       # schedule tick용 한 트랜잭션, 응답 {launch, priority, …} (9.1); Dagster UI 직접 실행은 단일 항목 origin=DAGSTER_UI (10.2)
POST   /v1/contracts/{id}/guard                  # Guard 단일 트랜잭션 (10.2)
POST   /v1/contracts/{id}/chunks:begin           # 모든 chunk 직전 Hold·lease·binding 재확인, k=1은 base_snapshot_id·payload digest 기록 (Run Pod)
POST   /v1/contracts/{id}/chunks/{n}:commit      # snapshot 검증·DQ 결과 + 본문 lineage(base(k) 이후 ancestor 목록) + ledger 기록 + watermark CAS (한 트랜잭션); lease 기록 없는 개입 snapshot이면 응답 RECONCILIATION_REQUIRED, ledger row만 기록·CAS 없음 (13.1)
POST   /v1/contracts/{id}/finalize               # {outcome: FINALIZED | FINALIZED_NO_DATA | CANCELLED_AT_SAFEPOINT | DQ_FAILED | RECONCILIATION_REQUIRED, sa_status} — SA 종료 확인 후, source token 반환 (DQ_FAILED·RECONCILIATION_REQUIRED는 token만 반환, window·target lease 유지)
POST   /v1/contracts/{id}/attempt-failure        # {reason: SPARK_FAILED | SPARK_TERMINATED_WITHOUT_RECEIPT | COMMIT_STATE_UNKNOWN | CHUNK_DQ_FAILED | EMPTY_FULL | SOURCE_ROLE_MISMATCH | TARGET_UNAVAILABLE(Guard 뒤 base_snapshot_id 조회 실패 — SA 미생성, 5.4) | CREDENTIAL_FAILURE(+ ora_code, credential_revision_id — 6.2 credential breaker를 같은 트랜잭션에서 적용) | …} (Run Pod)
POST   /v1/contracts/{id}/dagster-terminal-event # run_status_sensor 반입 (Idempotency-Key = run_id+status)
POST   /v1/contracts/{id}/adjudicate             # Commit Adjudication 수동 트리거
POST   /v1/contracts/{id}/retry                  # retry_authorization 발급 + Adapter launch (14.3); 진행 중 계약 409 ATTEMPT_IN_PROGRESS, PLANNED 409 CONTRACT_NOT_STARTED, 미소비 authorization 재요청은 200 ALREADY_SUBMITTED
POST   /v1/contracts/{id}/abort                  # ADJUDICATION_PENDING → ABORTED_NO_COMMIT | CANCELLED(OPERATOR) (14.3)
POST   /v1/contracts/{id}/dq:accept              # DQ_FAILED → FINALIZED, 사유·승인자 필수
POST   /v1/contracts/{id}/accept-empty           # ADJUDICATION_PENDING(EMPTY_FULL) → FINALIZED_NO_DATA, 사유·승인자 필수 (12.1)
POST   /v1/contracts/{id}/cancel                 # PLANNED → VOID(OPERATOR) + occurrence CANCELLED(actor, reason), 사유 필수 (6.2)
POST   /v1/backfill-plans/{id}/pause | resume | cancel   # 14.4 — cancel은 미실행 item의 contract를 VOID(OPERATOR)로, 실행 중 item은 DRAIN Hold 안내
POST   /v1/contracts/{id}/resolve                # RECONCILIATION_REQUIRED | DQ_FAILED → RESOLVED, 사유·승인자 필수
POST   /v1/jobs/{id}/watermark:seed              # 이관 시 기존 watermark 주입 또는 resolve 동반 조정, 승인 필요
POST   /v1/schedule-gap-recoveries               # Schedule Gap Recovery 수동 기동 {scope?, range?, dry_run?} → 202 + operation_id; 자동(heartbeat gap) 경로와 같은 operation, 겹치는 진행 중 operation은 200 기존 id (9.3)
```

Run Pod의 모든 contract 호출은 `(contract_id, attempt_no, dagster_run_id)`를 싣고, binding 불일치(`fence_reason: REBOUND`)이거나 그 binding run의 terminal 사실이 이미 반입된 뒤(`attempt.terminal_ingested_at IS NOT NULL`, `fence_reason: RUN_TERMINAL`)면 `412 ATTEMPT_FENCED {fence_reason}`다(10.2 소유권 검사). 반입 트랜잭션과 chunk 호출은 contract row lock으로 직렬화된다.

`POST /v1/contracts/{id}/guard`의 거부(10.2 표)는 계약 마감·backoff 기록을 commit한 **정상 응답(200)** `{result: <사유>, contract_state, next_eligible_at?}`이다 — HTTP 오류 클래스는 계약 외부 호출(RETRY·REPLAY·BackfillPlan·chunks·finalize)에만 적용한다. 예외: `INTERFACE_MISMATCH`·`DIRECT_LAUNCH_FORBIDDEN`은 412, Adapter 조회 실패는 503(`CONTROL_API_UNAVAILABLE`, client 재시도). 오류 의미: `409 CONFLICT`(계약 충돌·`STALE_WINDOW`·`OVERLAPS_OPEN_RANGE`·`PINNED_RELEASE_INACTIVE`·`CONTRACT_CLOSED`·`ATTEMPT_IN_PROGRESS`·`CONTRACT_NOT_STARTED`(RETRY 전용, `required_action` 동봉 — 14.3)·`CHUNKS_INCOMPLETE`·`FENCE_EXPIRED`·`RELEASE_NOT_ACTIVE`(release rollback) — 재시도 불가), `423 LOCKED`(`LEASE_BUSY`·`SOURCE_LAG_EXCEEDED`·`FENCE_UNAVAILABLE` — backoff 후 재시도 가능(`TARGET_UNAVAILABLE`은 Guard 200 거부와 attempt-failure reason으로만 존재, 5.4; `RETRY_REQUIRED`도 Guard 200 거부 전용); Hold 중인 Job에 대한 수동 실행·RETRY·BackfillPlan start 요청도 423이며 Hold 해제 후 재시도 가능. 단 Guard의 `HOLD` 거부는 계약을 `VOID(SKIPPED_BY_HOLD)`/`CANCELLED(HOLD)`로 마감하므로 재시도 대상이 아니다 — 10.2 표), `412 PRECONDITION_FAILED`(digest/인터페이스 불일치·`ATTEMPT_FENCED`·`BASE_SNAPSHOT_MISMATCH`·`CREDENTIAL_REVOKED`·`CONNECTION_REVOKED`·`OPEN_CONTRACTS_RUNNING`(release ACTIVE·rollback, ConnectionRevision revoke)·`DIRECT_LAUNCH_FORBIDDEN` — 재시도 불가), `422 VALIDATION_FAILED`(JobSpec·Source 설정의 규칙 위반 — draft 수정 후 재요청, 본문 형식은 아래). 인증·역할 모델(사람: 사내 SSO OIDC, 자동화: 서비스 계정, Run Pod: projected ServiceAccount token + TokenReview + contract-scoped 단기 토큰)은 22장 13번에서 확정한다.

publish/validate 검증 실패 응답: `POST /v1/jobs/{id}/validate`와 `POST /v1/jobs/{id}/publish`는 같은 validator를 실행하며, **거부 규칙**에 하나라도 걸리면 둘 다 `422 VALIDATION_FAILED` `{violations: [{rule_id, field, actual, computed_minimum, inputs, message}], warnings: [같은 형식]}`을 반환한다(409는 이 문서에서 계약·release의 상태 충돌 전용이므로 쓰지 않는다 — JobSpec 규칙 위반은 충돌할 상태가 없고 draft 수정으로 해소된다). 경고 규칙만 걸리면 validate는 `200 {valid: true, warnings[]}`, publish는 `202`에 `warnings[]`를 실어 진행한다. 7.2의 Gate는 각각 rule_id 1개를 가지며 대표 규칙은 다음과 같다. `OVERLAP_BELOW_MINIMUM`: `load.cutoff.kind = APPLICATION_TIMESTAMP_WITH_OVERLAP`에서 `computed_minimum = source_capability.max_open_txn_seconds + envelope.safety_lag + envelope.clock_skew`(초, `clock_skew`는 11.3의 3초 항 포함), `actual = load.cutoff.overlap`; `guarantee_grade = ZERO_GAP`이면 거부, `BEST_EFFORT`면 경고(`max_open_txn_seconds` 미등록 Source는 `computed_minimum = null`로 경고 — 이때 Data Reconciliation Audit 필수는 11.3 규칙). `ZERO_GAP_REQUIRES_MAX_OPEN_TXN`: `ZERO_GAP`인데 `max_open_txn_seconds` 미등록이면 거부(7.2 5번). `inputs`에는 계산에 쓴 `SourceCapability`·`SourceSafetyEnvelopeVersion` id를 실어, publish 이후 DBA가 값을 올려 최소값이 커진 Job을 재검증 대상으로 식별한다(자동 Hold 없음). Source 저장 시 validator(6.2 credential breaker 상한 등)도 같은 형식의 422를 쓴다.

## 18. Iceberg Maintenance

Compaction을 단순 부속 스크립트가 아니라 first-class Asset/Job으로 관리한다.

- data file compaction
- manifest rewrite
- snapshot expiration
- orphan file cleanup
- metadata cleanup

Maintenance Job은 ingestion과 별도 concurrency를 쓰되 13.3의 lease 체계를 따른다 — compaction·manifest rewrite는 `PARTITION_OR_FILESET`(APPEND ingestion과 병행, Iceberg OCC), schema/partition 변경은 `EXCLUSIVE_TABLE`. Full 테이블은 data compaction 대상에서 제외한다(13.3). 모든 maintenance commit은 13.1 commit-wrapper로 `etl.writer_kind=maintenance`를 남긴다. ingestion watermark와 freshness는 이동시키지 않는다. Snapshot expiration은 commit 증거와 무관하다(증거는 CommitEvidenceLedger). 단 Commit Adjudication이 진행 중인 contract의 `base_snapshot` 이후 snapshot은 만료 대상에서 제외한다(maintenance Job이 ledger의 열린 adjudication을 확인). [Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/)

## 19. PoC 범위와 합격 기준

PoC의 시험 환경·stub Source 요건·실 Job matrix·규모 시험·장애 주입·SLO·판정 쿼리·운영 파라미터 시작값은 별도 문서 **「신규 ETL Platform PoC 시험·합격 기준서 v1」(`etl-platform-poc-test-plan-v1.md`, 이하 기준서)**이 규정한다. v1.1 19.1~19.5는 기준서 §3(시험 matrix)·§5(합격 기준)·§6(즉시 No-Go)·§7(운영 파라미터)로 이관했고 본 장은 판정 구조만 요약한다. 두 문서가 다르면 기준서가 우선하며, 기준서 §8.4의 반영 후보는 본문 개정(v1.2 변경 이력)으로 흡수한다. 운영 파라미터 시작값은 22장 22번에 잠정 기본값으로 수록한다.

판정 등급(기준서 §1.2):

- **Go** — 즉시 No-Go 0건, 기준서 §5 ‘확정’ 행 중 Phase 1 범위 행 전부 충족, 가설 13건 모두 결론 도출(일정 축소로 미뤄진 가설은 “미결 — v1.2 기본값 채택”으로 결론 기록 가능)
- **Conditional Go** — No-Go 0건, 확정 기준 중 성능 행(latency·throughput)만 미달이고 원인과 개선 계획이 식별됨. 단 ρ < 0.7인 Source의 Job에서 H-12 freshness 미달이면 플랫폼 원인이므로 불가
- **No-Go** — 즉시 No-Go 1건 이상 재현. 원인 해결 후 해당 시험만 재실행해 재평가

게이트(기준서 §8.3): **Phase 1 게이트**는 20장 Phase 1 산출물 + 기준서 §2.2 구현 범위(Phase 2 항목의 API-only 최소 구현)로 실행 가능한 시험 전부다. **Phase 2 진입 게이트**(Phase 2 구현 후·실 DR shadow 접속 전)는 §2.2 표 밖 기능에 의존하는 시험만 — RERUN_LATEST 분기, `DQ_FAILED` 전이(MVP DQ), Template rollback, ConnectionRevision REVOKED, schema drift, run/event purge, 표준 Job 신규 등록, 운영 절차 체크리스트의 Custom UI 측. Phase 2 게이트에 속한 No-Go(아래 10·11번, 14번의 UI 측)는 그 시점에 판정한다.

즉시 No-Go 14건(기준서 §6 — 검출 방법·관련 시험은 기준서). 하나라도 재현되면 채택을 중단한다:

1. 설명할 수 없는 데이터 차이
2. 정상 Run 중복 또는 누락(기대 집합 = cron 전개 − 설명된 누락, 9.3)
3. 30분을 넘는 반복적 daemon/code-location 복구 실패
4. Source session/connection 절대 한도 초과
5. Primary DB 자동 fallback(11.3 role check 미검출 포함)
6. Watermark gap/regression(`ZERO_GAP` 등급)
7. Append 이중 commit
8. Commit Adjudication 없이, 또는 `WRITER_FENCED` 확정 없이 새 attempt/SparkApplication 제출
9. lease `RECLAIMED` 확인 전 Source token 재부여(11.2)
10. `DQ_FAILED` 계약에 RETRY 수락(13.1) — Phase 2 게이트
11. Template rollback 불능 — Phase 2 게이트
12. Hold 중 신규 SparkApplication 제출
13. Kafka Outbox event 유실
14. UI/API로 필수 운영 절차를 완료할 수 없음(Control API 측은 Phase 1, Custom UI 측은 Phase 2 게이트)

가설 13건(기준서 §4 — 합격/불합격이 아니라 **v1.2 결정 입력**. 측정·판정 기준·결과별 결정은 기준서):

- H-01 500 RunRequest tick의 batch create-or-get 1회가 schedule gRPC timeout 안에 끝난다
- H-02 1 주기 이상 daemon 장애 후 coalesced recovery가 중복·누락 없이 동작한다
- H-03 SourceVisibilityFence + overlap으로 DR lag·long transaction 누락이 0이다
- H-04 Run Pod 사망 후 재결합 경로가 Spark 재실행 없이 이어진다
- H-05 Commit Adjudication이 REST 5xx·driver 사망·부분 chunk·외부 writer에서 정확히 판정한다
- H-06 Critical Merge/Full의 attempt branch → DQ → `fast_forward`(WAP)가 사용 버전에서 동작하고 비용이 감당된다
- H-07 `PARTITION_OR_FILESET` lease + Iceberg OCC로 compaction과 APPEND ingestion이 병행된다
- H-08 Full 테이블 snapshot retention 등급별 용량이 예산 내다
- H-09 lease 2단계 회수 + 관측 fence가 `V$SESSION` 실측과 일치한다
- H-10 Run Pod-per-Run이 burst 500에서 자원·기동 시간 예산 내이고 Spark Operator가 병목이 아니다
- H-11 Source별 용량 모델 ρ = Σ(D×w)/C와 publish Gate 임계(0.7/1.0)가 실측과 맞는다
- H-12 Job class별 freshness SLO 수치가 달성 가능하다
- H-13 Bundle 전달 방식과 shard 수가 publish 가시화·cold load 기준을 만족한다

가설 결과로 확정하는 22장 항목: 1·3·6·7·8·11·20·22번. 기준서 §8.1 산출물 7번(v1.2 반영 항목 목록)이 그 입력이다.

## 20. 도입 순서

### Phase 0 — Baseline, 약 2주

- 기존 24개 대표 Job의 Source 부하, 처리량, 데이터, 실패/Retry 측정
- 실제 Data Guard/Flashback/hard delete capability 확인
- Spark/Iceberg/Polaris 정확한 버전 확정

### Phase 1 — Adoption PoC, 약 3~4주

- 10k synthetic Assets, 500 Burst, 40k/day soak
- SparkApplication adapter
- Execution Occurrence/Contract/Attempt, Watermark, `WRITER_FENCED` + Commit Adjudication, commit evidence ledger
- grouped schedule·occurrence batch create-or-get·PLANNED stale 검사
- target lease 3단계·Source weighted lease 2단계 회수·Source 모니터 세션
- Source quota와 Hold
- Daemon/PostgreSQL/Code Location 장애 주입
- PoC 기준서 §2.2 구현 범위 — Phase 2 항목 중 시험에 필요한 것을 **API-only 최소 구현**으로 앞당긴다(Wizard·Custom UI 없음): Data Reconciliation Audit(PK+UPDATE_DT 비교 → repair REPLAY 생성, 12.3), extract-once 경로(Append-large 2개, 11.5), CredentialRevision 상태 전이 API(6.2), `retry`·`abort`·`resolve`·`dq:accept`·`accept-empty`·`cancel`·`watermark:seed` API(17), repair REPLAY·단일 Job BackfillPlan start/pause/resume/cancel API(14.3·14.4), Control 측 lateness sensor(16.4). 항목을 빼기로 하면 의존 시험은 기준서 §8.3 규칙대로 Phase 2 게이트로 이관한다
- 이력·계측 테이블 5종(6.1)과 시험 훅(PoC 빌드 한정, 운영 이미지에서 제거 — 기준서 §2.2)
- 종료 판정: 기준서 §8.3 Phase 1 게이트, 등급은 19장

### Phase 2 — MVP/Shadow, 약 4주

- Source/TNS/CredentialRevision 관리
- Full/Append/Merge Wizard(cutoff·초기 적재·delete semantics·타입 표준)와 단일 Job 초기 적재
- immutable bundle/release와 release 고정 규칙
- 수동 NORMAL/RETRY/REPLAY(단일 window)/단일 Job BackfillPlan/Full RERUN_LATEST
- Hold DRAIN/FORCE_STOP/catch-up
- MVP DQ 검사와 `DQ_FAILED`, schema drift 검출·차단
- Iceberg maintenance first-class Job(lease·commit-wrapper)
- Kafka Outbox(run_status_sensor 반입), Dagster read-only 인스턴스 분리, run/event purge 도구
- 대표 24 Job shadow 비교
- **Phase 2 진입 게이트**(기준서 §8.3 — Phase 2 구현 후·실 DR shadow 접속 전): §2.2 표 밖 기능에 의존하는 시험만 여기서 판정한다 — RERUN_LATEST 분기(FI-21), `DQ_FAILED` 전이·RETRY 거부(FI-23, No-Go 10번), Template rollback(FI-24a/b, No-Go 11번), ConnectionRevision REVOKED(FI-27), schema drift(FI-32), run/event purge(SC-08), 표준 Job 신규 등록 median 10분, 운영 절차 체크리스트의 Custom UI 측(No-Go 14번). 통과 전에는 24 Job shadow를 시작하지 않는다

Critical Source의 Shadow는 Airflow와 Dagster가 Oracle을 각각 읽어 부하를 두 배로 만들지 않는다. extract-once staging을 공유하거나 DBA가 승인한 순차 시간대로 실행한다.

### Phase 3 — 신규 Job 우선

- 신규 Job 25 → 100 → 500 순차 확대
- 안정화 후 기존 Job wave 이관
- 이관 순서: small Full → large Full → simple Append → Merge → custom/StarRocks

### Phase 4 — 운영 확장

- 다중 Job Backfill
- schema drift 자동 승인 workflow
- Iceberg maintenance 자동화
- Critical Source staging 확대
- LLM Advisor Shadow 평가 후 auto-prefill

## 21. MVP 범위

MVP에 포함:

- Source/TNS/Secret reference, CredentialRevision, Source 모니터 세션
- Job Wizard와 Full/Append/Merge (cutoff 종류, 초기 적재, delete semantics, 타입 표준 포함)
- JobSpec/Template/Definition Bundle 불변 버전과 release 고정 규칙, OPEN_CONTRACT_CHECK
- grouped Dagster schedule (결정론적 이름, default RUNNING), occurrence batch create-or-get, PLANNED stale 검사
- ExecutionOccurrence/Contract/Attempt, disposition, CommitEvidenceLedger
- SparkApplication adapter (attempt별 이름), Run Pod 소유 chunk 루프, `ATTEMPT_FENCED`
- SourceVisibilityFence(`APPLICATION_TIMESTAMP_WITH_OVERLAP`), Critical Source `numPartitions=1`, 2단계 lease 회수
- target lease 3단계
- Hold HOLD_NEW/DRAIN/FORCE_STOP/release catch-up
- Manual NORMAL / RETRY / **REPLAY(단일 window) / 단일 Job BackfillPlan(API·CLI, UI 최소) / Full RERUN_LATEST**
- 초기 적재(단일 Job, `INITIAL_LOAD` contract)
- schema drift 검출·차단(자동 승인 제외): publish 시 Source column/type digest 고정, Guard에서 `ALL_TAB_COLUMNS` 비교, `accept-any-schema` off
- MVP DQ 검사 집합(13.1), `DQ_FAILED` 의미와 `dq:accept`
- Iceberg maintenance(expire snapshots·orphan cleanup·Full 제외 compaction)를 Control이 제출하는 first-class Job으로 포함 — `PARTITION_OR_FILESET` lease와 `writer_kind=maintenance` commit-wrapper 필수. MVP 기간 동안 lease를 쓰지 않는 외부 compaction은 신규 경로 테이블에 금지
- Dagster run/event purge 도구(OSS 미제공)
- Kafka Outbox (run_status_sensor 반입 포함)
- Dagster read-only 인스턴스 분리
- Dagster/DataHub/Grafana/OpenSearch 연결 (DataHub는 lineage producer 단일화만 확인)

MVP 이후:

- 복잡한 다중 Job Backfill UX
- 가변 `numPartitions` weighted lease 확대
- 모든 Critical Source extract-once
- schema drift 자동 승인
- StarRocks 공통 commit receipt
- LLM 다중 모델 routing/reviewer
- CDC/hard delete 자동 포착 (`CDC_OFFSET` cutoff), `STANDBY_VISIBLE_SCN` 운영 적용
- WAP/branch 경로(Critical 한정, PoC 결과에 따라)

## 22. 채택 전에 확정할 항목

1. 실제 Spark, Iceberg, Polaris, Spark Operator 버전(제출 경로: submitter vs controller 내부, replica/worker 값 포함)
2. Oracle별 Flashback SCN 사용 가능 여부, UNDO 보존, ADG standby에서의 Flashback Query 지원 범위
3. Data Guard lag metric 접근 방식과 **세션 가시 SCN의 출처**(`DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER` 기본, `V$RECOVERY_PROGRESS` 보조), ADG(READ ONLY WITH APPLY)에서 `V$DATABASE.CURRENT_SCN`과의 관계, Source 모니터 세션용 DBA 승인 뷰 목록
4. Source별 hard delete와 `UPDATE_DT` 신뢰성, **`max_open_txn_seconds`**, NLS_CHARACTERSET·DB 시간대
5. StarRocks가 동일 target lease와 commit metadata를 사용할 수 있는지
6. Snapshot/branch/WAP의 정확한 적용 테이블 등급(Critical 한정)
7. Run/Event/log/Snapshot 보관 기간 — Full 테이블 snapshot retention 등급, CommitEvidenceLedger 보관 기간, 6.1 이력·계측 테이블 보관 기간(history 3종·`guard_result` = ledger와 동일, `attempt_timeline` 온라인 90일 잠정 — 5.3) 포함
8. Code Location shard 수와 경계, Bundle 전달 방식(이미지 내장 vs 외부 저장소)
9. Source별 DBA 승인 connection/CPU/IO 한도와 **DB 측 강제 방식**(ETL 전용 user Profile, Resource Manager의 ADG 적용 여부, standby 전용 role-based service, `STANDBY_MAX_DATA_DELAY` 적용 계정), 현행 ETL 계정 profile 값 — 특히 `FAILED_LOGIN_ATTEMPTS`(`SourceCapability.failed_login_attempts`로 등록; 6.2 validator `credential_breaker_failures + 최대 동시 ETL Job ≤ FAILED_LOGIN_ATTEMPTS − 1`의 입력, `credential_breaker_failures` 기본값 1은 22번의 잠정값(상한식만 확정). profile 기본 10으로 상한이 부족한 Source는 DBA에 상향 요청); 추가 확인 — SUPERSEDED grace 동안 구 비밀번호가 유효한지(Oracle gradual password rollover 적용 여부). 미적용이면 precheck 통과 후 비밀번호가 바뀐 attempt의 executor/JDBC 재접속이 Spark task retry마다 실패할 수 있으므로 Template은 ORA-01017/28000에서 task retry를 끊는 fail-fast를 강제한다(상한식에 이 항을 더하지 않기 위한 조건)
10. ~~Dagster direct UI 실행 허용 여부~~ → 14.3에서 확정(Guard가 NORMAL 계약 생성, Critical 거부). 대신 **Dagster UI 노출 정책**: read-only 인스턴스 분리, write 인스턴스의 SSO proxy·mutation allowlist·감사
11. Source별 용량 모델 계수(D 분포, ρ 임계 0.7/1.0)와 publish Gate 운영
12. 전사 timestamp 타입(NTZ vs timestamptz)과 타입 매핑표 확정
13. Control API 역할 모델·Run Pod 신원. 2인 승인/SoD는 정책 선택사항
14. Secret 저장소와 주입 경로, Polaris principal/catalog role 모델, AIStor STS(credential vending) 지원 여부, Polaris의 REST Idempotency-Key 지원 여부
15. Control PostgreSQL RPO/RTO, 플랫폼 전체 DR 범위
16. 기존 Job 이관 도구(converter 커버리지), watermark seed 절차, Job별 cutover/롤백 runbook, 이관 기간 Airflow writer의 lease 처리
17. Java vs Python Control Plane 선택 근거와 JobSpec 스키마 단일 원본(JSON Schema codegen) 방식
18. 팀 규모·역량·DBA 리드타임 가정과 Phase 종료 게이트
19. 비용 모델과 현행 대비 잠정 기준
20. asset checks 재사용 여부(13.1 DQ → `AssetCheckResult`). freshness policy 위임은 16.4에서 확정 — Dagster freshness policy를 판정·알림에 쓰지 않고 Control lateness sensor가 단일 계산 주체다(Dagster UI의 freshness 표시는 선택 사항이며 알림 원천이 아니다)
21. 30분/5분 복구 목표의 출처(기존 SLA) 확정, 1시간 미만 cron 허용 여부
22. Job별 freshness SLO 수치와 운영 파라미터 기본값. v1.2는 PoC 기준서 §7의 시작값을 **잠정 기본값**으로 채택하고 기준서 결과(H-04·H-12·FI-02·FI-34·FI-35)로 확정한다. `freshness_slo`의 계산 주체·공식·dedup·분해 지표는 16.4에서 확정했고 수치만 잠정이다. 규칙으로 묶인 값은 규칙이 기본값이며 publish·기동 검증에서 위반을 거부한다:
    - `freshness_slo` — Job class별: 1h 주기 p95 20분, 24h 주기 p95 60분(잠정, Phase 0 baseline으로 보정). lateness = `contract.finalized_at − logical_scheduled_at`(16.4; `attempt_timeline.t7_finalized_at`은 그 복사), 분해 지표 = attempt 단위 플랫폼 지연·보호 지연·제출 지연·실행(6.1 `attempt_timeline`) 및 계약 단위 대기·적재·finalize(16.4 이벤트 payload) — 보호 지연 정의는 6.1·16.4 공통. 16.4 `freshness breach` 임계값 = 해당 Job의 `freshness_slo`, JobSpec(7.3) 미지정 시 class 기본값
    - `planned_stale_after` = max(5분, `run_monitoring.start_timeout_seconds`)(16.4 `expected occurrence missing`의 grace로도 쓰임), `planned_scan_interval` 60초(lateness sensor 주기로도 쓰임, 16.4), `guard_backoff_initial_seconds` 30(2배 증가, 상한 period/2), `guard_retry_budget_seconds` 120, `max_auto_attempts` 3(`RUN_WORKER_LOST` 전용), `adjudication_pending_alert_after` period/2, `adjudication_delay_seconds` 0
    - `reattach_grace_seconds` = `run_monitoring.start_timeout_seconds` + 60(시작값 300 + 60 = 360)
    - **`chunk_proceed_timeout_seconds` = `reattach_grace_seconds` + 120** — driver의 `proceed` 대기(10.1)는 재결합 창(10.2 복구 2번)을 덮어야 하므로 항상 `reattach_grace_seconds`보다 커야 하고, 여유 120초 = `run_monitoring.poll_interval_seconds` 60 + `run_status_sensor` 간격 30 + 30(재결합 탐지 지연 예산)이다. 정상 경로 CAS p99 × 여유는 하한일 뿐이다. 두 값은 JobSpec이 아니라 instance/Source 설정이므로 Control 기동·설정 검증이 `chunk_proceed_timeout_seconds ≤ reattach_grace_seconds`를 거부한다
    - `monitor_query_timeout_seconds` 5, `adapter_sync_timeout_seconds` 5, `lease_try_timeout_seconds` 10, `drain_timeout_seconds` = 예상 chunk 시간 × 2(Hold 생성 시 지정), `credential_grace_seconds` = instance `run_monitoring.max_runtime_seconds`(Source별), `datum_stale_seconds` 30
    - **`daemon_heartbeat_gap_seconds` = 2 × 최소 period**(최소 period 1시간이면 120분) — Schedule Gap Recovery 자동 기동 조건(9.3). 1 주기 누락은 Dagster 자체 catch-up tick이 덮으므로 2 주기부터 기동한다
    - `credential_breaker_failures` 1(신설 파라미터 — 첫 `source credential failure`에서 Source `HOLD_NEW`, 16.4·6.2) / `platform_breaker_failures` 3(신설 파라미터 — breaker key(Polaris catalog·AIStor) 단위 연속 `TARGET_UNAVAILABLE`에서 자동 `HOLD_NEW`(Job 목록/Global scope), 5.4·10.2) / `target_health_timeout_seconds` 5(신설 — Guard 5번 target health check 예산, 5.4) / `gap_recovery_max_range_seconds` 7일(신설 — Schedule Gap Recovery 수동 기동 range 상한, 9.3). 의미는 각 절이 정의하고 여기서는 기본값만 고정
    - Dagster `run_monitoring.poll_interval_seconds` 60(기본 120) — Run Pod 사망 탐지 지연의 상한이며 위 120초 여유의 근거(5.1)
    - **`run_monitoring.max_runtime_seconds` 규칙** — instance 기본값(dagster.yaml) = 최대 period + 여유, Job별 값 = max(period, 예상 chunk 시간 × 예상 chunk 수) + 여유. Job별 값은 Control이 `occurrences:batch-create-or-get` 응답(9.1)과 Adapter `launchRun`에 실어 RunRequest tag `dagster/max_runtime`으로 지정한다. 실행 중(`ATTEMPT_ACTIVE`·`COMMIT_OBSERVED`) 계약의 유일한 시간 상한(9.3)이므로 `expires_at`이 아니라 이 값이 장기 실행을 끊으며, 초과 시 Dagster가 Run을 종료하고 그 terminal 사실은 10.2 반입 표(`RUN_WORKER_LOST`, SA 생존 시 `reattach_deadline`)를 따른다
    - Dagster `max_concurrent_runs` 800(최대 동시 Spark + 여유), `max_tick_retries` 2, `run_monitoring.start_timeout_seconds` 300(burst 기동 분포로 조정)

## 23. 최종 판단

Dagster는 이 환경에 적합한 중심 오케스트레이터다. 특히 Asset 중심 UI, 설정 기반 Asset Factory, Kubernetes Run, 실행 이력은 Airflow에서 벗어나려는 목표와 잘 맞는다.

다만 성공 조건은 Dagster 자체가 아니라 다음 네 가지이다.

1. **Job 파일 10,000개 대신 불변 JobSpec과 공용 Factory를 사용한다.**
2. **Source 보호를 Dagster Run 개수가 아닌 실제 JDBC connection 기준으로 강제한다.**
3. **Retry 성공 여부를 Spark 상태가 아니라 Iceberg Snapshot으로 판정한다.**
4. **Control Plane이 Dagster의 scheduler/retry/logging을 다시 구현하지 못하게 경계를 지킨다.**

따라서 최종 권고는 **Dagster-first를 채택 PoC로 진행하되, PoC No-Go 기준을 통과하기 전에는 기존 Airflow를 대체한다고 결정하지 않는 것**이다.

## 참고 자료

- [Dagster asset factories](https://docs.dagster.io/guides/build/assets/creating-asset-factories)
- [Dagster state-backed components](https://docs.dagster.io/guides/build/components/state-backed-components)
- [Dagster schedules and sensors API](https://docs.dagster.io/api/dagster/schedules-sensors)
- [Dagster OSS deployment architecture](https://docs.dagster.io/deployment/oss/oss-deployment-architecture)
- [Dagster Kubernetes integration](https://docs.dagster.io/integrations/libraries/k8s/dagster-k8s)
- [Dagster concurrency pools](https://docs.dagster.io/guides/operate/managing-concurrency/concurrency-pools)
- [Dagster GraphQL API](https://docs.dagster.io/api/graphql)
- [Spark JDBC data source](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)
- [Iceberg Spark configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Iceberg Spark writes](https://iceberg.apache.org/docs/latest/spark-writes/)
- [Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
- [Oracle Flashback Query](https://docs.oracle.com/en/database/oracle/oracle-database/19/adfns/flashback.html)
- [Oracle Data Guard lag](https://docs.oracle.com/en/database/oracle/oracle-database/26/haovw/redo-apply-troubleshooting-and-tuning.html)

