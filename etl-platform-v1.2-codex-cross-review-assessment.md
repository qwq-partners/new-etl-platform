# Codex 교차 리뷰(v1.2) 검토서

- 검토일: 2026-08-23
- 검토 대상: `etl-platform-v1.2-codex-cross-review.md` (P0 10 / P1 23 / P2 8 + §8 규범 문장 4)
- 대조 문서: `etl-platform-target-architecture-v1.2.md`(A), `etl-platform-poc-test-plan-v1.md`(P)
- 방법: 45건 전수를 A·P 본문 인용으로 재검증(4개 독립 검증 + 종합). 각 건에 **판정(확인/부분/기각) · 등급 동의 여부 · 채택 방식**을 붙였다.

---

## 1. 총평

**Codex의 결론(방향 GO, v1.2 의미론 동결은 REWORK)에 동의한다.** 45건 중 **확인 26, 부분 확인 19, 기각 0** — 리뷰가 지적한 결함은 전부 본문에 실재하거나 최소한 규정 공백이다. 다만 P0 10건 중 **5건은 P1로 하향**하는 것이 맞다(피해 경로가 리뷰 서술보다 좁거나 v1.2에 이미 1차 방어가 있음). 수정안은 36건을 **축소 채택(ADOPT_MODIFIED)** — 리뷰가 제안한 새 상태·새 테이블·새 메커니즘(`ADJUDICATION_BLOCKED`, `recovery_epoch`, FenceBundle 동결, candidate/serving 이중 포인터, `run_submission` 테이블, `TARGET_UNCHANGED_EMPTY_SOURCE`, exact-range repair contract)은 대부분 **기존 상태·필드·경로의 문장 수정**으로 같은 효과를 낼 수 있다.

가장 뼈아픈 것은 리뷰 §11의 네 문장이 전부 맞다는 점이다:

| 리뷰 문장 | 본문 근거 | 판정 |
|---|---|---|
| "verdict를 모름은 commit 안 됨이 아니다" | 9.3·6.2·10.2의 만료 게이트가 `WRITER_FENCED`만 보고 verdict를 보지 않음. 5.4가 "Polaris 미조회 시 verdict 보류"를 도입했으므로 fenced ∧ verdict NULL 상태에서 `CANCELLED(EXPIRED)` → window 해제 → 다음 회차 재append | **P0 확인** |
| "SCN snapshot 고정은 UPDATE_DT 누락 없음과 같지 않다" | 11.3 overlap 공식의 `max_open_txn_seconds`는 트랜잭션 지속시간이지 `commit − UPDATE_DT` 상한이 아님. ZERO_GAP 부여 조건이 이 값 등록 하나뿐(7.2 5번) | **P0 확인** |
| "Pod가 없음은 Oracle session이 없음과 같지 않다" | 11.2 회수 = SA 삭제 + pod 부재 → `RECLAIMED` → token 반환. 단 grant 직전 `GV$SESSION` 대조·pool 여유·DB Profile 2차 겹이 이미 있어 초과 경로는 좁음. stub의 "TCP 단절 ≤5초 세션 종료"가 이를 은폐 | **P1 부분** |
| "ledger window가 연속은 row가 모두 들어옴과 같지 않다" | §5.1 ZERO_GAP 쿼리가 인접성만 검사. row-level 비교 A/B는 실 DR 24 Job 한정·No-Go 행과 미연결 | **P0 확인(기준서)** |

**판정 결정표(Codex 9.3)에 대한 입장:**

```text
Architecture direction:        GO                      — 동의
v1.2 semantic freeze:          REWORK REQUIRED         — 동의. v1.2.1로 P0 5건 + P1 핵심을 닫는다
Phase 0 baseline collection:   GO                      — 동의, 즉시 병행
Phase 1 semantic PoC:          GO after P0 closure     — 동의. 단 'P0 closure'의 범위는 아래 §2의 P0 유지 5건
Scale soak only ≠ semantic Go: 동의 — 기준서 §8.3에 'Scale/Control Go'와 'Oracle ZERO_GAP Go' 분리
Production shadow/pilot:       NO-GO until P0+P1       — 동의
```

---

## 2. P0 10건 재판정

| ID | 제목 | 판정 | 등급 | 핵심 근거(A·P 인용) | 채택 방식 — v1.2.1 변경 |
|---|---|---|---|---|---|
| P0-01 | verdict NULL인데 `expires_at`이 window·lease 해제 | **확인** | P0 유지 | A 9.3 "만료는 `WRITER_FENCED`가 확정된 계약에만 즉시 적용", A 5.4 "Polaris를 읽지 못하면 verdict를 내리지 않고 backoff 재시도", A 14.3은 RETRY만 "verdict 미확정 → 409 ATTEMPT_IN_PROGRESS"(비대칭). 만료 후 `adjudicate`는 409 CONTRACT_CLOSED라 회복돼도 반영 경로 없음 | ADOPT_MODIFIED — 새 상태 없이 만료 전제를 "verdict ∈ {NO_COMMIT, PARTIAL_COMMIT} 확정(부분 commit CAS 반영 완료)"으로 교체(9.3·6.2 표·10.2 backoff·13.4). verdict NULL은 `ADJUDICATION_PENDING` 유지 + window·target lease 유지 + 기존 `adjudication_pending_alert_after` 알림. Polaris 재시도 backoff 파라미터 1개(지수·상한). 기준서 FI-28 변형(outage > expires_at) |
| P0-02 | 0-row 접두부가 전체 완료로 오인 | **부분** | **P1로 하향** | 결함 확인: A 13.2 "S′ = ∅ → 0-row receipt 있으면 `FINALIZED_NO_DATA`"에 1..expected 완비 조건 없음(같은 절의 '0-row도 존재로 센다·접두 규칙'과 모순). 과장: watermark는 chunk별 `high_k` CAS로만 전진하고 `FINALIZED_NO_DATA` 전이는 watermark를 옮기지 않으므로 데이터 누락이 아니라 **잘못된 종결 상태**(no_data materialization·freshness 갱신·같은 fence RETRY 불가) | ADOPT_MODIFIED — 13.2 첫 bullet에 완비 조건(0-row 집합이 1..expected를 덮을 때만 NO_DATA, 접두 1..k면 `high_k`까지 CAS 대행 후 `PARTIAL_COMMIT`), 13.4에 "NO_DATA는 마지막 CAS 너머로 watermark를 옮기지 않는다", ledger `(contract_id, attempt_no, chunk_no)` unique. 기준서 FI-37a/b/c(기대값 watermark = `high_k`) |
| P0-03 | Incremental partial 뒤 `dq:accept`가 미실행 chunk까지 승인 | **확인** | P0 유지 | A 6.2 표 "`dq:accept` … watermark는 `window.high`로 전진", 13.1 검사 1·5는 chunk마다 수행 → chunk k 실패 후 k+1.. 미실행 → accept가 ledger 없는 `[high_k, window.high)`를 덮음(No-Go 6). 반대로 generic `resolve(OPERATOR_ACCEPT)`는 chunk k CAS 없이 window 해제 → 다음 회차 재append(No-Go 7). FI-23은 두 경로 모두 미검증 | ADOPT_MODIFIED — exact-range repair contract 신설 대신: `dq:accept` := commit된 chunk k의 DQ 승인 → ledger `dq_result=ACCEPTED` + CAS(`high_k`); k = expected(또는 Full)면 FINALIZED, 아니면 `ADJUDICATION_PENDING(PARTIAL_COMMIT, reason DQ_ACCEPTED)`로 기존 RETRY(low = `high_k`) 재개. `resolve`의 DQ_FAILED는 REPAIR_CONTRACT·WATERMARK_SEED만 허용. 기준서 FI-23c |
| P0-04 | PITR 뒤 과거 writer를 막을 recovery epoch 없음 | **부분** | **P1로 하향**(단 FI-05 전 반영) | A 5.4가 위험을 인지하고 Global Hold + resync Runbook을 두었고 원시 연산(FORCE_STOP fencing, token 부여 직전 GV$SESSION 대조, lineage `BASE_SNAPSHOT_MISMATCH`)은 전부 있음. 빠진 것은 **Runbook 순서**(살아 있는 SA/Run Pod fence → catalog head 안정 → 재구성 → lease·세션 확인 → 해제)와 Global Hold의 mode. 0-row CAS 소실은 watermark 후퇴 방향이라 재추출 비용일 뿐 중복·누락 아님. epoch는 Control API 호출만 막고 driver→Polaris 쓰기는 못 막음 | ADOPT_MODIFIED — 5.4 Runbook을 5단계 프로토콜로 고정(epoch는 SA label·Run Pod 토큰 claim 수준 선택). 기준서 FI-05를 soak 중 live Run Pod+SA+0-row CAS+미발행 Outbox 포함으로 확장 |
| P0-05 | fence와 extract가 다른 ConnectionRevision/DB identity | **확인** | P0 유지(범위 주석) | A 10.2 6번 "모니터 세션 자체도 ACTIVE revision의 descriptor로 접속", 6.2 "contract는 생성 시점 descriptor pin·SUPERSEDED 계속 사용", 7.1 "REVOKED 뒤 ACTIVE로 재해석(fence 불변)" — 셋을 합치면 fence(B)/extract(A) 조합이 정상 경로에서 성립. 같은 DBID면 ORA-08181 fail-fast지만 **다른 DBID(재구축 standby·clone)면 silent** | ADOPT_MODIFIED — FenceBundle 동결·재해석 금지 대신: SourceSystem에 불변 `db_identity(DBID, DB_UNIQUE_NAME, CON_GUID, resetlogs)`를 두고 모든 ConnectionRevision 연결 테스트에서 대조(422), contract에 `db_identity` pin, driver precheck에 identity 비교, Template SQL에 `SYS_CONTEXT('USERENV', …)` 술어로 executor 연결까지 검증. descriptor pin 시점은 첫 Guard로 이동. 기준서 FI-27 identity 단언 + FI-42 |
| P0-06 | `max_open_txn_seconds`만으로 ZERO_GAP 보장 불가 | **확인** | P0 유지 | A 7.2 5번 ZERO_GAP 조건 = `max_open_txn_seconds` 등록뿐, 11.3 공식은 '트랜잭션 지속시간'을 '`commit − UPDATE_DT` 상한'으로 오용, P §2.1은 "DBA/업무 확인"(OBSERVED). 11.3 "SCN_TO_TIMESTAMP 최대 3초 이를 수 있음"도 Oracle 문서의 'usual precision'을 상한으로 과주장 | ADOPT_MODIFIED — capability를 `max_commit_minus_watermark_seconds` + `bound_kind(ENFORCED \| OBSERVED)` + `timestamp_origin`·`not_null`·`updated_on_every_change` 3필드로 확장, ZERO_GAP 허용 rule = ENFORCED ∧ DB_TRIGGER ∧ not_null ∧ updated_on_every_change(hard delete는 기존 `delete_semantics`). 리뷰의 8필드 전부·별도 SCN 오차 필드는 불채택. 기준서 FI-41(eligible PK 차집합 0) |
| P0-07 | Pod 부재 = Oracle 세션 종료로 간주해 token 조기 반환 | **부분** | **P1로 하향** | A 11.2 "`RECLAIMED` → token 반환" 조건이 pod 부재인 것은 사실. 그러나 A 11.2 "token 부여 직전 모니터 세션이 `GV$SESSION` 집계를 pool 회계와 대조해 초과 시 거부", "pool 한도 < 절대 한도", 22장 9번 DB Profile 2차 겹이 이미 있어 경로가 좁음. 남는 결함: 801 비교식 미정의(관측+요청 vs cap), MODULE 태그 기반 집계의 누락, P §2.3 stub "≤5초 종료"의 은폐 | ADOPT_MODIFIED — `RECLAIMED`(= `WRITER_FENCED`, Adjudication 전제) 의미는 유지하고 lease 상태에 `RELEASED`(token 반환) 분리: 조건 = 해당 attempt 세션 0 연속 N회(상한 초과 시 운영자 알림). 801을 `observed(username/service 총수, fresh, pool lock 안) + requested_weight ≤ pool_cap`으로 정식화, 관측 실패는 fail-closed. stub ≤5초 종료를 구성 가능(기본 off)으로. 기준서 FI-43 |
| P0-08 | serving pointer와 Control ACTIVE 비원자 | **부분** | **P1로 하향** | split 자체는 A 6.2·17 rollback (4)가 인지·용인. 실행 내용은 contract pinned plan/image digest이고 인터페이스 불일치는 Guard 5번 `INTERFACE_MISMATCH`가 잡으므로 '잘못된 코드로 쓰기' 경로 없음. 남는 결함: ACTIVE 실패 분기 "—", split 지속 시 `INTERFACE_MISMATCH` 오VOID | ADOPT_MODIFIED — candidate/serving 이중 포인터는 불채택(VERIFIED가 실제 로드를 요구해 shard 메모리 2배). 실패 분기 = "포인터를 Control ACTIVE의 bundle로 복귀 + reload"(VERIFIED fallback 재사용), Run Pod가 `guard`에 `loaded_bundle_digest`를 실어 attempt에 기록, `INTERFACE_MISMATCH`를 '더 새 ACTIVE 없음 → PLANNED 유지+backoff / 있음 → VOID'로 2분기. 기준서 FI-24b 확장 |
| P0-09 | stub `visible_scn = primary_scn − lag` 단위 오류 | **확인** | P1(기준서 결함, 구현 전 필수) | P §2.3 88행이 시간(lag)을 SCN에서 감산 — 제 문구의 오류. idle DB와 burst DB에서 같은 `SCN−720`이 전혀 다른 시각을 가리킴 | ADOPT_MODIFIED — "`visible_scn` = `commit_ts ≤ stub_now − injected_lag`인 마지막 `commit_scn`(contiguous prefix), `SCN_TO_TIMESTAMP(visible_scn)` = 그 commit_ts(3초 granularity), `V$DATAGUARD_STATS` apply lag·DATUM_TIME은 같은 커서에서 파생"으로 교체. FI-40은 stub 자가검증으로 채택; 완전한 RedoEvent/gap_set 모델은 불채택(시험 가설 없음) |
| P0-10 | No-Go SQL이 안전 불변식을 증명하지 못함 | **확인** | P0 유지(기준서) | P §5.1: "같은 (job, logical_at)에 SA 2개 이상" → 순차 RETRY(attempt별 SA 이름) 오판; `(contract_id, chunk_no)` 그룹 → attempt마다 chunk 재번호(A 13.4)라 오판; FINALIZED window 겹침 → 닫힌 구간 BACKFILL·repair REPLAY 오탐; "Snapshot metadata 100%"는 FI-31/36이 soak 중 만드는 `etl.*` 없는 snapshot과 양립 불가 → **현재 문구로는 Go 판정 자체가 불가능** | ADOPT_MODIFIED — 4층 oracle(occurrence 정확히 1개 / writer·lease interval 겹침 0 / `(contract, attempt, chunk)` 증거 유일성+lineage / 주입 truth vs target 차집합 0) 채택. (4)(5)는 "같은 Job의 ingest snapshot `[logical_window_low, high)` 쌍별 겹침 0, lane은 NORMAL·CATCHUP·INITIAL_LOAD만"으로 단일화. FI 주입 snapshot은 id allowlist로 분모 제외(`writer_kind=FAULT_INJECTION`은 `etl.*` 키가 없는 snapshot에 붙일 수 없어 자기모순). `recovery_epoch` 항은 P0-04 결정에 종속 |

**순 결과: P0 유지 5건(01·03·05·06·10), P1 하향 5건(02·04·07·08·09).** §8 규범 문장 4개는 모두 채택하되 §8.3(FenceBundle 동결·재해석 금지)과 §8.4(RECLAIMED를 세션 drain에 묶기)는 위 표대로 축소한다.

---

## 3. P1 23건 · P2 8건 판정 요약

| ID | 판정 | 등급 | 채택 방식 — 요지 |
|---|---|---|---|
| P1-01 초기 `next_eligible_at`·inline expiry | 확인 | P1 | `next_eligible_at` NOT NULL DEFAULT `created_at`(6.1·9.1) — 없으면 FI-35(a) RunRequest 유실 계약이 stale 루프에 영원히 안 잡힘. `FENCE_EXPIRED`의 NULL 의미는 별도 플래그로. Guard 1번 PLANNED inline 만료는 P2 |
| P1-02 Gap Recovery 제출 Run을 tick이 못 봄 | 확인 | P1 | 9.1 `launch` 정의에 "non-terminal 제출 부재" 추가 — 계약 row `last_submitted_run_id`로 충분, `run_submission` 테이블 불필요 |
| P1-03 500 batch poison pill | 확인 | P1 | ADOPT — 항목별 savepoint 격리 + `ITEM_REJECTED` 응답(Guard의 "예약은 rollback, 결과는 commit" 패턴 재사용). SC-02b |
| P1-04 terminal 뒤 증거·cleanup까지 412 | 부분 | **P2** | 결과는 'reason이 `RUN_WORKER_LOST`로 1회 뭉개짐'뿐(다음 attempt precheck가 breaker를 연다). 증거 전용 endpoint 대신 Adjudication이 Pipes 경로 재생으로 verdict reason 확정 |
| P1-05 겹치는 Hold·CATCHUP coverage | 확인 | P1 | 핵심은 `REJECTED_AT_GUARD`가 14.2 catch-up 대상에 없어 24h Job이 하루를 잃는 것. 14.2 대상 확장 + 14.1 겹침 의미(any-held, max mode, `(scope, reason, key)` open unique). lattice 용어는 선택 |
| P1-06 freshness 세 시계 | 부분 | **P2** | 새 상태 불채택. 16.4에 `source_coverage_lag`(fence_ts 기준), Full `allow_empty` NO_DATA에 `target_unchanged` 표시, `recovered_by_contract_id` |
| P1-07 Dagster retry 설정·cancel 원인 | 확인 | P1 | ADOPT — 5.1 `max_retries`·`max_resume_run_attempts=0`, 10.2 반입 표에 CANCELED 원인 3종(`MAX_RUNTIME_EXCEEDED`·`OPERATOR_CANCELLED`·`PLATFORM_TERMINATE`) 분리·자동 재시도 비대상 |
| P1-08 SA identity·재생성 경계 | 부분 | P1 | 범위는 ownerReference 한 가지: SA에 ownerReference 금지(label 전용) + UID 관측 후 404면 재생성 금지·fencing. 나머지 필드는 선택 |
| P1-09 WAP fast-forward vs main 전진 | 확인 | P1(PoC 비교군) | H-06 'fast_forward 100%'가 SC-11 compaction과 같은 테이블에서 구조적으로 불가 → publish 구간 `EXCLUSIVE_TABLE` 승격 + ref 증거. H-06에 concurrent commit 변형 |
| P1-10 GC·compaction이 증거 삭제 | 부분 | P1 | 실제 공백은 `orphan_min_age` 불변식 하나 + 만료 제외 집합에 DQ_FAILED·RECONCILIATION_REQUIRED 추가. external writer_kind 요구는 기각(정의상 wrapper 밖) |
| P1-11 DQ row count 기준 필드 | 확인 | P1 | 검사 1을 `written_rows`(receipt에 이미 존재) 기준으로 — 현재 문구대로면 overlap 있는 모든 Append가 매 chunk DQ_FAILED. Merge metric null → `CHUNK_DQ_FAILED` |
| P1-12 Flashback/UNDO deadline | 확인 | P1 | `chunks:begin`에서 `now + 예상 소요 > fence_ts + undo_retention × 0.5` → `FENCE_EXPIRED`(DRAIN 처리); 11.4 'chunk 축소' 문구 삭제(ORA-01555 후 같은 SCN 재독은 무의미); INITIAL_LOAD는 chunk별 fence 예외 또는 extract-once 필수. 별도 `flashback_deadline` 필드 불필요. FI-44 |
| P1-13 role check를 모든 JDBC connection에 | 확인 | P1 | `sessionInitStatement`(driver·executor 모두 적용)에 PL/SQL 검사 블록 — connection별 'fence revision' 재검사는 불필요(SQL literal `AS OF SCN`). FI-45 |
| P1-14 ORA-28002·상한식 | 부분 | P1 | **ORA-28002는 로그인 성공 경고 — 확인**(breaker 입력에서 제거, `CREDENTIAL_EXPIRING` 경고). 상한식에 모니터 세션 1 + legacy 세션 항 추가. 'TNS address failover' 항은 틀림(인증 실패는 다른 address로 재시도하지 않음), task retry 항은 22장 9번 fail-fast로 이미 처리 |
| P1-15 `NO_LAG_SIGNAL` = FULL | 부분 | **P2** | "ZERO_GAP은 VERIFIED 아니면 거부"는 이미 규정(신호 없는 Source는 BEST_EFFORT만 publish 가능). delta는 `confidence_reason`에 `NO_LAG_SIGNAL` 값 추가(알림 제외, Audit 우선) |
| P1-16 logical/extract/CAS window 분리 | 확인 | P1 | overlap은 계약 최초 low에만 적용(같은 `visible_scn` 안에서는 late commit 불가) — `extract_window_low`·`overlap_recovered_rows`만 추가. FI-47 |
| P1-17 target provisioning·fault domain | 부분 | P1 | provisioning 확인: publish 시 table create-or-get + `table_uuid`·schema id·spec id pin, Guard 5번 불일치 → `INTERFACE_MISMATCH`. 새 상태 불필요. Polaris/AIStor 분리 시험은 FI-28 (a)/(b)로 이미 존재 |
| P1-18 Airflow cutover runbook | 확인 | Phase 3 게이트 | DEFER(22장 16번) — 'Phase 3 진입 게이트: cutover runbook + FI-48'로 승격. Phase 1 게이트 항목 아님 |
| P1-19 lock 순서·DB 제약 | 부분 | P1 | Guard 6번·revoke·breaker·credential ACTIVE가 `source_system` row(또는 advisory lock)를 공통으로 먼저 잡으면 write skew 3건이 닫힘. 제약 9종은 DDL 부록으로(P1-22와 합본), lease exclusion은 partition range 한정 |
| P1-20 ρ 단위·단일 모니터 세션 직렬화 | 부분 | P1 | ρ 식에 period 항(표기 결함, P2). 500건이 한 Source에 몰리면 모니터 세션 1개에서 Guard 6번이 직렬화 — fence precursor snapshot(모니터 세션이 주기 조회, Guard는 row lock 안에서 소비)을 PoC 비교안으로. SC-02c |
| P1-21 stub Go ≠ Oracle ZERO_GAP Go | 부분 | P1 | §8.3에 두 게이트 분리 + Phase 0 입력에 DG 시험 인스턴스 + `SourceCapability.zero_gap_verified`를 ZERO_GAP publish 조건에. 'BEST_EFFORT 한정 출시'는 이미 capability로 강제됨 |
| P1-22 계측 schema↔SQL 불일치 | 확인 | P1 | ADOPT — DDL 부록 1개 + §5.1 dry-run(PoC 주차 1). 구체: ledger `cas_applied, cas_at, window_low/high, actor`, attempt_state_history `terminal_ingested_at`, FI-22는 attempt history FENCED로, 재결합은 binding 컬럼 트리거 |
| P1-23 post-commit DQ 노출 | 확인 | P1 | 검사 5(Full −50%)는 driver가 commit **전** `total-records` 비교로 이동(`CHUNK_DQ_FAILED` 경로 수렴, main 불변). `DQ_FAILED` 알림·UI에 'main 이미 노출' 문구. WAP 범위는 H-06 그대로 |
| P2-01 B1 근거 자기모순 | 확인 | P2 | ADOPT — "PLANNED CATCHUP이 NORMAL을 흡수"는 13.4와 모순. 근거를 bounded stale intent·설명 주체 교체·queue hygiene로 교체(결론은 유지) |
| P2-02 문서 우선순위 | 확인 | P2 | ADOPT — "기준서 우선"을 뒤집어 규범 > executable schema/API > PoC 절차 > 산문. 불일치는 traceability failure로 기록 |
| P2-03 grouped schedule target 형태 | 확인 | P2 | ADOPT — 9.1에 "shard subsettable asset job + `RunRequest(asset_selection)`" 명시, SC-02에 RunRequest 생성 시간 |
| P2-04 `versions.lock` 범위 | 확인 | P2 | ADOPT — Phase 0 산출물에 image digest·CRD digest·기능 probe |
| P2-05 API status·attempt identity | 부분 | P2 | (a) attempt_no 주체·(d) rollback 재개는 이미 규정(기각). (b) 17장 오류 표를 Guard 200 집합 / HTTP 집합으로 분리, (c) 423에서 LEASE_BUSY 등 제거(Hold 전용) |
| P2-06 CATCHUP 부하식 | 확인 | P2 | `N_hold × D / C` → `max_S Σ_{i∈S}(D_i × w_i)/C_S`, `expires_before_completion`은 Source별 |
| P2-07 `allow_empty_full` 이름 | 확인 | P2 | ADOPT — `on_empty_source: RETAIN_PREVIOUS \| FAIL` |
| P2-08 잠정값 경계·음성 시험 | 부분 | P2 | max_runtime 경계 변형(FI-25)·retention 비용 집계(SC-08)·FI-34(c) partition만 추가, 1,200 burst 제외 |

---

## 4. 리뷰에서 기각하거나 축소한 주장

리뷰의 결함 지적은 전부 실재하지만, **처방**에는 기존 장치를 보지 못한 곳과 기술 전제가 틀린 곳이 있다.

1. **`recovery_epoch`로 과거 writer 차단(P0-04, §8.3)** — epoch는 Control API 호출만 412로 막는다. Iceberg 쓰기는 driver → Polaris 직접이므로 물리 fencing은 여전히 SA 삭제 + pod 부재(기존 FORCE_STOP fencing 단계)다. 핵심은 epoch가 아니라 resync Runbook의 **순서**(fence → catalog head 안정 → 재구성 → 해제).
2. **FenceBundle 동결·revision 재해석 금지(P0-05, §8.3)** — 재해석 금지는 C2(7.1·10.2 6번)를 폐기하고 revision 교체 때마다 실행 중 계약을 전부 닫아야 해 운영 비용이 크다. 결함의 실체는 "DB identity 검증 부재"이므로 SourceSystem 수준 불변 identity + 연결별 검증이면 재해석을 유지한 채 닫힌다.
3. **token 반환 = 세션 drain(P0-07, §8.4)** — `RECLAIMED`(= `WRITER_FENCED`)를 Oracle 세션 수명(SQLNET.EXPIRE_TIME 0이면 TCP timeout~keepalive 2h)에 묶으면 Adjudication과 후속 attempt가 그만큼 지연된다. fencing 의미는 유지하고 **반환 시점만** `RELEASED`로 분리한다.
4. **candidate/serving 이중 포인터(P0-08)** — VERIFIED가 manifest digest 대조·schedule RUNNING 확인을 위해 실제 로드를 요구하므로 shard마다 후보 code location이 하나 더 필요(10,000 asset 메모리 2배, SC-01 예산). Control DB commit과 AIStor/ConfigMap 포인터 + reload는 어차피 원자화되지 않는다. 실패 분기 정의 + loaded digest 기록으로 충분.
5. **`writer_kind=FAULT_INJECTION`(P0-10)** — FI-31이 만드는 외부 snapshot은 정의상 `etl.*` 키가 없으므로 그 키로 표시할 수 없다. 격리 table/namespace 또는 snapshot id allowlist만 가능.
6. **P0-02의 "watermark가 전체 window.high로 간다"** — 근거 없음. CAS는 chunk별 `high_k`이고 `FINALIZED_NO_DATA` 전이는 watermark를 옮기지 않는다. 누락이 아니라 종결 상태 오판이므로 P1.
7. **TNS address failover가 인증 실패 횟수를 늘린다(P1-14)** — 틀림. ORA-01017 같은 인증 실패는 다른 ADDRESS로 재시도하지 않는다(연결 수립 실패에만 failover). 반면 ORA-28002 경고 오분류 지적은 맞다.
8. **"ZERO_GAP은 VERIFIED 아니면 Guard 거부"(P1-15)** — 이미 규정돼 있다(신호 없는 Source는 BEST_EFFORT만 publish 가능, ZERO_GAP은 lag 불확실 시 `SOURCE_LAG_EXCEEDED`). delta는 enum 값 하나.
9. **Polaris/AIStor 분리 시험 부재(P1-17)** — FI-28 (a)/(b)가 이미 breaker key별로 분리돼 있다.
10. **새 상태 제안 전반**(`ADJUDICATION_BLOCKED`, `TARGET_UNCHANGED_EMPTY_SOURCE`, `TARGET_PROVISIONING/READY`) — 6.2 상태 집합·해제 불변식·PoC 쿼리를 모두 건드린다. 각각 만료 전제 한 줄, 메타데이터 플래그, pinned 속성으로 대체 가능.
11. **P2-05 (a)(d)** — attempt_no는 Guard 발급·이후 호출 echo로 단일 규칙이 이미 있고, rollback 재개 멱등성은 17장 (5)가 규정.

---

## 5. v1.2.1 범위 제안 (Phase 0A — Normative patch)

P0 유지 5건과 P1 중 의미론·판정력에 직결되는 항목만 v1.2.1로 묶고, 나머지 P1은 Phase 1 게이트 전(v1.2.2 또는 PoC 주차 1 DDL 부록)으로 둔다.

**v1.2.1 본문(A) — 반드시**

| 절 | 변경 |
|---|---|
| 9.3 / 6.2 / 10.2 / 13.4 | 만료 전제를 `verdict 확정`으로 (P0-01); Polaris 재시도 backoff 파라미터 |
| 13.2 / 13.4 / 6.1 | 0-row 접두부 완비 조건, NO_DATA watermark 불변식, ledger `(contract, attempt, chunk)` unique (P0-02) |
| 6.2 / 13.1 / 14.3 / 17 | `dq:accept` chunk 단위 CAS + PARTIAL_COMMIT 재개, `resolve`의 DQ_FAILED 제한 (P0-03) |
| 6.1 / 7.1 / 10.2 / 11.3 | SourceSystem `db_identity` 불변 + 연결별 검증, contract pin, driver precheck identity, Template SQL `SYS_CONTEXT` 술어, descriptor pin 시점 = 첫 Guard (P0-05, P1-13) |
| 6.1 / 7.2 / 11.3 / 17 | capability `max_commit_minus_watermark_seconds` + `bound_kind` + 3필드, ZERO_GAP rule 확장, `SCN_TO_TIMESTAMP` 문구 정정 (P0-06) |
| 11.2 / 6.1 | lease `RELEASED` 분리, grant 식 정식화, 관측 실패 fail-closed, capability에 `SQLNET.EXPIRE_TIME`/`IDLE_TIME` (P0-07) |
| 5.4 | PITR resync 5단계 프로토콜, Global Hold mode (P0-04) |
| 6.2 / 17 / 10.2 | release ACTIVE·rollback 실패 분기, `loaded_bundle_digest`, `INTERFACE_MISMATCH` 2분기 (P0-08) |
| 6.1 / 9.1 | `next_eligible_at` NOT NULL, `launch` 정의에 제출 부재 조건, per-item savepoint + `ITEM_REJECTED` (P1-01·02·03) |
| 5.1 / 10.2 | `max_retries`·`max_resume_run_attempts=0`, CANCELED 원인 3종 (P1-07) |
| 13.1 / 12.2 | 검사 1 = `written_rows`, 검사 5 pre-commit 이동, extract window·overlap 정의 (P1-11·16·23) |
| 11.4 / 12.2 / 10.2 | `chunks:begin` undo deadline, ORA-01555 규칙 통일, INITIAL_LOAD fence 예외 (P1-12) |
| 6.2 / 10 / 16.4 / 22장 9번 | ORA-28002 제거·`CREDENTIAL_EXPIRING`, 상한식 항 추가, `ADG_ACCOUNT_INFO_TRACKING` (P1-14) |
| 14.1 / 14.2 | `REJECTED_AT_GUARD`를 catch-up 대상에, 겹침 의미·open unique (P1-05) |
| 6.1 '구현 규약' 소절 | lock 순서 + 제약 9종 (P1-19, P1-22 DDL 부록과 합본) |
| 9.3 / 19장 / 12.1 / 14.2 / 9.1 / 17 | P2 전부(B1 근거 교체, 문서 우선순위 반전, schedule target, `versions.lock`, 오류 표 분리, CATCHUP 부하식, `on_empty_source`) |

**기준서 5차(P) — 반드시**

- §2.3 stub 가시성 모델 교체(P0-09) + ≤5초 세션 종료 구성화(P0-07)
- §5.1 4층 oracle로 교체, §5 "Snapshot metadata 100%" 분모 allowlist, ZERO_GAP No-Go 행에 비교 A 연결 (P0-10)
- §8.3 'Scale/Control Go' / 'Oracle ZERO_GAP Go' 분리, §2.1에 DG 시험 인스턴스·`versions.lock` (P1-21, P2-04)
- 신규·확장 시험: FI-05b, FI-09c, FI-23c, FI-24b 확장, FI-28 변형(outage > expires_at), FI-37a/b/c, FI-40~48, SC-02b/c, SC-04b, FI-06 변형(cancel·max_runtime), FI-16(28002), FI-27(identity)
- DDL 부록 + §5.1 dry-run을 주차 1 산출물로
- 머리말 "기준서 우선" 문장 삭제(P2-02)

**Phase 1 게이트 전(v1.2.2 / 구현 규약)** — P1-04(Pipes 재생 reason 확정), P1-06(`source_coverage_lag`), P1-08(ownerReference 금지), P1-09(publish lease), P1-10(`orphan_min_age`), P1-15(`NO_LAG_SIGNAL`), P1-17(table UUID pin), P1-20(fence precursor 비교안). **Phase 3 게이트** — P1-18 cutover runbook.

---

## 6. 결론

- Codex 리뷰는 v1.2의 안전성 경계를 정확히 짚었다. 특히 P0-01·03·05·06·10은 **silent duplicate / silent gap / 판정 불능**으로 이어지는 실제 결함이며 PoC 전에 닫아야 한다.
- 처방은 절반 이상 축소 가능하다 — 새 상태·새 메커니즘 없이 기존 상태·필드·경로의 문장 수정으로 같은 불변식을 얻는다. 이것이 v1.2.1의 원칙이다.
- 의사결정: **방향 GO 유지, v1.2.1 작성 후 의미론 동결, Phase 0 baseline은 즉시 병행, Phase 1 semantic PoC는 v1.2.1 + 기준서 5차 이후.**
