# Codex 2차 교차 리뷰(v1.2.1) 검토서

- 검토일: 2026-08-23
- 검토 대상: `etl-platform-v1.2.1-codex-second-cross-review.md` (P0 10 / P1 15 + 이전 41건 폐쇄 매트릭스)
- 대조 문서: `etl-platform-target-architecture-v1.2.1.md`(A), `etl-platform-poc-test-plan-v1.md` 5차(P)
- 방법: 34개 판정 단위(P0 10 — 8·10은 세부 분리 — + P1 15 + 매트릭스 REGRESSED 2·OPEN 2)를 A·P 본문 인용과 Oracle/Iceberg/Spark 기술 전제 확인으로 재검증. 1차 검토서와 같은 원칙: 결함은 채택하되 처방은 축소안.

---

## 1. 총평

**Codex의 결정표(방향 GO / v1.2.1 동결 NO-GO / 기준서 5차 동결 NO-GO / BEST_EFFORT 조건부 GO)에 동의한다.** 34건 중 **확인 19, 부분 확인 15, 기각 0**. 1차와 같은 구도다 — 지적은 전부 실재하고, 처방의 절반은 기존 장치로 더 작게 닫힌다.

이번 리뷰에서 가장 뼈아픈 것은 **제 회귀 2건**이다:
- **P0-02** `SYS_CONTEXT('USERENV','CON_DBID')`는 Oracle 19c USERENV 속성이 아니다(ORA-02003). v1.2.1에서 제가 도입한 identity 검증이 첫 연결 테스트부터 실행 불가다. 다만 fail-closed라 silent 경로는 아니다 → 매트릭스 상태는 REGRESSED가 아니라 PARTIAL이 정확하나, 수정은 P0로 처리.
- **P0-04** overlap을 "최초 attempt chunk 1에만" 적용한 v1.2.1 규칙은 attempt 1이 첫 CAS 전에 `NO_COMMIT`으로 끝나면 attempt 2가 overlap을 잃는다 — ZERO_GAP 계약의 silent gap. 조건을 attempt 번호가 아니라 `window.low == original_logical_low`(durable coverage)로 바꾸면 닫힌다.

P0 10건의 등급 재판정: **P0 유지 4**(01 late-apply commit, 02 CON_DBID, 04 overlap coverage, 10 oracle 모순), **P1 하향 6**(03 reservation gap — token pool 회계가 이미 막고 잔존 결함은 비태그 세션 조합; 05 PER_CHUNK_FENCE — 누락 집합이 경계 근처로 한정되고 Audit 필수; 06 serving pointer — 잘못된 코드 쓰기 경로 없음; 07 cross-type lease — Iceberg serializable·lineage가 silent 경로를 막음; 08 ZERO_GAP hard bound — Audit 회수; 09 PITR — 문서가 이미 conservative 의미를 택함).

처방에서 기각한 것(1차 §4와 같은 이유): catalog gateway + `commit_intent_id` journal(P0-01), `EXTRACT_ONCE`-only(P0-05), candidate code location·activation barrier(P0-06), exact recovery journal/RPO 0(P0-09), `run_submission` intent 테이블(P1-01), maintenance fencing generation(P1-06), sentinel 메타데이터(P1-08), 세 clock 분리(P1-04), version/topology digest(P1-13). 각각의 경량 대체안은 §2·§3에 있다.

```text
Architecture direction:          GO              — 동의
v1.2.1 semantic freeze:          NO-GO           — 동의. v1.2.2(semantic patch)로 P0 4 + P1 핵심을 닫는다
PoC 5차 acceptance oracle freeze: NO-GO          — 동의. FI-40·SC-02b·§5.1 (2)·ORA_HASH·FI-23c 5건은 자기 규칙으로 판정 불가
ZERO_GAP 제품 약속:               NO-GO(현 정의)  — 동의. 'ENFORCED + audited'임을 명시하거나 heartbeat witness로 hard화
BEST_EFFORT skeleton PoC:        조건부 GO        — 동의. scheduler/runtime 공통 P0(01·02·04·10) 수정 후
```

---

## 2. P0 10건 재판정

| ID | 판정 | 등급 | 핵심 근거 | 채택 방식 — v1.2.2 변경 |
|---|---|---|---|---|
| P0-01 Pod 부재 ≠ Polaris commit 종료 | **확인** | P0 유지 | 13.2 1번 원칙은 맞게 쓰고도 pod 부재를 충분조건으로 씀; head-settle은 5.4 PITR에만 있음("old writer의 in-flight commit이 착지한 뒤에 읽어야 … 유일한 순서"를 일반 Adjudication에 미적용), `adjudication_delay_seconds 0`. 기존 13.1 lineage는 3개 sub-case 중 1개만 탐지(old snapshot이 attempt 2의 base 읽기와 commit 사이에 착지), 나머지 2개(base 읽기 전 착지·finalize 후 착지)는 base에 흡수돼 silent. Iceberg `CommitStateUnknownException` 의미·Polaris 서버 측 처리시간 bound 부재 — 리뷰의 대안(서버 강제 상한)도 실현 불가 | ADOPT_MODIFIED — gateway/intent journal 대신 (a) 13.2 2번에 **head-settle**(5.4 (3)과 동일: `target_health_timeout_seconds` 간격 2회 연속 같은 `current-snapshot-id`) 필수, `adjudication_delay_seconds 0`을 이 규칙으로 대체; (b) attempt `adjudicated_head_snapshot_id`; (c) `chunks:begin(1)`의 **base 연속성 검사** — base가 '마지막 ledger `committed_snapshot_id`와 직전 attempt의 `adjudicated_head` 중 최신'과 다르면 그 구간 lineage를 13.1 분류로 판정(다른 attempt의 ingest → SA 생성 전 `RECONCILIATION_REQUIRED`). FI-50 합격식 = 'late apply가 언제 착지하든 중복 구간 CAS 0 ∧ RECONCILIATION_REQUIRED 정확히 1 ∧ repair 뒤 diff 0' |
| P0-02 `CON_DBID` 부재 | **확인** | P0(실행 불가) / 데이터 위험 P1 | A 6.1·10.2·11.3·12.3, P §2.3·FI-42에 `SYS_CONTEXT('USERENV','CON_DBID')`. 19c USERENV에는 `CON_ID`·`CON_NAME`·`CDB_NAME`만 있고 PDB `DBID`·`CON_UID`·`GUID`는 `V$CONTAINERS`/`V$PDBS`. PDB 안의 `V$DATABASE.DBID`는 CDB DBID → 같은 CDB의 PDB 구분 불가 | ADOPT — `db_identity = {cdb_dbid, db_unique_name, resetlogs_change_no, pdb_dbid, pdb_con_uid, pdb_guid}`, PDB tuple은 `V$CONTAINERS WHERE con_id = SYS_CONTEXT('USERENV','CON_ID')`, Template 술어는 `CON_NAME`(USERENV 존재)로, GUID는 init 블록이 검사, non-CDB(`CON_ID=0`)는 `NOT_APPLICABLE` 명시, 11.2 DBA 뷰 목록에 `V$CONTAINERS`, FI-42e(같은 CDB·다른 PDB) |
| P0-03 미실현 reservation·정상 finalize RELEASED·RESOURCE_LIMIT | **부분** | **P1로 하향** | v1.2.1 grant 식은 `observed + requested ≤ pool_cap`만 썼지만 6.2·11.2·FI-17(d)가 Σ 미반환 token 회계를 전제하므로 리뷰 시나리오(A·B 각 4 grant)는 token pool에서 `LEASE_BUSY`. 잔존 반례: 미실현 token + 비태그 legacy 세션 조합의 과소계산. 정상 finalize 즉시 `RELEASED`는 정의 불일치(안전 구멍 아님 — `observed`가 잔존 세션을 봄). `RESOURCE_LIMIT` 증거 누락은 확인(19c 기본 TRUE) | ADOPT_MODIFIED — grant 식을 `observed + Σ_t max(0, weight_t − tagged_observed_t) + requested ≤ pool_cap`으로 명문화(`guard_result.lease_grant`에 `reserved_unrealized` 추가); 리뷰의 보수식(`unreleased + observed + requested`)은 실현 token 이중 계산이라 기각; 정상 finalize도 `RECLAIMED → probe → RELEASED`로 통일(COMPLETED 경로만 `lease_release_zero_count = 1`); capability 증거 4항(`RESOURCE_LIMIT=TRUE`, 실제 `SESSIONS_PER_USER`, PDB/RAC 적용 범위, cap+1 ORA-02391 양성 대조); FI-43c |
| P0-04 첫 CAS 전 실패 시 overlap 소실 | **확인** | P0 유지 | 12.2 "`extract_low = original_logical_low − overlap`은 계약의 최초 attempt chunk 1에만" — 근거 문장("같은 visible_scn 안에서는 late commit 없음")은 attempt 1이 overlap 구간을 commit했을 때만 성립. NO_COMMIT 뒤 attempt 2(low = L, CAS 0)는 미규정 → 문자 해석상 `extract_low = L`. FI-47(b)는 chunk 1 commit 뒤 재개만 시험 | ADOPT — 조건을 `apply overlap iff contract.window.low == original_logical_low`로(low는 CAS로만 전진하므로 'chunk-1 구간 CAS 없음'과 동치, 별도 조회 불필요); Guard 응답에 `extract_window_low`를 Control이 계산; 12.2·P §3.2·FI-47 정정, FI-47c |
| P0-05 PER_CHUNK_FENCE mixed snapshot | **확인** | **P1로 하향**(ZERO_GAP publish 조건으로는 freeze 전 필수) | 12.2 PER_CHUNK_FENCE는 완료 chunk로의 late commit 재독 단계가 없고 11.4("모든 INITIAL_LOAD는 extract-once 필수")와 모순. 누락 집합은 `UPDATE_DT ≥ fence_ts_1 − overlap`인 경계 row로 상한(ENFORCED+DB_TRIGGER면 오래된 chunk 범위의 UPDATE_DT로 지금 commit 불가), 12.3 Audit 필수 | ADOPT_MODIFIED — `EXTRACT_ONCE`-only는 기각(P1-12가 PER_CHUNK를 도입한 undo 예산 이유를 되돌림). chunk 목록 끝에 **final sweep chunk** `[fence_ts_1 − overlap, wm)`(`AS OF visible_scn_last`, 기존 anti-join/PK dedup, ledger 형식 동일) 고정; sweep 불가(`delete_semantics = NONE_DECLARED` 등)면 validator가 BEST_EFFORT 강등; 11.4 정정; FI-44e 축소 |
| P0-06 serving pointer·effective_from | **부분** | **P1로 하향** | 시나리오 B(미래 `effective_from` → 오늘 tick R1 pin → Guard 5번 (b) 더 새 ACTIVE 있음 → VOID)는 문서 그대로 성립. 시나리오 A는 과장 — 16.4 lateness sensor가 Control 측 R1 cron으로 누락을 잡음(자동 복원만 없음). 잘못된 코드 쓰기 경로 없음(pinned plan + Guard 5번) | ADOPT_MODIFIED — candidate code location·activation barrier 기각(1차 §4-4). (1) 6.2 ACTIVE `effective_from = now()` 고정(rollback과 동일, 미래 activation 금지 → 시나리오 B 소멸); (2) release operation 말미에 `scope={shard}, range=[deployed_at − 1 period, now()]` 자동 Gap Recovery(같은 operation·단일 코드 경로) → 시나리오 A 자동 복원; FI-24c 축소 |
| P0-07 cross-type target lease DB 제약 | **확인** | **P1로 하향** | 6.1 (9)가 "EXCLUSIVE_TABLE ↔ 그 외 조합은 13.3 try-lock이 맡는다"고 공백을 인정; 13.3에 canonical lock key·conflict matrix 없음 → READ COMMITTED write skew. 단 silent 아님: Iceberg `serializable` + `validate-from-snapshot-id`(Full 뒤 Append는 ValidationException), 13.1 lineage(Append 뒤 Full은 `RECONCILIATION_REQUIRED`) | ADOPT — 6.1 lock 순서에 `watermark → target_table(table_id) row FOR UPDATE(또는 advisory) → target lease → source token`; 13.3 conflict matrix 한 줄(EXCLUSIVE_TABLE은 같은 table의 모든 non-RELEASED와 충돌, PARTITION_OR_FILESET끼리는 range && 또는 정규화 fileset key 교집합, APPEND는 EXCLUSIVE와만); 검사와 lease insert를 그 lock 아래 한 트랜잭션; FI-51(3쌍 × 100회, grant 정확히 1) |
| P0-08-A SCN_TO_TIMESTAMP hard bound | **부분** | P1(데이터) / 정의 확정은 freeze 전 | 11.3은 "3초는 상한·방향 보장 아님"을 인정하면서 실측값으로 ZERO_GAP 허용 — 즉 현 정의는 'ENFORCED + audited'이지 수학적 hard가 아님. 리뷰의 'DBA 집행 directional bound'는 Oracle에 장치가 없어 성립 불가, 'SCN/CDC offset 한정'은 기본 cutoff에서 ZERO_GAP 제거 | ADOPT_MODIFIED — **primary heartbeat witness**: DBA 소유 `ETL_HEARTBEAT(ts)` 1행을 primary 스케줄러가 3~5초마다 SYSTIMESTAMP로 갱신, Guard 6번이 `SELECT ts … AS OF SCN :visible_scn`을 `T_lb`로 읽어 `high = min(T_lb, SYSTIMESTAMP_standby) − safety_lag` — visible heartbeat는 visible_scn 이전 commit이라 방향 보장(오차는 보수 방향). capability `fence_time_witness ∈ {HEARTBEAT_TABLE, SCN_TO_TIMESTAMP}`, rule에 HEARTBEAT_TABLE 추가, SCN_TO_TIMESTAMP만이면 BEST_EFFORT. stub은 heartbeat row 1행. 대안(보수): 'ZERO_GAP(audited)' relabel |
| P0-08-B delete semantics | **부분** | P1 | `CDC_LATER`는 삭제 경로가 없어 ZERO_GAP 허용이 구멍(확인). `PK_RECONCILE`은 선언된 bounded-lag 경로 → 금지가 아니라 interval 노출. 비교 A/B는 설명된 차이로 hard delete를 이미 보지만 양방향 명시 부족 | ADOPT_MODIFIED — rule에 `delete_semantics ∉ {NONE_DECLARED, CDC_LATER}`; 데이터 계약에 `delete_semantics`·`PK_RECONCILE.interval` 병기("ZERO_GAP = window 내 insert/update 완전성, delete는 선언 경로의 bounded lag"); §3.2 양방향 차집합 명시. finalize 안 동기 delete reconciliation은 기각(계약마다 Source PK 전수 스캔 — 11.1 용량·10.2 "유일한 lag 조회" 원칙 충돌) |
| P0-09 PITR exact vs conservative | **부분** | **P1로 하향** | 문서는 이미 conservative(0-row CAS 후퇴 허용)를 택함. 실체는 (i) FI-05(exact 일치) vs FI-05b(후퇴 허용) 기대집합 모순, (ii) Outbox event_id 결정론이 3종만 명시, (iii) resync 단계 재개 규칙 부재 | ADOPT_MODIFIED — 선택지 1(별도 failure domain journal/RPO 0)은 기각(4장 '유일한 권위' 원칙). 문장 4개: 16.4 일반 규칙 `event_id = hash(aggregate_id, transition key, event_type)`(무작위 금지); FI-05 기대집합을 '0-row CAS 제외·후퇴 방향만 허용(conservative)'으로; 5.4 resync operation row에 완료 step 기록 + 재기동 시 재개; 18장 retention 하한 `≥ RPO + RTO + margin`; FI-05c(단계별 직후 재사망) |
| P0-10 oracle 모순 (A~E) | **확인**(A·B·C·D) / **부분**(E) | P0 유지(기준서) | A: stub `apply lag = now − commit_ts(visible_scn)`이면 idle 10분 = lag 10분 → FI-40 idle 합격 불가(실 Oracle은 heartbeat redo로 lag ≈ 0 — stub이 틀림). B: SC-02b 합격(거부 Job occurrence 0)과 No-Go 2(expected 키당 1개)가 양립 불가. C: §5.1 (2) Job 단위 SA 겹침 0이 FI-20(f) 정상 backfill(SA 2개)을 오탐. D: `ora_hash(row)`는 유효 SQL 아님(`row` pseudo-column 없음, 단일 expr). E: ledger actor `OPERATOR`가 P §2.4에 없음, FI-23c가 'attempt 2 chunk 3·4'로 13.4·§5.1 (3)(a) attempt별 재번호와 모순; DDL 부록 부재는 결함이 아니라 주차 1 산출물 | ADOPT_MODIFIED — A: stub에 3초 system tick(row 없음) 스트림 추가, 커서는 tick 포함 마지막 항목, idle = table commit 0; RedoEvent/gap set은 기각(소비 가설 없음). B: `REJECTED_AT_SCHEDULER` occurrence는 기각(거부 원인이 occurrence row 제약 위반일 수 있어 같은 savepoint에서 생성 불가) → Outbox `occurrence item rejected(job_id, logical_at)`을 §5.1 (1)의 '설명된 누락'으로 편입, 주입 건 외 0. C: (2)(a)(b)에 (3)(b)의 lane 분리 적용(ingest lane vs repair/backfill lane). D: `STANDARD_HASH(canonical_concat, 'SHA256')` 권위 + canonicalization 규칙 + 양방향 PK 차집합, 불일치 PK만 typed exact. E: actor 추가, FI-23c를 'attempt 2 chunk 1·2(`window_low = high_2`)'로 |

---

## 3. P1 15건 판정

| ID | 판정 | 등급 | 채택 방식 |
|---|---|---|---|
| P1-01 check→launch→record 경쟁 | 확인 | P1 | `run_submission` 테이블은 기각(1차 §4). contract row에 `submission_in_flight{resubmit_no, run_id, at}`를 **commit한 뒤** `launchRun`, Dagster `executionMetadata.runId = uuid5(contract_id, resubmit_no)`로 멱등 제출, 9.1 `launch`·Adapter 사전검사에 in-flight 조건. '중복 queued Run 500개'는 과장(경쟁 주체는 Control 루프 2~3개) |
| P1-02 만료된 known-verdict 계약의 RETRY 경쟁 | 확인 | P1 | Guard 1번·RETRY (a)의 inline expiry 범위를 'verdict 확정 `ADJUDICATION_PENDING`'까지 확장(verdict NULL은 여전히 만료 안 함); FI-22(a) 순서 변형 |
| P1-03 DQ partial accept 뒤 attempt 상태·chunk 번호 | 확인 | P1 | `DQ_SEALED` 신설 없이 `finalize{DQ_FAILED \| RECONCILIATION_REQUIRED \| CANCELLED_AT_SAFEPOINT}` → attempt `TERMINAL_OBSERVED → FENCED`, `dq:accept`가 `FENCED → ADJUDICATED`; FI-23c 재번호; P §2.4 actor `OPERATOR` |
| P1-04 freshness 단일 지표 | 부분 | **P2** | 세 clock 분리 기각(1차 §4-10). `FINALIZED_NO_DATA`에 `target_unchanged` 플래그 + read model 파생 2개(target publication age = 마지막 `committed_snapshot_id`의 `cas_at`, coverage = `window.high`); `covered_by_contract_id`는 `COALESCED_INTO`가 이미 표현 |
| P1-05 WAP branch 미publish 통과 | 부분 | P1 | `fast_forward`는 non-ancestor면 실패 → 그 자체가 base-main CAS이고 lease는 attempt 내내 보유(반 맞음). 빠진 것: WAP 경로의 CAS·ledger 기준. `chunks/{n}:commit`은 ff 성공 뒤에만, ledger `committed_snapshot_id = published_main_snapshot_id`, ff 실패는 `attempt-failure` 계열(main 불변); §5.1 (3)(a)·비교 A에 main-ancestor 조건; H-06 concurrent main commit 변형 |
| P1-06 maintenance fencing·보호집합 | 부분 | **P2** | fencing generation 기각(driver→Polaris 직접 commit이라 TOCTOU; 물리 fencing + 13.1/13.2 lease 구간 분류가 이미 판정). 18장에 active attempt branch ref 보호 한 줄, SC-11에 expire/orphan/branch/하한 미만 422 fixture(P §7이 이미 인용하나 행에 없음) |
| P1-07 Merge DQ delete action·버전 | 확인 | P1 | 식을 `written_rows == inserted + updated + deleted + ignored`로; 버전 gate 실패 시 application-side action counter fallback(`dq_basis = APP_COUNTER`) — Merge 전면 차단 해제; `versions.lock` 확정 전 metric gate 금지 |
| P1-08 init 성공 뒤 role 전환 → boolean 0-row | 부분 | P1(조건부) | 창은 init~첫 SELECT 사이 초 단위이고 Oracle이 전환 시 read-only 세션을 유지하는지 미검증. sentinel 메타데이터 기각(JDBC partition 쿼리에 실을 곳 없음). 술어를 예외 발생식(`UNION ALL … FROM DUAL WHERE CASE … THEN TO_NUMBER('SOURCE_ROLE_MISMATCH') …` → ORA-01722, 또는 DBA 허용 시 assert 함수)으로; FI-45d; DG G2 항목에 'role 전환 시 세션 종료 여부' |
| P1-09 credential 상한식의 physical fan-out | 확인 | P1 | `worst_case_inflight_physical_auth_attempts` 신규 필드 기각 — envelope에 이미 '최대 총 JDBC connection weight'가 있음. 상한식 항을 그것으로 교체, `PASSWORD_ROLLOVER_TIME ≥ max_runtime` 등록 Source만 Job 수 기준 완화 |
| P1-10 fence origin·실행 revision 감사 | 부분 | **P2** | fence origin은 contract pin(불변)으로 이미 감사 가능; attempt에 `connection_revision_id` 1필드만 추가 |
| P1-11 cutover watermark 캡처 시점 | 확인 | P1(Phase 3 게이트) | 22장 16번 순서 재배열(DRAIN → old writer 정지·세션 0 → 캡처 → seed → reconcile → lease 이전 → 해제); FI-48 주입 1건 |
| P1-12 Gap Recovery HA owner | 부분 | **P2** | cursor·unique key는 기존. 5.4에 Control scheduler 단일 프로세스(replicas=1 또는 advisory leader)·재개 규칙 한 단락 + `(scope, IN_PROGRESS)` partial unique |
| P1-13 `zero_gap_verified` versioned evidence | 부분 | **P2** | bool 유지 + `zero_gap_evidence` jsonb(g2_report_id, db_identity, capability digest); `db-identity:rotate`·capability 변경 시 자동 false. version/topology digest는 기각(`versions.lock` 별도) |
| P1-14 SC-04b conservation 이중 계산 | 확인 | P1(기준서) | `runnable`에 'non-terminal run 없음' 추가로 bucket 서로소화, `submitted_queued` 신설, 식 교체 |
| P1-15 FI-49 경합 단계가 Guard 6번 미도달 | 확인 | P1(기준서) | 경합 시험을 H1 생성 전(또는 H1 밖 Source)으로 이동 |

---

## 4. 이전 41건 폐쇄 매트릭스에 대한 입장

- CLOSED 18·PARTIAL 19는 대체로 동의한다.
- **REGRESSED 2 → PARTIAL로 정정**: P0-05(`CON_DBID`)는 결함이 실재하지만 ORA-02003으로 첫 연결 테스트부터 fail-closed라 안전 회귀가 아니다(수정은 P0). P1-22(enum·chunk 번호·ORA_HASH)는 P1-22의 폐쇄 장치(DDL 부록 + dry-run)가 바로 이런 drift를 잡기 위한 주차 1 게이트이므로 PARTIAL이 맞다 — 단 dry-run은 enum 참조만 잡고 chunk 의미·`ORA_HASH`는 못 잡으므로 문구 3건은 지금 고친다.
- **OPEN 2는 사실**: P1-06(freshness)은 1차 검토서가 v1.2.2로 예정한 이연 항목(P2 유지), P1-09(WAP concurrent main)는 1차 권고(publish 구간 EXCLUSIVE 승격 + ref 증거 + H-06 변형)가 v1.2.1에 반영되지 않은 누락 — 인정.

---

## 5. 리뷰에서 기각하거나 축소한 주장

1. **catalog gateway + `commit_intent_id` journal(P0-01)** — 쓰기 경로에 새 구성요소(epoch형). late landing을 막을 서버 측 bound가 없으므로 목표는 '방지'가 아니라 '다음 쓰기 전에 탐지' — head-settle + `adjudicated_head_snapshot_id` + `chunks:begin(1)` base 연속성 검사로 기존 13.1 분류를 재사용.
2. **`EXTRACT_ONCE`-only INITIAL_LOAD(P0-05)** — P1-12가 PER_CHUNK_FENCE를 도입한 이유(장시간 적재의 undo 예산)를 되돌림. final sweep chunk 하나로 닫힌다.
3. **candidate code location·activation barrier(P0-06)** — 1차 §4-4 그대로(VERIFIED가 실제 로드를 요구 → shard 메모리 2배; Control commit과 포인터+reload는 어차피 비원자). `effective_from = now()` 고정 + release 말미 자동 Gap Recovery로 충분.
4. **DBA 집행 directional error bound(P0-08-A)** — Oracle이 `SCN_TO_TIMESTAMP`에 제공하는 장치가 없어 성립 불가. heartbeat witness가 같은 효과를 방향 보장으로 낸다.
5. **finalize 안 동기 delete reconciliation(P0-08-B)** — 계약마다 Source PK 전수 스캔, 11.1 용량 모델·10.2 "Guard 6번이 유일한 lag 조회" 원칙과 충돌. `PK_RECONCILE` first-class Asset이 lease 체계 안에서 같은 효과.
6. **exact recovery journal / RPO 0(P0-09)** — Control PG 외 제2 권위 저장소(4장 위반). 문서는 이미 conservative.
7. **`REJECTED_AT_SCHEDULER` occurrence(P0-10-B)** — 거부 원인이 occurrence row 자체의 제약 위반일 수 있어 같은 savepoint에서 생성 불가. Outbox row가 이미 expected key와 1:1 disposition.
8. **typed columns 양방향 exact를 권위로(P0-10-D)** — Oracle I/O는 hash와 같고 전송량만 증가. `STANDARD_HASH(SHA256)`은 충돌 논거 소멸.
9. **`run_submission` intent 테이블(P1-01)** — 1차 §4 기각의 재제안. contract 컬럼 + deterministic runId로 충분.
10. **보수식 `unreleased + observed + requested`(P0-03)** — 실현된 token을 이중 계산해 유효 cap을 반토막.
11. **maintenance fencing generation(P1-06)·sentinel 메타데이터(P1-08)·세 clock(P1-04)·version/topology digest(P1-13)·`worst_case_inflight_physical_auth_attempts`(P1-09)** — 각각 기존 장치·필드로 대체.
12. **"Run/Guard가 없으므로 잡을 수 없다"(P0-06 시나리오 A)** — 16.4 lateness sensor가 Control 측 cron 전개로 잡는다(자동 복원만 없음). **"무작위 event id"(P0-09)** — 16.4는 3종을 결정론으로 명시, 나머지는 공백(무작위 단정은 근거 없음).

---

## 6. v1.2.2 범위 제안 (semantic patch — 기능 추가 없음)

**A(v1.2.2) — P0 4 + P1 핵심**

| 절 | 변경 |
|---|---|
| 13.2 / 6.1 / 10.2 | Adjudication head-settle 필수, `adjudicated_head_snapshot_id`, `chunks:begin(1)` base 연속성 검사 (P0-01) |
| 6.1 / 7.1 / 10.2 / 11.2 / 11.3 / 12.3 / 22장 9번 | `db_identity` v2(PDB tuple, `V$CONTAINERS`, `CON_NAME` 술어, `NOT_APPLICABLE`) (P0-02) |
| 12.2 / 10.2 | overlap 조건 = `window.low == original_logical_low`, Guard 응답 `extract_window_low` (P0-04) |
| 11.2 / 10.2 / 6.1 / 22장 9번 | grant 식에 `reserved_unrealized`, finalize도 RECLAIMED 경유, capability 증거 4항(RESOURCE_LIMIT 등) (P0-03) |
| 12.2 / 11.4 / 7.2 | PER_CHUNK_FENCE final sweep chunk + 강등 규칙 (P0-05) |
| 6.2 / 8.2 / 9.3 | `effective_from = now()` 고정, release 말미 자동 Gap Recovery (P0-06) |
| 6.1 / 13.3 | canonical target table lock + conflict matrix (P0-07) |
| 11.3 / 6.1 / 7.2 / 17 | heartbeat witness(`fence_time_witness`), `delete_semantics ∉ {NONE_DECLARED, CDC_LATER}`, 계약 노출 항목 (P0-08) |
| 16.4 / 5.4 / 18 | event_id 결정론 일반 규칙, resync step 재개, retention 하한 (P0-09) |
| 9.1 / 9.3 / 6.1 | `submission_in_flight` + deterministic runId (P1-01) |
| 10.2 / 14.3 | known-verdict inline expiry (P1-02) |
| 6.2 | DQ 뒤 attempt 전이(FENCED 경유) (P1-03) |
| 13.1 / 13.2 | WAP publish 규칙·`published_main_snapshot_id` (P1-05, 1차 이연분 포함) |
| 13.1 | Merge DQ 식 + APP_COUNTER fallback (P1-07) |
| 12.3 / 11.3 / 22장 9번 | 예외 발생식 identity 술어 (P1-08) |
| 6.2 / 22장 9번 | 상한식 항 = JDBC connection weight (P1-09) |
| 22장 16번 / 20장 | cutover 순서 재배열 (P1-11) |
| 소규모(P2, 함께 반영) | `target_unchanged` 플래그·read model 2개(P1-04), branch ref 보호(P1-06), attempt `connection_revision_id`(P1-10), scheduler 단일성 단락(P1-12), `zero_gap_evidence` jsonb(P1-13) |

**P(6차) — oracle 모순 5건 + 신규 시험**

- §2.3 stub system tick 모델, FI-40 양성 증거 정정 (10-A)
- §5.1 (1) `occurrence item rejected` 편입, (2) lane 분리, (4)·비교 A/B `STANDARD_HASH` + 양방향 (10-B·C·D)
- §2.4 actor `OPERATOR`, FI-23c 재번호, FI-22(a) 순서 변형, FI-49 단계 순서, SC-04b bucket 식, FI-05 conservative 기대집합 (10-E, P1-02·03·14·15, P0-09)
- 신규: FI-50(late apply), FI-42e(same-CDB wrong-PDB), FI-43c(reservation gap + linger), FI-47c(pre-first-commit retry), FI-44e(sweep 경계), FI-24c(activation boundary), FI-51(cross-type lease), FI-41e/f(witness·delete), FI-05c(resync restart), FI-45d(post-init role switch); SC-11 fixture 보강; FI-16·FI-48 변형
- §2.1 capability 행에 `RESOURCE_LIMIT`·heartbeat table·`V$CONTAINERS` 권한

**주차 1 executable appendix** — 2차 리뷰 §6.3 목록을 그대로 채택(PDB identity·canonical target lock·reservation accounting·submission in-flight·DQ attempt 전이·ledger actor·attempt-local chunk identity·WAP refs·deterministic event key·resync phase marker·freshness read model).

---

## 7. 결론

- 2차 리뷰는 v1.2.1의 새 구멍을 정확히 짚었다. 특히 **P0-02(제 회귀)·P0-04(overlap coverage)·P0-01(late-apply commit)·P0-10(oracle 자기모순)**은 semantic freeze 전에 반드시 닫아야 한다.
- 나머지 6건의 P0는 P1 등급이 맞고, 처방은 1차와 같은 원칙(새 메커니즘 없이 기존 상태·필드·경로 재사용)으로 축소 채택한다.
- 의사결정: **방향 GO 유지, v1.2.1 동결 보류, v1.2.2 semantic patch(§6 범위) + 기준서 6차 + 주차 1 executable appendix 순으로 진행. 그 전에는 BEST_EFFORT skeleton 외 안전성 Go를 선언하지 않는다.**
