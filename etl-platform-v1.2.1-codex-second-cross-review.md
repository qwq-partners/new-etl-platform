# ETL Platform 목표 아키텍처 v1.2.1 — Codex 2차 교차 리뷰

- 검토 대상 A: `etl-platform-target-architecture-v1.2.1.md`
- 검토 대상 P: `etl-platform-poc-test-plan-v1.md` 5차
- 이전 검토: `etl-platform-v1.2-codex-cross-review.md`
- 검토 일자: 2026-08-23
- 표기: `A:284`는 아키텍처 284행, `P:263`은 PoC 기준서 263행을 뜻한다.
- 검토 원칙: 문서의 의도보다 실제 전이·락·DB 제약·실행 가능한 판정식이 보장하는 결과를 우선한다.

---

## 1. 최종 판정

### 1.1 한 줄 결론

**Dagster 중심 목표 방향과 Control Plane 분리는 유지해도 된다. 그러나 v1.2.1을 semantic freeze하거나 현재 PoC 5차를 안전성 합격 기준으로 확정하는 것은 `NO-GO`다.**

v1.2.1은 이전 문서보다 분명히 좋아졌다. 특히 unknown commit 만료, 0-row 접두부, `dq:accept`, terminal ownership, 실제 Oracle 별도 gate는 상당히 정교해졌다. 다만 다음 종류의 결함이 남았다.

1. 정상적으로 보이는 경로에서 데이터 누락 또는 이중 commit이 가능한 반례
2. Oracle 19c에서 실제로 존재하지 않는 식별자 사용
3. Source hard cap과 target writer exclusion을 DB가 증명하지 못하는 경쟁 조건
4. release 활성화와 schedule serving의 비원자성
5. 자체 규칙으로 통과할 수 없거나 반대로 잘못 통과하는 PoC oracle

### 1.2 의사결정 요약

| 결정 대상 | 판정 | 이유 |
|---|---|---|
| Dagster + Control Plane + Spark Operator 방향 | **GO** | 10,000 Job을 개별 Python/DAG 파일로 만들지 않는 grouped definition 방향, 중앙 계약·lease·ledger 방향은 적절하다. |
| Control/Registry/UI 기본 구현 착수 | **조건부 GO** | P0 수정과 충돌하지 않는 CRUD, immutable version, audit, read-only UI부터 시작할 수 있다. |
| v1.2.1 semantic freeze | **NO-GO** | 신규/잔존 P0가 실제 누락·중복·원천 한도 초과를 허용한다. |
| PoC 5차 acceptance oracle freeze | **NO-GO** | FI-40, SC-02b, concurrency oracle, 비교 SQL이 서로 모순되거나 실행 불가능하다. |
| `ZERO_GAP` 제품 약속 | **NO-GO** | PDB identity, SCN→timestamp hard bound, delete semantics, 실 Oracle 증거 수명주기가 완결되지 않았다. |
| `BEST_EFFORT` 한정 skeleton PoC | **조건부 GO** | 아래 P0 중 scheduler/runtime 공통 결함을 먼저 고치고 feature flag로 격리할 때만 가능하다. |

### 1.3 이전 41건 폐쇄 요약

원 지적의 정확한 범위를 기준으로 다시 판정하면 다음과 같다.

| 상태 | 건수 | 의미 |
|---|---:|---|
| CLOSED | 18 | 원 지적의 핵심 불변식이 본문에 규범적으로 들어갔다. 단, 인접한 신규 결함은 별도로 집계했다. |
| PARTIAL | 19 | 방향은 반영됐으나 반례·DB 제약·PoC 증명이 남았다. |
| OPEN | 2 | 핵심 해결 규칙이 아직 없다. |
| REGRESSED | 2 | 수정 과정에서 문서/Oracle 호환성 또는 스키마 정합이 새로 깨졌다. |
| 합계 | 41 | 이전 P0 10 + P1 23 + P2 8 |

신규·잔존 결함은 본 검토에서 별도로 **P0 10개 묶음, P1 15개 묶음**으로 정리했다. 묶음 하나 안에 상호 의존하는 세부 결함이 둘 이상 있을 수 있다.

---

## 2. v1.2.1에서 확실히 좋아진 점

아래 항목은 다음 개정에서도 반드시 보존해야 한다.

1. **판정 불명 계약을 만료시키지 않는다.** `WRITER_FENCED`만으로는 부족하고 verdict와 부분 CAS가 확정돼야 만료한다(A:269, A:652). Polaris 조회 실패 중 window와 target lease를 유지하는 방향은 맞다.
2. **0-row 증거가 chunk-complete해졌다.** 일부 0-row 접두부는 `PARTIAL_COMMIT`이고 전체 1..expected를 덮을 때만 `FINALIZED_NO_DATA`다(A:1083–1086).
3. **`dq:accept`가 실제 commit된 chunk까지만 승인한다.** `high_k` CAS 뒤 나머지를 RETRY하며 미실행 chunk를 승인하지 않는다(A:414, A:1071, A:1175).
4. **terminal 반입과 mutation이 contract row lock에서 직렬화된다.** `terminal_ingested_at`과 binding 검사로 zombie 호출을 막는 기본 구조가 명확하다(A:770–772).
5. **PITR 순서가 물리 fencing 우선으로 바뀌었다.** Hold → old writer fencing → catalog reconstruction의 순서는 옳다(A:268). 아래 P0는 이 순서가 아니라, fencing의 완료 정의와 복구 증거의 내구성 문제다.
6. **모든 JDBC 물리 connection에 초기 검사를 적용하는 방향은 맞다.** Spark의 `sessionInitStatement`는 각 remote DB session이 열린 뒤 read 전에 실행된다. 다만 사용한 PDB 속성과 statement 이후 role 전환 race를 고쳐야 한다.
7. **unsafe한 current-read downgrade를 제거했다.** 같은 contract의 모든 일반 chunk가 고정된 `AS OF visible_scn`을 사용하고 ORA-01555 계열 뒤 같은 fence RETRY를 금지한다(A:876, A:905–906).
8. **`max_commit_minus_watermark_seconds`로 개념을 바로잡았다.** 트랜잭션 시간과 watermark 지연을 구분하고 `ENFORCED/OBSERVED`, column facts, 실 Oracle G2를 추가한 방향은 타당하다(A:286, P:450).
9. **ORA-28002를 성공 로그인 경고로 바로잡았다.** credential breaker 입력에서 제외하고 별도 경고로 처리한다(A:345).
10. **Full 급감 DQ를 pre-commit으로 옮겼다.** 60% 비중인 Full에서 잘못된 overwrite가 main에 노출되는 가장 큰 위험을 줄였다(A:1068).
11. **PoC oracle을 네 층으로 나눈 구조는 좋다.** occurrence, writer, commit, data completeness를 분리한 것은 맞다(P:344–350). 현재 문제는 이 구조가 아니라 세부 판정식이다.
12. **DDL과 판정 SQL dry-run을 주차 1 gate로 둔 점은 좋다.** 단, 현재 문서 안에 이미 enum·chunk identity·SQL 충돌이 있으므로 실제 부록을 만든 뒤에만 이 장점을 얻는다.

---

## 3. P0 — v1.2.2와 PoC 착수 전 반드시 닫을 항목

## P0-01. Pod 부재가 이미 전달된 Polaris commit의 종료를 보장하지 않는다

### 근거

- `WRITER_FENCED`는 SparkApplication terminal 또는 SA 삭제 + driver/executor pod 부재다(A:755, A:1081).
- Adjudication은 fencing 직후 catalog snapshot을 조회할 수 있고, 첫 지연 기본값은 0초다(A:1546).
- PITR도 head가 `target_health_timeout_seconds` 간격 두 번 같으면 안정됐다고 본다(A:268).

### 실패 시나리오

1. driver가 Polaris REST commit 요청을 보낸다.
2. Polaris가 요청을 수신했지만 처리 또는 catalog visibility가 지연된다.
3. 응답 전에 driver pod가 사라진다.
4. Control은 pod 부재를 `WRITER_FENCED`로 확정한다.
5. Adjudicator가 아직 바뀌지 않은 head를 읽고 `NO_COMMIT`으로 판정한다.
6. RETRY attempt가 새 writer를 시작한다.
7. 1번 요청이 뒤늦게 적용된다.

그 결과 같은 논리 구간에 old/new commit이 함께 존재할 수 있다. pod 부재는 **새 요청을 더 보낼 수 없음**만 증명하며, target server가 이미 받은 요청의 종료는 증명하지 않는다.

Apache Iceberg도 commit 결과를 확인할 수 없을 때 `CommitStateUnknownException`을 사용하며, 이 상태에서는 후속 행동이 table을 손상시킬 수 있다고 명시한다. 기본 commit status-check 총 timeout도 별도 설정이다.

### 필수 수정

다음 중 하나를 규범으로 고정해야 한다.

- 권고: ETL catalog gateway가 `commit_intent_id`와 fencing generation을 journal하고, target-side terminal 결과를 제공한다. retry는 old intent가 terminal이거나 generation이 catalog commit 직전에 거부된 뒤에만 허용한다.
- 대안: 서버에서 강제되는 최대 commit 처리시간 + visibility margin을 두고 그 기간 전에는 snapshot 부재를 `NO_COMMIT`으로 바꾸지 않는다. 단순 client timeout 실측값은 hard bound가 아니다.
- PITR resync는 catalog write barrier를 세우고 이미 수신된 commit 요청이 모두 terminal임을 확인한 뒤 head를 읽어야 한다.

`adjudication_delay_seconds=0`은 unknown commit 경로의 안전 기본값이 될 수 없다.

### 추가 시험

`FI-50 TARGET_COMMIT_LATE_APPLY`

- proxy가 commit 요청을 받은 뒤 응답과 실제 적용을 pod 삭제 및 첫 두 head probe 뒤까지 지연한다.
- old commit이 terminal 되기 전 새 attempt/SA/target lease grant는 0이어야 한다.
- 최종 `(contract, logical window)` ingest snapshot은 정확히 하나이고 data diff 0이어야 한다.

---

## P0-02. Oracle 19c에 `SYS_CONTEXT('USERENV','CON_DBID')`가 없다

### 근거

A:284, A:759, A:898, A:996과 P:94, P:265는 PDB identity로 `CON_DBID`를 사용한다. Oracle 19c 공식 `USERENV` 속성에는 `CON_ID`, `CON_NAME`은 있지만 `CON_DBID`는 없다. PDB의 DBID, CON_UID, GUID는 `V$CONTAINERS`에 있다.

### 실패 시나리오

1. 같은 CDB 안에 스키마와 테이블명이 같은 PDB A/B가 있다.
2. Guard는 A에 접속하고 executor는 TNS failover 또는 잘못된 service로 B에 접속한다.
3. CDB `V$DATABASE.DBID`, `DB_UNIQUE_NAME`, role, resetlogs 값은 같다.
4. 문서의 PDB 항은 유효한 식별자가 아니므로 PDB 차이를 증명하지 못한다.
5. B의 `AS OF SCN`이 성공하고 잘못된 데이터를 조용히 적재한다.

FI-42는 서로 다른 CDB DBID/DB_UNIQUE_NAME만 바꾸므로 이 반례를 잡지 못한다.

### 필수 수정

`db_identity`를 최소 다음 tuple로 바꾼다.

```text
{ cdb_dbid, db_unique_name, resetlogs_change_no,
  pdb_dbid, pdb_con_uid, pdb_guid }
```

현재 container의 PDB tuple은 다음 계열의 DBA 승인 view 또는 query로 읽는다.

```sql
SELECT c.dbid, c.con_uid, RAWTOHEX(c.guid)
FROM v$containers c
WHERE c.con_id = TO_NUMBER(SYS_CONTEXT('USERENV', 'CON_ID'));
```

non-CDB는 `pdb_identity = NOT_APPLICABLE`로 명시적으로 구분한다. null만으로 non-CDB를 추론하지 않는다.

### 추가 시험

`FI-42e SAME_CDB_WRONG_PDB`

- 같은 CDB의 PDB A/B, 같은 schema/table 이름, 다른 PDB GUID를 준비한다.
- Guard=A, driver/executor=B인 각 경로에서 ORA-20902 또는 명시 assertion error가 나야 한다.
- B의 data SELECT, Iceberg commit, watermark CAS는 모두 0이어야 한다.

---

## P0-03. Source admission은 아직 열리지 않은 GRANTED reservation을 세지 않는다

### 근거

A:761과 A:864의 grant 식은 다음뿐이다.

```text
observed + requested_weight <= pool_cap
```

`observed`는 현재 `GV$SESSION`에 보이는 세션이다. 직전 Guard가 `GRANTED`했지만 Spark가 아직 connection을 열지 않은 weight는 보이지 않는다.

### 실패 시나리오

cap=4라고 하자.

1. Guard A가 pool lock 안에서 `observed=0`을 보고 weight=4를 grant한다.
2. A의 executor connection이 아직 열리기 전 Guard B가 같은 lock을 잡는다.
3. B도 `observed=0`을 보고 weight=4를 grant한다.
4. 두 SparkApplication이 connection을 열면 실제 세션은 8이다.

lock이 두 관측을 직렬화해도 **예약과 실제 세션 사이의 시간차**는 닫지 못한다.

또한 정상 finalize는 세션 0 확인 전에 바로 `RELEASED`로 둔다(A:863, A:867). 이는 `RELEASED 전 재부여 금지`, DRAIN 완료(A:1145), PoC No-Go 9의 의미와 직접 충돌한다.

마지막으로 A:866/A:872는 Profile `SESSIONS_PER_USER`를 2차 hard limit으로 부르지만 `RESOURCE_LIMIT=TRUE`를 필수 capability로 두지 않는다. Oracle은 resource limitation이 활성화되어야 profile resource limit을 집행한다.

### 필수 수정

admission은 다음과 같은 **미실현 예약 포함 사용량**으로 계산한다.

```text
reserved_unrealized = Σ max(0, lease.weight - attributed_observed_sessions)
effective_used      = observed_sessions + reserved_unrealized
grant iff effective_used + requested_weight <= pool_cap
```

정확한 attribution이 어렵다면 `unreleased_weight + observed + requested`의 보수적 식을 먼저 사용해도 된다.

모든 종료 경로는 다음 하나로 통일한다.

```text
SA terminal/pod fenced -> RECLAIMED
N회 연속 attempt session 0 -> RELEASED
```

정상 finalize 예외를 삭제한다.

DB hard layer의 capability evidence에는 다음을 필수로 둔다.

- `RESOURCE_LIMIT=TRUE`
- ETL user에 실제 할당된 finite `SESSIONS_PER_USER`
- PDB/RAC/service 적용 범위
- cap+1 positive control에서 초과 session이 ORA-02391로 거부된 증거

### 추가 시험

`FI-43c RESERVATION_GAP_AND_NORMAL_LINGER`

- JDBC open을 지연한 채 weight 3과 2를 cap 4에 연속 Guard한다. 둘 다 OK이면 실패다.
- 별도 계약은 SA `COMPLETED` 뒤 server session을 60초 유지한다. 세션 0 전 `RELEASED` 및 DRAIN 완료가 없어야 한다.

---

## P0-04. 첫 attempt가 commit 전에 실패하면 overlap이 영구 누락된다

### 근거

A:956은 overlap을 **계약 최초 attempt의 chunk 1에만** 적용하고 재개 attempt에는 적용하지 않는다. P:269는 attempt 1의 chunk 1이 이미 commit된 뒤 재개하는 경우만 시험한다.

### 실패 시나리오

1. contract의 original low=L, overlap=O, fence=F다.
2. attempt 1이 `[L-O, …)`를 읽기 전 또는 commit 전 확실한 `NO_COMMIT`으로 실패한다.
3. ledger/CAS가 하나도 없다.
4. attempt 2는 같은 fence와 같은 low=L로 시작하지만 규칙상 `extract_low=L`이다.
5. F에서 이미 보이던 late row 중 watermark가 `[L-O,L)`인 row는 어느 snapshot에도 들어가지 않는다.
6. 이후 watermark는 전진하고 해당 row는 영구 누락된다.

“같은 fence 안에서 attempt 사이에 새 late commit이 없다”는 설명은 **attempt 1이 overlap 결과를 commit했다는 전제**가 있을 때만 맞다.

### 필수 수정

overlap의 적용 여부를 attempt 번호가 아니라 durable coverage로 결정한다.

```text
apply overlap iff
  contract.window.low == original_logical_low
  AND no cas_applied ledger row covers original chunk-1 range
```

첫 overlap chunk의 CAS가 완료된 뒤에만 재적용을 금지한다.

### 추가 시험

`FI-47c PRE_FIRST_COMMIT_RETRY`

- attempt 1을 overlap extract 전, extract 후 commit 전 두 지점에서 각각 죽인다.
- attempt 2의 `extract_window_low`는 `L-O`여야 하고 overlap row가 모두 회수돼야 한다.

---

## P0-05. `INITIAL_LOAD PER_CHUNK_FENCE`는 coherent snapshot도, 완전한 delta handoff도 아니다

### 근거

A:962는 각 chunk를 다른 SCN으로 읽고 마지막 fence의 high를 production watermark로 확정한다. P:267 FI-44(d)는 SCN 증가와 FINALIZED만 확인하며 이미 처리한 chunk로 들어오는 late commit 또는 chunk-key 이동을 검증하지 않는다.

### 실패 시나리오

1. historical day D1 chunk를 SCN F1에서 끝낸다.
2. 오래 열린 transaction이 `UPDATE_DT=D1`인 row를 F1 뒤에 commit한다. 이 지연은 ENFORCED bound 안일 수 있다.
3. 다음 chunk들은 D2, D3…을 각자 새 SCN으로 읽지만 D1을 다시 읽지 않는다.
4. 마지막 fence Fn 기준 production watermark를 seed한다.
5. row의 watermark가 최종 watermark보다 과거이므로 정상 incremental에서도 다시 보이지 않는다.

row가 chunk 경계를 이동하거나 이미 처리된 row가 delete되는 경우도 같은 종류의 mixed-snapshot 문제가 생긴다.

### 필수 수정

- `ZERO_GAP`과 mutable source에는 `EXTRACT_ONCE`만 허용한다.
- `PER_CHUNK_FENCE`를 유지하려면 `BEST_EFFORT`로 강등하거나, first fence 이후 final fence까지의 **최종 delta sweep/reconciliation**을 모든 기존 chunk 범위에 대해 수행한 뒤 watermark를 seed한다.
- stable immutable chunk key, delete 처리, LOB/UNDO capability를 별도 증거로 둔다.

### 추가 시험

`FI-44e MIXED_FENCE_INITIAL_LOAD`

- 완료된 앞 chunk에 bound 이내 late commit을 넣는다.
- row의 chunk key를 미처리→처리완료, 처리완료→미처리 양방향으로 바꾼다.
- SecureFile/BasicFile LOB churn과 ORA-08181도 포함한다.
- final fence source와 target의 양방향 exact diff가 0이어야 한다.

---

## P0-06. Release serving pointer와 Control ACTIVE/effective time이 여전히 원자적이지 않다

### 근거

- `DEPLOYED`가 candidate bundle을 shard ACTIVE pointer로 먼저 바꾼다(A:359).
- `ACTIVE`는 미래 `effective_from`을 허용한다(A:361).
- NORMAL은 logical time에 유효한 release를 pin한다(A:611).
- Guard의 fallback은 Run이 생성된 경우의 digest mismatch만 막는다(A:758).

### 실패 시나리오 A — schedule 삭제/cron 변경

candidate R2가 pointer를 차지했지만 아직 Control ACTIVE가 아니다. R2에서 R1 schedule이 삭제되거나 cron group이 이동했다면 해당 tick은 아예 평가되지 않는다. Run/Guard가 없으므로 `loaded_bundle_digest` 검사로 잡을 수 없다.

### 실패 시나리오 B — 미래 effective time

R2를 지금 ACTIVE로 만들되 `effective_from=내일`로 둔다. code location은 R2를 제공하지만 오늘 logical tick은 R1을 pin한다. R1은 이미 SUPERSEDED이고 더 새 ACTIVE R2가 있으므로 Guard가 유효한 R1 계약을 `SUPERSEDED_BY_RELEASE`로 닫을 수 있다.

### 필수 수정

- candidate 전용 code location/pointer에서 compile·load·schedule 검증을 수행한다.
- Control ACTIVE와 serving generation 전환을 하나의 activation barrier로 묶는다.
- 미래 activation은 그 시각까지 old serving pointer와 old ACTIVE를 유지하거나 금지한다.
- activation 경계에서는 scheduling을 짧게 pause하고 bounded gap recovery를 즉시 실행한다.

### 추가 시험

`FI-24c ACTIVATION_BOUNDARY`

- schedule add/remove/cron 이동 release를 정각 직전에 DEPLOYED하고 ACTIVE를 실패시킨다.
- 미래 `effective_from=now+2 periods`도 시험한다.
- pre-effective occurrence는 모두 R1로 실행되고 missing/VOID 0이어야 한다.

---

## P0-07. Target lease의 cross-type 충돌이 DB 제약으로 증명되지 않는다

### 근거

A:323은 다음만 DB 제약으로 둔다.

- `EXCLUSIVE_TABLE`끼리 partial unique
- partition range `PARTITION_OR_FILESET`끼리 exclusion

`EXCLUSIVE_TABLE` ↔ `APPEND/PARTITION`, fileset 충돌은 “13.3 bounded try-lock”에 맡기지만 A:1101–1110에는 canonical lock key와 한 transaction 안의 conflict matrix가 없다.

### 실패 시나리오

두 Control replica가 같은 table에 대해 Full `EXCLUSIVE_TABLE`과 Append `APPEND`를 동시에 검사한다. 서로 다른 lease row를 insert하면 각자 자기 유형의 제약만 통과할 수 있다. Full replace와 Append가 함께 commit하면 snapshot 순서에 따라 append가 사라지거나 Full 결과가 stale해진다.

### 필수 수정

- 모든 target lease grant가 먼저 하나의 canonical `target_table(table_id)` row 또는 `pg_advisory_xact_lock(table_id)`를 잡게 한다.
- 그 lock 안에서 전체 conflict matrix를 검사하고 lease row를 만든다.
- `EXCLUSIVE_TABLE`은 모든 non-released target lease와 충돌한다.
- fileset은 canonical normalized file/range key와 overlap 규칙을 갖는다.

### 추가 시험

동시 두 세션으로 다음을 각각 100회 실행한다.

- Full EXCLUSIVE vs APPEND
- Full EXCLUSIVE vs PARTITION
- overlapping fileset vs fileset

각 회차 grant 성공은 정확히 하나여야 한다.

---

## P0-08. `ZERO_GAP`은 여전히 hard guarantee로 증명되지 않는다

이 항목에는 서로 독립적인 두 결함이 있다.

### 8-A. `SCN_TO_TIMESTAMP` 오차는 hard upper bound가 아니다

A:880은 Oracle의 “usual precision 3 seconds”가 상한과 방향 보장이 아니라고 정확히 적었다. 그러나 바로 이어 `clock_skew`에 최소 3초 + Phase 0 실측값을 넣어 `ZERO_GAP`을 허용한다. 과거 표본의 최대 오차는 미래 실행의 강제 상한이 아니다.

따라서 application timestamp high가 실제 안전시점보다 앞설 가능성을 hard하게 배제하지 못한다.

필수 수정:

- formal `ZERO_GAP`은 SCN/CDC 기반 offset으로 제한하거나,
- DBA가 집행·버전 관리하는 directional error bound가 존재할 때만 application timestamp를 허용한다.
- 실측뿐이면 `BEST_EFFORT + Audit`이다.

### 8-B. `delete_semantics != NONE_DECLARED`는 충분하지 않다

A:466/A:1374는 `NONE_DECLARED`만 거부한다. 그러나 `CDC_LATER`는 아직 CDC가 없고, periodic `PK_RECONCILE`는 다음 reconcile까지 target에 삭제된 row가 남는다(A:978–984). 그 상태에서 `ZERO_GAP` label을 허용하면 consumer가 이해하는 “source와 target 간 gap 0”과 다르다.

필수 수정:

- `ZERO_GAP` delete 자격은 enforced soft delete + watermark coverage, finalize 안의 synchronous delete reconciliation, 또는 구현된 CDC delete offset만 허용한다.
- `CDC_LATER`와 비동기 `PK_RECONCILE`는 `BEST_EFFORT`다.
- data oracle은 source-minus-target뿐 아니라 target-minus-source도 반드시 본다.

### 추가 시험

`FI-41e/f ZERO_GAP_HARD_PREREQUISITES`

- configured clock skew를 넘는 SCN conversion error를 주입한다.
- watermark trigger disable/replace, precision/timezone change를 주입한다.
- `CDC_LATER`, delayed `PK_RECONCILE`, soft delete, 실제 CDC를 각각 hard-delete와 함께 시험한다.
- capability evidence digest가 달라지면 Guard가 window 예약 전에 Hold/reject해야 한다.

---

## P0-09. PITR 뒤 exact state와 Outbox를 현재 증거만으로 재구성할 수 없다

### 근거

A:268은 다음을 동시에 말한다.

- snapshot summary와 run tag로 contract/ledger/watermark를 재구성한다.
- 0-row CAS 손실은 watermark 후퇴로 허용한다.
- 미발행 Outbox는 재생성하고 `event_id` unique가 중복을 막는다.

그러나 0-row chunk에는 Iceberg snapshot이 없고, 무작위 event id는 PITR 뒤 동일하게 재생성할 수 없다. 이미 consumer에 전달됐지만 Control의 sent 표시가 PITR로 사라진 경우 새 event id로 재생성하면 consumer에서 중복이다. snapshot expiration이 restore point 이후의 유일한 ETL summary를 지울 가능성도 별도 제한이 없다.

P:220은 dump + 이후 snapshot과 watermark/state의 exact 일치를 요구하지만, P:221은 0-row CAS가 후퇴해도 된다고 한다. 두 시험도 서로 다르다.

### 필수 수정

둘 중 하나를 선택해야 한다.

1. **Exact recovery**: Control PostgreSQL과 다른 failure domain의 append-only recovery journal/RPO 0를 둔다. event id는 `(aggregate_id, transition_version, event_type)`에서 결정론적으로 만든다. zero-row CAS와 phase marker도 journal한다.
2. **Conservative recovery**: exact watermark/history 복원을 약속하지 않고 후퇴 + 재추출을 정상 의미로 둔다. FI-05는 최종 data/중복 0을 판정하며 exact state 비교를 삭제한다.

어느 선택이든 resync 5단계 각각의 durable phase marker와 restart 규칙이 필요하다.

### 추가 시험

- resync 1~5단계 각각 직후 Control을 다시 죽이고 재개한다.
- restore point 뒤 0-row CAS, sent-but-unmarked Outbox, expired intermediate snapshot을 포함한다.
- exact 모드면 event id 집합과 state exact match, conservative 모드면 final data exact + duplicate side effect 0을 요구한다.

---

## P0-10. PoC 5차 oracle은 현재 자체 규칙으로 합격 판정을 낼 수 없다

### 10-A. FI-40 idle 케이스가 수학적으로 불가능하다

P:89는 `visible_scn`을 마지막 table-row commit으로 잡고 `apply_lag = now - commit_ts(visible_scn)`로 정의한다. P:263은 10분간 commit 0인 idle + injected lag 0에서 apply lag 약 0, DATUM_TIME fresh, fence와 SCN timestamp 차이 ≤ 30초를 동시에 요구한다.

자체 모델대로면 그 차이와 apply lag는 약 10분이다. source idleness와 Data Guard apply lag를 혼동했다.

수정: table commit과 독립된 redo/control-event stream, contiguous apply cursor, heartbeat/system redo, explicit gap set을 둔다.

### 10-B. SC-02b가 No-Go 2를 의도적으로 위반한다

P:196은 poison item의 occurrence row 0과 `expected occurrence missing=1`을 합격으로 요구한다. P:346/P:359는 모든 expected key에 occurrence 정확히 1개가 아니면 No-Go다.

수정: rejected item도 `REJECTED_AT_SCHEDULER(ITEM_REJECTED)` occurrence를 만들거나, expected key와 1:1인 별도 durable disposition을 네 층 oracle에 포함한다.

### 10-C. 같은 Job SA overlap 0은 허용된 closed-range Backfill을 오탐한다

P:347은 같은 Job의 attempt/SA 구간 overlap 0을 요구한다. P:238(f)는 동일 Job의 닫힌 구간 Backfill SA와 정상 SA, 총 2개를 정상으로 요구한다.

수정: writer concurrency를 Job 단위가 아니라 operation lane + target range + lease conflict matrix로 판정한다. 동일/충돌 범위 writer만 중복이다.

### 10-D. `ORA_HASH(row)`은 실행 가능한 exact oracle이 아니다

P:183의 `SELECT pk, ora_hash(row)`에서 `row`는 Oracle의 전체-row scalar가 아니다. `ORA_HASH` 자체도 collision 가능성이 있어 “설명되지 않은 차이 0”의 최종 권위가 될 수 없다.

수정: 명시적 typed columns의 양방향 exact comparison을 권위로 쓰고 hash는 prefilter로만 쓴다. NULL, timestamp TZ/NTZ, decimal scale, Unicode, binary를 canonicalize한다.

### 10-E. DDL/enum/chunk identity가 이미 문서 사이에서 다르다

- A:1058의 ledger actor에는 `OPERATOR`가 있지만 P:112에는 없다.
- A:1115는 RETRY attempt가 남은 범위를 chunk 1..N으로 다시 번호 매기지만 P:242는 attempt 2가 original chunk 3·4를 기록한다고 한다.
- P:344는 executable DDL 위 dry-run을 요구하지만 실제 DDL 부록은 아직 다음 산출물이다.

수정: PoC 실행 전에 DDL, enum, attempt별 chunk identity, 네 층 SQL을 하나의 versioned artifact로 빌드하고 합성 row로 실제 실행한다.

---

## 4. P1 — Phase 1 gate 전에 수정할 항목

## P1-01. `last_submitted_run_id`는 check→launch→record 경쟁을 닫지 못한다

A:650 자체가 두 주체가 동시에 확인·제출하면 Dagster Run 둘이 생길 수 있다고 인정한다. 외부 `launchRun` 성공 뒤 Control row 기록 전 crash도 같다. 이는 P:216–217의 non-terminal Run ≤1과 충돌한다.

필수 수정:

- 작은 `run_submission` intent/outbox 또는 contract의 active submission generation을 **외부 호출 전에** commit한다.
- 모든 tick/Gap/Hold/Manual/RETRY 제출이 그 reservation을 사용한다.
- 가능하면 Dagster run id를 deterministic하게 만들고 launch를 idempotent하게 한다.

Guard CAS는 duplicate writer를 막지만 duplicate queued Run 500개가 쌓이는 운영 리스크까지 해결하지 않는다.

## P1-02. 만료된 known-verdict 계약이 RETRY 경쟁에서 다시 ACTIVE가 될 수 있다

A:746의 inline expiry는 `PLANNED`만 본다. A:1166–1173의 RETRY는 known `PARTIAL_COMMIT/NO_COMMIT`에서 `expires_at`을 검사하지 않는다.

RETRY가 expiry scanner보다 먼저 lock을 잡으면 authorization과 Run을 만들고, Guard도 ADJUDICATION_PENDING의 inline expiry를 하지 않아 새 attempt를 ACTIVE로 만든다. 이후 running contract는 expiry 면제다.

수정: RETRY transaction과 recovery Guard가 contract lock 안에서 먼저 known-verdict expiry를 적용한다. verdict NULL은 기존처럼 유지한다.

## P1-03. DQ partial accept 뒤 attempt state와 chunk 번호가 모순된다

A:414는 old attempt를 `ADJUDICATED(PARTIAL_COMMIT,DQ_ACCEPTED)`로 만든다. 그러나 attempt state machine A:422–426에는 정상 `finalize {DQ_FAILED}` 뒤 상태에서 ADJUDICATED로 가는 경로가 없다.

또한 A:1115의 attempt-local chunk numbering과 P:242의 “attempt 2 chunk 3·4”가 충돌한다.

수정:

- DQ stop 완료를 `TERMINAL_OBSERVED → FENCED → ADJUDICATED` 또는 명시 `DQ_SEALED → ADJUDICATED`로 정의한다.
- 새 attempt는 남은 범위를 항상 local chunk 1..N으로 번호 매기고 contract-wide 연결은 `window_low/high`로만 증명한다.
- P:112 actor enum에 `OPERATOR`를 추가한다.

## P1-04. freshness가 여전히 orchestration completion만 나타낸다

A:935의 Full `RETAIN_PREVIOUS`는 target snapshot을 바꾸지 않지만 A:1299–1304와 P:329–330은 `FINALIZED_NO_DATA.finalized_at`을 freshness 성공으로 취급한다.

수정: 최소 세 clock을 분리한다.

- orchestration completion
- target publication time/snapshot
- source coverage high/time

retained-empty는 첫째만 전진해야 한다. 만료/reject된 occurrence가 다음 성공으로 coverage를 회복했을 때 `covered_by_contract_id`와 recovery event를 남긴다.

## P1-05. WAP branch가 main에 publish되지 않아도 현재 data oracle이 통과할 수 있다

A:1095에는 branch→DQ→fast_forward만 있고 main ref 동시 전진 시 lease/rebase 규칙이 없다. P:177은 concurrent main commit을 넣지 않는다. P:183은 contract의 committed snapshot을 비교하므로 main에서 접근 불가능한 branch snapshot도 데이터가 맞으면 통과할 수 있다.

수정:

- publish phase에 table/main-ref lease 또는 base-main CAS를 둔다.
- `base_main_snapshot_id`, `staging_head`, `published_main_snapshot_id`를 ledger한다.
- 합격 SQL은 current main을 읽거나 committed snapshot이 current main ancestor임을 증명한다.

## P1-06. Maintenance worker의 최종 fencing과 staging/branch 보호집합이 없다

A:1386은 compaction이 token을 다시 확인할 필요가 없다고 한다. lease를 잃은 maintenance worker가 늦게 commit할 수 있다. `orphan_min_age`만으로 operator 대기 중 staging URI, active WAP branch, retry evidence를 항상 보호하지도 않는다.

수정:

- 모든 maintenance commit 직전에 lease id + fencing generation을 검증한다.
- active attempt staging URI와 branch/ref를 age와 별개인 protected set으로 둔다.
- P:208 SC-11에 lease expiry, orphan cleanup, snapshot expiry, branch retention을 실제로 넣는다. P:389가 말하는 below-minimum 422 fixture도 현재 SC-11에 없으므로 추가한다.

## P1-07. Merge DQ 식이 delete action을 세지 않으며 runtime version 전제가 크다

A:1064는 `written_rows == inserted + updated`다. delete-only MERGE는 유효한 input action이 있어도 inserted=updated=0이므로 실패한다.

또한 Apache Iceberg 공식 문서의 MERGE target-row snapshot metrics는 Spark 4.1+에서만 제공되며 unknown이면 field가 생략될 수 있다. `versions.lock`에서 Spark 3.x를 선택하면 현재 검사 1은 작동하지 않는다.

수정:

```text
action_rows = inserted + updated + deleted + ignored_by_rule
```

또는 application-side action counter를 authoritative하게 만든다. Spark/Iceberg/Polaris 조합을 고정하기 전에는 Merge를 이 metric에 의존해 gate하지 않는다.

## P1-08. `sessionInitStatement` 뒤 role 전환이 boolean WHERE를 정상 0-row로 만들 수 있다

A:996은 SQL에 `SYS_CONTEXT(...DATABASE_ROLE)='PHYSICAL STANDBY'`를 붙이고 init block이 먼저 실패하므로 불일치가 0-row가 될 수 없다고 주장한다.

하지만 init 성공 직후 data SELECT 전에 switchover가 일어나면 boolean predicate는 false가 되어 0-row를 반환할 수 있다. Run Pod는 이를 정상 empty receipt로 해석해 CAS할 수 있다. FI-45는 connection init 전 전환만 본다.

수정:

- data statement 자체에서 mismatch 시 false가 아니라 예외를 발생시키는 assertion을 실행한다.
- assertion이 실제 실행됐음을 sentinel/result metadata로 증명한다.
- standby role-based service가 role change 때 기존 session을 어떻게 종료하는지 실 Oracle에서 검증한다.

추가 `FI-45d`: init 완료 신호 뒤 첫 SELECT 직전에 role/identity를 전환한다. mismatch error ≥1, CAS 0, `FINALIZED_NO_DATA` 0이어야 한다.

## P1-09. Credential lockout 식은 Job 수를 세고 실제 physical auth fan-out을 세지 않는다

A:346의 식은 최대 동시 ETL Job을 쓴다. precheck 성공 뒤 password가 바뀌고 `numPartitions>1` executor들이 동시에 인증하면 task retry를 끊어도 첫 physical login 여러 건은 이미 발생한다.

수정: `worst_case_inflight_physical_auth_attempts`를 weight, executor partition, reconnect, monitor, legacy writer, TNS behavior에서 계산한다. 또는 최대 attempt duration 전체에 Oracle gradual password rollover를 강제한다.

## P1-10. ConnectionRevision의 fence origin과 attempt 실행 revision을 감사할 수 없다

A:334/A:759는 revoked revision을 새 ACTIVE로 재해석해 attempt에 기록한다고 하지만 A:299의 attempt 모델에는 `credential_revision_id`만 명시돼 있다.

수정:

- contract: `fence_connection_revision_id`, expanded descriptor digest
- attempt: `execution_connection_revision_id`, expanded descriptor/TNS/wallet digest

재해석해도 fence origin을 덮어쓰지 않는다.

## P1-11. Migration cutover는 old watermark를 너무 일찍 캡처한다

A:1538의 순서는 old watermark 캡처 → DRAIN → old writer 정지/session 0이다. old writer가 캡처 뒤 watermark를 전진시키면 seed가 stale하다.

권고 순서:

```text
DRAIN -> old writer stop -> session 0
-> authoritative old watermark + target head capture
-> seed/reconcile -> lease transfer -> release Hold
```

FI-48에는 기존 step 1과 3 사이 old commit을 추가한다.

## P1-12. 자동 Gap Recovery의 HA owner가 없다

A:643–644는 heartbeat gap이 자동 recovery를 기동한다고 하지만 process, scan cadence, leader lease, durable cursor, operation unique key를 정하지 않는다. P:217은 daemon이 죽어 있는 동안 60초 내 자동 기동을 요구한다.

수정: Control 내부 singleton leased scheduler, persisted cursor, `(scope, range)` unique operation key, leader crash 재개 규칙을 명시한다.

## P1-13. Source/Oracle `zero_gap_verified`는 boolean이 아니라 versioned evidence여야 한다

P:450의 G1/G2 분리는 좋지만 `zero_gap_verified=true` 하나는 DB rebuild, PDB identity, Oracle/OJDBC version, descriptor topology, watermark trigger, timezone/precision, undo policy 변경 뒤에도 남을 수 있다.

수정: evidence id와 다음 digest를 묶고 하나라도 바뀌면 자동 false/republish 대상으로 만든다.

- full DB/PDB identity
- Oracle/OJDBC/Spark/Iceberg version
- connection topology/service
- lag source
- watermark enforcement object DDL
- timestamp type/timezone/precision
- undo/LOB policy

## P1-14. SC-04b queue conservation 식은 queued contract를 이중 계산한다

P:201은 `runnable + dagster_nonterminal <= expected`를 사용한다. queued Run의 contract는 Guard 전까지 PLANNED라 runnable과 dagster_nonterminal 양쪽에 들어갈 수 있다. 반대로 “non-terminal ≤ max concurrent + waiting”은 waiting을 무제한 허용하는 항등식에 가깝다.

수정: mutually exclusive 상태로 집계한다.

- unsubmitted-runnable
- submitted-queued
- executing
- safety-blocked
- terminal/explained

각 occurrence가 정확히 한 bucket에 있도록 conservation query를 만든다.

## P1-15. FI-49의 자동 Hold 경쟁 단계는 Guard 6번에 도달할 수 없다

P:271은 Source DRAIN H1을 먼저 만든 뒤 H1 범위의 계약 두 개가 Guard 6번 `SCHEMA_DRIFT`에 도달한다고 요구한다. 그러나 A:757/A:1147에 따라 Guard 4번에서 먼저 HOLD가 반환된다.

수정: automatic-Hold unique 경쟁을 H1 생성 전에 실행하거나 H1이 덮지 않는 Source에서 실행한 뒤 Hold overlap 본 시험을 이어간다.

---

## 5. 이전 검토 41건 폐쇄 매트릭스

상태 정의:

- `CLOSED`: 원 지적 핵심이 규범적으로 닫힘
- `PARTIAL`: 일부 반영됐지만 같은 불변식의 반례/증명이 남음
- `OPEN`: 핵심 규칙 부재
- `REGRESSED`: 수정으로 새 직접 결함이 생김

### 5.1 이전 P0 10건

| ID | 상태 | v1.2.1 판정 |
|---|---|---|
| P0-01 unknown verdict expiry | CLOSED | verdict NULL 계약은 만료·lease 해제하지 않고 Polaris를 재조회한다. 신규 P0-01 target in-flight commit은 별도 결함이다. |
| P0-02 partial zero-row prefix | CLOSED | complete coverage만 NO_DATA, prefix는 high_m CAS + PARTIAL이다. |
| P0-03 incremental `dq:accept` | CLOSED | chunk k만 승인하고 remainder를 RETRY한다. attempt state 정합은 신규 P1-03이다. |
| P0-04 Control PITR/recovery epoch | PARTIAL | physical fencing 대안은 수용 가능하나 zero-row/Outbox/exact reconstruction evidence가 없다. |
| P0-05 fence/extract DB identity | REGRESSED | 불변 identity 개념은 추가됐지만 Oracle 19c에 없는 `CON_DBID`를 사용한다. |
| P0-06 application timestamp ZERO_GAP | PARTIAL | enforced watermark bound는 개선됐으나 SCN-time hard bound와 delete 자격이 남았다. |
| P0-07 source token reclaim | PARTIAL | abnormal RECLAIMED/RELEASED는 개선됐지만 정상 finalize 예외와 reservation gap이 남았다. |
| P0-08 release serving atomicity | PARTIAL | ACTIVE 실패 fallback은 생겼으나 candidate schedule 및 future-effective split이 남았다. |
| P0-09 stub SCN/lag model | PARTIAL | 시간-vs-SCN 단위는 고쳤지만 idle case가 모순이고 redo gap model이 없다. |
| P0-10 No-Go SQL/oracle | PARTIAL | 네 층 구조는 좋아졌지만 occurrence, concurrency lane, exact SQL이 아직 모순된다. |

### 5.2 이전 P1 23건

| ID | 상태 | v1.2.1 판정 |
|---|---|---|
| P1-01 initial eligibility/inline expiry | PARTIAL | PLANNED는 닫혔지만 known adjudication RETRY race가 남았다. |
| P1-02 queued submission reservation | PARTIAL | tracking field는 생겼지만 A:650이 duplicate Run race를 명시적으로 허용한다. |
| P1-03 batch poison isolation | CLOSED | item savepoint와 ITEM_REJECTED가 들어갔다. SC-02b occurrence 표현만 수정 필요하다. |
| P1-04 terminal 뒤 evidence | CLOSED | terminal_ingested ownership + Pipes replay 대안이 안전한 경로를 제공한다. |
| P1-05 overlapping Hold | CLOSED | effective mode와 마지막 Hold coverage가 정의됐다. FI-49 순서만 고치면 된다. |
| P1-06 freshness semantics | OPEN | target unchanged와 source coverage를 completion freshness가 계속 가린다. |
| P1-07 retry/cancel causes | CLOSED | worker loss와 operator/max-runtime/platform cancel이 분리됐다. |
| P1-08 SparkApplication identity | CLOSED | attempt-specific name/UID/no recreate 규칙은 강하다. create-loss/UID fault test는 추가 필요하다. |
| P1-09 WAP concurrent main | OPEN | publish-phase lease/CAS/rebase와 current-main oracle이 없다. |
| P1-10 GC/compaction safety | PARTIAL | snapshot/orphan 하한은 추가됐지만 final fencing과 protected staging/ref가 없다. |
| P1-11 DQ row metrics | PARTIAL | `written_rows`는 맞지만 Merge delete action과 Spark version gate가 남았다. |
| P1-12 Flashback/UNDO deadline | CLOSED | 일반 contract의 deadline, fail-fast, same-fence retry 금지는 닫혔다. 신규 initial-load mode 문제는 P0-05다. |
| P1-13 all physical JDBC connection check | PARTIAL | sessionInit 방향은 맞지만 PDB 식별자와 after-init role race가 남았다. |
| P1-14 credential Oracle mapping | CLOSED | ORA-28002 및 fatal mapping은 고쳤다. physical auth fan-out은 신규 P1-09다. |
| P1-15 NO_LAG_SIGNAL confidence | CLOSED | BEST_EFFORT DEGRADED로 명시되고 관측된다. |
| P1-16 logical/extract overlap ranges | PARTIAL | 필드는 분리됐지만 pre-first-CAS retry가 overlap을 잃는다. |
| P1-17 target provisioning | CLOSED | create-or-get와 UUID/schema/spec pin 규칙은 들어갔다. drop/recreate FI는 추가 필요하다. |
| P1-18 migration cutover | PARTIAL | Phase 3 gate는 생겼지만 watermark capture 순서가 안전하지 않다. |
| P1-19 DB constraints/lock order | PARTIAL | 전역 순서는 생겼지만 submission과 cross-type target lease는 DB가 증명하지 못한다. |
| P1-20 Source capacity/skew | PARTIAL | weighted Source 모델과 skew test는 생겼지만 unrealized reservation을 세지 않는다. |
| P1-21 real Oracle separate gate | CLOSED | G1/G2 분리는 명확하다. evidence versioning은 신규 후속이다. |
| P1-22 schema/PoC SQL alignment | REGRESSED | DDL dry-run gate는 좋지만 OPERATOR enum, chunk 번호, ORA_HASH SQL이 이미 어긋난다. |
| P1-23 post-commit DQ exposure | CLOSED | Full 급감은 pre-commit이고 남는 post-commit DQ는 main exposure를 명시한다. |

### 5.3 이전 P2 8건

| ID | 상태 | v1.2.1 판정 |
|---|---|---|
| P2-01 CATCHUP expiry rationale | CLOSED | bounded intent와 queue hygiene로 근거를 바로잡았다. |
| P2-02 문서 우선순위 | CLOSED | architecture normative, PoC 검증 순서로 정리됐다. |
| P2-03 grouped schedule target | CLOSED | subsettable shard job + per-asset RunRequest가 명시됐다. |
| P2-04 versions.lock | PARTIAL | 범위와 Phase 0 산출물은 정의됐지만 실제 lock이 아직 없다. |
| P2-05 API/attempt identity | PARTIAL | 대부분 통일됐으나 DQ attempt state와 rollback 재개 세부가 남았다. |
| P2-06 catch-up estimate | PARTIAL | Source weight/skew는 들어갔지만 percentile/confidence와 target contention이 없다. |
| P2-07 empty-source naming | CLOSED | `on_empty_source` 이름과 동작은 정리됐다. freshness 의미는 별도 P1이다. |
| P2-08 boundary tests | PARTIAL | 다수 경계 시험이 생겼으나 poll interval 경계 및 일부 fixture가 없다. |

---

## 6. PoC 6차에 바로 넣을 시험 delta

### 6.1 신규 P0 시험

| ID | 시험 | 핵심 합격 조건 |
|---|---|---|
| FI-50 | target commit late apply | target-side old intent terminal 전 new writer 0; 동일 logical range ingest commit 1 |
| FI-42e | same-CDB wrong-PDB | full PDB identity mismatch, wrong PDB SELECT/commit/CAS 0 |
| FI-43c | unrealized reservation + normal linger | effective_used ≤ cap 전 시점; session zero 전 RELEASED 0 |
| FI-47c | first commit 전 retry | attempt 2도 overlap 재적용, overlap truth 누락 0 |
| FI-44e | PER_CHUNK_FENCE mutation | final-fence source ↔ target 양방향 diff 0 또는 mode publish 거부 |
| FI-24c | release activation boundary | candidate/future-effective 구간 occurrence 누락·VOID 0 |
| FI-51 | cross-type target lease | 충돌 pair에서 동시 grant 0 |
| FI-41e/f | hard delete + SCN error/enforcement drift | ZERO_GAP은 hard evidence일 때만 publish, 양방향 diff 0 |
| FI-05c | resync restart/evidence loss | 선택한 exact 또는 conservative recovery 계약과 일치 |

### 6.2 기존 시험 즉시 수정

1. FI-40: business row와 독립된 redo cursor, idle heartbeat, explicit gap/fill을 모델링한다.
2. SC-02b: rejected item도 expected key를 설명하는 durable occurrence/disposition을 남긴다.
3. §5.1 writer oracle: same Job이 아니라 conflict lane/range로 겹침을 판정한다.
4. 비교 A/B: `ORA_HASH(row)`를 제거하고 typed exact symmetric diff를 권위로 둔다.
5. FI-23c: attempt 2 chunk는 local 1..N으로 바꾸고 `window_low/high`로 원 chunk 범위를 연결한다.
6. FI-49: SCHEMA_DRIFT 동시 경쟁을 outer Hold 전에 수행한다.
7. SC-11: maintenance lease expiry, below-min orphan config, protected staging/branch, snapshot expiry를 실제 fixture로 넣는다.
8. FI-24: schedule add/remove/cron move 및 future effective time을 포함한다.
9. FI-16: precheck 성공 뒤 credential rotation과 executor fan-out을 넣는다.
10. FI-45: sessionInit 성공 뒤 SELECT 직전 role/identity 전환을 넣는다.

### 6.3 주차 1 DDL 부록에 반드시 들어갈 것

- full source/PDB identity columns와 evidence digest
- target table canonical lock/grant procedure
- source reservation accounting columns 또는 materialization attribution
- run submission intent/generation
- attempt DQ terminal/adjudicated transition
- ledger actor `OPERATOR`
- attempt-local chunk identity와 contract-wide `window_low/high`
- WAP base/staging/published main refs
- deterministic Outbox semantic event key
- resync phase marker
- orchestration/publication/source-coverage freshness fields

---

## 7. v1.2.2 최소 수정 패키지

문서를 크게 다시 쓰기보다 다음 12개 patch로 닫는 것이 효율적이다.

1. **Target commit uncertainty protocol**: intent journal/gateway 또는 enforceable terminal horizon
2. **Oracle identity v2**: `V$CONTAINERS` PDB tuple, same-CDB FI
3. **Source admission v2**: unrealized reservation 포함 + 모든 경로 zero-session RELEASED
4. **DB hard cap evidence**: RESOURCE_LIMIT, actual profile, cap+1 positive control
5. **Overlap coverage rule**: attempt_no가 아니라 first-range CAS evidence 기준
6. **Initial load guarantee split**: mutable ZERO_GAP은 EXTRACT_ONCE/CDC handoff, PER_CHUNK는 reconcile 또는 BEST_EFFORT
7. **Release activation barrier**: candidate location과 effective-time atomic serving
8. **Canonical target lock**: 전체 target lease conflict matrix
9. **ZERO_GAP contract**: SCN/time hard evidence + synchronous delete eligibility + versioned certification
10. **Recovery contract**: exact journal 또는 conservative replay 중 하나 선택
11. **Submission/DQ/WAP state completion**: durable submission, DQ attempt state, WAP main publish evidence
12. **Executable PoC oracle bundle**: DDL + exact SQL + enum + fixtures를 한 artifact로 freeze

### 수정 우선순위

```text
1차: P0-01, 02, 03, 04, 05
   ↓
2차: P0-06, 07, 08, 09
   ↓
3차: P0-10 oracle/DDL 실제 실행
   ↓
4차: P1 submission, DQ, WAP, freshness, maintenance
   ↓
PoC 7일 soak 시작
```

---

## 8. Review Board 권고

### 지금 승인해도 되는 것

- Dagster를 orchestration/UI 기반으로 선택하는 방향
- JobSpec/TemplateVersion/DefinitionRelease의 immutable version 모델
- 10,000개 Job을 개별 소스 파일로 만들지 않는 sharded grouped definitions
- Control API를 모든 실행의 권위로 두는 방향
- Spark Operator와 Iceberg commit evidence/ledger 기반 runtime
- Source Protection Policy 및 LLM 추천을 draft-only advisory로 쓰는 방향
- Hold/CATCHUP, 중앙 운영팀 승인, Kafka 알림 통합의 큰 구조

### 지금 동결하면 안 되는 것

- `WRITER_FENCED` 정의와 `NO_COMMIT` 판정 시점
- Oracle `db_identity` 필드와 모든 connection assertion SQL
- Source/target lease DDL
- `ZERO_GAP` label과 certification rule
- `PER_CHUNK_FENCE` initial load
- release ACTIVE/effective serving protocol
- PITR exact recovery와 Outbox 보장
- PoC §5.1 SQL 및 SC/FI 합격식
- WAP production path
- freshness 단일 지표

### 권고 의사결정 문구

> v1.2.1은 목표 방향과 구현 분해를 승인하되 semantic freeze는 보류한다. v1.2.2에서 target commit uncertainty, Oracle PDB identity, source reservation accounting, initial-load handoff, release activation, target lease exclusion, ZERO_GAP hard evidence, recovery evidence를 닫고, executable DDL/PoC oracle dry-run을 통과한 뒤 7일 soak를 시작한다. 그 전에는 `BEST_EFFORT` skeleton 외의 안전성 Go를 선언하지 않는다.

---

## 9. 공식 근거

### Oracle

- [Oracle 19c `SYS_CONTEXT`](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SYS_CONTEXT.html) — `USERENV`에 `CON_ID`, `CON_NAME`, `DATABASE_ROLE`, `DB_UNIQUE_NAME` 등이 있으며 `CON_DBID`는 없다.
- [Oracle 19c `V$CONTAINERS`](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-CONTAINERS.html) — PDB `DBID`, `CON_UID`, `GUID`의 권위 source.
- [Oracle 19c `V$DATAGUARD_STATS`](https://docs.oracle.com/en/database/oracle/oracle-database/18/refrn/V-DATAGUARD_STATS.html) — apply lag와 `DATUM_TIME`의 의미.
- [Oracle 19c `SCN_TO_TIMESTAMP`](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SCN_TO_TIMESTAMP.html) — 결과는 approximate이고 3초는 “usual precision”이다.
- [Oracle 19c `RESOURCE_LIMIT`](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/RESOURCE_LIMIT.html) — profile resource limit 집행 활성화 조건.
- [Oracle 19c user resource limits](https://docs.oracle.com/en/database/oracle/oracle-database/19/dbseg/configuring-user-resource-limits.html) — resource limitation을 enable해야 profile limit이 집행된다.

### Spark / Iceberg

- [Spark 3.5.6 JDBC options](https://spark.apache.org/docs/3.5.6/sql-data-sources-jdbc.html) — `sessionInitStatement`는 각 DB session open 뒤 read 전에 실행되며 `numPartitions`는 최대 concurrent JDBC connection 수를 정한다.
- [Iceberg `CommitStateUnknownException`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/exceptions/CommitStateUnknownException.html) — 결과 불명 commit 뒤 후속 행동이 table을 손상시킬 수 있는 상태.
- [Iceberg table configuration](https://iceberg.apache.org/docs/latest/configuration/) — commit/status-check timeout 및 snapshot/ref retention 설정.
- [Iceberg Spark writes](https://iceberg.apache.org/docs/latest/spark-writes/) — MERGE target-row snapshot metrics는 Spark 4.1+이며 unknown이면 field가 생략될 수 있다.
- [Iceberg branching](https://iceberg.apache.org/docs/latest/branching/) — branch write와 main fast-forward workflow.
- [Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/) — snapshot expiration/orphan cleanup의 기본 의미.

---

## 10. 최종 의견

v1.2.1은 이전 검토를 피상적으로 반영한 문서가 아니다. unknown verdict, 0-row prefix, DQ partial accept, terminal ownership, G1/G2 분리처럼 어려운 부분을 실제 상태 전이 수준까지 상당히 잘 끌어내렸다. 그래서 이 설계는 폐기 대상이 아니라 **한 번 더 정확히 다듬을 가치가 있는 설계**다.

다만 현재 남은 결함은 문구 정리 수준이 아니다. `CON_DBID`, source reservation gap, late target commit, pre-first-CAS overlap, mixed-fence initial load는 정상적인 운영 타이밍에서도 데이터 누락·중복 또는 원천 한도 초과로 이어진다. PoC oracle 자체의 모순은 이런 결함이 있어도 통과하거나, 반대로 정상 backfill을 No-Go로 만들 수 있다.

따라서 다음 산출물은 UI나 baseline 체크리스트보다 먼저 다음 두 개가 적절하다.

1. **v1.2.2 semantic patch** — 본 검토 P0 10개를 본문에 반영
2. **주차 1 executable appendix** — 실제 PostgreSQL DDL + Oracle identity/assertion SQL + §5.1 네 층 판정 SQL + 합성 fixture 실행 결과

이 두 개가 닫히면 그 다음부터는 아키텍처 토론보다 구현·성능 검증의 비중을 높여도 된다.
