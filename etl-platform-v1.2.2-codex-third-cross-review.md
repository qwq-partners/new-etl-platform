# ETL Platform 목표 아키텍처 v1.2.2 — Codex 3차 교차 리뷰

- 검토 대상 A: `etl-platform-target-architecture-v1.2.2.md`
- 검토 대상 P: `etl-platform-poc-test-plan-v1.md` 6차
- 비교 기준 R: `etl-platform-v1.2.1-codex-second-cross-review.md`
- 검토 일자: 2026-08-23
- 표기: `A:1129`는 아키텍처 1129행, `P:281`은 PoC 기준서 281행을 뜻한다.
- 판정 원칙: 설계 의도나 정상 경로 설명보다, 장애 시점의 실제 상태 전이·외부 시스템의 보장·DB 제약·실행 가능한 합격식이 만들어 내는 결과를 우선한다.

---

## 1. 최종 판정

### 1.1 한 줄 결론

**Dagster + Control Plane + Spark Operator라는 목표 방향은 계속 `GO`다. 그러나 v1.2.2 semantic freeze, PoC 6차 acceptance freeze, `ZERO_GAP` 제품 약속은 모두 `NO-GO`다.**

v1.2.2는 v1.2.1보다 분명히 좋아졌다. PDB 식별, 첫 CAS 전 overlap 재적용, verdict 없는 계약의 만료 금지, DQ accept 전이, 정상 종료의 Source token 회수, WAP main ancestry, idle redo 모델은 실질적으로 개선됐다.

하지만 다음 세 가지가 아직 설계의 중심 보장을 깨뜨린다.

1. Polaris가 이미 받은 commit 요청이 나중에 적용되는 것을 target 경계에서 막지 못한다. 문서는 그 결과를 다음 writer가 사후 탐지하도록 바꿨지만, 그 사이 동일 구간 snapshot 두 개가 main에 존재할 수 있다.
2. Oracle application timestamp 기반 `ZERO_GAP`의 핵심 전제인 `commit_time - watermark <= B`가 실제 Oracle에서 강제되지 않는다. stub이 위반 commit을 거부하는 것은 Oracle 보장의 증거가 아니다.
3. PoC의 핵심 합격식이 FI-50, REATTACH 이력, canonical hash, 비동기 delete 의미에서 서로 충돌하거나 실제 SQL로 안전하게 실행되지 않는다.

### 1.2 의사결정 요약

| 결정 대상 | 판정 | 설명 |
|---|---|---|
| Dagster 중심 orchestration 방향 | **GO** | 10,000 Job을 개별 Python 소스로 만들지 않고 grouped definition + metadata-driven execution으로 운영하는 방향은 적절하다. |
| Control Plane/Registry/UI 개발 | **조건부 GO** | CRUD, immutable version, source/job wizard, audit, read model 등 P0와 독립적인 부분은 진행 가능하다. |
| `BEST_EFFORT` Scale/Control PoC | **조건부 GO** | Source lease의 충돌 없는 식별자, 단일 제출 경로, PoC oracle 수정 후 가능하다. |
| Oracle `ZERO_GAP` PoC | **NO-GO** | commit-ordered source offset 또는 실제 강제 가능한 commit bound가 없다. 비동기 delete와 evidence lifecycle도 보장을 위반한다. |
| `PER_CHUNK_FENCE ZERO_GAP` | **NO-GO** | final sweep이 hard delete와 LOB 정합성을 닫지 못하며 lower-bound 표현도 불명확하다. |
| v1.2.2 semantic freeze | **NO-GO** | target late commit, 제출 API 불일치, source cap tag 충돌 등 의미론·구현 차단 항목이 남았다. |
| PoC 6차 acceptance freeze | **NO-GO** | FI-50와 전역 commit oracle이 동시에 참일 수 없고 writer interval·hash oracle도 수정이 필요하다. |
| 주차 1 executable appendix 착수 | **조건부 GO** | 닫힌 부분의 DDL 작업은 시작할 수 있으나, 아래 P0 의미 패치를 먼저 확정하지 않고 전체 DDL·판정 SQL을 freeze하면 재작업이 크다. |

### 1.3 이전 25개 지적 재판정

v1.2.2가 반영 대상으로 삼은 2차 검토의 신규·잔존 P0 10개와 P1 15개를 원 지적의 범위로 다시 판정했다.

| 상태 | 건수 | 의미 |
|---|---:|---|
| CLOSED | 9 | 원 지적의 핵심 불변식이 규범 문장과 시험에 들어갔다. 일부는 상위 P0가 해결돼야 실제 안전하다. |
| PARTIAL | 14 | 방향은 맞지만 반례, fail-closed gate, DB 강제 또는 시험 증거가 남았다. |
| OPEN | 2 | 핵심 해결 규칙이 아직 없다. |
| 합계 | 25 | P0 10 + P1 15 |

이번 검토에서 독립적인 결함 묶음은 **P0 7개, P1 13개, P2 2개**로 정리했다. 같은 근본 원인이 PITR·migration·maintenance 등 여러 위치에 나타나는 경우에는 중복 집계하지 않았다.

---

## 2. v1.2.2에서 확실히 좋아진 점

아래 내용은 v1.2.3에서도 보존해야 한다.

1. **Oracle PDB identity가 실제 19c 모델로 교체됐다.** `V$CONTAINERS`의 `DBID`, `CON_UID`, `GUID`와 non-CDB 명시 분기를 사용한다(A:326, A:943; P:270). 같은 CDB의 다른 PDB를 막는 fixture도 생겼다.
2. **첫 CAS 전 실패의 overlap 유실을 닫았다.** attempt 번호가 아니라 불변 `original_logical_low`와 현재 `window.low`로 재적용 여부를 판단한다(A:1001–1005; P:277–278).
3. **verdict가 없는 fenced 계약을 만료시키지 않는다.** Polaris 조회 실패 중 window와 target lease를 유지하고 adjudication을 재시도한다(A:309, A:697; P:251).
4. **정상 종료도 `RECLAIMED -> session-zero probe -> RELEASED`를 탄다.** v1.2.1의 정상 finalize 예외가 제거됐다(A:475–476, A:908–912; P:271–272).
5. **Source admission 식에 미실현 reservation이 들어갔다.** `observed + reserved_unrealized + requested_weight <= pool_cap` 구조 자체는 맞다(A:909).
6. **DQ accept가 attempt state와 맞아졌다.** `FENCED -> ADJUDICATED`를 거쳐 승인된 chunk까지만 CAS하고 나머지는 RETRY한다(A:456, A:471; P:292).
7. **WAP의 main publication 증거가 강화됐다.** fast-forward 성공, main ancestry, `published_main_snapshot_id`, attempt 전체 `EXCLUSIVE_TABLE` lease가 연결됐다(A:1107, A:1142; P:184, P:291).
8. **idle source와 apply lag를 분리했다.** stub에 business row commit과 별개인 system tick/contiguous apply cursor가 들어가 이전 idle 모순이 해소됐다(P:89, P:266).
9. **Release split 복구 의도가 명확해졌다.** ACTIVE 성공 및 412 fallback 뒤 자동 Gap Recovery를 수행하고 미래 `effective_from`을 금지했다(A:401–403; P:247).
10. **Queue conservation과 Hold 경합 순서가 정리됐다.** backlog 5분해와 FI-49 실행 순서가 이전 모순을 닫았다(P:151, P:202, P:289).
11. **비교 oracle이 단방향 row count에서 양방향 PK/hash 비교로 진화했다.** 방향은 옳다(P:184–186). 다만 현재 직렬화 규칙은 P0-07의 수정이 필요하다.
12. **문서 우선순위와 주차 1 DDL dry-run gate가 명시됐다.** 실행 가능한 DDL과 판정 SQL을 먼저 검증하려는 운영 방식은 타당하다(A:1439; P:113).

---

## 3. 2차 검토 P0 10건 재판정

| 원 항목 | 판정 | v1.2.2 평가 |
|---|---|---|
| P0-01 late target commit | **OPEN** | A:1129가 server-side terminal 부재를 인정하고 탐지는 보강했지만, 두 번 같은 head를 `NO_COMMIT` 근거로 삼는다. P:281의 합격식은 늦은 snapshot의 실제 main 착지를 허용해 예방 불변식을 닫지 못한다. |
| P0-02 Oracle PDB identity | **CLOSED** | 실제 `V$CONTAINERS` tuple과 같은-CDB wrong-PDB 시험이 들어갔다(A:326; P:270). |
| P0-03 Source reservation/release | **PARTIAL** | 수식과 정상 release는 고쳤지만 truncated attempt tag 충돌로 `reserved_unrealized`를 과소계산할 수 있고 DB hard-cap evidence가 Guard gate가 아니다(A:909, A:917). |
| P0-04 pre-first-CAS overlap | **CLOSED** | `window.low == original_logical_low` 조건과 두 kill 지점 시험으로 닫혔다(A:1001–1005; P:277–278). |
| P0-05 PER_CHUNK_FENCE initial load | **PARTIAL** | final sweep은 late insert/update를 회수하지만 hard delete, LOB old version, lower-bound 의미를 닫지 못한다(A:1007; P:273–274). |
| P0-06 release serving atomicity | **PARTIAL** | 미래 effective time과 412 보상은 닫혔다. 그러나 외부 pointer 변경 후 durable operation 기록 전 crash, 정상 ACTIVE가 이전 cron 경계를 가로지르는 경우의 release별 expected expansion이 남았다(A:401–403). |
| P0-07 cross-type target lease | **PARTIAL** | canonical target row lock은 Control API 경합을 막지만 cross-type/fileset exclusion은 DB 제약이 아니라 application convention이다(A:362–365; P:282). |
| P0-08 formal ZERO_GAP | **PARTIAL** | heartbeat lower bound는 개선됐지만 commit-minus-watermark 상한, 비동기 delete, evidence lifecycle이 hard guarantee를 충족하지 않는다(A:328, A:511, A:925). |
| P0-09 PITR exact recovery/outbox | **PARTIAL** | conservative resync와 단계 cursor는 좋아졌다. 같은 불완전한 target head-settle을 사용하고 RESYNC history idempotence 및 반복 transition Outbox key가 DB로 강제되지 않는다(A:308, A:1340). |
| P0-10 executable PoC oracle | **PARTIAL** | 네 층 구조와 양방향 비교는 개선됐다. FI-50와 전역 commit oracle, REATTACH interval, canonical serialization, delete/LOB 표본이 여전히 충돌한다(P:184, P:281, P:358–360). |

---

## 4. 2차 검토 P1 15건 재판정

| 원 항목 | 판정 | v1.2.2 평가 |
|---|---|---|
| P1-01 queued submission uniqueness | **OPEN** | schedule RunRequest와 Adapter 경합이 명시적으로 남고(A:695), 핵심 수단인 GraphQL `executionMetadata.runId`는 검토한 Dagster 스키마에서 입력 필드가 아니다. |
| P1-02 expiry vs RETRY | **CLOSED** | scanner·Guard·RETRY가 contract lock 아래 같은 inline expiry를 적용한다(A:697, A:1218). 단 `NO_COMMIT` verdict 자체의 안전성은 P0-01에 의존한다. |
| P1-03 DQ attempt state | **CLOSED** | `COMPLETED -> ADJUDICATED` 없이 FENCED 경유가 일관된다(A:456, A:471). |
| P1-04 data freshness | **PARTIAL** | publication age와 unchanged 경고는 좋아졌지만 coverage가 last applied CAS가 아니라 reserved `contract.window.high`다(A:1317). |
| P1-05 WAP main exposure | **CLOSED** | fast-forward 전 CAS 금지, main ancestry, exclusive lease가 정리됐다(A:1107, A:1142). |
| P1-06 maintenance final fencing | **PARTIAL** | staging/branch/retention 보호집합은 보강됐지만, RECLAIMED 뒤 늦은 maintenance snapshot의 main 착지를 허용하고 후속 ingest에서 탐지한다(A:1435; P:209). |
| P1-07 Merge DQ metrics | **CLOSED** | delete/ignored action을 포함하고 version gate 실패 시 `APP_COUNTER` fallback 및 `dq_basis`를 남긴다(A:1111; P:168). |
| P1-08 post-init role transition | **PARTIAL** | zero-row 경로는 막았지만 generic ORA-01722를 role mismatch로 오분류하고 LONG/LONG RAW에는 UNION assertion을 적용할 수 없다(A:1041). |
| P1-09 credential auth fan-out | **PARTIAL** | validator 식은 physical JDBC weight로 고쳤지만 precheck 성공 뒤 password rotation 후 executor fan-out 시험이 없다(A:388; P:236). |
| P1-10 ConnectionRevision provenance | **PARTIAL** | attempt revision은 추가됐지만 fence를 실제 읽은 monitor revision 및 resolved TNS/wallet digest를 권위 필드로 증명하지 못한다(A:341, A:499, A:905). |
| P1-11 migration capture order | **CLOSED** | old writer stop·source session zero 뒤 watermark/head를 캡처하도록 순서가 바뀌었다(A:1526, A:1587; P:279). 단 target in-flight commit은 P0-01에 의존한다. |
| P1-12 Gap Recovery HA owner | **PARTIAL** | singleton/advisory-lock owner와 durable cursor는 생겼지만 leader loss와 lock handoff liveness FI가 없다(A:312). |
| P1-13 versioned ZERO_GAP evidence | **PARTIAL** | versioned JSON evidence와 일부 digest·무효화는 생겼지만 versions/topology/DDL을 제외하고, 무효화 뒤 기존 ACTIVE job은 경고만 받고 계속 실행한다(A:328, A:1346). |
| P1-14 queue conservation | **CLOSED** | 5개 상호배타 집합으로 정리됐다(P:151, P:202). |
| P1-15 FI-49 reachability | **CLOSED** | 자동 Hold 경합을 기존 Hold 생성 전으로 이동했다(P:289). |

P1-02와 P1-11은 원 지적 자체는 닫혔지만, 상위 P0-01이 해결되지 않으면 end-to-end 안전성이 성립하지 않는다.

---

## 5. P0 상세 발견사항

### P0-01. 이미 수신된 target commit의 terminal 사실이 없다

#### 근거

- A:1128은 pod 부재가 먼저라고 규정한다.
- A:1129는 pod 부재가 Polaris가 받은 commit의 종료를 뜻하지 않는다고 정확히 인정한다.
- 그럼에도 두 번 같은 `current-snapshot-id`를 본 뒤 `NO_COMMIT`을 허용하고, 더 늦게 착지한 요청은 다음 `chunks:begin(1)`이 잡도록 한다.
- P:281 FI-50의 (i)와 (iii)는 old snapshot이 다음 writer 시작 뒤 또는 다음 attempt FINALIZED 뒤에 main에 실제로 생기는 것을 합격으로 처리한다.
- P:359는 attempt/contract/finalized 여부와 무관하게 같은 Job ingest snapshot의 논리 window pairwise overlap을 0으로 요구한다.

Apache Iceberg는 commit 성공 여부를 확인할 수 없는 상태를 별도 예외로 표현하며, 이때 client가 추가 행동을 하면 table을 손상할 수 있다고 명시한다. 상태 확인 retry에도 별도 총 예산이 있다. [Iceberg `CommitStateUnknownException`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/exceptions/CommitStateUnknownException.html), [Iceberg commit status-check 설정](https://iceberg.apache.org/docs/latest/configuration/)

#### 실패 interleaving

1. attempt 1의 commit 요청이 proxy/Polaris에 수신되지만 응답과 적용이 지연된다.
2. driver와 SA가 제거되고 Control은 `WRITER_FENCED`를 기록한다.
3. head를 두 번 읽어 H0가 같으므로 attempt 1을 `NO_COMMIT`으로 판정한다.
4. attempt 2가 H0를 base로 `[L,H)`를 commit하고 FINALIZED한다.
5. attempt 1의 보관된 요청이 뒤늦게 성공해 같은 `[L,H)` snapshot을 main에 붙인다.

이때 watermark CAS가 attempt 1에 없더라도 물리 snapshot 두 개가 이미 소비자에게 보인다. 다음 contract의 base 검사는 **변형 이후 탐지**일 뿐 예방이 아니다. repair가 나중에 partition을 고쳐도 사고 창과 snapshot history의 overlap은 사라지지 않는다.

#### 영향 범위

- 일반 RETRY
- Control PostgreSQL PITR resync(A:308)
- migration cutover
- maintenance lease reclaim(A:1435)
- FI-50와 전역 No-Go 7번의 동시 합격 가능성

#### 최소 수정안

강한 보장을 유지하려면 다음 중 하나가 필요하다.

1. **권고:** target commit 경계에 `commit_intent_id + fencing_generation`을 검증하는 gateway/catalog extension을 두고, old intent가 `COMMITTED | REJECTED` terminal일 때만 target lease를 넘긴다.
2. 서버가 강제하는 최대 처리 horizon이 실제로 존재한다면 그 horizon과 visibility margin이 지난 뒤에만 `NO_COMMIT`을 허용한다. client timeout이나 두 번 같은 head는 hard bound가 아니다.
3. 위 둘을 할 수 없다면 `NO_COMMIT`을 자동 확정하지 않고 target lease/window를 계속 유지해야 한다. 이는 안전하지만 무기한 정지 가능성이 있어 30분 RTO와 양립하지 않는다.
4. 사후 탐지만 유지하려면 guarantee 명칭을 `DETECT_AND_REPAIR` 또는 `BEST_EFFORT`로 낮춰야 한다. `ZERO_GAP`이나 “이중 commit 0”으로 부르면 안 된다.

“새 상태·테이블·구성요소 0”이라는 제약 아래에서는 3 또는 4만 가능하다. **현재 외부 commit 경계가 fencing token을 검사하지 않는 한, Control DB semantic patch만으로 prevention을 만들 수 없다.**

#### FI-50 교정

FI-50의 합격 조건은 다음으로 바꿔야 한다.

```text
old target intent가 terminal이 아니면:
  new target lease grant = 0
  new SparkApplication create = 0
  new ingest snapshot = 0

모든 관측 시점에서:
  count(overlapping main ingest snapshots for same job/window) = 0
```

`late snapshot CAS = 0`이나 “repair 후 diff 0”은 보조 증거일 뿐 합격 기준이 될 수 없다.

### P0-02. Application timestamp의 commit bound가 Oracle에서 강제되지 않는다

#### 근거와 반례

A:328과 A:925는 `max_commit_minus_watermark_seconds`를 `ENFORCED`로 등록할 수 있게 하고, P:267의 stub은 bound를 넘는 commit을 `REJECTED_BY_BOUND`로 거부한다. 그러나 Oracle용 강제 장치가 구체화되지 않았다.

반례는 단순하다.

1. row trigger가 DML 시점에 `UPDATE_DT = SYSTIMESTAMP`를 기록한다.
2. transaction이 `B + epsilon` 동안 열린 채 유지된다.
3. heartbeat fence와 다음 logical high가 앞으로 이동한다.
4. transaction이 `commit_time - UPDATE_DT > B`인 상태로 정상 commit된다.
5. 이후 overlap의 low보다 UPDATE_DT가 과거여서 그 row를 영구히 놓친다.

Oracle profile의 session 자원 제한이 transaction-age hard bound를 제공하는 것은 아니다. 일부 session 자원 제한에 걸린 뒤에도 현재 transaction에 대해 COMMIT 또는 ROLLBACK이 허용될 수 있다. [Oracle 19c 사용자 자원 제한 동작](https://docs.oracle.com/en/database/oracle/oracle-database/19/dbseg/configuring-user-resource-limits.html)

#### 필수 수정

- `APPLICATION_TIMESTAMP_WITH_OVERLAP`를 `ZERO_GAP`으로 올리려면 commit 순서와 직접 연결된 SCN/CDC offset을 사용한다.
- 또는 Oracle에서 실제로 commit을 거부/강제하는 source-specific 장치를 정의하고 그 DDL·상태·digest·우회 권한을 evidence에 묶는다.
- row trigger + profile + 실측만 가능한 Source는 `BEST_EFFORT`로 고정한다.

#### 추가 시험

`FI-41g LONG_OPEN_TRANSACTION`

- watermark trigger 실행 뒤 transaction을 `B + 60s` 유지하고 commit한다.
- 합격은 `commit rejected` 또는 `ZERO_GAP publish/Guard rejected` 중 하나다.
- commit 성공과 ZERO_GAP 다음 두 회차의 source-target diff가 함께 관측되면 즉시 No-Go다.

### P0-03. 비동기 `PK_RECONCILE`은 `ZERO_GAP`이 아니다

A:511은 `PK_RECONCILE(interval)`을 ZERO_GAP에 허용하고, P:269는 interval 전 target에 stale delete 20건이 남는 것을 “설명된 차이”로 요구한다. 반면 P:329, P:360, P:374는 ZERO_GAP에서 양방향 차집합 0을 요구한다. 두 정의는 동시에 참일 수 없다.

사용자 관점에서 contract가 FINALIZED된 직후 source에서 삭제된 row가 target에 최대 1시간 남는다면 이는 `ZERO_GAP`이 아니라 **bounded delete lag**다.

#### 필수 수정

| delete 방식 | 허용 guarantee |
|---|---|
| watermark와 같은 transaction 의미로 전달되는 soft delete | `ZERO_GAP` 후보 |
| finalize 전 동기 PK reconciliation | `ZERO_GAP` 후보 |
| commit-ordered CDC delete offset | `ZERO_GAP` 후보 |
| 비동기 `PK_RECONCILE(interval)` | `BOUNDED_DELETE_LAG` 또는 `BEST_EFFORT` |
| `CDC_LATER`, `NONE_DECLARED` | `BEST_EFFORT` |

PoC는 “interval 뒤 diff 0”만 보지 말고 각 ingest `finalized_at` 직후의 `target - source`도 0인지 판정해야 한다.

### P0-04. `zero_gap_evidence`가 불완전하고 fail-open이다

A:328의 evidence digest는 일부 SourceCapability만 포함하고 Spark/Iceberg/Oracle/OJDBC 버전과 connection topology를 명시적으로 제외한다. DDL/trigger, heartbeat job, timestamp timezone/precision, resolved TNS/service/wallet, UNDO/LOB retention이 바뀌어도 같은 proof로 실행될 수 있다. 더 심각하게는 evidence가 무효화돼도 기존 ACTIVE release는 경고만 받고 계속 ZERO_GAP contract를 실행한다(A:328, A:1346).

#### 필요한 binding

`full_semantics_digest`에 최소 다음을 넣어야 한다.

- DB/PDB identity와 incarnation
- Oracle, OJDBC, Spark, Iceberg, Template/image digest
- resolved TNS descriptor, service, RAC/standby topology, wallet digest
- lag query와 heartbeat table/job DDL digest
- watermark trigger/constraint DDL, timestamp type·timezone·precision
- UNDO guarantee와 table/LOB별 retention
- delete semantics 구현 version

evidence ID와 digest는 JobSpecVersion, release, contract, attempt에 고정해야 한다. mismatch나 만료는 warning이 아니라 Source `HOLD_NEW` 또는 Guard fail-closed여야 하며, **새 publish만 막고 기존 ACTIVE를 계속 실행하는 경로는 제거**해야 한다.

#### 합격식

```sql
SELECT count(*)
FROM guard_result g
JOIN execution_contract c USING (contract_id)
JOIN source_system s ON s.id = c.source_id
LEFT JOIN zero_gap_evidence e ON e.id = c.zero_gap_evidence_id
WHERE c.guarantee_grade = 'ZERO_GAP'
  AND g.result = 'OK'
  AND (
       e.id IS NULL
    OR e.status <> 'PASSED'
    OR e.valid_until <= g.at
    OR e.full_semantics_digest <> c.full_semantics_digest
  );
-- 0
```

### P0-05. `PER_CHUNK_FENCE` final sweep가 coherent initial load를 만들지 못한다

#### 세 가지 반례

1. **Hard delete:** early chunk가 K를 읽은 뒤 K가 삭제되면 final sweep의 source input에는 K가 없다. 일반 anti-join/dedup/MERGE는 target의 K를 삭제할 source action을 만들지 못한다.
2. **LOB change/undo:** early chunk의 CLOB/BLOB이 바뀌고 old LOB version이 유지되지 않으면 final-fence exact comparison이나 재독이 실패한다. Oracle은 LOB column별로 Flashback용 `RETENTION` 설정이 필요하다고 설명한다. [Oracle Flashback의 LOB 설정](https://docs.oracle.com/en/database/oracle/oracle-database/19/adfns/flashback.html)
3. **Lower-bound ambiguity:** A:925의 안전한 시간 하한은 heartbeat의 `T_lb`인데 A:1007의 sweep low는 `fence_ts_1 - overlap`이다. `fence_ts_1 := T_lb_1`이라는 불변식이 없다. fence wall time이 T_lb보다 뒤라면 sweep low가 너무 높아져 bounded late row를 놓친다.

P:273–274는 late insert/update만 검증하고 hard delete, BasicFiles/SecureFiles LOB churn, delayed heartbeat와 sweep 경계를 결합하지 않는다. P:184는 LOB를 비교에서 제외하고 설명된 차이로 처리할 수도 있어 `미비교 0`과 충돌한다.

#### 필수 수정

- `ZERO_GAP` initial load 기본은 `EXTRACT_ONCE`로 제한한다.
- PER_CHUNK_FENCE를 유지하려면 마지막 한 SCN에서 source-minus-target와 target-minus-source를 모두 적용하는 **full exact reconciliation phase**를 둔다. LOB는 같은 final SCN에서 hash까지 비교해야 한다.
- sweep lower bound는 `T_lb_1 - overlap`로 명시하거나 `fence_ts_1 = T_lb_1`을 타입과 함께 불변식으로 고정한다.
- LOB를 제외한 회차는 `ZERO_GAP` 표본에서 탈락시키고 `uncompared_columns > 0`으로 기록한다.

#### 추가 FI

- early chunk 직후 이미 적재한 PK 20건 hard delete
- SecureFiles CLOB/BLOB 및 BasicFiles LOB 변경과 old-version pressure
- heartbeat 60초 지연 + bound 경계 late commit + final sweep 조합
- final SCN에서 양방향 PK 차집합 0 및 LOB SHA-256 차이 0

### P0-06. Source session tag 충돌로 `reserved_unrealized`가 과소계산될 수 있다

A:909는 길이 제한에 맞춰 `job·contract_short·attempt`를 MODULE/ACTION/CLIENT_IDENTIFIER에 기록하고 이를 token별 `tagged_observed_t` 계산에 사용한다. `contract_short`의 생성 길이와 충돌 처리도 규정돼 있지 않다. 이 값이 injective하지 않으면 같은 live session을 두 token 모두의 관측값으로 셀 수 있다.

예를 들어 A/B token weight가 각각 2이고 short tag가 같을 때 A 세션 2개만 열려 있어도 A와 B 각각 `tagged_observed=2`로 계산될 수 있다. B의 미실현 reservation 2가 0으로 사라지고 C가 grant된 뒤 B가 연결하면 pool cap을 넘는다.

#### 최소 수정

- `CLIENT_IDENTIFIER`를 축약 contract가 아니라 **완전한 `source_lease_id` UUID**로 사용한다. UUID text와 짧은 prefix는 64 bytes 안에 들어간다.
- MODULE/ACTION은 사람이 읽는 보조 정보로만 쓰고 회계 join key로 쓰지 않는다.
- session init이 성공해 exact lease ID가 관측되기 전까지 해당 token의 weight 전부를 unrealized로 센다.
- former-short encoding이 충돌하는 두 UUID를 의도적으로 만든 FI를 추가한다.

이 항목을 P0로 둔 이유는 충돌 가능성 자체보다 **Source 절대 한도를 hard invariant로 주장하고 있기 때문**이다. exact tag가 injective임을 구현·시험으로 증명한다면 즉시 종결할 수 있지만, 그 전에는 `BEST_EFFORT` PoC도 Source 절대 한도 합격을 주장할 수 없다.

### P0-07. PoC 6차 oracle은 현재 동시에 만족할 수 없거나 비주입적이지 않다

#### A. FI-50와 전역 commit oracle 충돌

P:281 (iii)은 attempt 2 FINALIZED 뒤 attempt 1 snapshot 착지를 합격으로 허용한다. 이 순간 attempt 1과 attempt 2의 `[L,H1)` ingest snapshot이 둘 다 존재한다. P:359의 pairwise overlap 0은 반드시 실패한다. repair lane은 과거 ingest snapshot 두 개를 지우지 않는다.

#### B. REATTACH가 SUBMITTED interval을 잘못 끝낸다

P:358은 `to_state=SUBMITTED` row부터 같은 attempt의 “다음 row”까지를 writer interval로 잡는다. A:348과 A:469는 REATTACH를 `from_state=to_state`, 상태 변화 없는 history row로 남긴다. 따라서 REATTACH 시점에 SUBMITTED interval이 잘리지만 SA/writer는 계속 살아 있다. 이후 실제 겹침을 oracle이 놓칠 수 있다.

수정식은 다음과 같아야 한다.

```text
writer interval = first SUBMITTED.at
                  .. first state-changing terminal/fenced exit.at

REATTACH history rows are ignored as interval boundaries.
```

#### C. canonical `STANDARD_HASH` encoding이 injective하지 않다

P:184는 delimiter `|`, 값 안의 `| -> \|`, NULL sentinel `<<NULL>>`을 사용한다. 다음 두 종류의 collision이 즉시 가능하다.

- SQL NULL과 실제 문자열 `<<NULL>>`
- 같은 2-column schema에서 row A `['a\\', 'b|c']`와 row B `['a|b\\', 'c']`: backslash를 escape하지 않으면 둘 다 `a\|b\|c`가 되어 delimiter 경계가 모호하다.

또한 넓은 row를 `VARCHAR2 || ...`로 만들면 Oracle 설정에 따라 4,000 또는 32,767자 제한을 만나며, CLOB로 승격하면 `STANDARD_HASH`의 입력 제약에 걸린다. Oracle 문서는 `STANDARD_HASH`가 LONG/LOB를 받지 않는다고 명시하고, VARCHAR2 concatenation 결과의 최대 길이도 설정별로 제한한다. [Oracle `STANDARD_HASH`](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/STANDARD_HASH.html), [Oracle concatenation 제한](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Concatenation-Operator.html)

권고 encoding은 타입 태그와 byte length를 포함한 다음 계열이다.

```text
column := type_tag || ':' || byte_length || ':' || raw_bytes
row    := column_count || ':' || column_1 || ... || column_n
```

wide row/LOB는 column별 SHA-256 후 `(column_id, type_tag, length, column_hash)`의 Merkle-like row hash를 만든다. 문자열 sentinel이나 escape 규칙에 의존하지 않는다. NLS, `SESSIONTIMEZONE`, numeric format, CHAR padding, Unicode normalization 주체도 sessionInit과 generator spec에 고정해야 한다.

#### D. ORA-02391 양성 대조와 전역 0건 조건 충돌

P:51은 cap+1 접속에서 ORA-02391 정확히 1회를 증거로 요구하고, P:189와 P:346은 ORA-02391 총 발생 0을 요구한다. 시험 population을 분리하지 않으면 둘 다 통과할 수 없다.

```text
tag = POSITIVE_CONTROL: ORA-02391 exactly 1
tag != POSITIVE_CONTROL: ORA-02391 exactly 0
```

#### E. authoritative ZERO_GAP 표본 누락

P:360과 P:374의 권위 표본 목록이 FI-41f(delete)와 FI-44e(initial sweep)를 명시적으로 포함하지 않는다. 두 시험은 바로 ZERO_GAP 의미를 검증하므로 필수 모집단에 포함해야 한다. LOB 제외 회차도 `설명된 차이`가 아니라 `미비교`로 불합격시켜야 한다.

---

## 6. P1 상세 발견사항

### P1-01. Dagster 제출 멱등성의 핵심 API 가정이 현재 스키마와 맞지 않는다

A:340, A:693–695와 P:258은 GraphQL `launchRun`에 `executionMetadata.runId = uuid5(...)`를 넘긴다고 가정한다. 그러나 검토한 Dagster 1.13.6/1.13.18 GraphQL `ExecutionMetadata` input에는 `tags`, `rootRunId`, `parentRunId`만 있고 `runId`가 노출되지 않는다. 내부 parser가 `runId`를 읽더라도 GraphQL schema가 unknown field를 먼저 거부한다. [Dagster 1.13.18 GraphQL input schema](https://github.com/dagster-io/dagster/blob/1.13.18/python_modules/dagster-graphql/dagster_graphql/schema/inputs.py), [Dagster launch mutation parser](https://github.com/dagster-io/dagster/blob/1.13.18/python_modules/dagster-graphql/dagster_graphql/schema/roots/mutation.py)

별도로 A:620의 schedule은 직접 RunRequest를 yield하고 A:695는 tick과 Adapter의 경합이 남는다고 인정한다. Guard binding CAS는 attempt를 하나로 만들 뿐 Dagster QUEUED/STARTING Run 두 개를 하나로 만들지 않는다.

#### 권고 수정

1. 모든 schedule은 occurrence 생성까지만 하고 실제 asset Run 제출은 하나의 durable submission worker로 보낸다. direct RunRequest와 Adapter 이중 채널을 제거한다.
2. GraphQL에서 explicit run ID를 쓸 수 없으면 `submission_in_flight`가 불명인 동안 절대 재제출하지 않고, exact generation tag 조회로 존재 여부를 확정한 뒤에만 진행한다.
3. 또는 검증된 Dagster instance API로 explicit run ID를 생성·submit하되 `versions.lock`에 API와 behavior probe를 넣는다.
4. 주차 1에서 실제 고정 버전 schema introspection으로 `ExecutionMetadata` field 목록과 crash-after-launch 시험을 실행한다.

### P1-02. Release pointer 변경의 durable activation intent가 없다

A:401–403은 외부 shard pointer를 바꾸고 `deployed_at`을 기록한 뒤 VERIFIED/ACTIVE로 진행한다. Control이 pointer 변경 직후, operation progress/deployed_at을 영속하기 전에 죽으면 실제 pointer는 R2인데 Control은 R1 ACTIVE로 남을 수 있다. ACTIVE 성공 또는 412 fallback 완료가 없으므로 자동 Gap Recovery도 기동되지 않는다.

또한 정상 ACTIVE가 이전 release의 cron 경계를 가로지를 때, Gap Recovery가 release별 effective interval의 **이전 cron**까지 전개한다는 규범이 없다. 예: R1 `:00`, R2 `:30`, pointer switch `00:59`, ACTIVE `01:05`라면 01:00 expected tick은 R1 규칙으로 복구돼야 한다.

#### 수정

- pointer CAS 전에 durable `activation_intent {from_digest,to_digest,range_start,phase}`를 commit한다.
- startup reconciler가 actual pointer, Control ACTIVE, operation phase를 대조해 rollback 또는 resume한다.
- expected cron은 `(release effective interval x 그 release의 schedule revision)`으로 전개한다.
- pointer CAS 전후, deployed_at, reload, VERIFIED, ACTIVE, fallback 각 경계에 crash FI를 둔다.

### P1-03. Cross-type target lease가 PostgreSQL invariant가 아니다

A:362는 PostgreSQL이 불변식을 증명한다고 선언하지만 A:365의 실제 제약은 EXCLUSIVE_TABLE partial unique와 partition range GiST뿐이다. `EXCLUSIVE_TABLE <-> APPEND`, fileset 교집합, fileset <-> EXCLUSIVE_TABLE은 application matrix와 `target_table FOR UPDATE` 관례다.

Control API만 통하면 안전할 수 있지만 migration, maintenance, StarRocks, 운영 스크립트, 미래 코드가 직접 INSERT하면 깨진다.

#### 수정

- target lease DML을 DB stored procedure 하나로만 허용한다.
- application role의 direct INSERT/UPDATE 권한을 회수한다.
- procedure가 canonical row lock, conflict matrix, insert를 한 transaction에서 수행한다.
- FI-51에 Control API뿐 아니라 raw PostgreSQL session 2개의 direct DML 대조를 넣는다.

### P1-04. Guard의 Source SHARE lock을 Source UPDATE로 승격하면 deadlock 가능하다

A:364는 Guard가 Source `FOR SHARE`를 먼저 잡고 role/identity mismatch를 Guard 6번에서 발견한 뒤 Source scope 자동 Hold를 만들도록 한다. Source Hold는 Source `FOR UPDATE`를 먼저 잡아야 한다. 두 Guard가 SHARE를 각각 보유한 채 둘 다 UPDATE로 승격하면 deadlock이 가능하다.

#### 수정

- Source-wide condition을 발견하면 현재 transaction을 rollback하고 Source `FOR UPDATE`부터 잡는 별도 transaction으로 재시작한 뒤 조건을 재검증한다.
- 또는 모든 Guard를 Source UPDATE로 직렬화하되 throughput 비용을 PoC에서 측정한다.
- role mismatch를 동시에 발견하는 Guard 20개의 deadlock FI를 추가한다.

### P1-05. Maintenance lease reclaim 뒤 늦은 commit을 허용한다

A:1435와 P:209는 maintenance lease가 회수된 뒤 snapshot이 착지하는 것을 허용하고 다음 ingestion이 `RECONCILIATION_REQUIRED`로 찾으면 합격으로 본다. 이는 fencing이 아니라 사후 감사다.

더구나 P:358은 lease interval을 GRANTED부터 RELEASED까지로 정의하므로 RECLAIMED 뒤 RELEASED 전 snapshot을 “lease interval 밖”이라고 한 A:1435의 설명과도 맞지 않는다.

두 시간을 구분해야 한다.

- writer authorization interval: `GRANTED .. RECLAIMED/FENCED`
- source capacity accounting interval: `GRANTED .. RELEASED`

maintenance snapshot 유효성은 첫 번째 구간으로 판정해야 한다. P0-01의 target token이 없으면 production table의 maintenance lease를 in-flight 요청이 사라졌다고 확정하기 전 재할당할 수 없다.

### P1-06. Outbox deterministic key가 transition instance를 유일하게 식별하지 않는다

A:1340의 예시 `(from_state,to_state,attempt_no)`는 같은 attempt에서 재사용될 수 있다. 예를 들어 worker-loss/reattach cycle이 두 번 발생하면 같은 전이가 반복되고 두 번째 event가 같은 ID로 suppress될 수 있다.

#### 수정

- aggregate마다 monotonic `transition_version`을 두고 `event_id = hash(aggregate_id, transition_version, event_type)`로 만든다.
- history row ID처럼 이미 유일하고 영속적인 전이 ID가 있으면 그것을 사용한다.
- 같은 attempt에서 reattach/fence 관련 cycle 두 번, Outbox publisher PITR replay를 결합해 검증한다.

### P1-07. RESYNC history “정확히 1건”을 강제하는 key가 없다

A:308은 단계 중간 crash 뒤에도 rebuilt entity별 `actor=RESYNC` history row 1건을 주장한다. 그러나 A:365의 10개 제약에는 이를 보장하는 unique key가 없다. append-only history에 같은 단계가 재실행되면 중복 row가 생길 수 있다.

다음 계열의 key가 필요하다.

```text
UNIQUE(resync_operation_id, step, entity_kind, entity_id)
```

또는 별도 reconstruction marker에 `INSERT ... ON CONFLICT`를 사용한다. FI-05c는 entity 경계마다 kill해야 한다.

### P1-08. Freshness coverage가 적용된 watermark보다 앞설 수 있다

A:1317은 coverage를 마지막 CAS 계약의 `contract.window.high`로 정의한다. 그러나 contract는 `[L,H)`를 예약한 뒤 일부 chunk만 CAS하고 DRAIN, expiry, abort, reconciliation 상태로 끝날 수 있다. 실제 coverage는 `h_k`인데 UI는 H를 보여줄 수 있다.

```text
coverage = current production watermark
         = max(ledger.window_high where cas_applied = true)
```

Full은 coverage를 별도 source snapshot/finalized 시각으로 표현하고 Incremental과 혼합하지 않는다. partial CAS 뒤 각 close mode를 시험해야 한다.

### P1-09. DB hard-cap evidence가 inventory일 뿐 gate가 아니다

A:328은 `RESOURCE_LIMIT`, applied profile, scope, cap+1 evidence를 저장하지만 A:511과 A:519의 publish rule, Guard rule에는 이를 fail-closed로 요구하는 조건이 없다. A:917도 22장 DBA 결정으로 남긴다.

Oracle은 profile resource limit이 `RESOURCE_LIMIT=TRUE`일 때만 집행된다고 설명한다. [Oracle 19c `RESOURCE_LIMIT`](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/RESOURCE_LIMIT.html)

`RESOURCE_LIMIT=false`, `SESSIONS_PER_USER=UNLIMITED`, 잘못된 PDB/RAC scope, 만료된 cap+1 evidence 중 하나라도 있으면 Source ACTIVE 또는 Guard를 거부해야 한다. `pool_cap + monitor/legacy margin < sessions_per_user`도 검증한다.

### P1-10. Credential fan-out의 실제 경쟁을 시험하지 않는다

P:236 FI-16은 password를 precheck 전에 바꿔 executor connection이 열리지 않는다. 따라서 “precheck 성공 -> password 변경 -> 여러 executor partition 동시 인증 실패”를 검증하지 않는다. `password_rollover_registered=true` 대조도 bool arithmetic만 보고 stub rollover를 구현하지 않는다.

#### 추가 FI

1. N개 attempt의 driver precheck를 성공시킨다.
2. executor connection 직전에 모두 pause한다.
3. rollover 없이 password를 바꾼다.
4. 모든 partition을 동시에 연다.
5. task retry가 첫 ORA-01017/28000 뒤 중지되고 lockout threshold를 넘지 않는지 본다.
6. 검증된 gradual rollover를 실제 Oracle에서 반복한다.

rollover는 boolean이 아니라 `verified_at`, DB identity, profile/Oracle version, duration을 가진 evidence여야 한다.

### P1-11. Fence origin과 execution ConnectionRevision을 완전히 감사할 수 없다

A:341은 attempt `connection_revision_id`를 추가했지만 contract에는 fence를 실제 읽은 monitor session의 revision ID와 expanded descriptor digest가 없다. A:905의 persistent monitor가 R1에 붙은 채 R2가 ACTIVE가 되면 Guard가 R2를 pin했다고 기록하면서 fence는 R1에서 읽을 수 있다.

필요 필드:

- contract: `fence_connection_revision_id`, `fence_monitor_sample_id`, `fence_resolved_descriptor_digest`
- attempt: `execution_connection_revision_id`, `execution_resolved_descriptor_digest`

ACTIVE 전이는 구 monitor session을 교체·검증한 뒤에만 새 Guard를 허용해야 한다. TNS alias, ADDRESS failover, wallet을 각각 digest에 포함한다.

### P1-12. Generic ORA-01722를 role assertion channel로 쓰면 오분류한다

A:1041의 DB-object-free assertion은 mismatch 시 `TO_NUMBER('SOURCE_ROLE_MISMATCH')`로 ORA-01722를 만든 뒤 모든 extract SQL의 ORA-01722를 Source role mismatch로 매핑한다. 실제 source data conversion 오류도 ORA-01722이므로 정상 standby에서 Source-wide Hold가 열릴 수 있다. LONG/LONG RAW projection은 set operator 제약 때문에 같은 UNION branch를 사용할 수 없다.

#### 수정

- DBA 승인 package/function이 `RAISE_APPLICATION_ERROR(-20901/-20902)`만 내도록 하는 경로를 production 필수로 둔다.
- DB object를 허용하지 않는 Source는 unsupported type/template을 publish에서 거부하거나 `BEST_EFFORT`로 낮춘다.
- generic ORA-01722는 `SPARK_FAILED(DATA_CONVERSION)`이며 monitor evidence 없이 role mismatch로 바꾸지 않는다.
- Spark의 `sessionInitStatement`가 각 remote session open 뒤 read 전에 실행되는 점은 공식 동작과 맞다. 문제는 그 이후 transition과 error channel이다. [Spark JDBC `sessionInitStatement`](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

### P1-13. Gap Recovery owner의 failover liveness가 시험되지 않는다

A:312의 singleton/advisory lock 및 durable cursor는 방향이 맞다. 그러나 scheduler leader kill, advisory lock loss, 새 leader handoff, cursor commit 직전/후 crash를 검증하는 FI가 없다.

추가 시험은 다음을 요구해야 한다.

- active leader kill 뒤 `2 x planned_scan_interval` 안에 새 owner가 lock을 획득
- 같은 recovery range operation 중복 0
- cursor gap/skip 0
- expected occurrence unexplained missing 0

---

## 7. P2 문서·스키마 정합 문제

### P2-01. PoC ledger 컬럼 목록에 `dq_basis`가 두 번 나온다

P:113의 `CommitEvidenceLedger` 열 목록에 `dq_basis`가 두 번 선언돼 있다. 주차 1 DDL의 단일 schema contract와 어긋난다. 한 번만 남기고 Merge 외 NULL이라는 조건을 명시한다.

### P2-02. FI-51의 `lease_grant non-null` 증거 필드가 의미상 맞지 않는다

P:282는 target lease 경합에서 진 Guard의 `guard_result.lease_grant`가 non-null이라고 요구한다. A:350의 `lease_grant` JSON은 Source admission 8번의 `observed/reserved_unrealized/requested_weight/pool_cap`이다. target conflict는 source token 평가 전 발생할 수 있으므로 이 값은 null이 정상이다.

`target_lease_conflict {table_id, conflicting_lease_id, requested_type, conflict_rule}`를 별도 evidence로 기록하거나 FI-51에서 `lease_grant` non-null 조건을 제거해야 한다.

---

## 8. v1.2.3 최소 semantic patch 제안

### 8.1 Freeze 전 필수 7건

| ID | 최소 결정 | 상태/테이블 증가 없이 가능한가 |
|---|---|---|
| M1 | target late commit을 target-side token으로 막거나 guarantee를 downgrade | 강한 보장은 **불가**. target-side mechanism 필요 |
| M2 | application timestamp ZERO_GAP를 commit-ordered offset으로 제한 | 가능. validator/등급 수정 |
| M3 | async PK_RECONCILE을 `BOUNDED_DELETE_LAG`로 재분류 | 가능. enum/계약 문구 수정 필요 |
| M4 | full semantics evidence digest + invalidation 시 Guard fail-closed | 기존 JSON/필드 확장으로 가능 |
| M5 | PER_CHUNK_FENCE를 BEST_EFFORT로 제한하거나 final exact reconciliation 추가 | downgrade는 가능, 강한 보장은 추가 실행 단계 필요 |
| M6 | Source session 회계 tag를 full lease UUID로 교체 | 가능 |
| M7 | FI-50·writer interval·canonical hash·positive-control population 교정 | 가능 |

### 8.2 이어서 닫을 P1 9개 묶음

1. schedule과 Adapter를 하나의 durable submission path로 합치고 실제 Dagster 버전 API를 probe한다.
2. release pointer CAS 전에 durable activation intent를 기록하고 release별 cron expansion을 규정한다.
3. target lease grant를 DB procedure로 강제하고 direct DML 권한을 제거한다.
4. Source-wide automatic Hold는 lock upgrade 대신 transaction restart 패턴을 쓴다.
5. Outbox에 transition instance ID를 넣고 RESYNC history unique key를 추가한다.
6. coverage를 last applied CAS/current watermark로 계산한다.
7. hard-cap evidence, credential rollover evidence, fence/execution revision evidence를 Guard에 묶는다.
8. ORA-01722 assertion을 제거하고 named error만 사용한다.
9. maintenance late commit과 Gap Recovery owner failover를 별도 FI로 만든다.

---

## 9. 주차 1 executable appendix 권고 범위

현재 바로 1,600줄 전체를 DDL로 옮기기보다 다음 순서가 안전하다.

### 9.1 먼저 1~2일 semantic errata

- `ZERO_GAP`, `BOUNDED_DELETE_LAG`, `BEST_EFFORT`, `DETECT_AND_REPAIR`의 소비자 관점 정의
- target late commit 처리 선택(M1)
- async delete와 PER_CHUNK_FENCE 등급
- canonical row serialization v1
- writer authorization interval과 capacity accounting interval
- Dagster 제출 단일 경로

### 9.2 병행 가능한 executable 작업

1. **DDL core**
   - occurrence/contract/attempt/ledger unique
   - window exclusion
   - source lease state와 full lease tag
   - target lease grant stored procedure와 role 권한
   - Outbox transition version
   - RESYNC reconstruction unique key

2. **판정 SQL dry-run**
   - expected occurrence conservation
   - REATTACH를 무시한 writer interval
   - main ancestry와 ingest lane overlap
   - last applied CAS coverage
   - positive-control population 분리

3. **Oracle executable probes**
   - long-open transaction `B + epsilon`
   - same-CDB wrong-PDB
   - exact lease tag collision 음성/양성
   - hard delete + soft delete + async reconcile
   - SecureFiles/BasicFiles LOB flashback
   - cap+1 ORA-02391 tagged positive control

4. **Dagster executable probes**
   - 고정 버전 GraphQL schema introspection
   - `executionMetadata.runId` 실제 요청의 성공/validation error
   - launch response 유실 뒤 generation tag 조회
   - schedule/Adapter 동시 제출과 non-terminal Run 수

5. **Canonical hash vectors**
   - NULL vs literal sentinel
   - delimiter/backslash collision
   - 4K/32K 초과 wide row
   - Unicode composed/decomposed forms
   - NUMBER scale, negative zero, timestamp timezone
   - CLOB/BLOB 0B·4K·1MB+

### 9.3 Gate를 두 개가 아니라 세 개로 분리

| Gate | 합격 의미 |
|---|---|
| G0 — Spec/Oracle Executability | DDL, API, canonical hash, 판정 SQL이 실제 고정 버전에서 실행되고 문서 모순 0 |
| G1 — Scale/Control | 40,000 occurrence/day, 500 burst, Hold/retry/queue/release가 `BEST_EFFORT`에서도 안전·운영 가능 |
| G2 — Oracle Strong Consistency | source별 commit/delete/flashback/evidence proof를 통과한 경우에만 강한 guarantee 활성화 |

G0가 실패하면 soak를 시작하지 않는다. G1 통과와 G2 실패는 플랫폼 자체의 실패가 아니라 해당 Source의 강한 guarantee 비활성화로 처리할 수 있다.

---

## 10. 리뷰 보드 권고안

### 지금 승인해도 되는 것

- Dagster + Control Plane + Spark Operator의 큰 구조
- metadata-driven grouped definitions
- immutable JobSpec/Template/Release
- Source/Job Wizard, audit, lineage, observability UI
- contract/attempt/occurrence 분리
- watermark/lease/ledger를 Control PostgreSQL의 권위로 두는 원칙
- Hold와 watermark-based catch-up
- `BEST_EFFORT` 중심 Scale/Control skeleton

### 승인 보류할 것

- v1.2.2 semantic freeze
- PoC 6차 acceptance freeze
- application timestamp `ZERO_GAP`
- async PK_RECONCILE을 포함한 `ZERO_GAP`
- PER_CHUNK_FENCE `ZERO_GAP`
- target commit의 두 번 head settle을 terminal verdict로 사용하는 규칙
- `executionMetadata.runId`에 의존한 제출 멱등성

### 최종 권고

**v1.2.3은 기능을 더 넣는 버전이 아니라 guarantee boundary와 acceptance oracle을 바로잡는 짧은 semantic patch여야 한다.** 그 뒤 주차 1 executable appendix를 freeze하는 것이 맞다.

권고 순서는 다음과 같다.

```text
v1.2.3 semantic errata
  -> G0 executable appendix
  -> G1 Scale/Control PoC
  -> Source별 G2 Oracle evidence
  -> 해당 Source/Job에만 strong guarantee 활성화
```

이 순서라면 Dagster 전환과 UI/운영성 개선은 지연시키지 않으면서, 증명되지 않은 `ZERO_GAP` 약속이 10,000개 Job에 확산되는 것을 막을 수 있다.

---

## 부록 A. 검토 artifact 식별값

| 파일 | 크기 | PowerShell `Get-Content` 행 수 | SHA-256 |
|---|---:|---:|---|
| `etl-platform-target-architecture-v1.2.1.md` | 314,452 B | 1,584 | `C8B3E5059ADB7DFFB50FEEE184CCDDC91BE5E14A77DDA610E7AA74FE34D2C315` |
| `etl-platform-target-architecture-v1.2.2.md` | 380,551 B | 1,633 | `5A19684A930E96DFDC45100FB877435CC715F8073F3C2FD1EA0572F9EAAB1970` |
| `etl-platform-poc-test-plan-v1.md` 6차 | 227,776 B | 551 | `AEE0574830ECF10543B617FEC10F4E61F6E9491D293BFDD574454D1A08F5ACFB` |
| `etl-platform-v1.2.1-codex-second-cross-review.md` | 50,803 B | 846 | `347D99D224AFB3664BF367EC476536B8D7359520E86C337DFB401AE9C8A3B9EB` |

사용자가 제시한 1,634/552행과 PowerShell 행 수의 1행 차이는 마지막 newline 계산 방식 차이로 보이며 내용 무결성 문제로 판정하지 않았다.

## 부록 B. 공식 구현·문서 확인 요약

- Iceberg는 commit 성공/실패를 확정할 수 없는 상태를 별도 취급하며, 그 상태에서 후속 행동은 위험하다고 규정한다.
- Dagster 1.13.18 GraphQL `ExecutionMetadata` input에는 현재 `runId`가 노출되지 않는다. 선택 버전의 schema introspection이 필수다.
- Spark JDBC의 `sessionInitStatement`는 각 remote session이 열린 뒤 read 전에 실행된다.
- Oracle profile resource limit은 `RESOURCE_LIMIT` 활성화가 전제다.
- Oracle `STANDARD_HASH`는 LONG/LOB 입력을 받지 않으며, VARCHAR2 concatenation에는 설정별 길이 제한이 있다.
- Oracle Flashback에서 LOB는 column별 retention 설정과 별도 검증이 필요하다.
