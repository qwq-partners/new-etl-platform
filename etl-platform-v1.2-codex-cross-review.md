# ETL Platform 목표 아키텍처 v1.2 — Codex 심층 교차 리뷰

- 검토일: 2026-08-23
- 검토 대상 A: `etl-platform-target-architecture-v1.2.md` (이하 `A`)
- 검토 대상 P: `etl-platform-poc-test-plan-v1.md` (이하 `P`)
- 범위: 아키텍처 의미론, Control Plane 동시성, Oracle/Data Guard fence, Dagster·Spark/Iceberg 런타임, PoC 판정력
- 방식: 문서 간 교차 추적 + 실패 시나리오 기반 검증 + Oracle/Apache Iceberg/Dagster/Kubernetes 공식 문서 대조
- 원본 문서는 수정하지 않았다.

---

## 1. 최종 판정

### 1.1 한 줄 결론

**Dagster 중심의 목표 방향은 `GO`다. 그러나 v1.2의 의미론 동결과 Phase 1 PoC 실행은 `CONDITIONAL NO-GO`다.**

이는 제품 선정이나 전체 설계를 뒤집으라는 뜻이 아니다. 현재 구조에는 실제 데이터가 빠졌는데도 `ZERO_GAP` 또는 `FINALIZED`로 끝날 수 있는 경로, 이미 commit된 쓰기를 모른 채 새 writer를 허용할 수 있는 경로, Oracle의 실제 session 수가 hard cap을 넘을 수 있는 경로가 남아 있다. 이 경로들을 먼저 닫아야 PoC가 설계를 검증하는 시험이 된다.

### 1.2 판정 요약

| 판정 대상 | 결과 | 이유 |
|---|---:|---|
| Dagster + 별도 Control Plane 방향 | GO | 10,000 Job을 파일 10,000개로 만들지 않고, 실행 UI와 실행 엔진을 Dagster로 대체하면서 중앙 정책·권위 상태를 Control Plane에 둘 수 있다. |
| Occurrence → Contract → Attempt 모델 | GO, 보완 필요 | 실행 정체성과 retry를 분리한 방향은 좋다. 다만 recovery epoch, submission reservation, DB 제약을 보강해야 한다. |
| `AS OF visible_scn` 기반 fence | GO, 조건부 | 동일 Oracle identity와 동일 revision에서 fence/extract가 수행되고 업무 watermark의 hard bound가 증명될 때만 `ZERO_GAP`이다. |
| Commit Adjudication | GO, 보완 필요 | writer fencing과 commit 판정을 분리한 구조는 타당하다. `verdict=NULL` 만료와 0-row 증거 규칙은 안전하지 않다. |
| Source 보호 | CONDITIONAL | weighted lease 방향은 좋지만 Pod 부재를 Oracle session 종료로 간주하면 실제 hard cap을 넘을 수 있다. |
| DefinitionRelease | CONDITIONAL | bundle 검증과 rollback operation은 적절하다. serving pointer와 Control `ACTIVE`가 분리되어 원자성이 깨진다. |
| PoC 기준서의 현재 판정력 | NO-GO | SCN과 시간 lag의 단위가 섞였고, 핵심 No-Go SQL이 retry를 중복으로 오판하거나 실제 row 누락을 못 잡는다. |
| Phase 0 baseline 수집 | 즉시 가능 | 읽기 전용 baseline, 버전 고정, Oracle capability 확인은 병행해도 된다. |
| Phase 1 장애·규모 시험 | P0 수정 후 | 먼저 의미론과 시험 oracle을 고쳐야 시험 통과가 실제 안전성을 뜻한다. |

### 1.3 발견 건수

이번 리뷰는 다음으로 분류했다.

- **P0 10건**: PoC 시작 전에 문서와 시험을 반드시 수정해야 하는 안전성·판정력 결함
- **P1 23건**: Phase 1 게이트 전에 수정하거나 명시적으로 제한해야 하는 운영·동시성 결함
- **P2 8건**: MVP 설계 동결 전에 정리할 명세·용어·구현 가능성 결함

가장 중요한 사실은 P0 중 네 건이 단순 가용성 문제가 아니라 **silent data loss 또는 silent duplicate** 경로라는 점이다.

---

## 2. v1.2에서 확실히 좋아진 점

v1.2는 이전 논의를 단순히 늘린 문서가 아니다. 실제 운영 실패를 상태·증거·복구 프로토콜로 바꾼 수준이 높다.

1. **Job 파일 폭증을 피한 Definition 전략**
   
   Job마다 DAG 소스 파일을 생성하지 않고 공용 template/asset code와 DB 기반 JobSpec을 결합한 방향은 현재의 10,000 Job 규모에 맞다. 공용 template 변경을 release 단위로 검증·활성화하려는 구조도 현재 “template 하나 수정 → 사용 Job 전체 변경”의 장점을 유지한다.

2. **Dagster를 실행 권위가 아닌 실행 표면으로 제한**
   
   watermark, contract, lease, commit evidence의 권위를 Control PostgreSQL에 둔 결정은 타당하다. Dagster 장애나 run history 정리 때문에 ETL 의미론이 사라지지 않는다.

3. **Occurrence / Contract / Attempt 분리**
   
   논리 실행 시점, 데이터 처리 약속, 물리 retry를 분리한 것은 중복 방지와 감사 가능성을 동시에 높인다.

4. **Guard의 fail-closed 순서**
   
   Hold·release·target·source·lease를 Spark 제출 전에 검사하는 구조는 원천 보호와 불필요한 Pod 생성을 줄인다.

5. **writer fencing과 commit adjudication 분리**
   
   “writer가 더 이상 쓸 수 없음”과 “외부 target에 commit됐는지”를 별개 사실로 다룬 점은 매우 좋다. 이 구분은 ambiguous commit을 안전하게 처리하는 핵심이다.

6. **chunk evidence + watermark CAS + Outbox의 Control 트랜잭션화**
   
   성공 여부를 Dagster SUCCESS 하나로 판단하지 않고 durable evidence로 판단하는 방향은 맞다.

7. **Hold 해제 후 CATCHUP을 원천 부하 중심으로 설계**
   
   수백 회차를 한꺼번에 재실행하지 않고 watermark부터 Hold 종료까지 coalesce하는 기본 정책은 생산장비계 DR DB 보호 목표와 일치한다.

8. **LLM Advisor를 승인 없는 실행기로 만들지 않음**
   
   통계와 metadata로 Full/Incremental, 시간칼럼, source policy를 추천하되 운영자가 최종 승인하는 구조가 적절하다.

9. **관측 이력을 1급 데이터로 승격**
   
   `contract_state_history`, `attempt_state_history`, `lease_state_history`, `guard_result`, `attempt_timeline`을 운영 테이블로 채택한 D1은 PoC와 운영의 설명력을 크게 높인다.

따라서 아래 지적은 “Dagster가 틀렸다”거나 “다시 Airflow로 가야 한다”는 결론이 아니다. **현재 설계의 안전성 경계를 더 정확히 닫기 위한 교정**이다.

---

## 3. v1.2 핵심 결정 14건 재판정

| ID | Codex 판정 | 리뷰 |
|---|---:|---|
| A1 `DEGRADED_CONFIDENCE` | 조건부 승인 | 모든 chunk가 동일한 `AS OF SCN`을 사용한다면 실행 중 lag 상승으로 CAS를 거부하지 않는 판단은 맞다. 단, fence와 extract가 동일 ConnectionRevision/DB identity를 써야 하고 `confidence`는 `FULL/DEGRADED`보다 `VERIFIED/DATUM_STALE/LAG_QUERY_FAILED/NO_SIGNAL`처럼 증거 상태로 분리해야 한다. |
| A2 `TARGET_UNAVAILABLE` | 조건부 승인 | Source 조회 전 target breaker는 좋다. 다만 신규 target table의 provisioning 상태, Polaris와 AIStor의 독립 fault domain, adjudication 장기 장애 시 `verdict=NULL` 만료를 보강해야 한다. |
| A3 zombie ownership | 조건부 승인 | `binding 일치 ∧ terminal_ingested_at IS NULL`은 Run Pod의 제어 호출 소유권으로는 일관된다. 그러나 terminal 반입 뒤에도 credential/source session 증거와 cleanup은 별도 immutable 채널로 수락해야 한다. |
| A4 external snapshot | 승인 | Adjudication과 구분한 `RECONCILIATION_REQUIRED(EXTERNAL_SNAPSHOT)`이 적절하다. 다만 fault injection이 남긴 metadata 없는 snapshot은 전체 metadata 100% 판정의 분모에서 명시적으로 격리해야 한다. |
| B1 CATCHUP `expires_at` | 조건부 승인 | 만료 면제 불채택 자체는 합리적이다. 다만 `PLANNED`끼리는 coalesce하지 않는다고 문서가 규정하므로 “CATCHUP이 NORMAL을 영구 흡수한다”는 근거는 자기모순이다. 만료 근거는 stale intent 정리와 bounded queue로 바꿔야 한다. |
| B2 RETRY 409 | 승인 | 진행 중 계약과 미시작 계약을 구분한 API는 명확하다. Dagster run retry가 실제 새 Run을 만들도록 instance 설정을 명시해야 한다. |
| B3 수동 NORMAL | 조건부 승인 | create-or-get과 `launch_result`는 좋다. Gap Recovery·stale loop·수동 호출 모두 durable submission reservation을 공유해야 “Run 미발행 창”과 중복 제출을 닫을 수 있다. |
| B4 수동 Gap Recovery | 조건부 승인 | 자동 경로와 operation을 공유하는 방향은 좋다. recovery가 coalesce 대상으로 선택한 최신 계약이 Hold/VOID이면 coverage가 사라지는 경로를 닫아야 한다. |
| C1 credential breaker | 수정 필요 | `ORA-28002`는 로그인 성공 warning인데 실패로 세면 오탐 Hold가 생긴다. `Job 수` 기반 상한식도 executor partition·failover·monitor·기존 Airflow의 물리 login 수를 반영하지 못한다. |
| C2 ConnectionRevision revoke | 수정 필요 | revoke 전후에 fence가 ACTIVE B에서, extract가 pinned A에서 일어날 수 있다. fence 이후 revision 재해석을 금지하고 force revoke는 기존 contract를 닫은 뒤 새 contract+새 fence로 가야 한다. |
| C3 release rollback | 수정 필요 | endpoint와 operation 모델은 좋지만 serving pointer가 `DEPLOYED`에 먼저 이동하고 Control `ACTIVE`가 나중이면 rollback 재검사 실패 때 split-brain release가 된다. |
| C4 overlap validator | 수정 필요 | `max_open_txn + safety_lag + clock_skew`만으로 application timestamp 누락을 막지 못한다. `commit_time - normalized_watermark`의 enforced hard bound가 필요하다. |
| C5 lateness sensor | 수정 필요 | `finalized_at - logical_scheduled_at`은 처리 완료 지연이지 데이터 freshness 전체가 아니다. source coverage, publication, completion 세 시계를 분리해야 한다. |
| D1 이력 5종 | 조건부 승인 | 채택은 좋다. `terminal_ingested_at`, actor, CAS range, attempt_no와 temporal interval을 포함하도록 schema와 판정 SQL을 맞춰야 한다. |

독립 재판정 결과는 **승인 2, 조건부 승인 7, 수정 필요 5**다.

---

## 4. P0 — PoC 전에 반드시 수정할 결함

## P0-01. 판정 불명(`verdict = NULL`)인데 `expires_at`이 window와 lease를 풀 수 있다

**근거:** A 215–219, 246, 591, 743, 1017–1024 / P 228, 235

### 실패 시나리오

1. Iceberg commit 요청 직후 writer가 죽는다.
2. writer fencing은 끝났지만 Polaris가 장시간 응답하지 않아 commit 여부를 조회하지 못한다.
3. Adjudication은 의도대로 verdict를 내리지 않는다.
4. 그 사이 contract의 `expires_at`이 지난다.
5. 일반 만료 처리기가 contract를 취소하고 window/target lease를 해제한다.
6. 다음 NORMAL 또는 CATCHUP이 같은 구간을 다시 쓴다.

실제 첫 쓰기가 commit된 상태였다면 Append 중복 또는 Merge의 설명 불가능한 재처리가 생긴다. **“모른다”는 것은 `NO_COMMIT`이 아니다.**

### 필수 수정

- 만료는 writer fenced 이후 verdict가 `NO_COMMIT` 또는 증거가 완결된 `PARTIAL_COMMIT`일 때만 자원을 풀 수 있어야 한다.
- `verdict=NULL`이면 `ADJUDICATION_BLOCKED(TARGET_EVIDENCE_UNAVAILABLE)`로 두고 window와 target lease를 유지한다.
- 장기 장애 시 자동 retry가 아니라 Job/target scope Hold와 운영자 escalation으로 전환한다.
- `adjudication_delay_seconds=0`을 허용하지 말고 exponential backoff + jitter + 상한을 둔다.

### 추가 시험

`Polaris outage > expires_at + 1 period`를 주입한다. 회복 전 새 attempt/SA 0, window·lease 해제 0, 회복 후 단 하나의 verdict와 그에 따른 단 하나의 후속 경로를 요구한다.

---

## P0-02. 0-row receipt의 부분 접두부가 전체 계약 완료 증거로 오인될 수 있다

**근거:** A 332, 346, 965–1008, 1019–1022 / P 244(FI-37)

### 실패 시나리오

4 chunk 계약에서 chunk 1만 실행됐고 결과가 0 row라고 하자. 그 직후 Run Pod가 죽는다. 현재 문구는 snapshot이 없는 receipt 집합을 빈 집합으로 축약해 `NO_DATA` 또는 전체 finalize로 판정할 여지가 있다. 그러면 실행되지 않은 chunk 2~4가 사라지지만 watermark는 전체 `window.high`로 갈 수 있다.

FI-37은 chunk 2와 4가 0 row인 정상·중간 OOM을 다루지만, **실행된 모든 chunk가 0 row이고 그 집합이 전체보다 짧은 경우**를 검증하지 않는다.

### 필수 수정

증거는 snapshot 집합이 아니라 chunk별 map이어야 한다.

```text
E[chunk_no] = SNAPSHOT(snapshot_id) | ZERO_ROW_RECEIPT | MISSING
```

- `FINALIZED`: `1..expected_chunk_count`가 모두 존재하고 결과 규칙을 만족
- `FINALIZED_NO_DATA`: `1..expected_chunk_count`가 모두 `ZERO_ROW_RECEIPT`
- `PARTIAL_COMMIT`: `1..k`만 연속 존재하고 `k < expected_chunk_count` — snapshot이 0개여도 동일
- `RECONCILIATION_REQUIRED`: gap, 중복, 상충, lineage 불일치

DB unique key는 최소 `(contract_id, attempt_no, chunk_no)`여야 한다.

### 추가 시험

- FI-37a: 4 chunk 중 chunk 1=0 row 후 crash
- FI-37b: chunk 1·2=0 row 후 crash
- FI-37c: 전체 4 chunk=0 row

앞의 두 시험은 전체 watermark로 갈 수 없고, 마지막만 `FINALIZED_NO_DATA`여야 한다.

---

## P0-03. Incremental partial commit 뒤 `dq:accept`가 실행되지 않은 뒤쪽 chunk까지 승인할 수 있다

**근거:** A 352–355, 1007, 1109, 1290, 1294 / P 229(FI-23)

### 실패 시나리오

4 chunk Incremental에서 chunk 1은 commit·CAS, chunk 2는 target commit 뒤 DQ 실패로 CAS하지 못하고 실행을 멈춘다. chunk 3·4는 실행되지 않는다. 현재 generic `dq:accept` 또는 `resolve`가 contract 전체 `window.high`로 watermark를 전진시키면 chunk 3·4가 영구 누락된다.

현재 FI-23은 주로 Full DQ accept를 검증하며 이 Incremental 부분 진행 경로를 닫지 못한다.

### 필수 수정

- `dq:accept`는 Full 또는 contract 전체의 연속 chunk evidence가 있는 경우에만 허용한다.
- Incremental partial coverage에는 `409 DQ_ACCEPT_REQUIRES_REPAIR`를 반환한다.
- `DQ_FAILED`를 일반 `resolve` 대상에서 제거하고, 남은 범위를 처리하는 repair contract 또는 명시적 watermark seed 승인으로만 닫는다.
- repair contract는 `parent_contract_id`, inherited logical window, exact remaining range를 가진다.

### 추가 시험

FI-23c로 chunk 2 DQ 실패 후 accept/resolve/retry/repair 네 경로를 대조한다. accept와 generic resolve는 watermark 불변, repair만 전체 증거 완료 후 전진해야 한다.

---

## P0-04. Control PostgreSQL PITR 뒤 과거 writer를 차단할 recovery epoch가 없다

**근거:** A 215–219, 251, 800, 994 / P 211, 236, 244

### 실패 시나리오

Control DB를 과거 시점으로 복구하면 최근 fencing token, active lease, ledger, outbox가 사라질 수 있다. 복구 전 생성된 Run Pod나 SparkApplication이 살아 있으면 복구된 Control이 모르는 token으로 계속 Iceberg에 쓸 수 있다. 동시에 resync가 과거 watermark를 기준으로 새 writer를 허용하면 이중 commit이 가능하다.

특히 zero-row CAS는 Iceberg snapshot이 없어서 catalog만 훑어서는 복원할 수 없다. 발행 전이었던 Outbox도 target에서 재구성할 수 없다.

### 필수 수정

- Control Plane에 단조 증가 `recovery_epoch`를 둔다.
- 모든 Run binding, lease, fencing token, SparkApplication label/tag, snapshot summary에 epoch를 포함한다.
- PITR 후 epoch를 증가시키고 과거 epoch의 모든 write/control 호출을 412로 거부한다.
- Global Hold 상태에서 Dagster 제출 중지, SparkApplication 및 Oracle session drain, catalog head 안정화가 끝나야 resync를 시작한다.
- zero-row receipt와 CAS 증거는 Control DB와 장애 도메인이 다른 immutable recovery journal 또는 Pipes artifact에 남긴다. 그렇지 않으면 해당 증거의 RPO는 0이어야 한다.
- 복구가 끝날 때까지 새 source/target lease를 발급하지 않는다.

### 추가 시험

기존 FI-05를 `Control PITR + 살아 있는 old Run Pod + 살아 있는 SparkApplication + zero-row CAS + 미발행 Outbox` 결합 시험으로 확장한다. 복구 이후 old epoch의 target commit과 Control mutation은 모두 0이어야 한다.

---

## P0-05. Fence와 extract가 서로 다른 ConnectionRevision/Oracle identity를 사용할 수 있다

**근거:** A 246–247, 274, 394, 679, 693–698, 813, 835, 860 / P 49, 92, 234

### 실패 시나리오

contract는 revision A를 pin하지만 source monitor는 현재 ACTIVE revision B로 접속한다. A가 `SUPERSEDED`이고 B가 더 앞선 standby라면 B에서 `visible_scn=1000`을 얻고 A에서 `AS OF SCN 1000`을 실행할 수 있다. A가 900까지만 적용했다면 오류가 나고, 더 위험하게 B가 같은 schema를 가진 다른 clone이면 쿼리가 성공하면서 잘못된 데이터가 들어간다.

force revoke 후 같은 contract가 B로 재해석되면서 기존 fence를 유지하는 경로도 같은 문제다.

### 필수 수정

첫 성공 Guard가 아래 `FenceBundle`을 원자적으로 확정해야 한다.

```text
FenceBundle = {
  connection_revision_id,
  descriptor_hash,
  dbid,
  db_unique_name,
  primary_db_unique_name,
  con_id_or_pdb_guid,
  resetlogs_change_or_incarnation,
  database_role,
  open_mode,
  visible_scn,
  fence_ts,
  lag_sample,
  recovery_epoch
}
```

- monitor와 모든 extract/retry 물리 세션은 같은 revision과 같은 identity를 사용한다.
- fence 생성 뒤 같은 contract에서 revision 재해석을 금지한다.
- force revoke는 source를 다시 읽어야 하는 기존 contract를 fence/cancel한다.
- 새 revision 사용은 새 contract + 새 fence로만 허용한다.
- staging만 재사용하는 target-only retry는 Oracle 재접속이 없으므로 예외다.

### 추가 시험

FI-42를 신설해 Guard 전 ACTIVE 교체, fence 뒤 ACTIVE 교체, force revoke, 동일 schema/다른 DBID clone을 검증한다. 모든 source query log의 revision·DBID·PDB GUID·incarnation·SCN이 FenceBundle과 같아야 한다.

---

## P0-06. `max_open_txn_seconds`만으로는 `ZERO_GAP`을 보장하지 못한다

**근거:** A 406, 414, 817, 892–897, 921, 1050–1054, 1303, 1350, 1453 / P 48, 89, 159–160, 178–180, 218, 269, 292, 318

### 핵심 구분

`AS OF SCN F`가 고정하는 것은 F에서 commit되어 보이던 Oracle row version 집합이다. 그 집합에 `UPDATE_DT` predicate를 적용하면 다음과 같다.

```text
추출 집합 = committed_and_visible_at(F)
          ∩ normalized_application_timestamp ∈ extract_window
```

SCN snapshot consistency와 application timestamp 완전성은 별개다.

### 반례

`UPDATE_DT=08:00`, transaction 시작 09:59:30, commit 10:00:00이라고 하자. `max_open_txn=20분`, overlap=30분, 이전 high=09:00이면 다음 extract low는 08:30이다. row는 fence에 보이지만 `UPDATE_DT=08:00`이라 predicate에서 영구 제외된다. ledger window는 완벽히 연속이므로 현재 P 318 검사도 통과한다.

또한 Oracle의 `SCN_TO_TIMESTAMP` “usual precision 3 seconds”는 hard maximum이나 오차 방향 보장이 아니다.

### 필수 수정

`ZERO_GAP`은 다음 hard bound가 DB 또는 애플리케이션 제약으로 **ENFORCED**된 경우에만 허용한다.

```text
overlap >= max_commit_minus_watermark
         + safety_lag
         + clock_skew
         + verified_scn_timestamp_error_bound
         + margin
```

필수 capability 필드:

- `timestamp_origin`
- `updated_on_every_change`
- `not_null`
- `timezone`
- `max_commit_minus_watermark_seconds`
- `bound_kind = ENFORCED | OBSERVED`
- `hard_delete_capture`
- `scn_timestamp_error_bound_kind`

`OBSERVED`, update 누락 가능, NULL 가능, hard delete 미포착, 또는 hard SCN-time bound 부재 중 하나라도 있으면 보증 등급은 `BEST_EFFORT + reconciliation`이어야 한다.

### 추가 시험

FI-41로 timestamp가 transaction 시작보다 과거, 변경 시 미갱신, NULL, clock ±skew, hard bound 직전/직후인 row를 fence 뒤 commit한다. 합격 기준은 ledger 연속성이 아니라 주입한 eligible PK와 target PK의 차집합 0이다.

---

## P0-07. Pod/SA 부재를 Oracle server session 종료로 간주해 source token을 너무 일찍 돌려준다

**근거:** A 368–372, 768–769, 795–809, 1017, 1334, 1339, 1356 / P 84, 93, 182–183, 193, 223, 275, 309–310, 332, 337

### 실패 시나리오

cap=4, weight=2인 Job 두 개 중 하나의 TCP를 blackhole 처리한다. Pod와 SA는 없어졌지만 Oracle server process/session 두 개가 살아 있다. Control이 token 2를 `RECLAIMED`하고 새 Job을 허용하면 회계상 4지만 실제 Oracle session은 6이다.

P 93의 stub은 TCP 단절 후 5초 내 session을 닫도록 만들어 이 현실 위험을 숨긴다. Oracle `SQLNET.EXPIRE_TIME` 기본값 0에서는 비정상 연결 탐지가 보장되지 않는다.

### 필수 수정

- `WRITER_FENCED`와 `SOURCE_SESSIONS_DRAINED`를 다른 상태/증거로 둔다.
- token 반환은 해당 attempt의 session이 `GV$SESSION`에서 연속 N회 0으로 관측된 뒤에만 허용한다.
- 모든 grant는 fresh observation을 사용해 다음을 원자적으로 검사한다.

```text
observed_user_sessions + requested_weight
  <= hard_cap - monitor_reserved
```

- 관측 실패, stale observation, tag 불일치는 fail closed다.
- 전용 DB username/PDB/service 전체 count를 hard limit에 사용하고 `MODULE/ACTION/CLIENT_IDENTIFIER`는 attribution에만 사용한다.
- RAC에서는 profile limit만 cluster-global cap으로 믿지 말고 instance 배분 또는 단일 service cardinality를 명시한다.

### 추가 시험

FI-43으로 실제 Oracle 앞 TCP proxy blackhole, `SQLNET.EXPIRE_TIME=0` 대조군, monitor 장애, RAC instance 분산, login 직후 tag 설정 전 session을 넣는다. 어떤 순간에도 실제 관측 session이 hard cap을 넘으면 즉시 No-Go다.

---

## P0-08. DefinitionRelease serving pointer와 Control `ACTIVE`가 원자적이지 않다

**근거:** A 299–302, 499, 1275 / P 231

### 실패 시나리오

Definition bundle을 `DEPLOYED`하면서 workspace/code-location pointer가 새 release로 이동한다. 그 뒤 ACTIVE 트랜잭션의 OPEN_CONTRACT_CHECK가 실패하거나 rollback 재검사가 실패하면 Control은 이전 release를 ACTIVE로 유지하지만 Dagster는 새 코드를 serve한다.

그 결과 scheduler가 이전 JobSpec/contract와 다른 interface를 실행하거나, 미래 `effective_from` release가 너무 일찍 실행되거나, rollback 이후에도 잘못된 code location이 남을 수 있다.

### 필수 수정

- `candidate_pointer`와 `serving_pointer`를 분리한다.
- DEPLOYED/VERIFIED는 candidate만 바꾼다.
- Control의 release `ACTIVE` 전이와 동일한 generation/epoch를 가진 serving pointer가 원자적으로 publish되어야 한다.
- RunRequest와 contract에 `serving_release_id`와 `definition_digest`를 모두 pin하고 Guard가 일치 여부를 확인한다.
- rollback ACTIVE 재검사 실패 시 serving pointer도 이전 상태여야 한다.

### 추가 시험

FI-24b로 pointer 교체 직전·직후 Control transaction 실패, future-effective release, rollback 재검사 실패를 주입한다. 모든 Run의 contract release/digest와 실제 loaded code digest가 같아야 한다.

---

## P0-09. PoC stub의 `visible_scn = primary_scn - lag`는 단위가 틀린 visibility model이다

**근거:** P 88, 216, 269

apply lag는 시간이고 SCN은 논리적 순번이다. `12분`을 SCN에서 720처럼 빼는 것은 의미가 없다. idle DB와 초당 수만 SCN이 증가하는 DB에서 `SCN-720`은 전혀 다른 시간을 뜻한다.

이 stub으로 FI-10~12/H-03이 통과해도 실제 Data Guard fence가 검증되지 않는다.

### 필수 수정

stub에 다음을 둔다.

```text
RedoEvent(commit_scn, commit_ts, primary_visible_at, apply_seq, payload)
ApplyState(contiguous_apply_cursor, applied_at, gap_set)
visible_scn = last commit_scn in the contiguous applied prefix
```

redo gap이 있으면 뒤 event가 일부 도착해도 cursor는 gap 앞에 머물러야 한다. SCN 증가율 변화, 장시간 idle, burst, out-of-order arrival을 모두 시험한다.

### 추가 시험

FI-40에서 모든 fence의 `visible_scn`이 contiguous apply cursor가 가리키는 마지막 commit SCN과 같은지, cursor 이후 event가 query result에 노출되지 않는지 검사한다.

---

## P0-10. 현재 No-Go SQL은 실제 안전 불변식을 증명하지 못한다

**근거:** P 318–321, 329–338

### 문제점

1. `window_low = prev.window_high`는 계획 구간 연속성만 증명한다. 그 안의 row가 실제로 모두 들어왔는지는 증명하지 못한다.
2. 같은 `(job, logical_at)`의 SA가 2개라는 조건은 정상적인 순차 retry도 중복으로 오판한다. 필요한 것은 **동시 active interval 겹침**이다.
3. `attempt_state_history`의 `SUBMITTED` row count는 상태 유효 구간을 만들지 않으면 동시성을 판정할 수 없다.
4. snapshot을 `(contract_id, chunk_no)`로만 묶으면 정상 retry의 `attempt_no`가 사라진다.
5. FINALIZED window overlap은 의도적인 REPLAY/BACKFILL까지 오탐할 수 있다.
6. `Snapshot metadata 100%`는 FI-31이 의도적으로 만든 `etl.*` 없는 외부 snapshot과 동시에 만족할 수 없다.

### 필수 수정

판정 oracle을 네 층으로 나눈다.

| 층 | 주 판정 |
|---|---|
| Logical occurrence | expected cron/hold disposition마다 occurrence 정확히 1개 |
| Writer concurrency | writer-active interval과 target/source lease interval의 temporal overlap 0 |
| Commit identity | `(contract_id, attempt_no, chunk_no, recovery_epoch)` evidence 유일성과 lineage |
| Data completeness | 주입 원천 truth 또는 real Oracle fence truth와 target row-level 차집합 0 |

REPLAY/BACKFILL/repair는 production NORMAL lane과 분리해 overlap을 판정한다. FI-31의 외부 snapshot은 격리 namespace/table로 만들거나 metadata 100% 분모에서 `writer_kind=FAULT_INJECTION`으로 명시 제외한다.

---

## 5. P1 — Phase 1 게이트 전 수정·명시할 항목

## P1-01. 최초 `PLANNED.next_eligible_at`과 inline expiry가 빠져 있다

**근거:** A 246, 516–519, 566, 587, 591, 685–701, 1105

- contract 생성 시 `next_eligible_at = created_at`을 필수화하고 NOT NULL/CHECK를 둔다.
- background scanner가 아직 만료를 처리하지 않았더라도 Guard·manual NORMAL·RETRY·stale loop가 row lock 안에서 `apply_expiry(now)`를 먼저 실행해야 한다.
- `now >= expires_at`인데 실행이 시작되는 race를 별도 시험한다.

## P1-02. Gap Recovery가 제출한 queued Run을 정상 tick이 못 보고 중복 Run을 만들 수 있다

**근거:** A 516, 522, 571–583

현재 정상 tick의 판단이 current attempt/binding 중심이면 Adapter가 제출했지만 Guard 전인 Run은 보이지 않는다. 정상 tick이 `launch=true`를 반환해 같은 contract의 Dagster Run이 둘 생길 수 있다. Guard가 SA 중복을 막더라도 “non-terminal Run ≤ 1” 목표는 깨진다.

`run_submission(contract_id, generation, run_id, state)` reservation을 Control DB에 먼저 쓰고 Gap Recovery, stale loop, manual NORMAL, RETRY 재요청, normal tick이 모두 같은 unique active generation을 사용해야 한다.

## P1-03. 500건 batch의 poison pill 격리 규칙이 없다

**근거:** A 515–519 / P 190, 241, 267

한 Job의 잘못된 cron/release/state가 전체 transaction을 rollback시키면 499건이 매 tick 막힐 수 있다. batch API는 per-item 결과를 반환하거나 bounded sub-batch/cursor로 처리하고, 실패 항목을 설명 가능한 rejection으로 격리해야 한다. SC-02b로 500건 중 1건의 validation/constraint 오류를 주입한다.

## P1-04. terminal 반입 뒤 안전 증거와 cleanup까지 모두 412로 막힐 수 있다

**근거:** A 709–711, 747–755

`terminal_ingested_at`은 Run Pod의 상태 제어권을 fence하는 데 적합하다. 그러나 terminal 직후 도착한 `attempt-failure`, credential failure evidence, Oracle session close evidence, token cleanup까지 버리면 실제 원인이 `RUN_WORKER_LOST`로 뭉개지고 credential breaker가 작동하지 않을 수 있다.

소유권이 필요한 상태 mutation과 idempotent immutable evidence/cleanup endpoint를 분리한다. 후자는 terminal 뒤에도 token과 payload digest가 맞으면 수락한다. FI-09C로 terminal event와 failure evidence의 순서를 뒤집어 검증한다.

## P1-05. 겹치는 Hold의 effective mode와 CATCHUP coverage가 정의되지 않았다

**근거:** A 1062–1092 / P 203

현재 PoC는 Hold overlap을 피한다. 운영에서는 인프라 Hold, credential Hold, platform breaker Hold가 겹칠 수 있다.

권장 lattice:

```text
FORCE_STOP > DRAIN > HOLD_NEW > NONE
```

- effective 상태가 `held → NONE`으로 바뀔 때만 CATCHUP을 한 번 만든다.
- 자동 Hold는 `(scope, reason, key)`당 open row unique를 둔다.
- Hold를 유발한 `REJECTED_AT_GUARD(SOURCE_ROLE_MISMATCH/SCHEMA_DRIFT)`도 impacted occurrence 집합에 넣는다. 그렇지 않으면 짧은 Hold 뒤 다음 tick이 없어 24시간 Job이 catchup되지 않을 수 있다.
- Gap Recovery는 launchable/open contract에만 coalesce하고 VOID/held 계약을 coverage 주체로 선택하지 않는다.

## P1-06. freshness를 completion 하나로 표현하면 실제 데이터 시점이 가려진다

**근거:** A 872, 1185–1239, 1472 / P 121, 178–181, 227, 289, 303–304

최소 세 시계를 분리해야 한다.

| 지표 | 의미 |
|---|---|
| `orchestration_lateness` | `first_guard_ok_at - logical_scheduled_at` |
| `publication_lateness` | target commit/CAS 완료 시각 - logical scheduled |
| `source_coverage_lag` | 현재 시각 - 성공 contract의 effective source high/fence coverage |

Full source가 0 row인데 기존 target snapshot을 유지하면서 `FINALIZED_NO_DATA`로 끝내면 orchestration은 성공했지만 target은 과거 데이터를 그대로 노출한다. 이 경우 dataset freshness를 갱신하면 안 된다. 상태를 `TARGET_UNCHANGED_EMPTY_SOURCE`처럼 분리하거나 실제 empty replace를 수행해야 한다. successor가 expiry/Hold gap을 회수했음을 나타내는 `coverage_contract_id`/`recovered_by_contract_id`도 필요하다.

## P1-07. Dagster run retry 설정과 cancel/max-runtime 원인 보존이 부족하다

**근거:** A 181–182, 668, 747–753, 1480 / P 212–214

- `run_retries.enabled=true`만으로 FI-06의 새 retry Run을 보장하지 않는다. 사용 버전에서 `max_retries`를 명시하고 same-run resume를 쓰지 않으면 `max_resume_run_attempts=0`도 고정한다.
- Dagster `FAILURE`/`CANCELED`를 모두 `RUN_WORKER_LOST`로 접으면 max-runtime 초과나 운영자 cancel도 crash로 오인해 자동 reattach/retry할 수 있다.
- `WORKER_CRASH`, `MAX_RUNTIME_EXCEEDED`, `OPERATOR_CANCELLED`, `PLATFORM_TERMINATE` 원인을 분리하고 후자의 자동 retry 정책을 명시한다.

## P1-08. SparkApplication의 물리 identity와 재생성 경계가 약하다

**근거:** A 206, 247, 657–675

attempt에 `namespace`, `name`, Kubernetes `uid`, operator `submissionID`, `executionAttempts`를 영속한다. UID를 한 번 관측한 뒤 404가 나면 같은 attempt 이름으로 새 CR을 만들지 말고 fencing/adjudication으로 간다. name reuse와 create-response-loss를 구분해야 한다. `restartPolicy=Never`, Spark retry 0, ownerReference/TTL 정책도 명시하고 ephemeral Dagster Run Job을 owner로 삼아 의도치 않게 cascade delete하지 않도록 한다.

## P1-09. WAP fast-forward는 main이 동시에 전진하면 실패할 수 있다

**근거:** A 1031, 1040, 1315 / P 172, 199, 272

branch fast-forward는 target ref가 branch의 ancestor일 때만 가능하다. partition lease를 쓰는 다른 writer나 compaction이 main을 전진시키면 staging branch publish가 실패할 수 있다. publish 구간의 main advance를 막는 target publish lease 또는 명시적 rebase/cherry-pick 판정이 필요하다. base/current/staging ref를 evidence에 보존하고 FI에 disjoint concurrent commit을 넣는다.

## P1-10. snapshot/orphan GC와 compaction이 active writer·adjudication 증거를 지울 수 있다

**근거:** A 209, 666, 1018–1024, 1037–1044, 1305–1315 / P 199, 238, 273

- `orphan_min_age > max_runtime + reattach_grace + adjudication_max_delay + margin` 불변식을 둔다.
- active contract/attempt가 참조하는 staging URI, snapshot, branch/tag는 GC 보호 집합에 넣는다.
- compaction 마지막 commit 직전 main ref와 lease fencing token을 다시 확인한다.
- external snapshot/maintenance snapshot도 contract lineage 판정에서 구분 가능한 `writer_kind`와 lease id를 가져야 한다.

## P1-11. Append/Merge DQ row count의 기준 필드가 일관되지 않다

**근거:** A 651, 894, 1000

dedup 또는 anti-join이 있으면 `extract_rows == snapshot.added-records`가 성립하지 않는다. `raw_extract_rows`, `normalized_rows`, `dedup_dropped_rows`, `anti_join_dropped_rows`, `written_rows`를 분리하고 기본식은 `written_rows == target_added_rows`로 둔다. 구현이 Iceberg/Spark가 자동 제공하는 MERGE snapshot metric에 의존한다면 정확한 버전을 gate하고, metric이 unknown이면 성공으로 간주하지 않는다.

## P1-12. Flashback/UNDO deadline이 hard guarantee가 아니다

**근거:** A 695, 813, 842–849, 860, 899, 1054, 1428, 1451 / P 48, 166, 178–180, 311, 379

`UNDO_RETENTION`은 일반적으로 best effort이고 space pressure에서 짧아질 수 있다. `ORA-01555` 뒤 같은 fence SCN으로 chunk를 작게 다시 시도해도 이미 덮인 undo가 복구되지는 않는다.

- contract에 `flashback_deadline`을 pin한다.
- Guard와 매 `chunks:begin`에서 `now + worst_case_remaining <= deadline`을 확인한다.
- critical/initial load는 retention guarantee 또는 extract-once staging을 요구한다.
- `ORA-01555`, `ORA-01466`, `ORA-08181` 뒤 source를 다시 읽는 same-contract retry/CAS를 금지한다. 새 fence가 필요하면 새 contract로 간다.
- FI-44로 NOGUARANTEE undo churn, DDL, LOB, deadline 전후를 검증한다.

## P1-13. role/identity check는 driver 한 번이 아니라 모든 물리 JDBC connection에 필요하다

**근거:** A 703, 801, 833–835 / P 91–92, 219, 233

driver precheck 뒤 Spark executor 또는 task retry가 TNS `ADDRESS_LIST`의 primary나 다른 standby로 연결될 수 있다. standby role-based service를 기본 방어로 쓰고, **각 물리 connection의 첫 data SELECT 전에** DBID/PDB/incarnation/role/open mode/fence revision을 검사한다. role transition 직후 executor reconnect를 FI-45로 넣는다.

## P1-14. credential breaker의 Oracle 오류 매핑과 상한식이 틀렸다

**근거:** A 234, 285–286, 653, 1229, 1458, 1478 / P 51, 94, 222, 240, 372

- `ORA-28002`는 password expiry warning이며 로그인 자체는 성공이다. breaker/Hold가 아니라 `CREDENTIAL_EXPIRING` 경고와 rotation workflow로 보내야 한다.
- 실제 fatal auth 집합은 환경 검증 후 `ORA-01017`, `ORA-28000`, `ORA-28001` 등으로 명시한다.
- `breaker_failures + 동시 Job 수`는 물리 login 상한이 아니다. executor partition, retry, TNS address failover, monitor, legacy Airflow를 포함한 `worst_case_inflight_physical_auth_attempts`로 계산해야 한다.
- ADG gradual password rollover를 쓰면 `ADG_ACCOUNT_INFO_TRACKING` 설정까지 capability로 확인한다.
- FI-46에서 ORA-28002 기인 Hold 0을 요구한다.

## P1-15. `NO_LAG_SIGNAL`을 `confidence=FULL`로 두면 증거 부재가 정상으로 보인다

**근거:** A 320, 823–831 / P 169, 217, 312

보증과 관측 신뢰도를 두 축으로 분리한다.

```text
guarantee_grade  = ZERO_GAP | BEST_EFFORT
fence_confidence = VERIFIED | DATUM_STALE | LAG_QUERY_FAILED | NO_LAG_SIGNAL
```

`NO_LAG_SIGNAL`은 `VERIFIED/FULL`이 아니다. ZERO_GAP은 `VERIFIED`가 아니면 Guard에서 거부해야 한다. BEST_EFFORT만 degraded 상태로 진행할 수 있다.

## P1-16. logical/CAS window와 overlap extract window를 분리 기록해야 한다

**근거:** A 699, 892–899, 982–994, 1003, 1013, 1051–1054 / P 178–180, 218, 292, 318

다음 필드를 immutable evidence로 남긴다.

- `original_logical_low/high`
- `extract_window_low/high`
- `cas_window_low/high`
- `overlap_recovered_row_count`

첫 chunk의 extract low가 `logical_low - overlap`인지 명시하고 이후 chunk 경계를 정의한다. FI-47로 overlap 하한, logical low, chunk 경계 ±1µs row와 partial retry를 검증한다.

## P1-17. 신규 target provisioning과 Polaris/AIStor 장애 경계가 빠져 있다

**근거:** A 219, 697, 1011, 1025 / P 58, 69, 76, 198, 220, 235

Guard 5번의 `loadTable`은 target table이 이미 있다는 전제다. 신규 Job publish 전 `TARGET_PROVISIONING → TARGET_READY` 상태를 두고 table UUID, schema id, partition spec id, sort order, catalog를 pin해야 한다. drop/recreate로 이름은 같고 UUID가 바뀐 경우 Guard가 거부해야 한다.

또한 Polaris catalog 장애와 AIStor object read/write 장애를 별도 phase로 시험해야 한다. 둘을 동시에 차단하면 어느 breaker와 복구 프로토콜이 맞는지 판정할 수 없다.

## P1-18. INITIAL_LOAD와 기존 Airflow writer의 migration cutover가 rollout gate가 아니다

**근거:** A 899, 1388, 1395–1404, 1428, 1465 / P 166, 178–180, 255, 318

최소 cutover runbook:

1. old Airflow writer DRAIN
2. old in-flight 종료와 Oracle session drain 확인
3. old watermark/evidence 캡처
4. 새 Oracle identity + SCN fence
5. 경계 구간 row-level reconcile
6. source/target lease와 watermark seed의 원자적 이전
7. 새 scheduler 활성화

첫 신규 commit 뒤 rollback은 단순히 Airflow를 다시 켜는 것이 아니라 forward repair다. FI-48로 old late commit, old/new 동시 시작, partial initial CAS crash, ORA-01555를 검증한다.

## P1-19. Aggregate lock 순서와 DB 제약이 구현 규약으로 충분히 내려오지 않았다

**근거:** A 274, 285, 301, 686–701

Guard↔revoke, breaker↔credential activation, rollback↔contract 생성이 서로 다른 row만 잠그면 write skew와 phantom이 가능하다. Source → Revision → Job/Occurrence → Contract → Attempt → target/source lease의 공통 lock order 또는 명시적 advisory lock key가 필요하다.

최소 DB 제약:

- occurrence natural key unique
- occurrence당 active contract 1개
- `(contract_id, attempt_no)` unique
- `(contract_id, attempt_no, chunk_no)` ledger unique
- contract generation당 active `run_submission` 1개
- outbox `event_id` unique
- `(scope, reason, key)` open Hold partial unique
- Source당 ACTIVE ConnectionRevision/CredentialRevision 각 1개
- lease time/range exclusion constraint

이 제약은 애플리케이션 `if`가 아니라 PostgreSQL constraint/transaction으로 증명해야 한다.

## P1-20. Source capacity 식과 Guard 외부 I/O가 500 동시 burst의 skew를 반영하지 않는다

**근거:** A 414, 679, 697–698, 745, 1358, 1481 / P 136, 190, 192–193, 277, 375

문서의 ρ 식은 단위가 불명확하다. 동시 source session의 1차 근사는 다음처럼 period와 duration을 같이 써야 한다.

```text
rho_source = Σ_i (duration_i × token_weight_i / period_i) / source_capacity
```

retry, CATCHUP, audit, monitor reserve를 추가하고 source별 p95/p99 skew를 봐야 한다.

또 Guard가 contract row lock을 잡은 채 Polaris/AIStor/Oracle 외부 I/O와 `ALL_TAB_COLUMNS`를 수행하면 500 Job이 같은 Source에 몰린 경우 단일 monitor session과 장기 Control transaction이 병목이 된다. target/source monitor가 짧은 TTL의 immutable health/fence precursor snapshot을 만들고 Guard가 이를 row lock 안에서 소비하는 방식도 PoC 비교안으로 둔다. 최소 500건이 같은 SourceSystem에 몰린 skew 시나리오를 추가한다.

## P1-21. stub 통과와 실제 Oracle ZERO_GAP 통과를 별도 gate로 둬야 한다

**근거:** P 45–50, 60–61, 78–95, 255

stub은 protocol fault를 반복 가능하게 만드는 도구이지 Oracle 의미론의 증거가 아니다. 다음 두 gate를 분리한다.

- **Scale/Control Go:** stub 기반 10,000 Job, 500/750/포화 burst, crash/retry
- **Oracle ZERO_GAP Go:** 실제 primary→physical standby에서 long transaction, late commit, redo gap, undo churn, role transition, executor reconnect, old timestamp를 주입

실 Oracle 시험이 불가능한 Source는 `BEST_EFFORT`로만 출시해야 한다.

## P1-22. 계측 schema와 PoC SQL 사이에 열·주체 불일치가 있다

**근거:** A 252–258, 994 / P 104, 212, 221, 228, 318–320

- P가 history에서 `terminal_ingested_at`을 기대하지만 A의 history schema에는 없다.
- ledger 판정이 actor를 기대하지만 A의 ledger에는 actor 주체가 명확하지 않다.
- `cas_at`, `cas_applied`, logical/extract/CAS window가 판정 SQL에 필요하지만 schema 정의가 부족하다.
- `WRITER_FENCED`는 attempt history인데 일부 시험은 contract history에서 찾는다.
- reattach의 `from_state=to_state` history가 일반 state-transition trigger로 생성 가능한지 불명확하다.

PoC 전에 executable DDL 또는 JSON schema를 단일 부록으로 만들고, 모든 판정 SQL을 그 schema 위에서 dry-run해야 한다.

## P1-23. commit 뒤 DQ는 WAP이 아니면 나쁜 데이터가 이미 main에 노출된다

**근거:** A 1000–1008, 1109 / P 229

post-commit DQ 실패 후 contract를 `DQ_FAILED`로 멈춰도 main snapshot을 소비자가 이미 읽을 수 있다. row count, schema, nullability, partition-bound 같은 cheap deterministic DQ는 publish 전에 수행하고, strict 등급은 WAP branch에서 DQ 통과 후 main publish해야 한다. WAP을 쓰지 않는 등급은 “DQ 실패가 노출을 막지 못함”을 UI와 알림에 명시해야 한다.

---

## 6. P2 — MVP 동결 전 정리할 항목

## P2-01. B1의 만료 결론은 유지할 수 있지만 근거는 바꿔야 한다

A 593과 A 1056/1092를 함께 보면 `PLANNED` CATCHUP이 NORMAL을 coalesce하지 않는다. 따라서 “무기한 CATCHUP이 모든 NORMAL을 흡수한다”는 설명은 성립하지 않는다. 만료 이유를 bounded stale intent, 운영자 설명 주체 교체, queue hygiene로 고친다.

## P2-02. “PoC 기준서가 아키텍처보다 우선”이라는 문서 우선순위는 뒤집어야 한다

P 7의 취지는 drift 방지지만, 시험서가 production semantics를 정의하면 시험을 고쳐 설계를 통과시키는 순환이 생긴다.

권장 우선순위:

```text
Normative protocol/specification
  > executable schema/API contract
  > PoC procedure and thresholds
  > explanatory architecture prose
```

불일치는 PoC 기준서 승리가 아니라 traceability failure로 처리한다.

## P2-03. grouped schedule은 가능하지만 정확한 Dagster target 형태를 명시해야 한다

A 512–516의 grouped schedule은 구현 불가능한 설계가 아니다. Dagster schedule evaluation은 여러 `RunRequest`를 반환할 수 있다. 다만 schedule target은 group-wide subsettable asset job + 각 `RunRequest.asset_selection`인지, generic runner job인지 명시해야 한다. 10,000 asset definition 로딩 시간과 selection 생성 비용을 PoC에서 측정한다.

## P2-04. `versions.lock` 범위를 확대해야 한다

Phase 0에 최소 Dagster, dagster-k8s, Python, Spark Operator CRD/controller, Spark, Iceberg, Polaris, Oracle JDBC, PostgreSQL, Kubernetes, Cilium, AIStor S3 API compatibility의 exact version/image digest가 필요하다. 기능 probe 결과와 CRD schema digest도 보존한다.

## P2-05. API status와 attempt identity 규칙을 통일해야 한다

- Guard가 `attempt_no`를 생성하는지 호출자가 보내는지 단일 규칙 필요
- credential/connection 오류의 200 Guard rejection과 412 command rejection을 표에서 구분
- 423의 의미를 상태 충돌과 Hold에 일관되게 사용
- failed rollback operation을 `rollback_of unique` 때문에 재개할 수 있는 idempotency 규칙 필요

## P2-06. CATCHUP 예상 부하식은 Source별 weight와 skew를 반영해야 한다

A 1091의 `N_hold × D / C`는 Job별 period, duration, token weight, Source 편향을 잃는다. Source별 `Σ(duration × weight)`와 target lease 경합을 함께 계산하고, release 응답의 estimate에는 p50/p95와 confidence를 표시한다.

## P2-07. `allow_empty_full`은 동작을 오해하게 만드는 이름이다

기존 target을 유지하는 정책이라면 `retain_previous_on_empty` 또는 `EMPTY_RETAINED`가 정확하다. 실제 empty table publish와 이름을 분리한다.

## P2-08. 22장 잠정 기본값은 정상값 시험뿐 아니라 경계·음성 시험으로 확정해야 한다

특히 다음을 함께 측정한다.

- `poll_interval=60`: 30/60/120초 비교와 API/DB 부하
- `chunk_proceed=reattach+120`: Control cold start, network partition, long GC pause
- `dagster/max_runtime`: 정상 p99 바로 아래·위, cancel 원인 분류
- history retention: 30/90/365일 비용과 감사 요구
- queue cap: 500/750뿐 아니라 cap을 실제로 포화시키는 1,200 또는 축소 cap 대조군

---

## 7. PoC 기준서에 바로 반영할 시험 delta

### 7.1 신규·확장 시험 목록

| ID | 시험 | 합격의 핵심 |
|---|---|---|
| FI-05b | Control PITR + old writer + zero-row CAS + pending Outbox | old epoch mutation/commit 0, resync 전 lease 발급 0 |
| FI-09c | terminal event와 late safety evidence 순서 역전 | 상태 제어는 fence, immutable evidence/cleanup은 정확히 1회 수락 |
| FI-23c | Incremental partial commit 뒤 `dq:accept` | accept/resolve는 watermark 불변, repair만 완료 후 전진 |
| FI-24b | serving pointer 전환과 ACTIVE/rollback 실패 race | loaded digest = pinned contract digest 100% |
| FI-37a/b/c | zero-row partial prefix | 전체 coverage 전 `FINALIZED_NO_DATA` 0 |
| FI-40 | contiguous redo apply stub | visible SCN이 contiguous apply prefix와 항상 일치 |
| FI-41 | old/NULL/unrefreshed application timestamp | enforced bound 대상 row의 target 누락 0 |
| FI-42 | ConnectionRevision/DB identity 교체 | fence와 모든 extract physical session identity 100% 일치 |
| FI-43 | TCP blackhole 후 Oracle zombie session | 실제 Oracle session hard cap 초과 0 |
| FI-44 | UNDO churn/DDL/LOB/deadline | old fence로 잘못된 재시도·CAS 0 |
| FI-45 | driver check 뒤 role transition/executor reconnect | primary/다른 identity data SELECT 0 |
| FI-46 | ORA-28002, password rollover, partition login fan-out | ORA-28002 Hold 0, lockout 0 |
| FI-47 | overlap·chunk 경계 ±1µs | eligible PK 차집합 0, PK 중복 0 |
| FI-48 | Airflow→신규 플랫폼 cutover race | old/new writer-active interval 겹침 0, 경계 row 차이 0 |
| SC-02b | 500 batch 중 poison item 1개 | 나머지 499 정상 처리, poison만 설명된 거부 |
| SC-02c | 500 Job이 동일 Source에 집중 | source hard cap 0회 초과, Guard/monitor p99 SLO 충족 |
| SC-04b | queue cap 실제 포화 | bounded backlog, priority starvation 없음, recovery slope 양수 |

### 7.2 No-Go 판정 SQL을 바꾸는 원칙

1. **동시성은 row count가 아니라 interval overlap으로 측정**한다.
2. **retry identity에 `attempt_no`와 `recovery_epoch`를 포함**한다.
3. **ZERO_GAP의 최종 oracle은 row-level source truth와 target의 차집합**이다.
4. **logical window adjacency는 보조 불변식**일 뿐 데이터 완전성 증거가 아니다.
5. **safety-blocked backlog를 queue에서 제외하지 않는다.**

권장 backlog 분해:

```text
runnable_backlog       = PLANNED and eligible
executing_backlog      = ATTEMPT_ACTIVE or COMMIT_OBSERVED
safety_blocked_backlog = ADJUDICATION_PENDING or DQ_FAILED or RECONCILIATION_REQUIRED
dagster_nonterminal    = QUEUED or STARTING or STARTED or CANCELING
```

각 집합의 count뿐 아니라 oldest age, p95 age, 증가/감소 slope를 기록한다.

### 7.3 PoC 단계 재배치

권장 순서는 다음과 같다.

1. **Phase 0A — Normative patch**: P0 의미론, executable DDL/API, lock order 확정
2. **Phase 0B — Oracle capability**: DB identity, Data Guard lag, undo, timestamp invariant, hard delete, login profile 수집
3. **Phase 1A — Semantic kernel**: PostgreSQL + stub로 P0 fault 전부 통과
4. **Phase 1B — Dagster/Spark scale**: 10,000 Job, 40,000 Run/일 모델, 500 burst와 saturation
5. **Phase 1C — Real Oracle gate**: 실제 primary/standby long transaction·role·undo·session 시험
6. **Phase 2 — Shadow**: 기존 Airflow와 동시 비교하되 target writer lease는 단일 권위 유지

`Phase 1A`가 실패하면 scale test를 계속해도 안전성에 대한 정보가 늘지 않는다.

---

## 8. v1.2.1에 넣을 최소 규범 문구

아래 네 문장은 구현 팀과 PoC 팀이 다르게 해석할 여지를 줄이는 핵심 수정문이다.

### 8.1 Unknown commit

> Writer fencing 완료는 commit 부재를 뜻하지 않는다. External target verdict가 `NULL`인 contract는 `expires_at`으로 window·target lease를 해제하거나 새 writer를 허용할 수 없다. `COMMIT`, `NO_COMMIT`, `PARTIAL_COMMIT` 중 하나가 durable evidence로 확정되거나 운영자 reconciliation이 완료될 때까지 safety-blocked 상태를 유지한다.

### 8.2 ZERO_GAP

> `AS OF SCN`은 fence 시점의 committed snapshot을 고정할 뿐 application timestamp predicate의 완전성을 보장하지 않는다. `ZERO_GAP`은 fence/extract Oracle identity 일치, queryable flashback horizon, enforced `commit_time - normalized_watermark` hard bound, hard-delete completeness가 모두 증명된 경우에만 허용한다. 하나라도 증명되지 않으면 `BEST_EFFORT + reconciliation`이다.

### 8.3 Fence identity

> 첫 성공 Guard는 immutable `FenceBundle(connection_revision_id, descriptor_hash, DBID, PDB identity, incarnation, role, open_mode, visible_scn, lag evidence, recovery_epoch)`을 원자적으로 확정한다. 모든 source-reading physical connection과 retry는 동일 bundle을 검증하며, fence 이후 같은 contract의 ConnectionRevision 재해석은 금지한다.

### 8.4 Source token reclaim

> SparkApplication/Pod 부재는 Oracle session drain의 증거가 아니다. Source token은 fresh server-side session observation에서 해당 attempt session이 연속 N회 0으로 확인된 뒤에만 반환한다. 관측 실패 또는 stale observation에서는 fail closed한다.

---

## 9. Review Board에 올릴 권고안

### 9.1 지금 승인해도 되는 것

- Dagster를 Airflow 대체 orchestration/runtime UI로 선택하는 방향
- Java Modular Monolith + PostgreSQL Control Plane
- DB 기반 JobSpec/TemplateVersion/DefinitionRelease
- Oracle → Spark Operator → Iceberg/Polaris 기본 실행 경로
- Hold/CATCHUP, source weighted lease, commit evidence ledger, Outbox
- Prometheus/Grafana + OpenSearch + Kafka 사내 메신저 연계
- 중앙 운영팀 UI와 LLM Advisor의 human approval 구조

### 9.2 지금 동결하면 안 되는 것

- `ZERO_GAP` 선언 조건
- ambiguous commit 만료와 zero-row adjudication
- Incremental `dq:accept/resolve`
- ConnectionRevision/fence identity
- PITR/resync writer fencing
- Oracle source token reclaim
- serving release pointer 원자성
- 현재 PoC No-Go SQL과 SCN stub

### 9.3 권고 의사결정

```text
Architecture direction:       GO
v1.2 semantic freeze:         REWORK REQUIRED
Phase 0 baseline collection:  GO
Phase 1 semantic PoC:         GO after P0 closure
Scale soak only:              not a substitute for semantic Go
Production shadow/pilot:      NO-GO until P0 + P1 gates pass
```

P0를 닫는 작업은 전체 아키텍처 재설계가 아니다. 핵심은 ① 상태 전이 3~4개 보강, ② FenceBundle/recovery epoch 도입, ③ evidence key와 DB constraint 명시, ④ PoC oracle 교체다.

---

## 10. 공식 근거

### Oracle

- [Oracle transaction과 SCN](https://docs.oracle.com/en/database/oracle/oracle-database/19/cncpt/transactions.html)
- [Oracle Flashback Query](https://docs.oracle.com/en/database/oracle/oracle-database/19/adfns/flashback.html)
- [SCN_TO_TIMESTAMP](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SCN_TO_TIMESTAMP.html)
- [Data Guard apply lag와 standby 관리](https://docs.oracle.com/en/database/oracle/oracle-database/19/sbydb/managing-oracle-data-guard-physical-standby-databases.html)
- [V$DATAGUARD_STATS](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-DATAGUARD_STATS.html)
- [SQLNET.EXPIRE_TIME](https://docs.oracle.com/en/database/oracle/oracle-database/19/netrf/parameters-for-the-sqlnet.ora.html)
- [Undo 관리](https://docs.oracle.com/en/database/oracle/oracle-database/19/admin/managing-undo.html)
- [ORA-01555](https://docs.oracle.com/en/error-help/db/ora-01555/)
- [ORA-01466](https://docs.oracle.com/en/error-help/db/ora-01466/)
- [ORA-28002](https://docs.oracle.com/en/error-help/db/ora-28002/)
- [ADG_ACCOUNT_INFO_TRACKING](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/ADG_ACCOUNT_INFO_TRACKING.html)

### Dagster / Kubernetes / Spark Operator

- [Dagster run retries](https://docs.dagster.io/deployment/execution/run-retries)
- [Dagster run monitoring](https://docs.dagster.io/deployment/execution/run-monitoring)
- [Dagster schedules and sensors API](https://docs.dagster.io/api/dagster/schedules-sensors)
- [Spark Operator API](https://kubeflow.github.io/spark-operator/docs/api-docs.html)
- [Spark Operator user guide](https://kubeflow.github.io/spark-operator/docs/user-guide.html)
- [Kubernetes object names and UIDs](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/)
- [Kubernetes owners and dependents](https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/)

### Apache Iceberg

- [Iceberg branching and tagging](https://iceberg.apache.org/docs/latest/branching/)
- [Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
- [Iceberg Spark writes](https://iceberg.apache.org/docs/latest/spark-writes/)
- [Iceberg table specification](https://iceberg.apache.org/spec/)

---

## 11. 최종 의견

v1.2는 이미 일반적인 “Airflow를 Dagster로 바꾸자” 수준을 넘어섰다. Control Plane 권위, source 보호, ambiguous commit, Hold/CATCHUP, release 관리까지 실제 운영 플랫폼에 필요한 골격이 들어 있다. 이 방향은 유지할 가치가 충분하다.

다만 문서가 정교해질수록 작은 단어 하나가 안전성 계약이 된다. 현재 가장 위험한 부분은 다음 네 문장으로 압축된다.

1. `verdict를 모름`은 `commit 안 됨`이 아니다.
2. `SCN snapshot 고정`은 `UPDATE_DT 기반 누락 없음`과 같지 않다.
3. `Pod가 없음`은 `Oracle session이 없음`과 같지 않다.
4. `ledger window가 연속`은 `row가 모두 들어옴`과 같지 않다.

이 네 구분을 v1.2.1의 규범과 PoC oracle에 반영하면, 이후의 논의는 제품 취향이 아니라 측정 가능한 engineering decision으로 바뀐다. 그 시점에는 Dagster 채택과 Phase 1 PoC를 자신 있게 승인할 수 있다.
