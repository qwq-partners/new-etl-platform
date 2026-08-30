# ETL 관리 UI — 정보구조 v0.1 (Phase 0)

> 8차 교차 리뷰 의사결정표가 조건부 GO 한 항목의 첫 산출물이다.
>
> > **비의미적 플랫폼 골격/UI/Dagster spike | 조건부 GO** | correctness grade를 고정하지 않고
> > BEST_EFFORT만 표시하는 Phase 0 작업은 병행 가능하다.
>
> **그 조건이 이 문서의 §3 이다.** 조건을 어기는 화면은 설계 단계에서 거른다.
>
> 근거: A v1.2.3.1 §1(책임 경계) · §3(논리 아키텍처) · §7(Job 생성 UX) · §14(Hold·Backfill·수동 운영)
> · §17(Control API). **A 에 근거가 없는 것은 이 문서에서 §9 미결로 분리한다** — 화면을 그리려고
> 규범에 없는 규칙을 지어내지 않는다.

---

## 1. 이 UI 가 존재하는 이유 — 한 문장

Dagster 는 **실행**을 보여 주고, 이 UI 는 **의도와 계약**을 다룬다.

A §1 이 그 경계를 이미 규정한다.

> Dagster는 Asset 정의, 스케줄 평가, Run Queue, 재실행, 실행 이력과 운영 UI를 담당한다.
> 별도 Control Plane은 Dagster가 알 수 없는 Source/TNS, Job 등록 Wizard, Source 보호,
> 전역 실행 멱등성, Hold, Template Release, 그리고 Dagster가 보장하지 못하는 실행 계약
> (Occurrence/Contract/Attempt)·commit evidence·watermark·lease·DQ 판정만 담당한다.

그리고 같은 절이 **만들지 않을 것**을 명시한다.

| 만들지 않는다 | 어디가 대신하는가 |
|---|---|
| 범용 DAG 편집기 | 없음 — JobSpec + Asset Factory 가 10,000 Asset 을 생성한다 |
| Job 별 DAG/Python 파일 편집 | 없음 — 그 자체가 안티패턴이다 |
| 자체 로그 화면 | Dagster UI · OpenSearch |
| 자체 lineage 화면 | DataHub |
| 자체 인프라 모니터링 | Prometheus / Grafana |
| Dagster run 상태(queued/running/failure) 복제 | Dagster UI. 이 UI 는 **read model 로만** 보여 준다(A §4) |
| Task/step retry 엔진 | 없음 — Control 의 `RETRY` 는 운영자 명령이지 자동 재시도가 아니다 |

**이 표를 어기는 화면 요구가 들어오면 그것은 UI 요구가 아니라 아키텍처 변경 요구다.** 화면으로
해결하지 말고 A 를 고쳐야 한다.

---

## 2. 화면이 다루는 도메인 객체

A §6·§7 에서 UI 가 직접 만들거나 상태를 바꾸는 객체만 추린다.

```
SourceSystem ──< ConnectionRevision (CANDIDATE → VERIFIED → ACTIVE → SUPERSEDED/REVOKED)
     │        └─< CredentialRevision
     │        └─  SourceCapability          ← §3 의 표시 규칙이 걸리는 곳
     │        └─  SourceSafetyEnvelope (budget)
     │
     └──< Job ──< JobSpec (불변) ──< DefinitionRelease ──< DefinitionBundle
                     │
                     └──< Occurrence ──< ExecutionContract ──< Attempt
                                              │                  └─ Lease
                                              └─ CommitEvidenceLedger
                                              └─ Watermark

Hold (Global | Source | Domain | Job목록) × (HOLD_NEW | DRAIN | FORCE_STOP)
BackfillPlan ──< BackfillItem
Template ──< TemplateRelease (channel: approved | …)
```

**Occurrence·Contract·Attempt 는 UI 가 만들지 않는다.** Dagster tick 과 Guard 가 만든다.
UI 는 그것을 **읽고, 종결·재시도 같은 운영자 명령만 보낸다**(A §14.3).

---

## 3. ★ Phase 0 표시 규칙 — 이 문서에서 가장 중요한 절

8차 리뷰의 조건부 GO 는 조건이 붙은 GO 다. 조건은 하나다.

> **correctness grade 를 고정하지 않고 BEST_EFFORT 만 표시**

이것을 화면 규칙으로 옮기면 다음 넷이다.

### 3.1 `ZERO_GAP` 을 표시하지 않는다

`upsert_consistency = ZERO_GAP` 은 A §7.2-5 가 **일곱 조건의 논리곱**으로 규정한다
(`bound_kind=ENFORCED` ∧ `timestamp_origin=DB_TRIGGER` ∧ `not_null` ∧ `updated_on_every_change`
∧ `fence_time_witness=HEARTBEAT_TABLE` ∧ `delete_semantics ∉ {NONE_DECLARED, CDC_LATER}`
∧ `zero_gap_verified=true`). 그 조건들은 **G0-0 이 재야 확정되는데 G0-0 은 원천에 대해 한 번도
실행되지 않았다**(README §1).

- Wizard 의 `guarantee_grade` 선택지에 `ZERO_GAP` 을 **띄우지 않는다**
- 데이터 계약 표시에 `upsert_consistency: BEST_EFFORT` 만 나온다
- 그 옆에 **왜 그것뿐인지**를 한 줄로 보여 준다 — "원천 capability 미측정(G0-0 미실행)"

> 선택지를 비활성 상태로 **보여 주는 것**과 아예 **띄우지 않는 것**은 다르다. 비활성 선택지는
> "곧 될 것"이라는 신호를 준다. Phase 0 에서는 띄우지 않는다.

### 3.2 capability 축을 확정값으로 그리지 않는다

7차 리뷰 P0-05 조치로 축이 13개로 재분리됐지만, 8차 리뷰는 **"capability 9축 및 A/P v2.0 동결
= NO-GO"** 로 판정했다. 축 이름·값이 아직 바뀔 수 있다.

- Source 상세 화면에 capability 를 **표**로 그리지 않는다(표는 확정을 시사한다)
- 대신 **"미측정"** 이라는 단일 상태와 "무엇을 재면 채워지는가"(G0-0A/B0/B1/C) 를 보여 준다
- 축 이름을 UI 문자열에 하드코딩하지 않는다 — 서버가 주는 것을 그대로 렌더한다

### 3.3 측정되지 않은 값과 측정된 값을 시각적으로 구분한다

이 저장소의 규칙 — "확인하지 못한 것은 미확인이라고 쓴다"(README §4) — 은 화면에도 적용된다.

| 상태 | 표시 |
|---|---|
| 측정됨 | 값 + 측정 시각 + 근거(probe id) |
| **미측정** | `—` 와 "미측정" 라벨. **빈칸으로 두지 않는다** |
| 측정 실패(transient) | "재측정 필요" — **기능 없음과 구분한다**(7차 P1-03) |
| 만료(stale) | 이전 값을 회색으로 + effective 값은 floor 로. 둘을 **같이** 보여 준다(7차 P1-02) |

빈칸은 "없다"로도 "안 쟀다"로도 읽힌다. Phase 0 에서 대부분의 값이 미측정이므로 이 구분이
화면 전체의 신뢰도를 좌우한다.

### 3.4 UI 는 판정하지 않는다

A §7.2-13 이 **`validate` 와 `publish` 가 같은 validator·같은 응답 형식**을 쓴다고 규정한다.

- 버튼 활성/비활성은 **편의**일 뿐이고, 진짜 판정은 `POST /v1/jobs/{id}/validate` 의 응답이다
- `422 VALIDATION_FAILED` 의 `violations[].rule` 을 **그대로 보여 준다** — UI 가 문구를 지어내
  rule 이름을 감추면 운영자가 A 문서에서 근거를 찾을 수 없다
- 클라이언트 검증이 서버 규칙을 **앞질러 통과시키는 일은 없어야 한다**. 막을 때는 앞질러 막아도
  되지만, 통과시킬 때는 서버가 통과시킨 것만 통과다

---

## 4. 최상위 정보구조

```
①  대시보드            지금 무엇이 멈춰 있는가
②  Source              SourceSystem · Connection · Credential · capability · budget
③  Job                 목록 · 상세 · Wizard(신규/개정) · Release 이력
④  실행                Occurrence/Contract/Attempt 조회 · Guard 거부 사유
⑤  운영                Hold · Backfill · 수동 실행 · Adjudication · DQ
⑥  Template            Template · Release · channel 승격
⑦  증거                CommitEvidenceLedger · watermark · lease  (읽기 전용)
⑧  설정                권한 · 승인 정책 · 알림
```

**여덟 개가 이 UI 의 전부다.** 로그·lineage·메트릭은 ①~⑦ 각 화면에서 **딥링크**로 나간다.

### 4.1 왜 이 순서인가

①이 맨 위인 이유는 이 플랫폼의 운영자가 화면을 여는 **가장 흔한 이유**가 "무엇이 막혔는가"이기
때문이다. A §14 가 규정한 자동 Hold 여섯 종류·Guard 거부 사유·`ADJUDICATION_PENDING`·`DQ_FAILED`
·`RECONCILIATION_REQUIRED` 는 전부 **사람이 풀어야 끝나는 상태**다.

②가 ③보다 앞인 이유는 Job 이 `source_system_id` 를 참조하기 때문이다(A §7.1). Source 없이
Job 을 만들 수 없다.

---

## 5. 화면별 설계

### ① 대시보드

**목적**: 사람이 개입해야 할 것만 보여 준다. 정상 실행은 여기 나오지 않는다.

| 블록 | 내용 | 근거 |
|---|---|---|
| 열린 Hold | scope·mode·reason·만든 주체(자동/운영자)·경과 | §14.1 |
| 판정 대기 | `ADJUDICATION_PENDING` 계약 — verdict 별 분류 | §13.2 |
| 사람 대기 | `DQ_FAILED` · `RECONCILIATION_REQUIRED` · `EMPTY_FULL` | §12.1·§13.4 |
| Guard 거부 추이 | 사유별 건수(`SOURCE_LAG_EXCEEDED`·`LEASE_BUSY`·`TARGET_UNAVAILABLE`·…) | §10.2 |
| Source 압력 | source 별 utilization ρ — **0.7 경고 / 1.0 초과 표시** | §7.2-13 |

**하지 않는 것**: 성공률 그래프, 처리량 차트. 그것은 Grafana 다.

### ② Source

**목록**: 이름 · 소유 부서 · 중요도 · Primary/DR · ACTIVE revision · 열린 Hold · ρ

**상세 탭 넷**

1. **기본** — 이름·부서·중요도·Oracle service·기본 schema·`db_identity` 6항
2. **연결** — ConnectionRevision 목록과 상태 전이
   - `CANDIDATE → VERIFIED` 전이는 **연결 테스트가 성공해야** 한다. 그 테스트는 `V$DATABASE`·
     `V$CONTAINERS` 로 읽은 identity 6항을 SourceSystem `db_identity` 와 대조하고, 불일치면
     `422 DB_IDENTITY_MISMATCH` 로 거부한다(§7.1)
   - **화면은 `actual` 과 기대값을 나란히 보여 준다** — 어느 항이 다른지 사람이 즉시 알아야 한다
   - Raw Descriptor 입력은 허용 host/port/protocol 검증 결과를 인라인으로 표시하고
     `IFILE`·외부 경로를 차단한다
   - `REVOKED` 버튼은 실행 중 attempt 가 있으면 `412 OPEN_CONTRACTS_RUNNING` 목록을 보여 주고
     **DRAIN Hold 로 안내**한다. `force` 는 별도 승인 흐름
3. **Capability** — **§3.2 규칙이 걸리는 화면.** Phase 0 에서는 "미측정"과 측정 경로 안내만
4. **보호 정책** — SourceSafetyEnvelope, 동시성 승인값, 변경 이력

### ③ Job

**목록**: job_id · Source · 적재 모드 · 등급(Phase 0 은 전부 BEST_EFFORT) · ACTIVE release ·
freshness SLO · 최근 상태

**Wizard** — A §7.2 의 13단계를 그대로 화면 흐름으로 옮긴다. 단계를 줄이거나 합치지 않는다.

| # | 단계 | 화면이 반드시 해야 하는 것 |
|---|---|---|
| 1 | Source 선택 | ACTIVE revision 없으면 진행 차단 |
| 2 | Schema/Table 탐색 | **위험 타입 경고** — 정밀도 미지정 NUMBER · LONG/RAW · CLOB · VARCHAR2 BYTE · DATE vs TIMESTAMP |
| 3 | Advisor 추천 | **선택 단계**. Advisor 장애 시 건너뛸 수 있어야 한다 |
| 4 | Full/Append/Merge | Full 은 `FULL_STATIC_REPLACE`/`PARTITION_REPLACE` 택일 |
| 5 | Watermark·cutoff | **§3.1 — `ZERO_GAP` 선택지를 띄우지 않는다** |
| 6 | initial_load · delete_semantics | 선택 즉시 `delete_consistency`·`delete_lag_slo_seconds` **파생값 미리보기**(§7.2-6). Critical 테이블은 `NONE_DECLARED` 거부 |
| 7 | 컬럼 mapping | 표준 매핑표 기본 적용 + 등록자 수정 |
| 8 | 표준화·가공식·WHERE | 복잡한 코드는 **Template 승격 안내** |
| 9 | 파생 partition | `dt/wt/mt/yt` 는 메타데이터임을 명시 |
| 10 | Target 확인 | create-or-get 결과의 `table_uuid`·`current_schema_id`·`default_spec_id` 표시 |
| 11 | 스케줄 | **최소 주기 1시간 강제**(validator) |
| 12 | Spark/read profile | |
| 13 | 검증·publish | Preview SQL · 실행계획 · **Source 용량 Gate**(ρ>0.7 경고, >1.0 거부) · overlap 검증 · 예상 row/시간/부하 |

**13단계의 응답을 화면이 어떻게 다루는가가 이 Wizard 의 핵심이다.**
`422 VALIDATION_FAILED` 의 `violations[]` 를 `field`(JobSpec 키) · `rule` · `computed_minimum`
그대로 표시하고, 해당 단계로 되돌아가는 링크를 붙인다. rule 이름을 감추지 않는다.

**개정**: JobSpec 은 불변이므로 "수정"이 아니라 **새 release** 다. 화면도 그렇게 부른다.

### ④ 실행

**목적**: 계약 하나의 생애를 추적한다. Dagster 의 run 목록을 복제하지 않는다.

- Occurrence → Contract → Attempt 트리
- 각 상태 전이의 **actor**(SCHEDULER / GUARD / OPERATOR / EXPIRY / ADJUDICATION)와 사유
- Guard 거부는 사유 코드와 그때의 입력(fence·lag·lease)을 함께
- chunk 진행(`chunks:begin`/`commit`)과 watermark CAS 지점
- **Dagster run 으로 나가는 딥링크** — run graph·로그는 거기서 본다

`bound_dagster_run_id` 가 있으면 그 링크를 항상 노출한다. 그것이 두 UI 를 잇는 유일한 접합점이다.

### ⑤ 운영

**Hold** — scope 4종 × mode 3종. 화면이 반드시 표현해야 할 것 둘:

1. **겹침 의미** — 어떤 Job 이 held 인 조건은 그 Job 을 덮는 open Hold 가 1개 이상이고,
   effective mode = `max(FORCE_STOP > DRAIN > HOLD_NEW)` 다(§14.1). 화면은 Job 단위로
   **effective mode 와 그것을 만든 Hold 목록**을 함께 보여 준다
2. **자동 Hold 6종** — Control 이 만든 것과 운영자가 만든 것을 구분한다. 자동 Hold 는
   `reason` 과 원인 식별자를 반드시 표시한다

`FORCE_STOP` 은 **명시적 승인 흐름**이다. 프로토콜(SparkApplication delete → pod 부재 확인 →
Adjudication → CAS)의 각 단계를 진행 표시로 보여 준다 — 중간에 멈추면 어디서 멈췄는지 알아야 한다.

**해제** — `T_catchup` 예상값을 Source 별 목록으로 보여 주고(§14.2), impacted Job 수를 함께.

**수동 실행 5종**(§14.3) — 각 모드의 **제약을 화면이 먼저 설명한다**.

| Mode | 화면이 표시할 제약 |
|---|---|
| `NORMAL` | 열린 window 있으면 COALESCED |
| `RETRY` | `ADJUDICATION_PENDING` ∧ verdict ∈ {NO_COMMIT, PARTIAL_COMMIT} 만. 아니면 버튼 자리에 **왜 안 되는지** |
| `REPLAY` | `client_request_id` 필수 · window 조건 |
| `BACKFILL` | §14.4 |
| `RERUN_LATEST` | `client_request_id` 필수. `replace_with_empty`·`accept_row_drop` 는 **사유·승인자 필수** |

**Backfill** — Plan → Item 분해, 실행 전 Preview(예상 Source query 수 · JDBC weight · Spark
resource · 대상 partition), 승인/일시정지/재개/취소. Flashback/Archive 없으면 **"지원 불가"로
표시**한다(회색 버튼이 아니라 사유와 함께).

**Adjudication** — verdict 별로 가능한 운영자 종결을 보여 준다(`abort` / `dq:accept` / `resolve`).

### ⑥ Template

Template · TemplateRelease · channel(`approved` 등) 승격 · `POST /v1/releases/{id}/rollback`.
어떤 Job 들이 그 release 를 쓰는지 **역참조**를 반드시 보여 준다 — rollback 의 영향 범위다.

### ⑦ 증거 (읽기 전용)

CommitEvidenceLedger · watermark 이력 · lease 현황. **여기서는 아무것도 바꿀 수 없다.**
`(contract_id, attempt_no, chunk_no)` 로 조회하고 `cas_applied`·`window_low/high`·`actor` 를 본다.

### ⑧ 설정

권한(누가 `FORCE_STOP`·`dq:accept`·`force revoke` 를 할 수 있는가) · 승인 정책 · 알림 라우팅.

---

## 6. 두 UI 의 접합 규칙

| 상황 | 어디로 |
|---|---|
| run graph·step 로그·재실행 이력 | **Dagster UI** (contract 화면의 `bound_dagster_run_id` 링크) |
| Job 정의·계약·Hold·증거 | **이 UI** |
| Dagster UI 에서 contract 없이 시작된 실행 | Run Pod 가 `occurrences:batch-create-or-get(origin=DAGSTER_UI)` 로 계약을 만든다. **Critical Job 은 `DIRECT_LAUNCH_FORBIDDEN`**(§14.3) — 이 UI 는 그 거부를 계약 화면에 남긴다 |
| 메트릭·대시보드 | Grafana |
| lineage·스키마 이력 | DataHub |

**같은 것을 두 곳에서 편집할 수 있게 만들지 않는다.** 실행 규칙은 어느 쪽에서 시작하든 공통
Control API 와 Guard 를 통과한다(§14.3).

---

## 7. Phase 0 에서 실제로 만들 것

8차 리뷰의 조건은 "비의미적 골격"이다. 의미(정합성 등급·capability)를 고정하지 않는 범위에서
**골격이 서는지** 보는 것이 목적이므로, 첫 구현 범위를 이렇게 좁힌다.

| 우선 | 화면 | 왜 |
|---|---|---|
| **1** | ② Source 목록·상세(기본/연결) | Job 의 전제. `DB_IDENTITY_MISMATCH` 표시가 이 플랫폼의 안전 규칙을 화면에서 처음 구현하는 지점이다 |
| **2** | ③ Job Wizard 1·2·4·6·7·10·11·13 단계 | **5(cutoff)·3(Advisor)·12(profile)는 Phase 0 에서 뺀다** — 5는 §3.1 때문에 선택지가 하나뿐이고, 3·12는 측정·튜닝 결과가 있어야 의미가 있다 |
| **3** | ① 대시보드(열린 Hold + 사람 대기) | 골격이 도는지 확인하는 창 |
| **4** | ⑤ Hold 목록·생성·해제 | 겹침 의미와 effective mode 를 화면으로 표현할 수 있는지가 관건 |

**Phase 0 에서 만들지 않는 것**: ④ 실행 상세 · ⑥ Template · ⑦ 증거 · Backfill · Adjudication.
전부 **G0-0 이후의 의미가 필요한 화면**이다.

---

## 8. 이 문서가 지키는 것 / 지키지 못하는 것

**지키는 것**

- A §1 의 책임 경계 — 만들지 않을 것 7종을 화면 목록에서 제외했다
- 8차 리뷰의 조건 — §3 네 규칙으로 옮겼고, `ZERO_GAP` 을 Wizard 에서 뺐다
- A §7.2 13단계를 줄이지 않았다 — Phase 0 에서 **뺄 단계**를 명시하되 단계 자체를 합치지 않았다
- rule 이름을 화면에 그대로 노출한다 — 운영자가 A 문서에서 근거를 찾을 수 있어야 한다

**지키지 못하는 것 — 알고 남긴다**

- **실제 화면을 그리지 않았다.** 이것은 정보구조이지 와이어프레임이 아니다. 레이아웃·컴포넌트·
  반응형은 다음 단계다
- **Control API 가 이 화면들을 다 지원하는지 확인하지 않았다.** A §17 에서 수집한 엔드포인트는
  24개인데 그중 다수가 Guard·Run Pod 용이다. **운영자용 조회 API(목록·검색·필터)는 A 에 거의
  없다** — §9-1 참조
- **권한 모델이 A 에 없다.** ⑧ 설정의 내용을 지어내지 않고 미결로 뒀다(§9-2)
- **10,000 Job·40,000 Run 규모에서 이 정보구조가 서는지 모른다.** 목록 화면의 페이징·검색·
  필터 설계가 규모 문제다(§9-3)

---

## 9. 미결 — A 에 근거가 없어 지어내지 않은 것

| # | 미결 | 왜 지금 정하지 않는가 |
|---|---|---|
| 1 | **운영자용 조회 API** | A §17 은 Guard·Run Pod·운영자 명령 API 는 규정하지만 목록/검색/필터 API 가 없다. UI 요구에서 역으로 도출해야 하며 그것은 A 개정이다 |
| 2 | **권한·승인 모델** | `FORCE_STOP`·`dq:accept`·`force revoke`·`accept_row_drop` 이 "승인 필요"라고만 돼 있고 **누가** 승인하는지가 없다 |
| 3 | **목록 규모 대응** | 10,000 Job 목록의 기본 정렬·필터·저장된 뷰. 규모 시험(G1)이 답할 부분이 있다 |
| 4 | **Advisor UX** | A §15 가 LLM Advisor 를 규정하지만 추천을 **어떻게 보여 주고 어떻게 거절하는가**가 없다. "LLM 추천보다 Source 보호 정책이 항상 우선"(§2-2)을 화면이 어떻게 표현할지 |
| 5 | **다국어·시각(timezone) 표시** | `logical_scheduled_at` 은 `Asia/Seoul` cron 인데 계약 시각은 UTC 다. 어느 쪽으로 보여 줄지 규정이 없다 |
| 6 | **capability 표시 형태** | §3.2 로 Phase 0 은 막았지만, G0-0 이후 무엇을 어떻게 보여 줄지는 축이 확정된 뒤에 |

**이 여섯을 채우는 것이 v0.2 다.** 1·2 는 A 개정이 필요하고, 3·6 은 측정이 필요하며,
4·5 는 이 문서 안에서 정할 수 있다.
