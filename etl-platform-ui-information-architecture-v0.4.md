# ETL 관리 UI — 정보구조 v0.4 (Phase 0)

> **v0.3 대비 변경**(2026-08-30). v0.3 §9 미결 **2(권한·승인)** 를 사용자 결정으로 닫았다.
>
> > 이건 우리 운영팀만 쓸 거라 권한관리는 필요시 구현으로 놔둬. 우선은 심플하게 초기
> > 접속시 ID/PW 로 로그인하는 걸로 간단하게 가고, 추후 사내 SSO 를 붙여서 권한 관리하도록.
>
> | 무엇 | 어디 |
> |---|---|
> | 인가는 미루고 **인증·감사는 미루지 않는다** — A 가 `AuditEvent.actor`·`auth_method` 를 필수로, `사유·승인자 필수`를 7곳에서 요구한다 | **§14 신설** |
> | Phase 0 이 지킬 것 넷 — 1인 1계정 · `auth_method` 선반영 · 로그인 한 겹 격리 · 사유 입력 | §14.2 |
> | **두 UI 의 신원이 갈린다** — A §4 는 Dagster write 를 SSO proxy 뒤에 두고 그 access log 로 actor 를 보강한다. Control UI 가 ID/PW 면 잇는 키가 없다 | §14.3 |
> | §5-⑧ 설정 화면을 결정에 맞춰 축소 | §5-⑧ |
> | Phase 0 범위에 **로그인·감사 기록**을 넣는다 | §7 |
>
> **v0.1~v0.3 은 이력으로 남긴다. 이 문서가 현행이다.**
>
> ---
>
> **v0.3 이 채운 것**(v0.1 §9 의 미결 1·3) — §12 운영자 조회 · §13 목록 규모.
> v0.3 에서 §9-1 의 전제(“A 개정이다”)도 정정했다 — A 는 §16.2 에서 이미 답했다(§12.0).
>
> ---
>
> **v0.2 가 채운 것**(v0.1 §9 의 미결 4·5) — §10 Advisor UX · §11 시각 표시.
> v0.2 에서 v0.1 §9-5 의 전제 "계약 시각은 UTC 다" 도 정정했다(§11.0).
>
> ---
>
> 8차 교차 리뷰 의사결정표가 조건부 GO 한 항목의 첫 산출물이다.
>
> > **비의미적 플랫폼 골격/UI/Dagster spike | 조건부 GO** | correctness grade를 고정하지 않고
> > BEST_EFFORT만 표시하는 Phase 0 작업은 병행 가능하다.
>
> **그 조건이 이 문서의 §3 이다.** 조건을 어기는 화면은 설계 단계에서 거른다.
>
> 근거: A v1.2.3.1 §1(책임 경계) · §2(설계 원칙) · §3(논리 아키텍처) · §7(Job 생성 UX)
> · §11.1(정책 계층) · §11.3(세션·fence) · §13.4(window) · §14(Hold·Backfill·수동 운영)
> · §15(Advisor) · **§16.2(운영자 조회 경로)** · §17(Control API).
> **A 에 근거가 없는 것은 이 문서에서 §9 미결로 분리한다** — 화면을 그리려고 규범에 없는
> 규칙을 지어내지 않는다. 그리고 **"A 에 없다"고 쓰기 전에 다른 절을 본다**(§12.0 의 교훈).

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
| Guard 거부 | **현재 열린 계약의** 사유별 건수(`SOURCE_LAG_EXCEEDED`·`LEASE_BUSY`·`TARGET_UNAVAILABLE`·…) | §10.2 |
| Source 압력 | source 별 utilization ρ — **0.7 경고 / 1.0 초과 표시** | §7.2-13 |

**하지 않는 것**: 성공률 그래프, 처리량 차트, **그리고 추이(시계열)**. 그것은 Grafana 다.

> **v0.3 정정.** v0.1·v0.2 는 이 표에 "Guard 거부 **추이**"라고 썼는데 바로 아래 줄에서
> 차트를 Grafana 로 보냈다. 두 줄이 어긋난다. A §16.2 도 **집계는 Grafana, contract 단위
> 조회는 Custom UI** 로 나눈다. 이 화면이 보여 주는 것은 **지금 막혀 있는 것의 사유별
> 건수**이고, 시간에 따른 변화는 Grafana 다.

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
| 3 | Advisor 추천 | **선택 단계**. Advisor 장애 시 건너뛸 수 있어야 한다 — **화면 규칙은 §10** |
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

**Phase 0 에서는 권한 화면이 없다**(§14 결정 — 운영팀만 쓰고 전원 같은 권한).
남는 것은 셋이다.

| 블록 | 내용 |
|---|---|
| 계정 | **1인 1계정** 목록·생성·비활성. 공용 계정을 만들 수 없어야 한다(§14.2-1) |
| 감사 기록 조회 | `AuditEvent` — `actor`·`auth_method`·`source_ip`·시각·대상·사유. **읽기 전용** |
| 알림 라우팅 | A §16.4 Outbox 이벤트별 수신처 |

**권한·승인 정책 화면은 인가 모델이 생길 때 만든다.** 지금 빈 화면으로 만들어 두지 않는다
— 있는데 아무것도 없는 화면은 "아직 안 된 것"이 아니라 "권한이 없다"로 읽힌다.

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
| **2** | ③ Job Wizard 1·2·4·6·7·10·11·13 단계 | **5(cutoff)·3(Advisor)·12(profile)는 Phase 0 에서 뺀다** — 5는 §3.1 때문에 선택지가 하나뿐이고, 12는 튜닝 결과가 있어야 의미가 있으며, **3은 A §15.4 가 auto-prefill 앞에 Shadow 평가·측정을 두기 때문이다**(§10.7 — v0.1 의 "측정·튜닝 결과가 있어야 의미가 있다"는 이유를 흐리게 썼다) |
| **3** | ① 대시보드(열린 Hold + 사람 대기) | 골격이 도는지 확인하는 창 |
| **4** | ⑤ Hold 목록·생성·해제 | 겹침 의미와 effective mode 를 화면으로 표현할 수 있는지가 관건 |
| **0** | **로그인 + 감사 기록**(§14) | 화면이 아니라 **전제**다. `AuditEvent` 네 항이 첫 명령부터 채워져야 하고, `auth_method` 를 나중에 넣으면 전환 시점에 감사 기록이 갈라진다(§14.2-2) |

**Phase 0 에서 만들지 않는 것**: ④ 실행 상세 · ⑥ Template · ⑦ 증거 · Backfill · Adjudication.
전부 **G0-0 이후의 의미가 필요한 화면**이다.
그리고 **⑧ 설정의 권한·승인 정책** — 인가 모델을 미뤘으므로 빈 화면조차 만들지 않는다(§5-⑧).

**Phase 0 이 조회에서 지켜야 할 것**(v0.3 §12·§13 에서). 화면 4개뿐이어도 이 넷은 처음부터
넣는다 — 나중에 넣으면 계약을 깨야 한다.

| 무엇 | 왜 지금 |
|---|---|
| **커서 페이징** (offset 금지) | offset→커서는 API 계약 변경이다(§13.1) |
| **정렬 키 `(시각 UTC, id)`** | tiebreaker 를 나중에 넣으면 저장된 커서가 전부 무효다(§13.6) |
| **replica 지연 표시** | 감추면 R2 규칙이 있어도 운영자가 화면을 신뢰한다(§12 R3) |
| **열린 Hold 는 한 번에 가져와 겹친다** | 겹침 계산이 두 군데면 ①과 ⑤가 다른 답을 낸다(§13.4) |

**Phase 0 이 잠정으로 두는 것**: Job 목록 기본 필터는 "상태가 정상이 아닌 것"만이다.
"내 부서"는 §9-2 권한 모델이 있어야 하고 그것은 §12.6-a 뒤에 온다(§9.3).

---

## 8. 이 문서가 지키는 것 / 지키지 못하는 것

**지키는 것**

- A §1 의 책임 경계 — 만들지 않을 것 7종을 화면 목록에서 제외했다
- 8차 리뷰의 조건 — §3 네 규칙으로 옮겼고, `ZERO_GAP` 을 Wizard 에서 뺐다
- A §7.2 13단계를 줄이지 않았다 — Phase 0 에서 **뺄 단계**를 명시하되 단계 자체를 합치지 않았다
- rule 이름을 화면에 그대로 노출한다 — 운영자가 A 문서에서 근거를 찾을 수 있어야 한다
- **A 를 인용할 때 어느 절인지 확인하고 쓴다.** v0.3 이 §9-1 의 전제를 뒤집은 것은 §17 만 보고
  "A 에 없다"고 쓴 것을 §16.2 에서 찾았기 때문이다(§12.0). 없다고 쓰기 전에 다른 절을 본다

**지키지 못하는 것 — 알고 남긴다**

- **실제 화면을 그리지 않았다.** 이것은 정보구조이지 와이어프레임이 아니다. 레이아웃·컴포넌트·
  반응형은 다음 단계다
- ~~**Control API 가 이 화면들을 다 지원하는지 확인하지 않았다.**~~ **v0.3 에서 확인했다(§12.3).**
  화면이 필요로 하는 조회 21건 중 A 가 규정한 것은 6건이고, `GET` 엔드포인트는 3개뿐이다.
  나머지 15건은 A 가 replica 라고도 API 라고도 말하지 않았다 — **IA 가 구현 가능한지는 A 가
  §12.6-a 를 고른 뒤에 답해진다**
- ~~**권한 모델이 A 에 없다.**~~ **v0.4 에서 결정으로 닫았다**(§14) — 인가는 미루고
  인증·감사는 지금 한다. 다만 **§14.3 이 새 문제를 남겼다**: A §4 가 Dagster write 를
  SSO proxy 뒤에 두고 그 access log 로 `OPERATOR_CANCELLED` 의 actor 를 보강하는데,
  Control UI 가 ID/PW 면 같은 사람이 두 신원을 갖고 **잇는 키가 없다.**
  §14.3-(a)(ID 문자열을 사내 계정과 같게)를 권했지만 그것은 강제가 아니라 규율이다
- ~~**10,000 Job·40,000 Run 규모에서 이 정보구조가 서는지 모른다.**~~ **v0.3 이 구조를 정했다(§13).**
  커서 페이징·정렬 키·N+1 회피·대시보드 상한은 지금 정했고 **값 5개는 G1 이 답한다**(§13.7).
  구조를 지금 정한 이유는 나중에 못 바꾸기 때문이다 — offset→커서는 API 계약 변경이고,
  정렬 tiebreaker 를 나중에 넣으면 저장된 커서가 전부 무효가 된다
- **Advisor 계약이 A 에 없다.** §10 은 화면 규칙을 정했지만 그 규칙이 읽을 필드·키·감사 기록이
  A 에 규정돼 있지 않다(§9.1 의 8~12). 규칙은 섰고 **결선할 데가 없다**
- **시각 필드의 저장 시간대가 A 에 선언돼 있지 않다.** §11 은 표시 규칙을 정했지만, 무엇을
  표시하는지의 절반(저장측 의미)은 A 개정을 기다린다(§9.1 의 13~15)

---

## 9. 미결 — A 에 근거가 없어 지어내지 않은 것

| # | 미결 | 왜 지금 정하지 않는가 |
|---|---|---|
| 1 | ~~**운영자용 조회 API**~~ | **v0.3 에서 해소 → §12.** 화면 21건에서 역으로 도출, 조회 경로 규칙 5개, `etl_ui` 뷰 계층 제안. **v0.1 의 전제("A 개정이다")를 정정했다** — A 는 §16.2 에서 read replica 로 이미 답했고, 다만 그 한 문장이 21건 중 6건만 덮는다(§12.0·§12.3). A 가 골라야 할 것은 §12.6 |
| 2 | ~~**권한·승인 모델**~~ | **v0.4 에서 결정으로 닫았다 → §14.** 인가는 미룬다(운영팀 전용·전원 동일 권한). **인증·감사는 미루지 않는다** — A 가 `AuditEvent.actor`·`auth_method` 를 필수로, `사유·승인자 필수`를 7곳에서 요구한다. 새로 드러난 것은 §14.3(두 UI 의 신원이 갈린다) |
| 3 | ~~**목록 규모 대응**~~ | **v0.3 에서 해소 → §13.** 커서 페이징·총 건수·기본 필터·N+1·대시보드 상한·정렬 키. **구조는 정했고 값 5개는 G1 이 답한다**(§13.7) |
| 4 | ~~**Advisor UX**~~ | **v0.2 에서 해소 → §10.** 화면 규칙 7절. A 가 답해야 할 잔여는 §10.8 |
| 5 | ~~**시각(timezone) 표시**~~ | **v0.2 에서 해소 → §11.** 표시 규칙 4개. **다국어는 분리해 아래 7번으로 남긴다** |
| 6 | **capability 표시 형태** | §3.2 로 Phase 0 은 막았지만, G0-0 이후 무엇을 어떻게 보여 줄지는 축이 확정된 뒤에 |
| **7** | **다국어** | v0.1 이 5번에 묶어 뒀던 것을 떼어 냈다. **근거가 없다** — A 는 언어를 규정하지 않고 사용자층도 정해지지 않았다(§11.4). 다만 rule 이름·enum·거부 사유 코드를 번역하지 않는다는 것 하나는 §11.4 에서 정했다 |

### 9.1 v0.2~v0.4 를 쓰다 새로 드러난 미결 — 전부 A 개정이 필요하다

미결 1·3·4·5 를 채우려고 A 를 정독한 결과, **A 자체의 공백이 열둘** 나왔다. 이것들은
"UI 가 정하지 않은 것"이 아니라 **"A 가 정하지 않아서 UI 가 정할 수 없는 것"**이다.

| # | 무엇 | 어디 |
|---|---|---|
| 8 | `AdvisorAnalysis` 의 필드 — §15.3 이 저장을 요구하는 confidence·근거 metadata ID·버전 4종의 **이름이 하나도 없다** | §10.8 |
| 9 | `AdvisorAnalysis` → Job 의 **키가 없다.** draft 수정 후 기존 추천이 낡았다는 것을 지금 A 로는 말할 수 없다 | §10.8 |
| 10 | `POST /v1/jobs/{id}/advisor-analyses` 의 **본문·응답·상태코드가 없다** | §10.8 |
| 11 | 추천 수락·거절의 감사 기록. **A 의 다른 모든 운영자 재정의가 갖는 `사유·승인자 필수` 구절이 Advisor 경로에만 없다** | §10.8 |
| 12 | A §16.1 은 `Advisor` 를 독립 화면으로, §7.2 는 Wizard 3단계로 둔다 — **A 안에서 두 절이 어긋난다** | §10.8 |
| 13 | **`*_at` 필드 약 25개의 저장 시간대가 선언되지 않았다.** `TIMESTAMP WITH TIME ZONE` 은 A 전체에 0회다 | §11.5 |
| 14 | 전사 NTZ vs timestamptz — **A §22-12 자체가 미결** | §11.5 |
| 15 | `DB 시간대` capability 의 필드 이름이 없다 — A §22-4 의 미결 | §11.5 |
| **16** | **목록·검색·필터 15건을 replica 로 할지 API 로 할지 A 가 정하지 않았다.** §16.2 는 "contract 단위 조회"만 말하고 그것이 21건 중 6건이다 | §12.3 · §12.6-a |
| **17** | 판정 직전 재확인 조회(`GET /v1/contracts/{id}` · `GET /v1/jobs/{id}`)가 **A §17 에 없다.** replica 로는 대신할 수 없다 | §12.6-b |
| **18** | `POST /v1/holds` 의 **응답 형식이 A 에 없다.** 명령 응답만으로 화면을 갱신하려면 영향 범위가 필요하다 | §12.6-c |
| **19** | `target_lease`·`target_table` **밖의 6.1 테이블에 대한 role 규정이 없다.** UI 가 replica 를 직접 읽으면 권한을 걸 데가 없다 | §12.1 · §12.4-(4) |
| **20** | ④ 실행 목록의 **보존 기간 정책이 A 에 없다.** 40,000/일이면 화면이 덮을 구간을 정해야 한다 | §13.7 |
| **21** | **Control UI 신원과 Dagster proxy 신원을 잇는 키가 A 에 없다.** A §10.2 는 `OPERATOR_CANCELLED` 의 actor 를 proxy access log 로 보강한다고 하는데, 그 log 의 주체와 `AuditEvent.actor` 가 같은 사람이라는 것을 말할 방법이 없다 | §14.3 |
| **22** | A §22-10 **"Dagster UI 노출 정책"이 A 자신의 미확정 항목이다** — read-only 분리·mutation allowlist·감사 | §14.4 |

**그리고 v0.1 의 전제 둘을 정정했다.**

1. v0.1 은 5번에서 "계약 시각은 UTC 다" 라고 썼는데 **A 에 그런 규정이 없다**(§11.0).
   표시 시간대를 고르는 것은 UI 결정이지만 **저장 시간대 확정은 A 개정**이다.
2. v0.1 은 1번에서 조회 API 도출이 **"A 개정이다"** 라고만 썼는데, **A 는 §16.2 에서
   read replica 로 이미 답해 뒀다**(§12.0). 남은 문제는 "A 에 없다"가 아니라 **"그 한 문장이
   화면의 3분의 1만 덮는다"** 이고, 그 차이가 §12.4 의 문제 넷을 만든다.

### 9.2 남은 것을 무엇이 푸는가

| 미결 | 푸는 것 |
|---|---|
| 2 | ~~미결~~ **결정됨**(§14). 인가는 미루고 인증·감사는 지금 한다. 다만 **§14.3 이 새 문제를 남겼다** — 두 UI 의 신원을 잇는 키 |
| 6 | **G0-0 실측** — 축이 확정돼야 표시 형태가 정해진다 |
| 7 | 사용자층·운영 조직 결정 |
| 8~12 | **A §15·§16.1·§17 개정** — Advisor 계약을 실제로 규정해야 한다 |
| 13~15 | **A §6.1·§22-4·§22-12 개정** — 시각 타입과 DB 시간대 등록 |
| 16~19 | **A §16.2·§17·§6.1 개정** — 조회 경로 선택(§12.6-a)이 먼저다. 그것이 17~19 의 형태를 정한다 |
| 20 | 보존 정책 결정 + 규모 시험(G1) |
| 21 | **지금 규율로 막을 수 있다** — §14.3-(a). A 개정 없이도 ID 문자열을 맞추면 이어진다 |
| 22 | **A §22-10 확정** |

**8~22 를 UI 문서가 채울 수 없다.** 채우면 규범에 없는 것을 지어내는 것이고, 이 저장소가
여덟 차례 리뷰에서 반복해 지적당한 바로 그 결함이다.

### 9.3 미결 1·2·3 은 하나였다 — 그리고 §14 결정이 그 사슬을 끊었다

v0.3 은 이 사슬을 지적했다.

```text
조회를 replica 로 할 것인가 API 로 할 것인가 (§12.6-a)
   └─▶ 권한을 DB role 로 걸 것인가 API 에서 걸 것인가 (§9-2)
          └─▶ "내 부서" 를 화면이 알 수 있는가 (§13.3)
                 └─▶ 10,000 Job 목록의 기본 필터를 정할 수 있는가
```

**§14 의 결정(인가를 미룬다)이 가운데 고리를 뺀다.** 그 결과가 둘이다.

**(1) `replica` 직접 읽기가 쉬워진다.** §12.4-(4)가 "replica 직접 읽기는 권한 모델을
DB role 로 구현하게 만드는데 화면 단위 권한은 role 로 표현하기 어렵다"였다.
**인가가 없으면 그 어려움이 지금은 없다.** §12.6-a 의 선택에서 replica 안(①)의 부담이
그만큼 줄어든다.

> 다만 **없어진 것이 아니라 미뤄진 것이다.** 인가가 생기는 시점에 §12.4-(4)가 그대로
> 돌아온다. 그래서 §14.2-(3)이 "권한 분기를 화면에 넣지 않는다 — 생기면 조회 계층과
> Control API 양쪽에 붙는다"고 미리 자리를 정해 둔 것이다.

**(2) Job 목록 기본 필터는 잠정이 아니라 확정이다.** "내 부서"를 알 방법이 없으므로
기본은 **"상태가 정상이 아닌 것"만**이고, 부서 필터는 수동 선택이다(§13.3).
이것은 Phase 0 의 임시 조치가 아니라 인가가 생길 때까지의 정식 동작이다.

---

## 10. Advisor UX — v0.1 §9-4 해소

### 10.0 A 가 이미 정한 것

| A | 내용 |
|---|---|
| §2 원칙 2 | > **LLM 추천보다 Source 보호 정책이 항상 우선한다.** |
| **§11.1 정책 계층** | 원칙 2 를 **실제로 집행하는 곳**. 4단 사다리이고 LLM 이 맨 아래다 |
| §11.1 | > LLM은 Source 전체의 동시 세션/CPU 한도를 변경하지 못한다. LLM이 추천하는 것은 그 절대 상한 안의 Job별 read profile뿐이다. |
| §15.1 | 추천 대상 9종 — 적재 모드 · watermark 후보와 `INSERT_DT + UPDATE_DT` 전략 · PK/unique 후보 · `dt/wt/mt/yt` · Template · Spark/read profile · Source protection 범위 안의 JobReadProfile · 단순 컬럼 표준화와 mapping · **확인이 필요한 위험 항목** |
| §15.2 | 저부하 metadata 만. **큰 테이블 `COUNT(*)` 금지**, 실제 row sample 은 기본적으로 LLM 에 보내지 않는다 |
| §15.3 | 파이프라인 `Deterministic safety rules → LLM structured recommendation → 정책 validator → 운영자 확인/수정 → publish`. JSON Schema output 강제 · confidence 와 근거 metadata ID · model/prompt/rule/metadata 버전 저장 · **추천값과 운영자 수정값을 모두 audit** · **column/table comment 는 prompt injection 가능 입력으로 분리·표시** · LLM 장애 시 수동 Wizard 정상 동작 · **LLM 은 SQL 을 실행하거나 Job 을 자동 publish 하지 않음** · "Master/Event"·hard delete·`UPDATE_DT` 신뢰성은 컬럼명만으로 확정하지 않음 |
| §15.4 | 도입 순서 — ① Rule engine + 모델 1개로 **Shadow 평가** ② 안전하지 않은 추천률·운영자 수정률 **측정** ③ 통과 후 **UI auto-prefill** ④ 이후 escalation. 그리고 마지막 줄: > Advisor는 실행 경로의 필수 구성요소가 아니며 MVP 안정화 후 도입한다. |
| §7.2-3 | Wizard 3단계 = "LLM/Rule Advisor 추천 확인 **(선택 단계 — Advisor 장애 시 건너뜀)**" |
| §16.1 | Custom UI 의 화면 목록에 **`Advisor` 가 `Source`·`Job Wizard/CRUD` 와 나란한 항목으로** 있다 |
| §17 | `POST /v1/jobs/{id}/advisor-analyses` — **본문·응답·상태코드 없음** |
| §6.1 | 객체 `AdvisorAnalysis` — **이름만 있고 필드가 없다** |

> **A 의 금지는 RFC 2119 어휘가 아니다.** §15 에 `MUST NOT` 류가 하나도 없고 전부
> `…하지 않는다` / `…하지 못한다` 형태다. 아래 규칙도 그 어조를 따른다 — 규범이 쓰지 않은
> 강도를 UI 문서가 만들어 붙이지 않는다.

**§10.5 가 이 절의 핵심이다.** 원칙 2 는 한 문장뿐이고 §15 는 그것을 인용조차 하지 않는다.
집행 구조를 가진 것은 §11.1 의 사다리 하나뿐이므로, 화면이 원칙 2 를 표현하려면 그 사다리를
그려야 한다.

### 10.1 Advisor 는 입력 칸을 채우지 않는다

A §15.4 는 **auto-prefill 을 3단계**에 둔다. 1·2단계(Shadow 평가, 안전하지 않은 추천률·운영자
수정률 측정)가 선행 조건이고 그 측정은 아직 없다.

- 추천은 입력 칸 **옆**에 카드로 놓는다. 칸에 값이 미리 들어가 있지 않다
- [적용] 은 **필드 하나 단위**로만 있다
- **"전체 적용" 버튼을 만들지 않는다.** 전체 적용은 auto-prefill 과 같은 것이고 그것은
  §15.4 의 3단계다

> 미리 채워진 칸과 옆에 놓인 카드는 같은 정보를 담지만 사람의 행동이 다르다. 채워진 칸은
> 기본값으로 읽혀 검토 없이 통과하고, 옆의 카드는 **옮겨 적는 행위**를 요구한다. §15.4 가
> 측정하려는 "운영자 수정률"은 그 행위가 있어야 측정된다.

### 10.2 추천 카드가 반드시 담는 것

A §15.3 이 **저장을 요구하는 것**을 화면에도 올린다. 저장만 하고 보여 주지 않으면 운영자는
근거 없이 판단한다.

| 요소 | A 근거 | 화면 |
|---|---|---|
| 추천값 | JSON Schema output | 대상 필드 이름과 함께 |
| confidence | §15.3 | 숫자 그대로. **막대로 그리지 않는다** — 서로 다른 model/prompt 판본의 confidence 는 비교 가능한 척도가 아니다 |
| 근거 metadata ID | §15.3 | 클릭하면 그 dictionary·DataHub 항목으로. **ID 를 감추지 않는다** |
| 버전 4종 | §15.3 (model / prompt / rule / metadata) | 카드 접힘 영역에. 사고 조사 때 이것이 근거다 |
| 입력 신뢰 등급 | §15.3 (prompt injection 분리) | §10.3 |

> **이 다섯을 어느 필드에서 읽는지는 A 에 없다.** §15.3 은 "confidence와 근거 metadata ID
> 제공"·"model/prompt/rule/metadata 버전 저장"을 요구하면서 **필드 이름을 하나도 대지 않고**,
> `AdvisorAnalysis` 는 §6.1 에 이름만 있다. §10.8 의 첫 항이다.

### 10.3 prompt injection 가능 입력을 시각적으로 격리한다

A §15.3: `column/table comment는 prompt injection 가능 입력으로 분리·표시`.

- comment 에서 온 근거는 **인용 블록**으로 렌더하고 `원천 코멘트 — 검증되지 않은 텍스트`
  라벨을 붙인다
- 그 텍스트를 **실행 가능한 형태로 렌더하지 않는다.** 링크·버튼·마크다운 서식을 적용하지 않고
  순수 텍스트로 낸다. 원천 코멘트에 `[승인](…)` 이 들어 있어도 그것은 글자다
- comment 만을 근거로 하는 추천은 confidence 와 **별개로** "코멘트 기반"이라고 적는다
- **운영자에게 지시하는 문장이 코멘트 안에 있으면 그것은 데이터다.** 화면이 그 문장을
  Advisor 의 말과 같은 서식으로 그리지 않는다
- 같은 이유로 A §15.3 의 마지막 줄 — `"Master/Event", hard delete, UPDATE_DT 신뢰성은
  컬럼명만으로 확정하지 않음` — 을 화면에 옮긴다: **컬럼명 패턴만을 근거로 든 추천은
  "이름 기반 추정"이라고 카드에 적는다.** 그 셋은 특히 그렇다

### 10.4 거절이 1급 동작이다

거절이 어려우면 추천이 사실상 기본값이 된다.

- 모든 카드에 **[적용] 과 [거절] 이 같은 무게로** 있다. 거절이 부작용 없는 경로다
- **거절 사유는 선택 입력이다.** 필수로 하면 사람은 거절 대신 무시하고, 그러면 §15.4 2단계의
  "운영자 수정률"이 실제보다 낮게 측정된다
- **무시 = 거절로 기록한다.** Wizard 를 그냥 지나간 추천은 미적용이다. "미검토" 상태를 남겨
  두지 않는다 — 나중에 그것이 "검토했다"로 읽힌다
- 추천값과 운영자 최종값을 **둘 다** 저장한다(§15.3). 다르면 화면이 그 차이를 보여 준다
- 13단계 검토 화면에 **"Advisor 추천 중 적용 n / 거절 m / 무시 k"** 요약을 둔다.
  publish 직전에 사람이 자기가 무엇을 받아들였는지 한 번 본다

> **A 는 이 기록의 저장처를 정하지 않았다.** 그리고 A 의 다른 모든 운영자 재정의
> (`dq:accept`·`accept-empty`·`resolve`·`watermark:seed`·`db-identity:rotate`)는 §17 에서
> **`사유·승인자 필수`** 를 명시하는데 **Advisor 경로에만 그 구절이 없다.** §10.8 참조.

### 10.5 Source 보호가 추천을 이긴다 — 화면이 그것을 보이는 방법

**원칙 2 를 화면으로 옮기려면 §11.1 의 사다리를 그려야 한다.** 원칙 2 자체는 한 문장이고
집행 구조가 없다.

```text
DB-enforced hard limit (ETL 전용 user Profile / Resource Manager / standby 전용 service)
  > DBA·플랫폼이 승인한 SourceSafetyEnvelope
    > JobReadProfile override
      > LLM recommendation          ← 맨 아래
```

**(a) 사다리를 카드에 그대로 그린다.**
Advisor 가 read profile 계열(§15.1 의 `Spark/read profile`·`Source protection 범위 안의
JobReadProfile`)을 추천할 때, 카드 안에 위 4단을 **그 Job 의 실제 값과 함께** 보여 준다.
운영자가 "이 추천이 어느 칸을 건드리는가"를 그림으로 안다.

**(b) 상한을 넘는 추천은 카드 자체를 거부 상태로 렌더한다.**
SourceSafetyEnvelope·DB hard limit 을 넘는 값은 **보여 주되 [적용] 을 없앤다.**
"이 추천은 {어느 단}의 한도를 넘는다 — 한도 {값}" 을 카드 안에 적는다.
값을 숨기지 않는 이유는, 운영자가 Advisor 가 무엇을 제안했는지 알아야 §15.4 2단계의
"안전하지 않은 추천률"이 측정되기 때문이다.

**(c) 동시 세션·CPU 한도를 바꾸는 추천 자리를 아예 만들지 않는다.**
A §11.1: `LLM은 Source 전체의 동시 세션/CPU 한도를 변경하지 못한다.`
화면에 그 필드를 대상으로 하는 추천 카드가 **나타날 수 있는 자리를 두지 않는다.**
거부 상태로 렌더하는 것도 아니다 — 애초에 추천 대상이 아니다.

> **판정 주체는 validator 다**(§3.4). (b)의 렌더는 편의이고 권위는
> `POST /v1/jobs/{id}/validate` 다. 화면은 앞질러 **막을** 수는 있어도 앞질러
> **통과시킬** 수 없다.

**(d) `422` 위반이 Advisor 값에서 온 것이면 그렇게 표시한다.**
13단계 검증의 `violations[].field` 가 Advisor 추천을 적용한 필드면 카드로 되돌아가는 링크를
붙인다. 운영자가 자기 입력을 의심하며 시간을 쓰지 않게 한다.

**(e) Advisor 는 Source 용량 Gate 를 계산하지 않는다.**
ρ·예상 row·예상 시간·예상 부하의 유일한 권위는 Wizard 13단계의 Gate 결과다(§5-③).
**추천 카드에 예상 부하를 적지 않는다** — 적으면 화면에 숫자가 둘 생기고 사람은 먼저 본
것을 믿는다.

### 10.6 Advisor 장애는 오류 화면이 아니다

A §7.2-3 이 "선택 단계 — Advisor 장애 시 건너뜀", §15.3 이 "LLM 장애 시 수동 Wizard는 정상
동작"을 규정한다.

- 3단계가 실패해도 Wizard 는 멈추지 않는다. **"Advisor 사용 불가 — 수동으로 진행"** 을
  오류가 아니라 **정보**로 표시하고 [다음] 을 활성 상태로 둔다
- 재시도 버튼은 두되 **자동 재시도하지 않는다.** §15.2 가 저부하를 요구하는데 자동 재시도는
  부하를 사람이 통제하지 못하는 형태로 만든다
- 응답 대기에 **화면 타임아웃**을 둔다. 값은 A 에 없다(§10.8)

### 10.7 Phase 0 에는 이 화면이 없다 — v0.1 의 이유를 정정한다

v0.1 §7 은 Wizard 3단계를 Phase 0 범위에서 빼면서 이유를 12단계와 묶어 "측정·튜닝 결과가
있어야 의미가 있다"라고 썼다. **12단계는 그 이유가 맞지만 3단계는 아니다.**

정확한 이유는 **A §15.4 의 도입 순서**다. auto-prefill 이 3단계이고 그 앞에 Shadow 평가와
측정이 있으며, 같은 절이 마지막 줄에서 못박는다.

> Advisor는 실행 경로의 필수 구성요소가 아니며 MVP 안정화 후 도입한다.

즉 3단계를 빼는 것은 Phase 0 의 편의가 아니라 **A 가 정한 순서를 지키는 것**이다.
§10.1~10.6 은 **Advisor 를 켤 때 지켜야 할 규칙**이고, 지금 만들 화면의 명세가 아니다.

### 10.8 §10 이 채우지 못한 것 — 전부 A 개정이 필요하다

| 무엇 | 상태 |
|---|---|
| `AdvisorAnalysis` 의 필드 | A §6.1 에 **이름만** 있다. §10.2 의 5요소를 어느 필드에서 읽는지 정해지지 않았다 |
| **`AdvisorAnalysis` → Job 의 키** | A 에 **없다.** §6.1 에서 이 객체는 `Job` 하위가 아니라 최상위 형제로 놓여 있고, Job 과의 연결은 §17 의 경로 파라미터 `{id}` 뿐이다. **그래서 draft 가 수정됐을 때 기존 추천이 낡았다는 것을 지금 A 로는 말할 수 없다** — 화면이 "이 추천은 수정 전 draft 기준" 이라고 쓰려면 그 키가 있어야 한다 |
| `POST /v1/jobs/{id}/advisor-analyses` 의 본문·응답·상태코드 | A §17 은 **경로만** 적었다. 같은 블록의 이웃 엔드포인트들은 인라인 주석으로 의미를 규정하는데 이것만 없다 |
| **추천 수락·거절의 감사 기록** | A §15.3 은 "추천값과 운영자 수정값을 모두 audit"을 요구하면서 저장처·필드·enum 을 정하지 않았고, **A 의 다른 모든 운영자 재정의가 갖는 `사유·승인자 필수` 구절이 Advisor 경로에만 없다.** 이 비대칭이 의도인지 누락인지 A 가 답해야 한다 |
| 거절 사유 어휘 | enum 이 없다. §15.4 2단계가 무엇을 측정하는지 정해져야 어휘가 나온다 |
| 응답 타임아웃 값 | A 에 없다 |
| Shadow 평가 화면 | §15.4 1~2단계의 "안전하지 않은 추천률·운영자 수정률"을 **누가 어디서 보는가**가 A 에 없다. 이 UI 인지 Grafana 인지도 미정 |
| **Advisor 가 독립 화면인가 Wizard 단계인가** | A §16.1 은 Custom UI 목록에서 `Advisor` 를 `Source`·`Job Wizard/CRUD` 와 **나란한 항목**으로 적어 독립 화면을 시사하는데, A §7.2 는 Wizard 3단계로만 둔다. v0.1·v0.2 는 Wizard 단계로 읽었다 — **A 안에서 두 절이 어긋난다** |

---

## 11. 시각 표시 — v0.1 §9-5 해소 (시각 부분)

### 11.0 먼저, v0.1 의 전제를 정정한다

v0.1 §9-5 는 이렇게 썼다.

> `logical_scheduled_at` 은 `Asia/Seoul` cron 인데 **계약 시각은 UTC 다.** 어느 쪽으로 보여
> 줄지 규정이 없다

**"계약 시각은 UTC 다" 는 A 에 없다.** 확인한 결과는 이렇다.

- A 전체에서 `TIMESTAMP WITH TIME ZONE` 은 **0회**다. `*_at`·`*_ts` 필드 약 25개 중
  **선언된 타입이나 시간대를 가진 것이 하나도 없다**
- UTC 가 규범으로 나오는 곳은 **두 군데뿐**이다 — ① 제약 이름 `logical_scheduled_at_utc`
  (§9.2 DDL 코드블록 안의 이름이지 컬럼 타입 선언이 아니다) ② `window_range` 정규화
  (§13.4 — `APPLICATION_TIMESTAMP` 는 UTC epoch 마이크로초)
- 그리고 `fence_ts` 는 **명시적으로 standby wall-clock 이다**(§6.1) — Control 호스트 시계가
  아니고 UTC 라는 보증도 없다

그러니 실제 상황은 "계약 시각이 UTC 다"가 아니라 **"필드 하나에 `_utc` 접미사가 있고, 범위
하나가 UTC epoch μs 로 정규화되며, 나머지는 선언이 없다"** 이다.

**따라서 v0.1 §9-5 의 "이 문서 안에서 정할 수 있다"는 절반만 맞다.** 표시 시간대를 고르는
것은 UI 결정이지만, **저장 시간대가 무엇인지 확정하는 것은 A 개정이다**(§11.4).

### 11.1 A 가 고정한 것

| 대상 | 값 | A |
|---|---|---|
| JobSpec `schedule.timezone` | Job 별 필드. **`Asia/Seoul` 은 예시값이다** | §7.3 |
| cron 전개·timezone·DST 의 권위 | **Dagster** — A 가 자기 것이 아니라고 명시한다 | §9.1 |
| occurrence 자연키 | `UNIQUE (job_id, operation_class, **logical_scheduled_at_utc**)` | §9.2 |
| window — `APPLICATION_TIMESTAMP` | **UTC epoch 마이크로초로 정규화** | §13.4 |
| window — `STANDBY_VISIBLE_SCN` | **SCN 정수 — 시각이 아니다** | §13.4 |
| 세션 시간대 | **`ALTER SESSION SET TIME_ZONE = '+00:00'`** — canonical row hash 재현성 때문 | §11.3 · §12.3 |
| Spark 세션 | `spark.sql.session.timeZone=Asia/Seoul` · JVM TZ · `preferTimestampNTZ` 를 **Runner 이미지에 고정** | §12.4 |
| 대상 컬럼 타입 | `timestamp_type: TIMESTAMP_NTZ` — **잠정 기본** | §7.3 |
| freshness | lateness = `contract.finalized_at − logical_scheduled_at` | §16.4 |
| 시계 세 개 | primary(`T_lb`·`UPDATE_DT`) / standby(`fence_ts`·`SYSTIMESTAMP_standby`) / Control 호스트(`now()`) | §11.3 · §12.2 |

**`Asia/Seoul` 은 A 전체에서 두 번 나오고 둘 다 위의 것이다** — JobSpec 예시의 cron 시간대와
Spark 세션 설정. `logical_scheduled_at` 에 대해 `Asia/Seoul` 을 말하는 문장은 없다.

**A 가 고정하지 않은 것: 화면 표시 시간대.** `KST`·`현지 시각`·`로컬 시각`·`표시 시각` 을
찾아도 0건이다. §16.1 은 어느 UI 가 무엇을 보여 주는지만 정하고 시간대를 말하지 않는다.
그리고 전사 NTZ vs timestamptz 선택 자체가 A §22-12 의 미결이다.

아래 §11.2 의 네 규칙은 **A 가 고정한 값들과 모순되지 않는 범위에서 이 문서가 정하는 것**이다.

### 11.2 표시 규칙 넷

**규칙 1 — 표시는 고정 `Asia/Seoul`, 오프셋 라벨을 항상 붙인다.**

- 모든 시각에 오프셋을 함께 쓴다: `2026-08-30 14:05 KST(+09:00)`
- **라벨 없는 시각을 화면에 쓰지 않는다.** 이 플랫폼은 시계를 셋 쓰고 저장 시간대는 대부분
  선언조차 없다. 라벨 없는 숫자는 어느 쪽인지 말하지 않고, 사고 조사에서 그 모호함이 가장 비싸다
- **브라우저 시간대를 따르지 않는다.** 기본 표시는 고정 `Asia/Seoul` 이다 — 운영 cron 이 그
  시간대로 쓰이고, 운영자가 어디서 보든 같은 숫자여야 두 사람의 사고 보고서가 맞물린다
- 상대 시각("3분 전")은 절대 시각의 **보조**로만 쓴다. 단독으로 쓰지 않는다

**규칙 2 — `logical_scheduled_at` 은 논리 시각이지 실행 시각이 아니다.**

- 목록·상세에서 **"논리 시각"** 라벨을 붙인다. 실행 시각과 같은 서식으로 나란히 두지 않는다
- 옆에 실제 실행 시각(`attempt_timeline.t1_launch_at`)을 같이 보여 준다.
  둘의 차이가 lateness 이고 그것이 §16.4 의 지표다
- 저장 키는 `logical_scheduled_at_utc` 지만 **화면은 그 Job 의 `schedule.timezone` 으로
  되돌려** 보여 준다. 운영자가 cron 을 그 시간대로 썼기 때문이다
- **Job 마다 timezone 이 다를 수 있다**(A §7.3 은 JobSpec 필드로 둔다). 서로 다른 timezone 의
  Job 을 한 목록에 세울 때 **정렬은 UTC 로 한다.** 정렬 기준이 행마다 달라지면 안 된다.
  표시는 각 Job 의 시간대로 하되 그 시간대를 열에 함께 적는다
- **DST 경계의 중복·결손 시각을 화면이 해석하지 않는다.** A §9.1 이 cron 전개·DST 의 권위를
  Dagster 에 남겼으므로, 화면은 Dagster 가 만든 논리 시각을 **그대로** 보여 준다

**규칙 3 — window 는 시각으로 그리지 않는다. `window_kind` 를 먼저 보여 준다.**

- `STANDBY_VISIBLE_SCN` window 는 **SCN 정수**다. 시각으로 변환해 보여 주면 **없는 정밀도를
  만든다** — SCN 과 시각의 대응은 원천이 보증하지 않는다. 숫자 그대로 두고
  `window_kind: STANDBY_VISIBLE_SCN` 을 앞에 적는다
- `APPLICATION_TIMESTAMP` window 는 UTC epoch 마이크로초다. 시각으로 보여 주되
  **μs 를 자르지 않는다.** 반개구간 `[low, high)` 의 경계가 μs 단위인데 초로 반올림하면
  경계에 걸친 행의 포함 여부가 화면에서 뒤집힌다
- **두 종류를 같은 열에 섞어 그리지 않는다.** 한쪽은 시각이고 한쪽은 시각이 아니다
- 같은 규칙이 **coverage 지표**에 걸린다. A §16.2 의
  `coverage = max(ledger.window_high WHERE cas_applied = true)` 는 그 Job 의 현재 production
  watermark 인데, `APPLICATION_TIMESTAMP` Job 에서는 **UTC epoch μs 정수**다. 화면이 이
  숫자를 사람이 읽는 시각으로 바꾸는 유일한 지점이므로 여기서 규칙 1 의 라벨(`UTC`)을 붙이고
  μs 를 유지한다

**규칙 4 — 시계가 셋이다. 화면이 그것을 섞지 않는다.**

A §11.3·§12.2 는 세 시계를 명시적으로 구분하고, §12.2 는 섞지 말라는 이유까지 적는다.

| 시계 | 값 | 화면 |
|---|---|---|
| **primary** | `T_lb`(heartbeat) · 원천 `UPDATE_DT`·watermark 컬럼 | 원천 값 그대로 |
| **standby** | `fence_ts` · `SYSTIMESTAMP_standby` | `fence_ts` 라벨에 **"standby 시계"** 를 붙인다 |
| **Control 호스트** | `finalized_at`·`expires_at`·`next_eligible_at`·`effective_from`·`granted_at`·`hold_release_at`·`t0`~`t7` | 규칙 1 적용 |

- **watermark 값은 원천 컬럼 값이다.** Oracle 타입(`DATE` / `TIMESTAMP` /
  `TIMESTAMP WITH TZ`)마다 시간대 의미가 다르고, `TIMESTAMP_NTZ` 로 저장되면 **시간대가 없다**.
  화면은 원천 값을 **그대로** 보여 주고 **시간대 라벨을 붙이지 않는다.** 대신 원천 컬럼 타입을
  옆에 표시한다. 규칙 1 을 여기에 적용하면 없는 사실을 만든다
- **`fence_ts` 를 window 하한처럼 읽히게 그리지 않는다.** A §6.1 이 명시한다 —
  `window 하한에도, 12.2 extract window 하한에도 fence_ts 를 쓰지 않는다`. 하한의 권위는
  `T_lb` 다. 화면이 둘을 같은 열에 세우면 그 구분이 사라진다
- **④ 실행 상세가 세 시계를 한 트리에 그리는 화면이다.** `logical_scheduled_at`(cron 유래) ·
  `fence_ts`(standby) · `first_guard_ok_at`·`last_cas_at`·`finalized_at`(Control 호스트)이
  같은 타임라인에 온다. **그 화면은 시계별로 구획을 나누고 각 구획에 시계 이름을 적는다**
- **다른 시계의 값을 화면에서 빼지 않는다.** A 에 `commit_time`·`watermark_value` 라는 필드는
  **없다** — 있는 것은 상한 `max_commit_minus_watermark_seconds` 하나이고 그 두 피연산자는
  산문으로만 언급된다. 그리고 그 값은 G0-0 이 재야 하는 독립 축(`watermark_commit_bound`)이며
  **아직 미측정이다**(§3.3). 화면이 그 뺄셈을 하면 측정하지 않은 것을 측정한 것처럼 보여
  준다 — 7차 리뷰 P0-05 가 문서에서 지적한 오류를 화면에서 반복하는 것이다

### 11.3 Phase 0 에서 이 규칙이 걸리는 곳

| 화면 | 표시하는 시각 | 규칙 |
|---|---|---|
| ① 대시보드 — 열린 Hold | 생성 시각 · 경과 | 1 |
| ① 대시보드 — 사람 대기 | 상태 진입 시각 | 1 |
| ② Source — 연결 | ConnectionRevision 전이 시각 | 1 |
| ② Source — Capability | 측정 시각(`verified_at` 등) · stale 표시 | 1 · §3.3 |
| ③ Job 목록 | freshness SLO · 최근 상태 시각 | 1 · 2 |
| ③ Wizard 11단계 | cron 과 그 시간대 | 1 · 2 |
| ⑤ 운영 — Hold 해제 | `T_catchup` 예상 · `hold_release_at` | 1 |

④ 실행 상세와 ⑦ 증거는 Phase 0 범위 밖이지만 **규칙 3·4 가 가장 강하게 걸리는 화면**이다.
그 화면을 만들 때 이 절을 먼저 편다.

### 11.4 다국어는 v0.2 도 정하지 않는다 — 시각과 분리한다

v0.1 §9-5 는 "다국어·시각(timezone) 표시"를 한 항목으로 묶었다. **둘은 다른 문제이고 근거의
양도 다르다.** 시각은 A 가 고정한 값이 열 개 있어 규칙을 도출할 수 있었지만 다국어는 근거가
없다 — A 는 언어를 규정하지 않고, 사용자층이 사내 운영자만인지도 정해지지 않았다.
**지어내지 않는다.**

다만 **한 가지는 지금 정한다.**

> **rule 이름 · 상태 enum · 거부 사유 코드는 번역하지 않는다.**

§3.4 가 이미 "`violations[].rule` 을 그대로 보여 준다 — UI 가 문구를 지어내 rule 이름을
감추면 운영자가 A 문서에서 근거를 찾을 수 없다"를 요구한다. 번역은 감추는 것의 한 형태다.
`SOURCE_LAG_EXCEEDED` · `ADJUDICATION_PENDING` · `DIRECT_LAUNCH_FORBIDDEN` 은 원문으로 두고
**설명 문장만** 번역 대상이다.

### 11.5 §11 이 채우지 못한 것

| 무엇 | 상태 |
|---|---|
| **`*_at` 필드들의 저장 시간대** | **A 개정 사항이다.** 약 25개 필드 중 타입·시간대가 선언된 것이 없다. `_utc` 접미사는 `logical_scheduled_at` 하나뿐이고 그것도 제약 이름일 뿐이다 |
| 전사 NTZ vs timestamptz | **A §22-12 자체가 미결**이다. 정해지면 규칙 4 의 "시간대가 없다" 전제가 바뀐다 |
| **`DB 시간대` capability** | A §6.1 이 `SourceCapability` 목록에 산문으로만 적었고 필드 이름이 없다. §22-4 의 미결이다. 정해지기 전에는 원천 시간대를 화면 계산에 쓰지 않는다 |
| 운영자별 표시 시간대 설정 | 규칙 1 은 고정 `Asia/Seoul` 을 택했다. 해외 운영 조직이 생기면 재검토이며, 그때도 **감사 로그·사고 보고서는 UTC 병기**가 전제다 |
| 내보내기 형식 | 목록·증거 CSV/JSON 내보내기의 시각 형식(ISO 8601 UTC 고정을 제안하나 요구가 없다) |
| 다국어 전반 | §11.4 |

> **§11 을 쓰다 코드 결함을 하나 찾았다.** A §11.3 이 세션에 `TIME_ZONE = '+00:00'` 을
> 고정하고 G0-0A·G0-0B0 도 그 값인데 **G0-0B1 의 `Preamble.java` 만 `DBTIMEZONE` 이었다.**
> `DBTIMEZONE` 은 원천 DB 생성 시 정해진 값이라 무엇인지 모르고, A 는 그것을 등록조차 하지
> 않는다(위 표 3행). 그러면 B1 이 재는 세션이 규범 세션이 아니게 된다 — 7차 리뷰 P0-06 의
> `NLS_NUMERIC_CHARACTERS` `'. '` → `'.,'` 와 **정확히 같은 결함**이며, 그때 NUMBER 축만 보고
> TIMESTAMP 축을 놓친 것이다. 2026-08-30 에 고쳤다.

---

---

## 12. 운영자 조회 — v0.1 §9-1 해소

### 12.0 먼저, v0.1·v0.2 의 전제를 정정한다

v0.1 §9-1 은 이렇게 썼고 v0.2 가 그대로 이어받았다.

> A §17 은 Guard·Run Pod·운영자 명령 API 는 규정하지만 목록/검색/필터 API 가 없다.
> UI 요구에서 역으로 도출해야 하며 **그것은 A 개정이다**

**앞 절반은 맞고 뒷 절반은 틀렸다.** A §17 에 조회 API 가 없는 것은 사실이다 — 전체
엔드포인트 중 `GET` 은 셋뿐이다(`/v1/sources/{id}/schemas/{schema}/tables` ·
`/v1/jobs/{id}/releases` · `/v1/operations/{operation_id}`). 나머지는 전부 `POST` 다.

**그런데 A 는 운영자 조회를 §17 이 아니라 §16.2 에서 답해 뒀다.**

> contract·attempt 단위 감사·지표의 원천은 Control DB의 이력·계측 테이블(6.1)이다.
> Prometheus/Grafana로는 Job class·Source·Guard 사유 단위로 집계한 값만 내보내고(위 label
> 규칙 유지), **contract 단위 조회는 Custom UI가 Control DB(read replica)를 읽는다.**
> PoC 기준서 §5.1 판정 쿼리는 운영에서도 같은 테이블에 대해 실행한다

그리고 §6.1 제약 (9) 가 그 읽기의 권한 모형을 한 조각 규정한다.

> `target_lease`·`target_table`의 **DML 권한은 Control API role에만** 부여하고
> **운영·분석용 role은 SELECT만** 갖는다

즉 **A 의 답은 "조회 API 를 만든다"가 아니라 "UI 가 read replica 를 직접 읽는다"** 이다.
그러면 §9-1 은 "A 에 없는 것"이 아니라 **"A 가 한 문장으로 정해 둔 것이고, 그 한 문장이
만드는 결과를 아무도 따라가 보지 않은 것"** 이다. §12.3 이 그 결과다.

### 12.1 A 가 정한 것

| 무엇 | A |
|---|---|
| contract 단위 조회 = **Custom UI 가 Control DB read replica 직접 읽기** | §16.2 |
| 조회 원천 = **6.1 이력·계측 테이블** (`contract_state_history`·`attempt_state_history`·`lease_state_history`·`guard_result`·`attempt_timeline`) | §16.2 · §6.1 |
| Grafana 는 **집계만** — Job class·Source·Guard 사유 단위 | §16.2 |
| `target_lease`·`target_table` 은 **운영·분석 role SELECT only** | §6.1 (9) |
| 파생 지표는 read model 계산 — target publication age · coverage · `upsert_consistency` · `delete_consistency` · `delete_lag_slo_seconds` · `last_reconcile_at` | §16.2 |
| `GET` 엔드포인트 **3개** | §17 |

**A 가 정하지 않은 것**

- **목록·검색·필터를 replica 에서 하는가 API 로 하는가.** §16.2 는 "contract 단위 조회"만
  말한다. Source 목록·Job 목록·Hold 목록은 contract 단위가 아니다
- **`target_lease`·`target_table` 밖의 테이블에 대한 role 규정** — 나머지 6.1 테이블에는 없다
- **replica 지연을 UI 가 어떻게 다루는가** — 한 줄도 없다

### 12.2 조회 경로가 둘이다 — 규칙 다섯

경로가 둘이면 **어느 것을 언제 쓰는가**가 규칙이어야 한다. 정하지 않으면 화면마다 달라진다.

**R1 — 명령의 결과는 그 명령의 응답으로 읽는다.**
Hold 를 만들고 목록을 replica 로 다시 조회해 확인하지 않는다. `POST /v1/holds` 의 응답이
권위이고, 화면은 그 응답으로 갱신한다. **replica 재조회로 확인하면 지연 때문에 방금 한 일이
안 보인다.**

**R2 — replica 값으로 버튼을 끄지 않는다.**
§3.4 는 "막을 때는 앞질러 막아도 되지만, 통과시킬 때는 서버가 통과시킨 것만 통과다"라고 했다.
**replica 에서는 그 절반도 성립하지 않는다** — 지연된 값으로 앞질러 막으면 **할 수 있는 일을
못 하게 한다.** RETRY 가 가능해진 계약을 replica 가 아직 이전 상태로 들고 있으면 버튼이 꺼진다.

> 그래서 replica 값은 **켜는 데만** 쓴다. 끄는 것은 primary(쓰기 API 응답 또는 §12.5 의
> 재확인 조회)가 근거일 때만. 눌러서 `409` 를 받는 것이 눌리지 않는 것보다 낫다 —
> `409` 는 이유를 말하지만 꺼진 버튼은 아무 말도 하지 않는다.

**R3 — replica 지연을 화면이 표시한다.**
목록 상단에 `n초 전 기준`. 임계를 넘으면 목록 자체에 경고를 띄운다. 임계값은 실측이
필요하다(§13.7). **지연을 감추면 R2 의 규칙이 있어도 운영자가 화면을 신뢰한다.**

**R4 — Wizard 는 replica 를 읽지 않는다.**
`drafts → advisor-analyses → validate → publish` 는 순차 의존이다. 앞 단계 결과를 replica 로
읽으면 lag 가 낀 순간 없는 draft 를 참조한다. **Job 생성 경로는 전부 Control API 다.**

**R5 — 판정 직전에는 primary 를 다시 읽는다.**
운영자가 `FORCE_STOP`·`dq:accept` 같은 되돌리기 어려운 명령을 누르기 직전, 화면은 그 대상의
**현재 상태를 primary 에서 다시 확인**하고 확인 대화상자에 그 값을 보여 준다.
목록에서 본 값으로 확인 대화상자를 채우지 않는다.

> **그런데 그 재확인 조회가 A §17 에 없다.** `GET /v1/contracts/{id}` 가 없다. §12.6 의
> 개정 제안 (b) 가 이것이다.

### 12.3 화면별로 필요한 조회 — 그리고 어디서 오는가

| 화면 | 필요한 조회 | 경로 | A 에 있는가 |
|---|---|---|---|
| ① 열린 Hold | open hold 전체 + scope 전개 | replica | §16.2 범위 밖(contract 단위가 아니다) |
| ① 판정 대기 | `ADJUDICATION_PENDING` 계약 + verdict | replica | **있다**(contract 단위) |
| ① 사람 대기 | `DQ_FAILED`·`RECONCILIATION_REQUIRED`·`EMPTY_FULL` | replica | **있다** |
| ① Guard 거부 | **사유별 현재 건수** (§5-① 정정 — 추이는 Grafana) | replica(`guard_result`) | **있다** |
| ① Source 압력 | source 별 ρ | replica | 범위 밖 |
| ② Source 목록 | Source + ACTIVE revision + ρ + 열린 hold | replica | 범위 밖 |
| ② 연결 | ConnectionRevision 목록 + 전이 이력 | replica | 범위 밖 |
| ② Capability | `SourceCapability` (Phase 0 은 "미측정") | replica | 범위 밖 |
| ② 보호 정책 | `SourceSafetyEnvelope` + 변경 이력 | replica | 범위 밖 |
| ③ Job 목록 | Job + ACTIVE release + 파생 4항 + 최근 상태 | replica | 범위 밖(파생 4항은 §16.2 가 정의) |
| ③ Wizard 2단계 | schema/table 탐색 | **API** | **있다** — `GET /v1/sources/{id}/schemas/{schema}/tables` |
| ③ Release 이력 | release 목록 | **API** | **있다** — `GET /v1/jobs/{id}/releases` |
| ③ Wizard 13단계 | validate 결과 | **API** | **있다** — `POST …/validate` |
| ④ 실행 상세 | contract + 이력 3종 + `guard_result` + `attempt_timeline` | replica | **있다** — §16.2 가 정확히 이것 |
| ④ 재확인 | 명령 직전 계약 현재 상태 | **API 여야 한다** | **없다**(§12.6-b) |
| ⑤ Hold | open hold + effective mode + impacted Job | replica | 범위 밖 |
| ⑤ 수동 실행 5종 | 각 모드의 가능 여부 근거 | replica(켜기) + API(판정) | 부분 |
| ⑤ Backfill | plan · item | replica | 범위 밖 |
| ⑥ Template | release → **그 release 를 쓰는 Job 역참조** | replica | 범위 밖 |
| ⑦ 증거 | ledger · watermark · lease | replica | **있다**(contract 단위) |
| ⑧ 설정 | 권한·승인 정책 | — | **§9-2 미결** |

**세어 보면 §16.2 의 "contract 단위 조회"가 덮는 것은 21건 중 6건이다.** 나머지 15건 —
목록·검색·필터 전부 — 는 A 가 replica 라고도 API 라고도 말하지 않았다.

### 12.4 replica 직접 읽기가 만드는 문제 넷

**(1) read-your-writes.** R1 이 각 명령을 덮지만 **목록으로 돌아오는 경로**는 못 덮는다.
Hold 를 만들고 ⑤ 목록으로 이동하면 지연 안에서는 안 보인다. 운영자는 "안 걸렸나" 하고 또
만든다. 제약 (7) 의 insert-or-get 이 중복 row 는 막지만 **운영자의 판단은 이미 틀어졌다.**

**(2) 판정.** R2 로 규칙은 세웠지만, 그 규칙이 성립하려면 **replica 값으로 끄지 않을 버튼을
어디까지로 볼지**가 정해져야 한다. §12.6-b 의 재확인 조회가 없으면 R5 를 지킬 수 없다.

**(3) 스키마 결합.** A §6.1 은 v1.2.1 → v1.2.3.1 사이에 컬럼을 계속 늘렸다 — `rebind_count`
를 event key 에 넣고, `terminal_ingested_at`·`verdict` 를 추가하고, `lease_grant` 에 `stage`
판별자를 붙였다. **UI 가 그 테이블을 직접 SELECT 하면 A 개정마다 UI 가 깨진다.**
그리고 **A 는 자기 개정의 영향 범위에 UI 를 세지 않는다** — 그 절들은 전부 감사·판정 근거를
말하지 화면을 말하지 않는다.

**(4) 권한.** A 는 `target_lease`·`target_table` 두 테이블에만 SELECT-only role 을 규정한다.
나머지에는 없다. 그리고 replica 직접 읽기는 §9-2 의 권한 모델을 **DB role 로** 구현하게
만드는데, 화면 단위 권한("이 사람은 어느 Source 를 보는가")은 DB role 로 표현하기 어렵다.
**§9-1 과 §9-2 가 여기서 만난다** — 조회 경로를 정하지 않으면 권한 모델도 정할 수 없다.

### 12.5 제안 — `etl_ui` 조회 뷰 계층

위 (3)·(4)를 한 번에 다루는 최소 구조는 **UI 전용 read-only 뷰**다.

```text
UI  ──읽기──▶  etl_ui.*  (read replica 의 뷰)  ──▶  6.1 테이블
UI  ──쓰기──▶  Control API (primary)
PoC 기준서 §5.1 판정 쿼리 ──▶ 6.1 테이블 직접 (A §16.2 유지, 바꾸지 않는다)
```

- **6.1 개정을 뷰가 흡수한다.** 컬럼이 늘거나 이름이 바뀌어도 뷰가 계약을 유지한다
- **권한을 뷰 단위로 건다.** 테이블 전체 SELECT 를 주지 않는다
- **판정 쿼리는 건드리지 않는다** — A §16.2 의 "같은 테이블에 대해 실행한다"는 그대로다

화면에서 역으로 도출한 뷰는 일곱이다.

| 뷰 | 담는 것 | 화면 |
|---|---|---|
| `v_source_list` | Source + ACTIVE connection/credential revision + ρ + 열린 hold 수 | ② |
| `v_job_list` | Job + ACTIVE release + §16.2 파생 4항 + 최근 계약 상태 | ③ |
| `v_open_hold` | 열린 hold + scope 전개(어느 Job 을 덮는가) | ① ⑤ |
| `v_awaiting_human` | `ADJUDICATION_PENDING`·`DQ_FAILED`·`RECONCILIATION_REQUIRED`·`EMPTY_FULL` | ① |
| `v_contract_detail` | contract + 이력 3종 + `guard_result` + `attempt_timeline` | ④ |
| `v_evidence` | ledger + watermark 이력 + lease 현황 | ⑦ |
| `v_release_usage` | release → 그것을 쓰는 Job (rollback 영향 범위) | ⑥ |

**`v_open_hold` 의 scope 전개가 뷰여야 하는 이유는 성능이 아니라 일관성이다.** Hold 겹침
규칙(effective mode = `max(FORCE_STOP > DRAIN > HOLD_NEW)`, A §14.1)이 화면 코드에 흩어지면
① 대시보드와 ⑤ 운영이 서로 다른 답을 낸다. §13.4 참조.

### 12.6 A 개정 제안

**(a) §16.2 의 한 문장이 화면 전체를 덮지 못한다 — A 가 골라야 한다.**
"contract 단위 조회는 replica"만 있고 목록·검색·필터 15건은 규정이 없다(§12.3).
둘 중 하나여야 한다 — ① 조회 전부를 replica 로 명시하고 §12.5 의 뷰 계층을 §6.1 에 규정하거나,
② §17 에 조회 API 를 두거나. **UI 문서가 고를 일이 아니다** — ①은 DB 결합·권한을,
②는 Control API 부하와 구현량을 각각 떠안는다.

**(b) 최소한 이 둘은 API 여야 한다 — replica 로는 대신할 수 없다.**

| 제안 | 왜 replica 로 안 되는가 |
|---|---|
| `GET /v1/contracts/{id}` | R5 의 판정 직전 재확인. 지연된 값으로 되돌리기 어려운 명령의 확인 대화상자를 채울 수 없다 |
| `GET /v1/jobs/{id}` | 같은 이유. 그리고 R4 — Wizard 개정 경로가 replica 를 읽으면 안 된다 |

**(c) `POST /v1/holds` 응답에 영향 범위를 싣는다.**
현재 응답 형식이 A 에 없다. R1 을 지키려면 **명령 응답만으로 화면을 갱신**할 수 있어야 하고,
Hold 는 그 영향이 Job 집합이므로 응답에 `impacted_job_count` 와 effective mode 변화가 필요하다.
(A §14.2 가 해제 시 `T_catchup`·impacted 를 이미 다루므로 생성 쪽도 같은 모양이면 된다.)

### 12.7 §12 가 채우지 못한 것

| 무엇 | 왜 |
|---|---|
| 조회를 replica 로 할지 API 로 할지 | **A 가 골라야 한다**(§12.6-a). 이 선택이 §9-2 권한 모델의 형태를 정한다 |
| `etl_ui` 뷰의 컬럼 정의 | 뷰 목록은 화면에서 도출했지만 컬럼은 6.1 DDL 부록(기준서 주차 1 산출물)이 확정된 뒤에 |
| replica 지연 임계값 | R3 의 경고 임계. **실측이 답한다**(§13.7) |
| 나머지 6.1 테이블의 role | A §6.1 은 두 테이블만 규정한다 |

---

## 13. 목록 규모 — v0.1 §9-3 해소

### 13.0 규모의 실체 — 큰 것은 셋뿐이다

10,000 Job · 40,000 Run 이라는 숫자가 모든 화면에 걸리는 것은 아니다.

| 화면 | 규모 | 성질 |
|---|---|---|
| ① 대시보드 | **작다** — 열린 Hold·사람 대기 | 크면 그것 자체가 사고다(§13.5) |
| ② Source | 수십~수백 | 페이징이 사실상 필요 없다 |
| ③ Job | **10,000** | 진짜 목록 |
| ④ 실행 | **40,000/일** | 가장 큰 것. Phase 0 범위 밖 |
| ⑤ Hold | 작다 | ①과 같다 |
| ⑦ 증거 | 크지만 **키 조회 전용** — `(contract_id, attempt_no, chunk_no)` | 목록이 아니다 |

**그래서 규모 설계가 실제로 필요한 것은 ③ 과 ④ 다.** ① ⑤ 는 반대 문제 — 작아야 정상인데
커졌을 때 화면이 어떻게 행동하는가다.

### 13.1 커서 페이징만. offset 금지

이유가 둘인데 **두 번째가 더 중요하다.**

**(a) 성능** — 40,000 행에서 깊은 offset 은 건너뛸 행을 전부 스캔한다.

**(b) 정확성** — **운영 목록은 움직인다.** offset 페이징에서 1페이지를 본 뒤 2페이지로 넘어가는
사이에 앞쪽으로 행 하나가 들어오면 **항목 하나가 두 페이지 사이로 사라진다.**
운영 화면에서 그것은 "열린 Hold 하나를 못 봤다"는 뜻이다. 커서는 마지막으로 본 자리를 들고
가므로 그 자리를 잃지 않는다.

> (b) 는 성능이 충분해도 남는 문제다. offset 을 "지금은 데이터가 작으니까" 로 쓰면 안 되는
> 이유가 이것이다.

### 13.2 정확한 총 건수를 돌려주지 않는다

목록 응답은 `has_more` 만 준다. `total` 을 주지 않는다.

- 필터가 걸린 `COUNT(*)` 는 매 요청마다 돈다. 그 비용으로 화면이 하는 일은 "1 / 400 페이지"
  표시뿐이다
- **그리고 그 숫자는 돌려준 다음 순간 틀린다.** 정확하지도 않은 값을 정확한 모양으로 보여 준다

**예외 — 상한 있는 집합은 정확히 센다.** 열린 Hold·판정 대기·사람 대기는 작고,
**개수 자체가 운영 신호**다. "판정 대기 37건"은 정보이지만 "Job 10,000건"은 아니다.

### 13.3 기본 필터가 비어 있지 않다

10,000 Job 목록의 기본이 "전체"면 아무도 못 쓴다. 기본은 둘의 교집합이다.

```text
기본 = (내 부서·내 소유 Source) ∩ (최근 상태가 정상이 아닌 것)
전체 = 명시적으로 골라야 나온다
```

**그런데 "내 부서"를 화면이 알려면 §9-2 권한 모델이 있어야 한다.** 없으면 기본 필터를 정할 수
없고, 기본 필터가 없으면 10,000행 목록이 첫 화면이 된다.
**§9-1·§9-2·§9-3 이 여기서 하나로 묶인다** — 셋 중 하나만 풀 수 없다.

**v0.4 확정**(§14 결정 반영): 인가 모델을 미루기로 했으므로 "내 부서"를 화면이 알 방법이
당분간 없다. 기본 필터는 **"최근 상태가 정상이 아닌 것"만**이고 부서 필터는 수동 선택이다.
**이것은 임시 조치가 아니라 인가가 생길 때까지의 정식 동작이다**(§9.3).

### 13.4 N+1 을 만들지 않는다 — Hold 겹침은 한 번에 가져와 겹친다

Job 목록의 "열린 Hold"·"effective mode" 를 행마다 계산하면 10,000회다.

- **열린 Hold 를 한 번 전부 가져온다**(작다 — §13.0). Job 목록 API 는 행마다 hold 를 조인하지
  않는다
- 겹침은 `v_open_hold` 의 scope 전개(§12.5)를 받아 **한 곳에서** 계산한다

**이것은 성능만의 문제가 아니다.** 겹침 규칙 — effective mode = `max(FORCE_STOP > DRAIN >
HOLD_NEW)`, A §14.1 — 이 화면마다 따로 구현되면 ① 대시보드와 ⑤ 운영이 같은 Job 에 대해 다른
답을 낸다. v0.2 §5-⑤ 1항이 요구하는 **"effective mode 와 그것을 만든 Hold 목록을 함께"** 는
계산이 한 곳일 때만 지켜진다.

### 13.5 대시보드는 목록이 아니다 — 상한을 두고, 초과 자체를 경고한다

열린 Hold·판정 대기·사람 대기는 **작아야 정상**이다.

- 상한(잠정 200)을 두고 초과하면 전량 렌더하지 않는다 — "200건 이상"으로 끊고 필터로 보낸다
- **그리고 상한 초과 자체를 경고로 표시한다.** 판정 대기가 200건이면 그것이 사고다.
  화면이 그냥 페이징해 버리면 사고가 목록으로 보인다

> 대시보드에 페이징이 생기는 순간 그 화면은 목적을 잃는다. v0.2 §5-① 이 "사람이 개입해야 할
> 것만 보여 준다"고 했는데, 200건은 사람이 개입할 수 있는 양이 아니다.

### 13.6 정렬은 UTC 단조 키 + tiebreaker

커서가 안정적이려면 정렬 키가 **단조**이고 **유일**해야 한다. 시각 하나로는 동시각 행이
커서 경계에서 흔들린다 — 같은 초에 만들어진 Hold 둘이 페이지 경계에 걸리면 하나가 샌다.

- 정렬 키는 항상 **`(시각 UTC, id)` 쌍**이다
- 사용자가 고른 정렬(중요도·이름)에도 `id` tiebreaker 를 붙인다
- **§11 규칙 2 의 "정렬은 UTC 로 한다"가 여기서 구현된다.** Job 마다 `schedule.timezone` 이
  다를 수 있으므로 표시 시간대로 정렬하면 정렬 기준이 행마다 달라진다

### 13.7 규모 시험(G1)이 답할 것 — 지금 정할 수 없는 값

위 결정 중 **구조는 지금 정할 수 있고 값은 측정이 답한다.**

| 값 | 왜 지금 못 정하는가 |
|---|---|
| 대시보드 상한(잠정 200) | 실제 운영에서 열린 Hold·사람 대기가 몇 건인지 모른다 |
| 기본 페이지 크기 | 렌더 비용과 왕복 비용의 균형은 측정값이다 |
| replica 지연 임계(§12 R3) | 실측 분포가 없다 |
| Job 목록에 파생 4항을 싣는 비용 | §16.2 파생 지표는 ledger 집계다. 10,000행에 대해 얼마인지 모른다 |
| ④ 실행 목록의 보존 기간 | 40,000/일이면 화면이 덮을 구간을 정해야 한다 — A 에 보존 정책이 없다 |

**구조를 지금 정하는 이유는 나중에 못 바꾸기 때문이다.** offset → 커서 전환은 API 계약 변경이고
정렬 키에 tiebreaker 를 나중에 넣으면 저장된 커서가 전부 무효가 된다.

---

---

## 14. 인증·감사 — v0.1 §9-2 를 결정으로 닫는다

### 14.0 결정 (2026-08-30)

> 이건 우리 운영팀만 쓸 거라 **권한관리는 필요시 구현으로** 놔둬. 우선은 심플하게
> **초기 접속시 ID/PW 로 로그인**하는 걸로 간단하게 가고, 추후 **사내 SSO 를 붙여서
> 권한 관리**하도록 할 거야.

이것으로 §9-2 는 미결이 아니라 **결정**이다. 다만 **결정이 덮는 것은 절반**이고,
나머지 절반은 A 가 이미 요구하고 있어 미룰 수 없다. §14.1 이 그 구분이다.

### 14.1 미룰 수 있는 것과 없는 것 — 세 가지를 나눈다

| | 무엇 | Phase 0 |
|---|---|---|
| **인가**(authorization) | 누가 무엇을 **할 수 있는가** | **미룬다.** 운영팀만 쓰고 전원 같은 권한 |
| **인증**(authentication) | **누구인가** | **필요하다.** ID/PW |
| **감사**(audit) | 누가 **했는가** | **필요하다. A 가 요구한다** — 아래 표 |

**A 가 인가 없이도 요구하는 것 둘**

| A 요구 | 어디 | 왜 인가와 무관한가 |
|---|---|---|
| `공통 AuditEvent (actor, auth_method, source_ip, idempotency_key 필수)` | §6.1 | **`actor` 가 필수 필드다.** 권한이 없어도 누가 했는지는 남아야 한다. 그리고 **`auth_method` 가 필수라는 것은 A 가 로그인 방식의 교체를 이미 전제했다는 뜻이다** — ID/PW → SSO 가 정확히 그 교체다 |
| **`사유·승인자 필수`** — A 전체에서 **7곳** (`dq:accept` · `accept-empty` · `resolve` · `watermark:seed` · `db-identity:rotate` · connection revision `force` revoke · `accept_row_drop`) | §17 · §14.3 | **"누가 승인할 수 있는가"(인가)와 "누가 승인했는가"(감사)는 다르다.** A 가 요구하는 것은 후자다. 전자가 없어도 후자는 기록해야 한다 |

> **그래서 "권한관리는 나중에"가 "로그인 이후는 아무것도 안 한다"가 되면 안 된다.**
> 인가를 미루는 것과 감사를 빼는 것은 다른 일이다.

### 14.2 Phase 0 이 지켜야 할 것 넷

**(1) 공용 계정을 만들지 않는다. 1인 1계정이다.**

운영팀만 쓰고 권한 구분이 없어도 그렇다. `AuditEvent.actor` 가 필수인데 공용 계정이면
그 필드에 들어갈 값이 팀 이름뿐이고, 그러면 필드가 있으나 마나다.
**`FORCE_STOP` 을 누가 걸었는지 모르는 것은 권한 문제가 아니라 사고 조사 문제다.**

**(2) `auth_method` 를 처음부터 기록한다.**

Phase 0 값은 `PASSWORD`, SSO 도입 후는 그 방식. 필드를 나중에 넣으면 **전환 시점을
경계로 감사 기록이 갈라진다** — 그 이전 기록은 어떤 방식으로 로그인한 것인지 영원히
알 수 없게 된다. A 가 이 필드를 필수로 둔 이유가 그것이고, 지금 값이 하나뿐이라고
빼도 되는 필드가 아니다.

**(3) 로그인을 한 겹으로 격리한다 — 어차피 버릴 코드다.**

- 세션 확립은 **인증 미들웨어 한 곳**에서만. 화면 코드는 "현재 사용자"만 읽는다
- **화면 코드에 권한 분기를 넣지 않는다.** 지금 없기도 하지만, 더 중요한 이유는
  나중에 생길 때 그 분기가 어디 있는지 찾아야 하기 때문이다. 권한이 생기면 §12 의
  조회 계층과 Control API **양쪽**에 붙지 화면에 붙지 않는다
- 비밀번호 정책(해싱·잠금·만료·재설정)은 **표준 라이브러리 기본값을 그대로 쓴다.**
  직접 설계하지 않는다 — SSO 가 오면 통째로 버릴 것이다

**(4) 되돌리기 어려운 명령은 사유를 받는다.**

권한이 없어도 A 는 7곳에서 사유를 요구한다. 확인 대화상자가 **사유 입력**을 받고,
§12 R5(판정 직전 primary 재확인)와 한 흐름으로 묶는다.

```text
[명령 누름] → primary 재확인(§12 R5) → 현재 상태 + 영향 범위 표시 → 사유 입력 → 실행
                                                                    └ actor·auth_method·source_ip 자동
```

### 14.3 두 UI 의 인증이 갈린다 — 이건 지금 봐야 한다

**A 는 Dagster 쪽 인증을 이미 규정해 뒀다.** §4:

> 일반 사용자용 `dagster-webserver --read-only` 인스턴스와 플랫폼팀 전용 write 인스턴스를
> 분리하고, **write 인스턴스는 SSO reverse proxy 뒤에 둔다**(22장 10번).
> Dagster OSS에는 인증·RBAC·감사 로그가 없으므로 **proxy access log가 감사 기록이다.**

그리고 A 는 그 로그에 **실제로 의존한다.** Dagster UI 에서 run 을 terminate 하면 verdict
reason 이 `OPERATOR_CANCELLED` 인데, A §10.2 표가 괄호로 이렇게 적는다.

> (write 인스턴스 **proxy 감사 로그로 actor 보강**)

**그러면 Control UI 가 ID/PW 로 가는 순간 같은 사람이 두 신원을 갖는다.**

| 어디서 한 일 | 신원의 출처 |
|---|---|
| Control UI 에서 Hold 를 걸었다 | `AuditEvent.actor` — **ID/PW 계정** |
| Dagster UI 에서 run 을 끊었다 | proxy access log — **SSO 계정** |

**둘을 잇는 키가 없다.** 한 사건을 두 로그로 추적할 수 없고, `OPERATOR_CANCELLED` 의
actor 보강이 Control 쪽 계정과 이어지지 않는다.

> v0.3 §6 은 "같은 것을 두 곳에서 편집할 수 있게 만들지 않는다"고 했다. **신원은 그
> 규칙 밖에 있었다** — 편집 대상이 아니라 편집하는 사람이기 때문이다.

**셋 중 하나여야 한다.**

| 안 | 내용 | 대가 |
|---|---|---|
| **(a) ID 문자열을 사내 계정과 같게 맞춘다** *(권장)* | ID/PW 의 ID 를 사번·사내 메일 등 **SSO 가 쓸 값과 같은 문자열**로 강제 | 규율이지 강제가 아니다. 대신 **비용이 0 이고 SSO 전환 때 두 로그가 그대로 이어진다** |
| (b) Dagster write 도 같은 ID/PW 뒤에 둔다 | 인증을 하나로 만든다 | A §4·§22-10 의 SSO proxy 규정과 어긋난다 |
| (c) SSO 를 먼저 붙인다 | 문제 자체가 없어진다 | 사용자가 "추후"로 정했다 |

**(a) 를 권한다.** 지금 드는 비용이 계정 생성 규칙 한 줄이고, SSO 도입 시점에
계정 매핑표를 따로 만들지 않아도 된다. 반대로 지금 아무 ID 나 쓰게 두면 **전환할 때
과거 감사 기록을 사람이 손으로 이어 붙여야 한다.**

### 14.4 §14 가 정하지 않는 것

| 무엇 | 왜 |
|---|---|
| **인가 모델** | **사용자 결정으로 미룬다.** 필요해지는 시점은 "운영팀 아닌 사람이 이 화면을 본다"일 때이고, 그때 §12 조회 계층과 Control API 양쪽에 붙는다 |
| SSO 프로토콜·연동 방식 | 사내 표준이 정해져야 한다 |
| 계정 매핑표 | §14.3-(a) 를 택하면 필요 없고, 안 택하면 SSO 도입 시 필요하다 |
| **A §22-10 의 Dagster UI 노출 정책** | **A 자신의 미확정 항목이다** — "read-only 인스턴스 분리, write 인스턴스의 SSO proxy·mutation allowlist·감사". 이것이 확정되면 §14.3 의 선택을 재검토한다 |

---
