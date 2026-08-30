# A v1.2.3.1 개정 요청 — UI 정보구조에서 도출한 17건

- 작성일: 2026-08-30
- 대상: `etl-platform-target-architecture-v1.2.3.1.md` (이하 **A**)
- 출처: `etl-platform-ui-information-architecture.md` §9.1 (항목 8~24)
- 방법: 화면 8개가 필요로 하는 것을 세고, 각각을 A 에서 찾고, **없으면 없다고 적었다**

---

## 0. 이 문서가 무엇이고 무엇이 아닌가

**이것이다** — UI 정보구조를 쓰는 동안 A 를 정독하다 발견한 **A 자체의 공백**이다.
A 가 이미 규정한 것들 사이에 생긴 빈칸, A 안에서 서로 어긋나는 두 절, 그리고 A 가 자기
§22("채택 전에 확정할 항목")에 미뤄 둔 것 중 UI 가 지금 걸리는 것이 섞여 있다.

**이것이 아니다** — UI 요구사항 목록이 아니다. "화면을 이렇게 만들고 싶으니 A 를 바꿔
달라"가 아니라 **"A 를 그대로 구현하려는데 이 지점에서 A 가 말을 하지 않는다"** 이다.
그래서 각 건은 A 인용에서 시작한다.

**제안은 제안이다.** 특히 §1 의 **D-01** 은 UI 가 고를 수 없다 — 두 안의 대가가 UI 밖에
있기 때문이다. 나머지 제안도 "이렇게 하면 닫힌다"는 예시이지 유일한 답이 아니다.

### 각 건의 형식

```
[번호] 제목                                        등급
  A 현행 : A 가 지금 뭐라고 하는가 (인용)
  공백   : 무엇이 없는가
  결과   : 그래서 무엇이 막히는가
  제안   : 어떻게 닫는가
```

### 요약

| 등급 | 건수 | 무엇 |
|---|---|---|
| **P0** | **2** | D-01 조회 경로 선택 · **A-13 시각 필드의 저장 시간대** |
| P1 | 6 | 조회 API 3건 · Advisor 계약 2건 · role 규정 1건 |
| P2 | 6 | Advisor 나머지 3건 · §22 미확정 3건 |
| P3 | 3 | 보존 정책 · 신원 결합 · `message` 언어 |

> **P0 둘의 성격이 다르다.** D-01 은 **고르기만 하면 되는 것**이고, A-13 은
> **지금 틀렸을 수 있는 것**이다. A-13 을 먼저 읽기를 권한다.

---

## 1. 먼저 골라야 하는 것 하나 — D-01

### D-01. 운영자 조회를 read replica 로 하는가, Control API 로 하는가 (P0)

**A 현행.** §16.2 가 한 문장으로 답해 뒀다.

> contract·attempt 단위 감사·지표의 원천은 Control DB의 이력·계측 테이블(6.1)이다.
> Prometheus/Grafana로는 Job class·Source·Guard 사유 단위로 집계한 값만 내보내고,
> **contract 단위 조회는 Custom UI가 Control DB(read replica)를 읽는다.**

**공백.** 그 문장이 덮는 것은 **"contract 단위 조회"** 뿐이다. 화면이 필요로 하는 조회를
세어 보면 **21건 중 6건**이다. 나머지 15건 — Source 목록, Job 목록, 열린 Hold, Backfill
plan, Template release 역참조 등 **목록·검색·필터 전부** — 는 A 가 replica 라고도 API
라고도 말하지 않았다.

**결과.** 경로가 정해지지 않으면 그 아래 넷(A-17·A-18·A-19 와 권한 모델)의 **형태가
정해지지 않는다.** 그래서 이것이 먼저다.

**두 안과 대가.**

| 안 | 내용 | 떠안는 것 |
|---|---|---|
| **①** | 조회 전부를 replica 로 명시하고, UI 전용 read-only 뷰 계층(`etl_ui.*`)을 §6.1 에 규정한다 | **DB 스키마 결합**과 **권한을 DB role 로 거는 부담**. A §6.1 은 v1.2.1→v1.2.3.1 사이에 컬럼을 계속 늘렸고(`rebind_count`·`terminal_ingested_at`·`lease_grant.stage`), UI 가 테이블을 직접 읽으면 그 개정마다 UI 가 깨진다. 뷰 계층이 그것을 흡수하지만 뷰도 유지해야 한다 |
| **②** | §17 에 운영자 조회 API 를 둔다 | **Control API 부하와 구현량.** 목록·검색·필터·페이징을 API 로 다시 만들어야 한다 |

**UI 쪽 참고.** ① 을 택할 경우 화면에서 역으로 도출한 뷰는 일곱이다 —
`v_source_list` · `v_job_list` · `v_open_hold` · `v_awaiting_human` · `v_contract_detail`
· `v_evidence` · `v_release_usage`. (UI IA §12.5)

> **PoC 기준서 §5.1 판정 쿼리는 어느 안에서도 건드리지 않는다.** A §16.2 의
> "같은 테이블에 대해 실행한다"는 그대로 둔다.

---

## 2. P0 — 지금 틀렸을 수 있는 것

### A-13. `*_at` 필드 약 25개의 저장 타입·시간대가 선언되지 않았다 (P0)

**이 건은 UI 문제가 아니다.** UI 없이도 A 의 계산식이 성립하지 않을 수 있다.

**A 현행.** A 는 시간 필드를 25개 넘게 이름으로 규정하면서 **타입이나 시간대를 선언한
곳이 한 군데뿐**이다. §13.4:

> `execution_contract.window_range numrange NOT NULL`(`STANDBY_VISIBLE_SCN`은 SCN 정수,
> **`APPLICATION_TIMESTAMP`는 UTC epoch 마이크로초로 정규화**, `window_kind` 병기)

그리고 §9.2 의 제약 이름에 접미사가 하나 있다.

> `UNIQUE (job_id, operation_class, logical_scheduled_at_utc)`

**그게 전부다.** `TIMESTAMP WITH TIME ZONE` 은 A 전체에서 **0회**다. `logical_scheduled_at`
은 다른 여섯 곳에서 접미사 없이 쓰인다.

**공백.** 아래 필드들의 저장 타입·시간대가 선언돼 있지 않다.

`created_at` · `next_eligible_at` · `expires_at` · `submitted_at` · `first_guard_ok_at` ·
`last_cas_at` · `finalized_at` · `fence_ts` · `deployed_at` · `effective_from` ·
`reattach_deadline` · `terminal_ingested_at` · 이력 3종의 `at` · `guard_result.run_started_at`
· `attempt_timeline.t0`~`t7` · `cas_at` · `last_reconcile_at` · `verified_at` ·
`cap_plus_one_verified_at` · `released_at` · `hold_release_at` · `publish_at` ·
`last_expected_checked_at` · `submission_in_flight.requested_at`

**결과 — 세 시계를 선언 없이 뺀다.** A 는 시계가 셋이라는 것을 **알고 있다.** §12.2 가
명시적으로 경고한다.

> `fence_ts`는 standby wall-clock이므로 primary 시계로 찍히는 `UPDATE_DT`의 하한으로
> 쓰지 않는다

그런데 **그 경고는 `fence_ts` 하나에만 붙어 있고 나머지 24개는 무방비다.** 그리고 A 는
이 필드들을 실제로 **뺀다**.

| 계산 | 어디 | 피연산자의 시계 |
|---|---|---|
| `lateness = t7 − logical_scheduled_at` | §16.4 | Control 호스트 − cron 유래 |
| `now() − logical_scheduled_at > freshness_slo` | §16.4 | Control 호스트 − cron 유래 |
| `now() + 예상 chunk 소요 > fence_ts + undo_retention × 0.5` | §10.2 | Control 호스트 + **standby** |
| `now() − max(coalesce(cas_at, finalized_at))` | §16.2 | Control 호스트 − Control 호스트 |

PostgreSQL 에서 `now()` 는 `timestamptz` 다. 컬럼이 naive `timestamp` 면 **세션
`TimeZone` 이 무엇을 저장할지 정하고**, Control API·scheduler·Run Pod 가 서로 다른 TZ 로
뜨면 위 비교가 조용히 틀린다. `freshness breach` 가 9시간 일찍/늦게 뜨고,
undo deadline 검사는 **fence 만료를 놓친다** — 그건 데이터 문제다.

> A 는 원천 세션에는 시간대를 못박아 뒀다(§11.3 — `TIME_ZONE = '+00:00'`, canonical row
> hash 재현성 때문). **Control DB 쪽에는 같은 규정이 없다.**

**제안.**

1. §6.1 에 한 줄을 넣는다 — **"모든 `*_at`·`*_ts` 컬럼은 `TIMESTAMP WITH TIME ZONE`
   (`timestamptz`)이고 UTC 로 저장한다. 예외는 `window_range`(§13.4 가 규정) 뿐이다."**
2. `fence_ts` 는 예외를 **명시**한다 — 값 자체는 standby 시계에서 온 순간이므로
   `timestamptz` 로 저장하되 **"다른 시계에서 온 값"** 임을 필드 설명에 남긴다
   (§12.2 의 경고를 §6.1 로 끌어올린다).
3. Control 프로세스(API·scheduler·Run Pod)의 **세션 `TimeZone` 을 `UTC` 로 고정**하는 것을
   배포 요건에 넣는다(§5.4 또는 §22-22 config 표).

**연쇄.** 이것이 정해지면 UI IA §11(시각 표시)의 규칙 4가 확정되고, A-14·A-15 의 성격도
분명해진다.

---

## 3. P1 — 조회 API 3건

**셋 다 D-01 이 정해진 뒤 형태가 결정된다.** 다만 A-17 은 D-01 의 어느 안에서도 API 여야
한다.

### A-17. 판정 직전 재확인 조회가 없다 (P1)

**A 현행.** §17 의 `GET` 은 셋뿐이다 —
`/v1/sources/{id}/schemas/{schema}/tables` · `/v1/jobs/{id}/releases` ·
`/v1/operations/{operation_id}`. 계약·Job 자체를 읽는 `GET` 이 없다.

**공백.** `GET /v1/contracts/{id}` · `GET /v1/jobs/{id}` 가 없다.

**결과.** 운영자가 **되돌리기 어려운 명령**(`FORCE_STOP`·`dq:accept`·`accept-empty`·
`resolve`·`force revoke`)을 누르기 직전, 화면이 대상의 현재 상태를 확인해 확인
대화상자에 보여 줘야 한다. **replica 로는 대신할 수 없다** — 지연된 값으로 확인
대화상자를 채우면 사람이 없는 상태를 보고 결정한다.

같은 이유로 Job Wizard 의 순차 경로(`drafts → advisor-analyses → validate → publish`)도
replica 를 읽을 수 없다. lag 가 낀 순간 없는 draft 를 참조한다.

**제안.** §17 에 두 줄을 더한다.

```text
GET    /v1/contracts/{id}     # 판정 직전 재확인. 200 {contract_state, current_attempt, …}
GET    /v1/jobs/{id}          # 같은 목적 + Wizard 개정 경로
```

### A-18. `POST /v1/holds` 의 응답 형식이 없다 (P1)

**A 현행.** §17 은 경로만 적었다 — `POST /v1/holds`. 같은 블록의 이웃 엔드포인트들은
인라인 주석으로 응답과 상태코드를 규정하는데 이것만 없다.

**공백.** 응답 본문이 규정돼 있지 않다.

**결과.** replica 지연 때문에, 명령을 보낸 화면은 **그 명령의 응답만으로** 갱신되어야
한다(UI IA §12 R1). Hold 는 영향이 Job 집합이므로 응답에 영향 범위가 없으면 화면이
"몇 개 Job 이 멈췄는가"를 replica 에 다시 물어야 하고, 그러면 방금 한 일이 안 보인다.

**제안.** A §14.2 가 **해제** 쪽에서 이미 `T_catchup`·impacted 를 다루므로 생성 쪽도 같은
모양으로 맞춘다.

```text
POST /v1/holds   # 201 {hold_id, scope_kind, scope_key, mode, reason,
                 #      impacted_job_count, effective_mode_changed_job_count}
```

### A-19. `target_lease`·`target_table` 밖 테이블의 role 규정이 없다 (P1)

**A 현행.** §6.1 제약 (9) 가 두 테이블에만 규정한다.

> `target_lease`·`target_table`의 **DML 권한은 Control API role에만** 부여하고
> **운영·분석용 role은 SELECT만** 갖는다(raw session이 lease row를 직접 넣는 경로 차단)

**공백.** 나머지 6.1 테이블(계약·attempt·이력 3종·`guard_result`·`attempt_timeline`·
ledger·watermark·hold·source·job·release)에 대한 role 규정이 없다.

**결과.** D-01 에서 ① 을 택하면 UI 가 replica 를 직접 읽는데, **권한을 걸 근거가 A 에
없다.** ② 를 택하면 이 건은 대체로 사라진다 — **그래서 D-01 뒤에 다뤄야 한다.**

**제안(① 을 택할 경우).** 제약 (9) 의 원칙을 일반화한다 — "**6.1 의 모든 테이블에 대해
DML 권한은 Control API role 에만 부여하고, 운영·분석·UI role 은 SELECT 만 갖는다.**
UI 는 테이블이 아니라 `etl_ui.*` 뷰에 대해서만 SELECT 를 갖는다."

---

## 4. P1 — Advisor 계약 2건

**A §15 는 안전 구조를 규정하면서 그것을 담을 자료 구조를 규정하지 않았다.** 다섯 건이
한 덩어리이고 그중 둘이 P1 이다.

> **급하지 않다는 것은 A 자신이 말한다** — §15.4 마지막 줄: "Advisor는 실행 경로의 필수
> 구성요소가 아니며 MVP 안정화 후 도입한다." 그래서 P1 이고 P0 이 아니다. 다만
> **A-11 은 성격이 다르다**(아래).

### A-11. Advisor 경로에만 `사유·승인자 필수` 가 없다 (P1)

**A 현행.** A 는 운영자 재정의마다 사유와 승인자를 요구한다 — §17 에서 `dq:accept` ·
`accept-empty` · `resolve` · `watermark:seed` · `db-identity:rotate` ·
connection revision `force` revoke · `accept_row_drop` **일곱 곳**이 `사유·승인자 필수`
또는 `승인자 기록`을 명시한다.

그리고 §15.3 은 Advisor 에 대해 이렇게만 적는다.

> - 추천값과 운영자 수정값을 모두 audit

**공백.** 저장처·필드·승인자 요구가 없다. **A 의 다른 모든 운영자 재정의가 갖는
`사유·승인자 필수` 구절이 Advisor 경로에만 없다.**

**결과.** 이 비대칭이 **의도인지 누락인지 A 가 답해야 한다.** 의도라면(추천 수락은
운영자 재정의가 아니라 입력이므로 승인자가 불필요하다) 그 근거를 A 에 한 줄 남기면
되고, 누락이라면 다른 일곱과 같은 형식이 필요하다.

> UI 는 어느 쪽이든 **수락·거절·무시를 기록**하도록 설계했다(UI IA §10.4) — §15.4 2단계의
> "운영자 수정률 측정"이 그것을 요구하기 때문이다. 승인자 요구 여부만 A 에 달렸다.

### A-9. `AdvisorAnalysis` → Job 의 키가 없다 (P1)

**A 현행.** §6.1 객체 목록에 이름만 있다.

> - `AdvisorAnalysis`

이 객체는 `Job` 하위가 아니라 **최상위 형제**로 놓여 있다(같은 목록의 `Job` 은
`JobDraft`·`JobSpecVersion` 만 하위로 갖는다). Job 과의 연결은 §17 의 경로 파라미터
`POST /v1/jobs/{id}/advisor-analyses` 의 `{id}` 뿐이다.

**공백.** `AdvisorAnalysis` 에서 Job·draft·JobSpecVersion 으로 가는 키가 선언돼 있지 않다.

**결과.** **draft 가 수정된 뒤 기존 추천이 낡았다는 것을 지금 A 로는 말할 수 없다.**
운영자가 2단계에서 테이블을 바꾸고 3단계로 돌아왔을 때, 화면이 "이 추천은 수정 전
draft 기준입니다"라고 쓰려면 그 키가 있어야 한다. 없으면 **낡은 추천이 새 draft 의
추천처럼 보인다.**

**제안.** `AdvisorAnalysis` 를 `Job` 하위로 옮기고 **draft 판본에 묶는다** —
`(job_id, draft_revision)` 또는 `job_spec_digest`. 어느 쪽이든 "이 추천이 어느 입력에
대한 것인가"가 값으로 남아야 한다.

---

## 5. P2 — Advisor 나머지 3건

### A-8. §15.3 이 저장을 요구하는 필드의 이름이 하나도 없다 (P2)

**A 현행.** §15.3:

> - JSON Schema output 강제
> - **confidence와 근거 metadata ID 제공**
> - **model/prompt/rule/metadata 버전 저장**

**공백.** JSON Schema 자체, `confidence` 의 필드명과 척도, "근거 metadata ID" 의 필드명,
버전 4종의 필드명 — **하나도 없다.**

**결과.** 화면은 이 다섯을 추천 카드에 그대로 올려야 한다(UI IA §10.2 — 저장만 하고
안 보여 주면 운영자가 근거 없이 판단한다). **어느 필드에서 읽을지 정해지지 않았다.**

**제안.** §15.3 에 `AdvisorRecommendation` 의 최소 필드를 규정한다 —
`target_field`(JobSpec 키) · `value` · `confidence` · `evidence_ids[]` ·
`versions{model, prompt, rule, metadata}` · `evidence_kind`(§10.3 의 prompt injection
격리를 위해 근거가 comment 유래인지 표시).

### A-10. `POST /v1/jobs/{id}/advisor-analyses` 의 본문·응답·상태코드가 없다 (P2)

**A 현행.** §17 에 경로 한 줄만 있다. **같은 블록의 이웃들은 전부 인라인 주석으로 응답과
상태코드를 규정한다** — `validate` 는 `200 {valid, warnings[]} | 422 …`,
`publish` 는 `202 + operation_id`, `runs` 는 `{launch_result}`.

**공백.** 요청 본문·응답 본문·상태코드·Advisor 장애 시의 응답.

**결과.** §7.2-3 이 "**Advisor 장애 시 건너뜀**"을 요구하는데, **장애가 어떤 응답으로
오는지가 없다.** 화면이 그것을 오류가 아니라 정보로 표시하려면(UI IA §10.6) 구분할 수
있어야 한다 — `503` 인지 `200 {available: false}` 인지.

**제안.** 최소한 이 둘을 규정한다 — 정상 `200 {analysis_id, recommendations[]}`,
Advisor 불가 `200 {analysis_id: null, available: false, reason}`.
**`5xx` 로 두지 않는 것을 권한다** — 선택 단계의 장애를 오류로 만들면 클라이언트가
재시도하게 되고, §15.2 의 저부하 요건과 어긋난다.

### A-12. §16.1 과 §7.2 가 어긋난다 — Advisor 는 화면인가 단계인가 (P2)

**A 현행 — 두 절이 다르게 말한다.**

§16.1 (화면 역할):
> Custom UI: Source, Job Wizard/CRUD, Template/Release, Hold, Backfill plan, **Advisor**,
> **contract 상세**(…)

여기서 `Advisor` 는 `Source`·`Job Wizard/CRUD` 와 **나란한 항목**이다 — 독립 화면을 시사한다.

§7.2 (Job Wizard 13단계):
> 3. LLM/Rule Advisor 추천 확인 (선택 단계 — Advisor 장애 시 건너뜀)

여기서는 **Wizard 의 한 단계**다.

**결과.** UI 는 Wizard 단계로 읽었다. 독립 화면이 맞다면 그 화면의 목적이 따로 있다는
뜻인데(예: §15.4 2단계의 Shadow 평가 결과 — 안전하지 않은 추천률·운영자 수정률 조회)
**A 는 그 목적을 말하지 않는다.**

**제안.** 둘 중 하나로 통일한다. 독립 화면이 의도였다면 §15.4 의 **Shadow 평가 지표를
누가 어디서 보는가**(이 UI 인지 Grafana 인지)를 함께 규정해야 한다 — 그것도 지금 A 에 없다.

---

## 6. P2 — A 자신의 §22 미확정 3건

**셋 다 A 가 "채택 전에 확정할 항목"에 이미 올려 둔 것이다.** 새로 발견한 결함이 아니라
**UI 가 지금 그것에 걸린다는 보고**다.

| # | A §22 | 현행 문구 | UI 가 걸리는 지점 |
|---|---|---|---|
| **A-14** | **12번** | `전사 timestamp 타입(NTZ vs timestamptz)과 타입 매핑표 확정` | 화면이 watermark 값에 시간대 라벨을 붙일 수 있는가. NTZ 면 **시간대가 없으므로** 붙이면 안 된다(UI IA §11 규칙 4). §7.3 의 `timestamp_type: TIMESTAMP_NTZ` 도 "잠정 기본"이라고 스스로 적는다 |
| **A-15** | **4번** | 항목 끝의 `DB 시간대` | `SourceCapability` 목록에서 **이 항목만 필드 이름이 없다**(이웃은 전부 snake_case 로 명명돼 있다). 이름이 없으면 등록도 조회도 표시도 할 수 없다. **G0-0B1 의 `Preamble` 이 한때 `TIME_ZONE = DBTIMEZONE` 을 쓴 것도 이 공백과 무관하지 않다** — A 가 등록하지 않는 값을 세션에 걸고 있었다(2026-08-30 `'+00:00'` 으로 정정) |
| **A-22** | **10번** | `Dagster UI 노출 정책: read-only 인스턴스 분리, write 인스턴스의 SSO proxy·mutation allowlist·감사` | 이것이 확정돼야 두 UI 의 신원 결합(A-21)을 어떻게 할지 정해진다 |
| **A-23** | **13번** | `Control API 역할 모델·Run Pod 신원. 2인 승인/SoD는 정책 선택사항` | 인가 모델. **다만 마지막 구절이 "인가를 미룬다"는 이번 결정과 어긋나지 않는다는 근거이므로, 지금 급하지 않다** |

> A-15 는 등급을 올려도 좋다. **필드 이름 하나 정하는 일**인데 그것이 없어서 원천 세션
> 시간대를 등록·검증할 수 없고, 실제로 코드가 한 번 어긋났다.

---

## 7. P3 — 나머지 3건

### A-20. 실행 이력의 보존 기간 정책이 없다 (P3)

**A 현행.** Iceberg snapshot retention 하한은 규정한다(§18 — `≥ RPO + RTO + margin`).
**Control DB 의 계약·attempt·이력 보존은 규정이 없다.**

**결과.** Run 40,000건/일이면 이력 3종·`guard_result`·`attempt_timeline` 이 그 배수로
쌓인다. 화면이 덮을 구간(④ 실행 목록)과 판정 쿼리가 볼 구간을 정하려면 보존 정책이 필요하다.

**제안.** §18 또는 §22 에 항목을 세운다. **UI 요구가 아니라 운영 요구다** — PoC 기준서
§5.1 판정 쿼리도 같은 테이블을 읽는다.

### A-21. Control UI 신원과 Dagster proxy 신원을 잇는 키가 없다 (P3 — 규율로 막았다)

**A 현행.** §4 는 Dagster write 인스턴스를 SSO reverse proxy 뒤에 두고
"**proxy access log가 감사 기록이다**"라고 한다. 그리고 §10.2 표가 그 로그에 실제로
의존한다 — `OPERATOR_CANCELLED` 는 "(write 인스턴스 **proxy 감사 로그로 actor 보강**)".
한편 §6.1 은 `공통 AuditEvent (actor, auth_method, source_ip, idempotency_key 필수)` 다.

**공백.** **두 actor 가 같은 사람이라는 것을 말할 키가 없다.**

**결과.** 한 사건을 두 로그로 추적할 수 없다.

**제안 — A 개정 없이 막았다.** Control UI 계정 ID 를 **사번**으로 강제하고
`AuditEvent.actor` 에 그 문자열을 그대로 넣기로 했다(UI IA §14.3). **A 개정은 불필요하되,
규정으로 승격하면 더 안전하다.** 그리고 이 규율이 성립하려면 확인이 셋 남아 있다 —
사내 IdP 가 사번을 claim 으로 주는가 / proxy access log 에 사번을 남길 수 있는가 /
사내 인사 규칙이 사번을 재사용하는가(UI IA §14.4). **셋 다 A 가 아니라 사내 확인 사항이다.**

### A-24. `violations[].message` 의 언어가 규정돼 있지 않다 (P3)

**A 현행.** §17:
> `422 VALIDATION_FAILED {violations: [{rule_id, field, actual, computed_minimum, inputs,
> **message**}], warnings: […]}`

**공백.** `message` 의 언어.

**결과.** UI 는 이 `message` 를 **그대로 보여 준다**(UI IA §3.4 — 문구를 지어내 rule
이름을 감추지 않는다). API 가 영문 message 를 주면 한국어 화면에 영문 문장이 섞이는데
**UI 가 고칠 수 없다.** `rule_id` 는 원문 유지가 규칙이지만(UI IA §11.4) `message` 는
설명 문장이라 다르다.

**제안.** §17 에 한 줄 — "`message` 는 한국어 설명 문장이고 `rule_id`·`field` 는 식별자
원문이다." 또는 구현 규약으로 고정한다. **경미하다.**

---

## 8. 처리 순서 제안

```
1. D-01 을 고른다                     ← 이것이 A-17·A-18·A-19 와 권한 모델의 형태를 정한다
2. A-13 을 닫는다                     ← 지금 틀렸을 수 있다. UI 와 무관하게
3. A-15(필드 이름 하나) · A-24(한 줄)  ← 값싸고 즉시 닫힌다
4. A-17 · A-18                        ← D-01 뒤. 어느 안에서도 API 다
5. A-19                               ← D-01 이 ① 이면 필요, ② 면 대체로 소멸
6. A-11                               ← 비대칭이 의도인지 답만 하면 된다
7. Advisor 나머지(A-8·A-9·A-10·A-12)  ← A §15.4 대로 MVP 안정화 후
8. A-20 · A-22 · A-23                 ← §22 확정 일정에 맡긴다
```

> **1·2 를 제외하면 전부 급하지 않다.** 그리고 **2 는 A 개정이 아니라 정정에 가깝다** —
> A 가 이미 세 시계를 알고 있으면서 한 곳에만 경고를 붙여 둔 것이므로.

---

## 9. 이 요청서가 A 에 요구하지 않는 것

- **화면 설계를 A 에 넣어 달라고 하지 않는다.** 화면 규칙은 UI 문서가 갖는다
- **capability 축을 확정해 달라고 하지 않는다.** 8차 리뷰가 동결을 NO-GO 로 판정했고
  G0-0 실측이 답할 문제다(UI IA §3.2 가 Phase 0 을 이미 막아 뒀다)
- **인가 모델을 지금 규정해 달라고 하지 않는다.** 운영팀 전용이라 미루기로 했고,
  A §22-13 의 "2인 승인/SoD는 정책 선택사항"이 그것과 어긋나지 않는다
- **§16.2 를 뒤집어 달라고 하지 않는다.** D-01 은 그 문장을 **넓히거나 대체하는** 선택이지
  틀렸다는 지적이 아니다

---

## 10. 근거 문서

| 무엇 | 어디 |
|---|---|
| 각 건의 도출 과정 | `etl-platform-ui-information-architecture.md` §9.1 |
| 화면 21건의 조회 표 · 경로 규칙 · 뷰 계층 | 같은 문서 §12 |
| 시각 규칙과 세 시계 | 같은 문서 §11 |
| Advisor 화면 규칙 | 같은 문서 §10 |
| 인증·감사와 사번 결정 | 같은 문서 §14 |
