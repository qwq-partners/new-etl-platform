# 신규 ETL Platform 목표 아키텍처 v1 — 상세 리뷰 (리뷰 v1.1)

- 리뷰 대상: `etl-platform-target-architecture-v1.md` (2026-08-22, PoC 승인 후보안, 977줄)
- 리뷰 일자: 2026-08-22
- 리뷰 방법: 7개 관점(Dagster OSS 정확성 · Oracle/Source 보호 · Iceberg/Spark/Polaris · 분산 일관성/멱등성 · 규모/용량/SLO · 운영/보안/이관 · 문서 정합성) 독립 리뷰 → 관점별 반박 검증(초안 재대조 + 공식 문서 확인) → 누락 점검 → 재검증. 초안이 이미 다루는 항목은 기각 또는 하향했고, 외부 시스템 동작은 가능한 한 공식 문서·소스로 확인해 표시했다.
- 결과: 발견 97건 — **P0 2건, P1 37건, P2 37건, P3 21건** (반박 검증에서 기각 0건, 하향 37건)
- 개정: **v1.1 (2026-08-23)** — GPT 교차 리뷰 반영. P1을 실행 시점 3분류(§1.4)로 재편, release 고정 규칙(CON-01), `SourceVisibilityFence`(ORA-01), 조건부 증분 허용(ORA-01), batch 호출의 정합성/성능 분리(A-1), WAP 범위 축소(B-1), 정각 schedule 유지·admission queue(C-5), SoD 정책화(E-2), lease 3단계(ICE-05), `ORA-03172` 표기 정정. 리뷰 v1.0 본문의 판정·근거는 유지.

---

## 0. 한 줄 결론

**방향은 맞다. 그러나 초안이 내세운 핵심 보장 두 가지(“정상 Run 중복/누락 0”, “Watermark gap 0”)를 초안 자신의 규칙이 깨뜨리는 결함이 2건(P0) 있다. P1 37건은 한 덩어리가 아니다 — **v1.1에 먼저 반영할 설계 결정 / PoC에서 답을 얻을 가설 / 운영 전 보강** 세 부류로 나뉘며(§1.4), 첫 부류를 반영한 v1.1 없이 PoC 코드에 들어가면 PoC가 잘못된 의미론을 검증하게 된다.** Dagster-first + 얇은 Control Plane, 권위 저장소 분리, Iceberg snapshot 기반 retry 판정이라는 뼈대는 유지할 가치가 있다.

---

## 1. 총평

### 1.1 판정

**조건부 승인(PoC 진입 가능) — 단, 아래 선행 조건 충족 후.**

1. P0 2건(§4)을 설계에 반영한다 — 둘 다 한 문단짜리 규칙 변경이지만 멱등성·정합성 보장의 근간이다.
2. §1.4의 “① v1.1 선반영” 항목을 초안 v1.1에 반영한다. 이 항목들은 PoC의 fault injection 합격 기준이 의존하는 의미론(키·상태·복구·evidence)이라, 빠진 채 PoC를 돌리면 PoC가 틀린 설계를 검증하게 된다.
3. “② PoC에서 판정” 항목은 PoC 시험·합격 기준서에 가설로 싣고, “③ 운영 전 보강” 항목은 MVP가 실제 Oracle DR에 붙기 전(Phase 2 진입 전)까지 확정한다. **P1 37건을 모두 구현한 뒤 PoC를 시작할 필요는 없다.**

### 1.2 핵심 메시지

1. **멱등 키가 멱등하지 않다.** `job_spec_digest`가 occurrence 키에 들어가 있어 republish 직후 같은 시각의 occurrence가 둘 생긴다(CON-01). digest는 occurrence의 “정체성”이 아니라 contract의 “고정 속성”이다.
2. **Watermark high가 standby의 적용 시점에 묶여 있지 않다.** lag 구간의 row는 이번 window에도, 다음 window에도 없다(ORA-01). overlap PT5M은 lag가 5분을 넘는 순간 무력해진다.
3. **Dagster 동작 전제 중 세 가지가 틀렸다.** (a) 비파티션 grouped schedule에는 catch-up 설정이 없다 — 놓친 tick은 최신 1개만 재생된다(DAG-01). (b) in-process executor에서는 Run Pod가 죽으면 resume이 없다 — “재접속”의 주체가 정의되지 않았다(DAG-04). (c) schedule 평가 안에서 Control API를 500회 호출하는 구조는 기본 gRPC timeout 60초·tick 무재시도와 충돌한다(DAG-02).
4. **Control Plane은 “얇다”고 선언했지만 12개 Aggregate·lease manager·outbox·bundle 파이프라인·Guard·Reconciler·Wizard UI를 가진 본격 시스템이다.** 경계 선언 자체는 옳지만 Dagster 이벤트 → contract 전이 매핑, read model 갱신 경로, 상태 종결(COALESCED/SKIPPED/CANCELLED)이 없어 구현자가 결정을 대신하게 된다(CON-03, CON-06, DAG-03, DOC-02).
5. **Source 보호는 한 겹이다.** Control DB lease는 회계상 회수일 뿐 Oracle 세션을 끝내지 못하며(CON-05), DB 측 강제(Profile·Resource Manager·standby 전용 service)가 없어 “절대 한도 단 한 번도 초과 금지”를 보장도 측정도 할 수 없다(ORA-04). switchover가 일어나면 같은 descriptor가 primary를 가리키는데 이를 감지하지 않는다(ORA-10).
6. **PoC 합격 기준이 측정 불가능한 곳이 있다.** 플랫폼 내부 단계 SLO만 있고 Job별 freshness/lateness가 없으며(SCL-04), Source 용량(ΣD/C ≤ period) 검산이 없고(SCL-01), stub Source 요건이 없어 한도 초과·lease·Polaris commit 경합이 측정되지 않는다(SCL-10).
7. **첫 달 운영에 필요한 것이 MVP에 빠져 있다.** REPLAY/단일 Job Backfill/RERUN_LATEST, schema drift 검출, DQ 정의, 신규 Job 초기 적재, 타입·시간대 매핑 규범, 기존 Airflow Job 이관·watermark 인계 절차(OPS-04/05/06/13, CRT-05/06).
8. **Dagster OSS에는 인증·RBAC·감사가 없다.** Guard는 Spark submit만 막는다. schedule off·run terminate/delete·pool 편집은 누구나 할 수 있고 기록이 남지 않는다(OPS-01). `dagster-webserver --read-only`로 일반 사용자 인스턴스를 분리하는 것이 현실적 해법이다.

### 1.3 심각도 기준

| 등급 | 의미 | 건수 |
|---|---|---|
| **P0** | 데이터 손실/중복/장애를 직접 유발하거나 초안의 핵심 주장을 무효화 | 2 |
| **P1** | PoC 전에 결정이 필요한 설계 항목 — 실행 시점은 §1.4의 3분류를 따름 | 37 |
| **P2** | MVP(실 Source 접속) 전에 확정해야 함 | 37 |
| **P3** | 문서 정리·용어·근거 보강 | 21 |

ID 접두어: CON=분산 일관성, DAG=Dagster, ORA=Oracle, ICE=Iceberg/Spark, SCL=규모, OPS=운영/보안, DOC=문서, CRT=누락 점검.

### 1.4 실행 시점 3분류 — P0·P1 전체와 관련 P2

등급(P0~P3)은 *중요도*이고, 아래는 *언제 답해야 하는가*다. GPT 교차 리뷰의 분류를 채택했다.

**① v1.1 선반영 — PoC 코드 전에 설계로 확정**

| 항목 | finding |
|---|---|
| Occurrence–Contract–Attempt 3계층, NORMAL key, release 고정 규칙 | CON-01, DOC-03, CON-03, CON-12 |
| `SourceVisibilityFence`(cutoff 3종)와 보증 등급의 계약 노출 | ORA-01, ORA-02 |
| resume을 전제하지 않는 복구 5단계, `WRITER_FENCED`, attempt별 SA 이름 | CON-02, DAG-04, ICE-06 |
| Full 0건 기본 실패, `FULL_STATIC_REPLACE` / `PARTITION_REPLACE` 분리 | ICE-02 |
| commit evidence ledger(contract / attempt / chunk / base·committed snapshot)를 Control DB에 영속화 | ICE-01(증거 분리), ICE-04, CON-08 |
| occurrence disposition, RETRY stale 규칙, overrun coalesce | CON-04, CON-06, CRT-07 |
| grouped schedule 결정론적 이름 + `default_status=RUNNING` | DAG-06 |
| Dagster run 종료 사실의 Control 반입(`run_status_sensor`), `max_tick_retries`, PLANNED 고아 탐지 | DAG-03, DAG-02(정합성 부분), DOC-01 |
| Guard 거부·lease 대기 시 Run 결과 표 | CRT-04 |
| 타입·시간대·문자집합 표준 | CRT-06 |
| 초기 적재 모드 | CRT-05 |
| 접속 직후 `DATABASE_ROLE` / `OPEN_MODE` 검증 | ORA-10(Guard 검사) |
| 9.3 catch-up 문구 정정 | DAG-01 |
| MVP 범위에 REPLAY / 단일 Backfill / RERUN_LATEST 명시 | OPS-13 |

**② PoC에서 판정 — 가설로 시험**

| 가설 | finding |
|---|---|
| 500 RunRequest tick의 wall time과 gRPC timeout — batch endpoint 필요 여부 | DAG-02(성능 부분), SCL-05 |
| 1시간 이상 daemon 장애 후 coalesced recovery | DAG-01, SCL-12 |
| DR lag + long transaction 누락 시험 | ORA-01, ORA-02 |
| Run Pod 사망 후 Spark 재접속 | CON-02, DAG-04 |
| ambiguous Iceberg commit(REST 5xx 포함) | CON-08, ICE-12 |
| branch/WAP와 Iceberg·Polaris 버전 호환(Critical 한정) | ICE-03 |
| compaction 동시성(lease 3단계) | ICE-05 |
| Full snapshot 보존 용량 | ICE-01(용량), SCL-09 |
| lease 회수 vs `V$SESSION` 실측, weight 계수 | CON-05, ORA-03 |
| Run Pod 자원·run coordinator 튜닝·Spark Operator 처리량 | SCL-02, DAG-08, CRT-10 |
| Source 용량 모델 계수(D 분포, ρ 임계) | ORA-06, SCL-01 |
| Job별 freshness SLO 수치 | SCL-04 |
| Bundle 전달 방식(이미지 vs 외부 저장소) 비교 | CRT-01 |

**③ 운영(Phase 2) 전 보강**

| 항목 | finding |
|---|---|
| Dagster UI read-only 분리·SSO proxy·감사 | OPS-01 |
| Control API 역할·Run Pod 신원(2인 승인/SoD는 정책 선택) | OPS-02 |
| 자격증명 주입·마스킹·rotation·Polaris principal | OPS-03 |
| schema drift 검출·차단 | OPS-04 |
| DQ 정의와 실패 의미 | OPS-05 |
| 기존 Job 이관·watermark seed·cutover/롤백 | OPS-06 |
| Control PostgreSQL HA/DR, Polaris/AIStor 장애 동작 | OPS-09 |
| DB 측 강제(Profile, Resource Manager, standby 전용 service) | ORA-04, ORA-10(service) |

---

## 2. 잘 설계된 점

리뷰 관점 7개가 공통으로 인정한 강점이다. 수정 과정에서 이 결정들은 지켜야 한다.

- **권위 저장소 분리(4장)**: 의도(Registry) · 실행(Dagster) · 데이터 commit(Iceberg snapshot)을 나누고 Control DB에 Dagster 상태 머신을 복제하지 않겠다는 경계는 HAflow 2.0화를 막는 올바른 기준점이다.
- **성공의 정의(13.1)와 원칙 3**: SparkApplication `COMPLETED`가 아니라 Iceberg snapshot summary의 contract metadata를 최종 증거로 삼고, 불확실하면 `RECONCILIATION_REQUIRED`로 멈춘다. Iceberg `CommitStateUnknownException`의 공식 지침과 정확히 부합한다.
- **Dagster 제약을 정확히 인용**: Daemon 단일 replica·Code Location 단일 replica(5.1), run_key의 tick 범위 dedup(9.1), `free_slots_after_run_end_seconds`(11.2), state-backed component의 “reload는 최신 state”(8.1), GraphQL API evolving(17) — 모두 공식 문서와 일치한다.
- **Source 보호 계층(11.1)**: Platform hard limit > SourceSafetyEnvelope > JobReadProfile > LLM, “LLM은 Source 전체 한도를 바꿀 수 없다”, Primary 자동 fallback 금지, DR lag 신호 없으면 최신성을 추정하지 않음(11.3).
- **extract-once staging(11.5)**: Target 단계 실패 시 생산 DB 재조회를 피하고, Shadow 기간에도 Oracle 이중 읽기를 막는다(Phase 2).
- **Hold 해제 coalesce(원칙 6, 14.2)**: Full은 최신 1회, Incremental은 논리적 1회 + chunk/cooldown — catch-up 폭주를 데이터 의미로 줄인다.
- **Spark Operator restartPolicy Never(10.1)**: Operator 자동 재제출이 ambiguous commit을 무시하고 재실행하는 경로를 원천 차단한다.
- **PoC 설계(19장)**: synthetic Source 분리 측정, fault injection 14종, 잠정 SLO, 즉시 No-Go — 검증 가능한 형태다. 22장의 미결 항목을 솔직하게 드러낸 점도 좋다.

---

## 3. 리뷰 결과 요약표

| ID | 등급 | 시점(①v1.1 선반영 ②PoC 판정 ③운영 전) | 절 | 제목 |
|---|---|---|---|---|
| CON-01 | **P0** | ① | 9.2 / 14.3 | NORMAL 키에 `job_spec_digest` 포함 → republish 직후 같은 시각 occurrence 2개 |
| ORA-01 | **P0** | ①·② | 11.3 / 13.4 / 14.2 | window high가 standby 적용 SCN이 아닌 호스트 시각 → lag 구간 row 영구 누락 |
| DAG-02 / SCL-05 / DOC-01 | P1 | ①정합성·②성능 | 9.1 / 10 / 원칙 5 | tick 안 Control API 500회 동기 호출 — gRPC 60초·tick 무재시도·부분 실패 |
| CON-03 | P1 | ① | 6.2 / 9.1 / 10 | 계약 상태 전이 주체 미정의, PLANNED 고아 occurrence 탐지 주체 없음 |
| DAG-01 | P1 | ①문구·② | 9.3 | 비파티션 grouped schedule에는 catch-up 설정이 없다 |
| DAG-06 | P1 | ① | 9.1 / 8.2 | grouped schedule 이름·default_status 미정의 → 새 그룹이 STOPPED |
| CON-04 | P1 | ① | 13.4 / 12.2 / 14.3 | overrun 시 다음 회차 처리 미정의, 고정 window RETRY가 stale |
| CON-06 | P1 | ① | 14.1 / 13.4 | chunk 제출 시 Hold 재검사 미명시, CANCELLED/SKIPPED 상태 부재 |
| DOC-03 | P1 | ① | 9.2 / 14.3 | MANUAL 키 vs “수동 NORMAL은 schedule occurrence 재사용” 충돌 |
| CON-02 | P1 | ①·② | 13.2 / 10.1 | 재접속 불가 시 orphan driver fencing 순서 부재, attempt 식별자 없음 |
| DAG-04 | P1 | ①·② | 5.1 / 19.3 | in-process executor는 resume 불가; run_retries와 원칙 3 충돌 |
| DAG-03 | P1 | ① | 10 / 6.2 | Dagster run 인프라 실패·취소가 Control로 돌아오는 경로 없음 |
| ICE-06 | P1 | ① | 10.1 / 5.3 | deterministic SA 이름 + 동일 contract retry → 이름 충돌, TTL GC 후 재제출 |
| ICE-02 | P1 | ① | 12.1 / 13.2 | 0-row 결과와 overwrite 모드 미정의 → Full no-op / 판정 오류 |
| ICE-01 | P1 | ①증거·②용량 | 5.3 / 18 | Full 테이블 snapshot 보관 = 스토리지 수십~백 배 |
| ORA-02 | P1 | ①·② | 7.3 / 12.2 / 13.4 | UPDATE_DT는 commit 시각이 아님 — overlap으로 late-commit 누락 bound 불가 |
| CON-05 / ORA-03 | P1 | ② | 11.2 / 19.4 | lease 회수 ≠ 세션 종료; weight 정의·heartbeat 주체 미정 |
| ORA-04 | P1 | ③ | 11.1 / 19.4 | DB 측 강제(Profile/Resource Manager/전용 service) 없음 |
| ORA-10 | P1 | ①검증·③service | 7.1 / 11.1 / 19.3 | switchover 후 같은 descriptor가 primary — 역할 검증 없음 |
| ORA-06 / SCL-01 | P1 | ② | 11.2 / 14.2 | Source별 용량 제약 ΣD/C ≤ period 검산 없음 |
| SCL-02 / DAG-08 | P1 | ② | 5.1 / 19.4 | Run Pod-per-Run burst 자원·기동시간 미산정; `max_concurrent_runs` 기본 10 |
| SCL-04 | P1 | ② | 19.4 | Job별 freshness/lateness SLO 없음 |
| OPS-01 | P1 | ③ | 10.2 / 5.1 / 22.10 | Dagster OSS UI 인증·RBAC·감사 없음 — Guard 밖 mutation |
| OPS-02 | P1 | ③ | 17 / 9.2 / 14.1 | Control API 인증·역할·SoD, Run Pod 신원 없음 |
| OPS-03 | P1 | ③ | 7.1 / 10 | Spark Pod 자격증명 주입·마스킹·Polaris principal 없음 |
| OPS-04 | P1 | ③ | 20 / 21 / 11.5 | schema drift 시 MVP 동작 미정의 |
| OPS-05 | P1 | ③ | 13.1 / 13.2 | DQ Check 내용·실행 위치·Source 재조회 여부 없음 |
| OPS-06 | P1 | ③ | 20 Phase 3 | 기존 10k Job 변환·shadow 도구·watermark 인계·롤백 없음 |
| OPS-09 | P1 | ③ | 5.1 / 6 | Control PostgreSQL HA/백업/DR, Polaris/AIStor 장애 시 동작 없음 |
| OPS-13 | P1 | ① | 21 | MVP에 REPLAY/단일 Backfill/RERUN_LATEST/drift 검출 빠짐 |
| CRT-01 | P1 | ② | 8.1 / 6.2 / 8.2 | Bundle 전달·활성화·실패 동작 프로토콜 미정의 |
| CRT-04 | P1 | ① | 10.2 / 11.2 / 14.2 | Guard 거부·lease 대기 시 Run 결과·우선순위·Control API 장애 정책 없음 |
| CRT-05 | P1 | ① | 12.2 / 12.3 / 13.4 | 신규 Append/Merge Job 초기 적재·watermark 초기값 없음 |
| CRT-06 | P1 | ① | 7.2 / 7.3 / 12.4 | Oracle→Spark→Iceberg 타입·시간대·문자집합 규범 없음 |
| (P2 37건, P3 21건) |  |  |  | §6, §7 참조 |

---

## 4. P0 — 즉시 수정

### CON-01. NORMAL occurrence 키에 `job_spec_digest`가 들어 있어 republish 직후 같은 logical time의 occurrence가 2개 생긴다

**초안(9.2)**: `NORMAL: job_id + schedule_revision_id + logical_scheduled_time + job_spec_digest`, `MANUAL: idempotency_key`. 같은 절에서 “UI에서 같은 시간의 NORMAL 실행을 눌러도 이미 schedule occurrence가 있으면 같은 계약을 반환한다.”

**문제**:
1. 10:00 tick이 Job J의 occurrence O1 = (J, S1, 10:00, D1)을 만들고 RunRequest를 큐에 넣는다(정시 burst라 launch까지 수 분).
2. 10:02 운영자가 J를 publish → `job_spec_digest` = D2.
3. 10:04 다음 중 하나가 일어난다 — 운영자가 UI에서 “10:00 NORMAL 실행”을 누름 / 9.3 Reconciler가 expected cron time 10:00을 검사 / 새 schedule_revision으로 tick이 평가됨. create-or-get 키가 (J, S1 또는 S2, 10:00, D2)이므로 O1과 매치되지 않고 **O2가 생성된다.**
4. Full이면 Oracle 이중 읽기 + overwrite 2회(19.5 No-Go “정상 Run 중복”). Append/Merge는 13.4의 단일 window 규칙에 막혀 이중 commit은 피하지만 false failure·queue 점유·오탐 알림이 생긴다.

또한 MANUAL 키가 HTTP `idempotency_key`뿐이면 “같은 시간의 NORMAL을 눌러도 같은 계약 반환”은 성립할 수 없다(Idempotency-Key는 호출자마다 다르다). 두 문장이 모순이다. 초안 7.1은 “TNS를 승격해도 10,000 Job을 다시 publish할 필요가 없다”며 descriptor hash를 **계약 시점에 고정**한다고 하여 digest가 계약의 고정 속성임을 스스로 인정하고 있다 — 같은 원칙을 job_spec_digest에 적용하지 않은 것이다.

**근거**: Dagster schedule의 run_key는 “one run per tick, across failure recoveries”로 tick 범위 억제만 제공한다([schedules-sensors API](https://docs.dagster.io/api/dagster/schedules-sensors)). tick 밖(수동·Reconciler·revision 변경)의 중복 억제는 전적으로 Control 키에 의존한다.

**권고**:
1. occurrence 정체성 키를 데이터 의미로만 구성: **`UNIQUE(job_id, operation_class=NORMAL, logical_scheduled_at_utc)`**. `schedule_revision_id`는 키가 아니라 속성으로 기록.
2. `job_spec_digest` / `template_digest` / image digest / connection descriptor hash는 ExecutionOccurrence의 자식인 **ExecutionContract의 `pinned_*` 컬럼**으로 이동. create-or-get은 기존 contract의 pinned 값을 그대로 반환하고, republish는 기존 contract를 바꾸지 않는다(필요 시 운영자가 명시적으로 SUPERSEDE 후 새 contract). **어떤 버전을 고정할지는 “먼저 요청한 쪽”이 아니라 `logical_scheduled_at`에 유효했던(effective) Definition Release로 결정한다** — 배포와 정각 tick이 경합해도 결과가 결정론적이다(GPT 교차 리뷰 보정). 단 CATCHUP(Hold 해제)·REPLAY·RERUN_LATEST는 논리 시각이 과거라도 **현재 승인 release**를 고정한다(버그 수정 release를 catch-up이 못 받는 역효과 방지). DefinitionRelease에 `effective_from`이 필요하다.
3. MANUAL을 별도 키 타입이 아니라 “채널 속성”으로 강등: 수동 NORMAL은 Control API가 logical_scheduled_time(가장 최근 cron 경계)을 계산해 동일 NORMAL 키로 create-or-get. HTTP Idempotency-Key는 `IdempotencyRecord`에만 사용. REPLAY/RERUN_LATEST만 (job_id, parent_contract_id, replay_seq) 같은 고유 키.
4. PostgreSQL `UNIQUE (job_id, logical_scheduled_time) WHERE channel='NORMAL'`로 고정. 19.3 fault test “동일 Job에 schedule/manual/backfill 동시 요청”에 **“republish 직후 수동 NORMAL/Reconciler”** 케이스 추가.

### ORA-01. window high bound가 standby의 적용 SCN이 아니라 ETL 호스트 시각에 묶여 있어 lag 구간 row가 영구 누락된다

**초안(11.3, 13.4, 14.2)**: “신호가 없으면 최신성을 추정하지 않는다. Source DB의 기준 시각, safety lag, overlap window를 사용하고 `DEGRADED_CONFIDENCE`로 표시”, “Hold 종료 시점의 safe high watermark까지 catch-up”. **high가 무엇에서 파생되는지는 어디에도 정의되지 않았다.** lag 신호가 *있는* 경우(capability 1·2번)에 high를 어떻게 보정하는지도 없다.

**문제**: high = ETL 호스트 시각(또는 Oracle SYSDATE − safety_lag)인데 standby가 T−10분까지만 적용된 상태라면 (T−10m, T] 구간의 row는 지금 standby에 없어 이번 window에서 빠지고, 다음 window는 low = T부터 시작하므로 **영원히 빠진다.** overlap PT5M은 lag가 5분을 넘는 순간 무력해지고, `DEGRADED_CONFIDENCE`는 표시일 뿐 watermark CAS는 그대로 전진한다. 11.1 envelope의 “lag threshold → 자동 Hold”는 threshold 미만 lag(예: 6~10분)에서는 작동하지 않는다. 19.4 “Watermark gap 0”, “설명되지 않은 row 차이 0”이 standby lag 하나로 깨진다.

**근거**: Oracle Data Guard 관리 문서 — `V$DATAGUARD_STATS`의 apply lag, `DATUM_TIME`/`TIME_COMPUTED` 차이가 30초를 넘으면 지표가 부정확할 수 있음, `STANDBY_MAX_DATA_DELAY` 초과 시 ORA-03172로 쿼리 거부, `ALTER SESSION SYNC WITH PRIMARY` ([19c sbydb](https://docs.oracle.com/en/database/oracle/oracle-database/19/sbydb/managing-oracle-data-guard-physical-standby-databases.html)). 초안은 이 메커니즘 중 어느 것도 high 산출 규칙으로 연결하지 않았다.

**권고 — watermark를 `SourceVisibilityFence`로 재설계**(GPT 교차 리뷰의 정식화를 채택):
1. 계약에 **cutoff 종류**를 명시한다: `CDC_OFFSET` / `STANDBY_VISIBLE_SCN`(동일 SCN의 `AS OF SCN`으로 snapshot을 고정하고 window도 SCN) / `APPLICATION_TIMESTAMP_WITH_OVERLAP`(UPDATE_DT 같은 애플리케이션 시각 컬럼). 어느 종류든 **fence = standby에서 실제 관측한 적용 시점**이며, 실행 시작 시 standby 세션에서 applied_scn / applied_ts / apply_lag / DATUM_TIME을 읽어 계약에 기록한다. APPLICATION_TIMESTAMP 방식의 high는 `min(applied_ts, SYSTIMESTAMP_standby) − safety_lag`로 fence 아래에 두되, `SCN_TO_TIMESTAMP`는 commit 시각의 근사(±3초)일 뿐 애플리케이션 시각 컬럼과 범용 변환 관계가 없으므로 **overlap ≥ 최대 트랜잭션 지속시간 + DR lag + clock skew**와 PK 기반 Merge/Dedup을 함께 적용한다(ORA-02). 그 상한을 보장할 수 없으면 정기 Data Reconciliation이 필수다.
   - 주의: `V$DATABASE.CURRENT_SCN`은 standby에서 checkpoint SCN이라 **항상 마지막 적용 SCN보다 작다**([V$DATABASE](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-DATABASE.html)). 적용 SCN은 `V$RECOVERY_PROGRESS` 또는 세션 가시 SCN(`DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER`, ADG에서의 정확한 의미는 DBA 확인)에서 얻는다. 이는 ORA-05(AS OF SCN 출처)와 동일한 수정이다.
2. apply lag가 envelope threshold를 넘거나 DATUM_TIME이 30초 이상 stale이면 해당 Run을 SKIP(ABORTED_NO_COMMIT, watermark 미이동)하거나 이전 high를 재사용.
3. Critical Source는 `sessionInitStatement`로 `ALTER SESSION SET STANDBY_MAX_DATA_DELAY=<초>`를 걸어 DB가 ORA-03172로 거부하는 fail-fast를 기본값으로 검토.
4. capability 3번(신호 없음) Source는 incremental을 금지하지 않되 **조건부로 허용**한다(v1.0의 “금지 또는 수동 승인”을 GPT 교차 리뷰에 따라 완화): overlap·Merge·정기 reconciliation을 필수로 걸고 해당 Job의 **zero-gap 보증 등급을 낮춰 데이터 계약(freshness/gap grade)에 노출**한다 — 조용히 등급만 낮추면 안 된다. lag 지표의 `DATUM_TIME`이 stale이면 watermark 전진 금지. `DEGRADED_CONFIDENCE`가 CAS를 막는지를 등급별로 명시.

---

## 5. P1 — PoC 착수 전 확정

### 5.A 스케줄 tick · 멱등성 · 계약 상태 머신

#### A-1. tick 안에서 Control API를 500회 동기 호출하는 구조 (DAG-02, SCL-05, DOC-01)

- **초안**: 9.1 “Schedule 평가 시 Hold와 publish 상태를 확인하고 ExecutionOccurrence를 create-or-get”, 10장 시퀀스 첫 화살표, 19.4 “500 occurrence 저장 2분”.
- **문제**: schedule `execution_fn`은 daemon이 code server에 gRPC로 호출하며 **기본 timeout 60초**(`DAGSTER_SCHEDULE_GRPC_TIMEOUT_SECONDS`, `_grpc/utils.py` 확인). 19.4의 “2분” SLO는 이 기본값과 정면 모순이다. 함수가 300번째에서 예외를 던지면 Control DB에는 PLANNED occurrence 300개가 생겼지만 Dagster run은 0개이고, `max_tick_retries` 기본 0이므로 재시도도 없다. daemon은 정상이라 9.3 Reconciler(“복구 이벤트 때만”)는 돌지 않는다 → **탐지 경로 없는 회차 누락.** Control API는 모든 tick의 hard dependency가 되는데 장애 시 fail-open/closed 정책이 없다. 원칙 5(“매분 due Job을 계산하지 않는다”)는 schedule 시각 생성 주체를 말하고 9.1은 admission 판정 주체를 말하므로 양립은 가능하나, 9.3의 “precomputed occurrence + cursor sensor 승격안”은 원칙 5의 예외임이 명시되지 않았다.
- **구분**(v1.1): 이 finding은 두 성격이 섞여 있다. **정합성(① v1.1 선반영)** — `max_tick_retries`, Control API 장애 시 fail-closed, 부분 실패 후 PLANNED 고아 탐지(A-2). **성능(② PoC에서 판정)** — batch 엔드포인트 필요 여부와 tick wall time. 500 RunRequest 자체는 Dagster가 지원하므로 60초 timeout 안에 드는지는 실측으로 결정한다.
- **권고**:
  1. tick 내 호출을 **단일 batch 엔드포인트** `POST /v1/occurrences:batch-create-or-get`(schedule_revision_id + logical_time + job_id 목록 → contract_id 목록, 한 트랜잭션)로 교체. 17장 API 목록에 추가.
  2. `DAGSTER_SCHEDULE_GRPC_TIMEOUT_SECONDS` 명시 설정, `schedules.max_tick_retries ≥ 1`, `schedules.use_threads/num_workers`를 dagster.yaml 기준안(5.1)에 기재. 19.4 SLO를 “tick 시작 → 마지막 run QUEUED p95 X분”으로 통합하고 별도 tick SLO(p95 30초 등)를 둔다.
  3. Control API timeout/5xx 시 **fail-closed**(해당 Job skip + 사유 기록) 원칙과 Reconciler 복원 경로를 9.1에 명시. 원칙 5를 “Control Plane은 schedule 시각을 생성하지 않으며 admission(Hold/멱등성)만 판정한다”로 재서술하고 9.3 승격안을 예외로 표기.
  4. shard 경계가 Source 기준이면 burst가 한 shard에 몰리는 worst case(단일 shard 500)를 19.2에 추가.

#### A-2. 계약 상태 전이 주체가 없고 PLANNED 고아 occurrence를 탐지할 주체가 없다 (CON-03)

- **초안**: 6.2 `PLANNED → WINDOW_RESERVED → DAGSTER_BOUND → COMMIT_OBSERVED → FINALIZED`. 10장 시퀀스는 Guard 시점(Run이 이미 존재)에 window를 반환.
- **문제**: tick은 Dagster run_id를 알 수 없고(run은 daemon이 비동기 생성), run_id와 window를 동시에 아는 유일한 지점은 Run Pod의 Guard 호출이다. WINDOW_RESERVED를 tick 시점으로 읽으면 launch되지 못한 Run의 window가 영구 점유되어 다음 회차가 13.4 단일 window 규칙에 막혀 **연쇄 누락**이 생기고, Guard 시점으로 읽으면 6.2의 순서가 틀린 것이 된다. DAGSTER_BOUND의 전이 조건(누가, 무엇을 근거로)이 비어 있다. A-1의 tick 부분 실패, code location 제거, 큐 정체로 PLANNED에 머무는 occurrence는 어디에서도 발견되지 않는다.
- **권고**:
  1. 상태 머신 수정: `PLANNED`(occurrence 생성, window 없음) → `DAGSTER_BOUND`(Guard가 `(contract_id, dagster_run_id, attempt_no)`를 `UPDATE … WHERE current_attempt = ?` CAS로 기록) → `WINDOW_RESERVED`(같은 Guard 트랜잭션에서 watermark lock 하에 [low, high) 계산·예약 + lease 획득) → `COMMIT_OBSERVED` → `FINALIZED`.
  2. 값싼 연속 탐지기: `state='PLANNED' AND created_at < now() − X AND NOT held` 인덱스 쿼리를 1~5분 주기로 실행해 Adapter로 동일 run_key 재제출. 이는 due 계산이 아니라 Control이 이미 만든 row의 stale 검사이므로 원칙 5와 충돌하지 않는다.
  3. PLANNED에 `expires_at`을 두고 만료 시 `EXPIRED_UNLAUNCHED`로 천이 + 운영 알림.

#### A-3. 비파티션 grouped schedule에는 Dagster catch-up 설정이 없다 (DAG-01)

- **초안(9.3)**: “Dagster catch-up 관련 설정을 30분 RTO와 실제 cron에 맞게 명시적으로 설정”.
- **문제**: Dagster scheduler는 파티션이 없는 ScheduleDefinition에 대해 놓친 tick이 2개 이상이면 경고만 남기고 **가장 최근 tick 1개만** 평가한다(`_scheduler/scheduler.py`: `if not remote_schedule.partition_set_name and len(tick_times) > 1: … tick_times = tick_times[-1:]`). `max_catchup_runs`(기본 5)는 파티션 schedule 전용이다. 초안의 grouped schedule은 비파티션이므로 **조정할 설정 자체가 존재하지 않는다.** 30분 RTO 가정 안(놓친 tick 1개)에서는 Full(최신 1회 coalesce)·Incremental([last_wm, now) window) 모두 데이터 의미상 안전하지만, 장애가 1 cron 주기를 넘으면 중간 tick은 조용히 버려진다.
- **권고**: 9.3 문구를 “Dagster는 비파티션 schedule의 놓친 tick 중 최신 1개만 재생한다”로 교체하고 설정 항목 문구 삭제. 30분 RTO 안에서 tick 1개 손실이 안전한 이유를 명시. 장애가 1 주기를 넘는 경우에 한해 Reconciler를 **daemon heartbeat gap 감지로 자동 기동**하는 트리거를 추가(현재는 “복구 또는 운영자 요청”에만 묶여 있음). 대안으로 time-partitioned asset + `build_schedule_from_partitioned_job`을 PoC 비교군에 넣되 10k asset × hourly partition의 UI/파티션 상태 로드 비용을 함께 측정.

#### A-4. grouped schedule의 이름 규칙과 default_status가 없다 (DAG-06)

- **문제**: Dagster는 schedule의 started/stopped 상태를 인스턴스 DB에 **이름 기준**으로 저장하며 신규 schedule은 `default_status`가 RUNNING이 아니면 STOPPED로 시작한다([API](https://docs.dagster.io/api/dagster/schedules-sensors)). 운영자가 Job 하나의 cron을 `0 */2 * * *`로 바꿔 없던 (cron, tz, shard) 조합이 생기면 새 ScheduleDefinition이 STOPPED로 배포되어 **그 Job은 조용히 실행되지 않는다.** 반대로 그룹이 비었다가 다시 생기면 과거 상태(누군가 UI에서 끈 STOPPED)가 되살아난다. 이름에 digest를 넣으면 bundle마다 이름이 바뀌어 tick 복구 범위와 UI 이력이 끊긴다.
- **권고**: 이름을 `sched__{shard}__{cron_slug}__{tz}`처럼 digest 비포함·결정론적으로 고정. 모든 생성 schedule에 `default_status=DefaultScheduleStatus.RUNNING`. DefinitionRelease VALIDATED→DEPLOYED 단계에 “Bundle이 기대하는 schedule 집합이 instance에서 RUNNING인지” 대조 추가, Reconciler가 주기적으로 STOPPED를 알림. 그룹 이동 시 `schedule_revision_id` 증가 규칙을 9.2에 명시.

#### A-5. overrun 시 다음 회차 처리가 없고, 고정 window RETRY가 stale해진다 (CON-04)

- **초안**: 13.4 “같은 Job의 NORMAL execution은 한 번에 하나만 window 예약”, 14.3 “RETRY: 기존 digest/window 고정”, 13.4 “중간 chunk 실패 시 성공한 마지막 watermark부터 재개”.
- **문제**: (1) hourly Append job의 10:00 회차 O10이 90분 걸리거나 실패. 11:00 회차 O11은 window를 예약할 수 없는데 — Run을 실패시키는지(“정상 Run 누락” + false alarm), Run Pod 안에서 대기하는지(pod·slot 점유), skip하는지 **정의가 없다.** (2) O10 실패 → watermark w0. 다음 회차 O12가 [w0, h12)를 예약해 성공, watermark = h12. 운영자가 O10을 RETRY(고정 window [w0, h10))하면 이미 O12가 덮은 구간을 다시 읽는다 — 12.2 step 3 PK 검사 실패 또는 commit 후 CAS 실패(RECONCILIATION_REQUIRED). “고정 window retry”와 “다음 회차가 현재 watermark 기준으로 새 window 계산”이 양립하지 않는다. (3) high를 누가 언제 계산하는지(occurrence 생성 vs Guard vs extract), 11.3 safety lag 반영 시점, Hold 해제 catch-up occurrence의 채널/키(9.2 3종 중 무엇인지)도 미정.
- **권고**:
  1. Incremental window 계산 시점을 Guard로 고정: job 단위 watermark row를 `SELECT … FOR UPDATE` 후 low = current_watermark, high = min(safe_high(logical_time), low + max_chunk_span).
  2. 불변식 “열린 incremental window는 job당 최대 1개”를 PostgreSQL exclusion constraint로 강제. 다음 회차 Guard가 열린 window를 만나면 Run을 실패시키지 말고 SkipReason을 반환하며 occurrence를 `COALESCED_INTO={open_contract_id}`로 마감(새 상태). Full에도 동일 정책(이전 occurrence 미완료 시 coalesce)을 적용해 queue 누적을 구조적으로 차단.
  3. RETRY 유효성: `contract.window.low == current_watermark`일 때만 허용, 아니면 409 `STALE_WINDOW` + `SUPERSEDED`로 마감. Full은 예외. “RETRY는 같은 contract·같은 window”라는 원칙은 이 규칙이 있어야 성립한다 — 실패 contract가 ABORTED로 닫힌 뒤 다음 회차가 watermark를 전진시킬 수 있기 때문이다(대안인 ‘ABORTED contract가 다음 window를 막는 retryable-open’은 freshness를 희생한다).
  4. Hold 해제 catch-up은 channel=CATCHUP, logical_scheduled_time=hold_release_time으로 생성. chunk별 CAS 성공 후 `contract.window.low`를 전진시켜 재개 지점을 contract에 남긴다.

#### A-6. Hold 검사점이 chunk 제출에 적용되는지 미명시, DRAIN/FORCE_STOP 후 정리 상태가 없다 (CON-06)

- **문제**: (1) 대용량 catch-up은 Run Pod 안에서 chunk마다 SparkApplication을 제출하는데, 10.2 “Spark submit 직전 Guard”가 Run당 1회인지 SA 제출마다인지 명시되지 않았다(10장 시퀀스는 Guard 1회 후 SA 1개). Run당 1회면 Hold 생성 후에도 chunk 2..N이 계속 제출되어 19.5 No-Go “Hold 중 신규 SA 제출” 위반. (2) FORCE_STOP이 SA를 삭제하면 그 SA가 이미 commit을 마쳤을 수 있다. 6.2에 CANCELLED/SKIPPED 계열 상태가 없어 contract가 DAGSTER_BOUND/WINDOW_RESERVED에 남아 window·lease를 계속 점유하고, Hold 해제 시 catch-up occurrence가 “열린 window 1개” 규칙에 막힌다. (3) Hold 중 tick이 occurrence를 만드는지 여부가 없어 9.3 Reconciler가 Hold 회차를 누락으로 오판할 수 있다.
- **권고**: (1) “chunk 제출마다 Guard 재검사”(`POST /v1/contracts/{id}/chunks:begin`)를 10.2에 명문화. (2) DRAIN = “현재 chunk 완료·CAS 후 중단”, contract를 `CANCELLED_AT_SAFEPOINT`(window.low = 마지막 CAS 값)로 마감하고 window·lease를 같은 트랜잭션에서 해제. (3) FORCE_STOP 프로토콜: SA delete → driver 부재 확인(WRITER_FENCED) → 증거 검색 → COMMIT_OBSERVED(부분 commit 반영 CAS) 또는 ABORTED_NO_COMMIT → CANCELLED. 증거 검색을 건너뛰는 경로 금지. (4) Hold 중 tick은 occurrence를 `SKIPPED_BY_HOLD(hold_id)`로 생성. (5) 상태 머신에 `SKIPPED_BY_HOLD, COALESCED, SUPERSEDED, CANCELLED_AT_SAFEPOINT, CANCELLED`를 추가하고 각 상태의 window/lease 해제 불변식을 표로.

#### A-7. MANUAL 키와 수동 실행 5모드가 대응되지 않는다 (DOC-03)

- **문제**: 9.2 unique key는 NORMAL/BACKFILL/MANUAL 3종, 14.3 모드는 NORMAL/RETRY/REPLAY/BACKFILL/RERUN_LATEST 5종 — “MANUAL” 모드는 없다. REPLAY/RERUN_LATEST는 “새 계약”인데 unique key가 없어 두 번 누르면 두 계약이 생긴다. 수동 NORMAL의 `schedule_revision_id + logical_scheduled_time`을 누가 계산하는지 없다. 10.2 “없으면 허용된 Manual 계약 생성”이 14.3의 어떤 모드·어떤 window로 생성되는지도 없다(DOC-05와 연결).
- **권고**: 9.2를 **모드 × unique key × 충돌 시 동작(return existing / reject / new)** 표로 재작성. REPLAY/RERUN_LATEST = (parent_contract_id 또는 job_id) + client Idempotency-Key. CON-01 권고와 함께 적용.

### 5.B 실행 · 재시도 · commit 증거

#### B-1. 재접속이 불가능할 때 orphan Spark driver를 fencing하는 순서가 없고 attempt 식별자가 없다 (CON-02)

- **초안의 방어**: 10.1 create-or-get + watch reconnect, 19.3 “Run Pod 종료 후 SparkApplication 재접속” — 살아 있는 SA에 새 driver를 만들지 않는다는 의도는 있다.
- **남는 공백**: SA가 terminal이 아닌데 재접속이 불가능한 경우(SA delete 중, CR 유실, driver pod만 orphan)에 13.2 “증거 없음 → ABORTED_NO_COMMIT → retry”로 가면, 증거 검색은 “그 시점까지 commit이 없었다”는 관측일 뿐 “앞으로도 없다”는 보장이 아니므로 orphan driver와 retry driver가 같은 window를 동시에 write한다. Iceberg append는 OCC로 **두 commit이 모두 성공**한다([reliability](https://iceberg.apache.org/docs/latest/reliability/)) → 19.5 “Append 이중 commit”. retry가 같은 contract_id를 쓰면 snapshot summary의 `etl.execution_contract_id`만으로는 attempt를 구분할 수 없다.
- **권고**:
  1. ExecutionContract에 `attempt_no`, SparkApplication 이름을 `{contract_id}-a{attempt_no}`로. `current_attempt` 증가는 단일 row CAS로만.
  2. retry 전 순서를 상태로 강제: `DAGSTER_BOUND → WRITER_FENCED`(이전 attempt SA 삭제 + driver pod 부재 확인 또는 SA terminal 확인) → 증거 검색 → `COMMIT_OBSERVED | ABORTED_NO_COMMIT`. **WRITER_FENCED 전에는 ABORTED_NO_COMMIT 천이 금지.**
  3. snapshot summary에 `etl.attempt_no` 추가. Spark job은 commit 직전 Control의 current_attempt와 자기 attempt를 비교(best-effort).
  4. WAP/branch는 **모든 Append/Merge의 기본 경로로 강제하지 않는다**(v1.0의 권고를 GPT 교차 리뷰에 따라 축소 — 40k run/일에 attempt별 branch는 과하다). fencing은 (2)의 `WRITER_FENCED` + (3)의 `attempt_no` evidence로 충족하고, branch → DQ → fast_forward 경로는 **Critical Merge/Full 한정 PoC 후보**로 13.2에 남긴다(Iceberg/Polaris 버전 고정 후 비교, ICE-03 제약 참조).
  5. Dagster 인스턴스 `run_retries`는 crash 전용으로 제한하고 모든 retry를 Control RETRY API로 단일화(B-2).

#### B-2. in-process executor에서는 Run Pod 사망 시 Dagster resume이 없다; 자동 run retry는 원칙 3과 충돌 (DAG-04)

- **문제**: run monitoring의 resume은 “K8sRunLauncher + k8s_job_executor” 또는 “DockerRunLauncher + docker_executor”에서만 지원된다([run-monitoring](https://docs.dagster.io/deployment/execution/run-monitoring)). 초안 구성(in-process)에서 Run Pod가 죽으면 run은 poll_interval 후 FAILED로 마킹될 뿐, 같은 run이 SA에 “재접속”하지 않는다. 재접속은 (a) `run_retries`가 만든 새 run 또는 (b) Control RETRY가 contract tag + deterministic SA 이름으로 재결합하는 방식이어야 한다. 그런데 `run_retries`는 기본적으로 step 실패에도 발동(`retry_on_asset_or_op_failure` 기본 true)하므로 Spark 실패 시 Iceberg commit 확인 없이 재실행 → 원칙 3, 19.5 No-Go 위반. 10.2 Guard 검사 목록에 “Iceberg commit 증거 선확인”이 없다.
- **권고**: 5.1/19.3의 “재접속”을 “Dagster resume이 아니라 새 run이 contract tag + deterministic SA 이름으로 재결합”으로 정정. dagster.yaml: `run_monitoring.enabled: true`, `run_retries.enabled: true`, `retry_on_asset_or_op_failure: false`. Guard 첫 단계에 “기존 SA 존재 여부 + contract ID 기반 snapshot 증거 조회”를 추가하고 COMMIT_OBSERVED면 Spark 재제출 없이 검증 단계로 직행. `k8s_job_executor`(step pod +1) 채택 트레이드오프를 PoC 비교 항목으로.

#### B-3. Dagster run의 인프라 실패·취소가 Control Plane으로 돌아오는 경로가 없다 (DAG-03, CON-07)

- **문제**: 10장 시퀀스의 Dagster→Control 호출은 Guard와 commit evidence 둘뿐이다. Run Pod OOM/노드 축출, run monitoring FAILED 마킹, UI Terminate, `start_timeout`(기본 180초) 실패는 Run Pod가 아무것도 보고하지 못한다. 계약은 DAGSTER_BOUND에 머문다(lease는 11.2 heartbeat 만료로 회수되지만 계약 상태는 수렴하지 않음). 40k run/일에서 이는 하루 수십 건의 일상 사건이다. 16.4 “Job failed” outbox 이벤트의 생산자도 정의되지 않아 Run Pod 사망 실패는 알림에서 유실된다.
- **권고**: Dagster Adapter에 shard별 `@run_status_sensor(FAILURE/CANCELED/SUCCESS)` → `POST /v1/contracts/{id}/dagster-terminal-event`(Idempotency-Key = run_id + status). Control은 run_id→contract 매핑으로 상태 변경과 outbox insert를 **한 트랜잭션**으로. 보조로 DAGSTER_BOUND 계약의 주기적 run 상태 polling(Reconciler 일부). 계약 상태에 `DAGSTER_RUN_TERMINATED_UNVERIFIED` 추가 후 13.2 절차 재사용. sensor 처리 지연(기본 30초 간격)을 19.2에 포함. 4장 책임표에 “Dagster run 종료 사실의 Control 반입 경로 = run status sensor”를 명시(상태 머신 복제가 아니라 사실 반입).

#### B-4. deterministic SA 이름 + 동일 contract retry → 이름 충돌, TTL GC 후 중복 제출 (ICE-06)

- **문제**: (a) Retry가 같은 contract_id를 쓰면 같은 이름의 SA가 FAILED로 이미 존재한다. create는 AlreadyExists, get은 실패한 CR. spec을 수정해 재제출하면 Operator의 “update → 기존 앱 중지 후 재제출” 의미로 흘러 상태 추적이 꼬인다([spark-operator](https://spark.kubeflow.org/en/latest/user-guide/working-with-sparkapplication.html)). (b) `timeToLiveSeconds`가 짧으면 Run Pod 재시작 후 get이 “없음”을 반환해 create-or-get이 새 앱을 만들고 이미 commit된 실행을 다시 수행 — 19.5 No-Go. (c) K8s 이름은 DNS-1123(소문자·숫자·`-`·`.`, ≤253자), label 값은 ≤63자 — contract id 형식 결정 필요.
- **권고**: B-1의 `sa-{contract_short}-a{attempt}`. 계약에 SA UID·`status.sparkApplicationId`·마지막 관측 상태를 기록하고 get “없음”이면 Control 기록과 대조해 13.2 “증거 만료” 경로로. `timeToLiveSeconds ≥ ambiguous commit 조사 기간`을 5.3에 명시. spec 불변(업데이트 금지)을 adapter 규칙으로. 13.2의 snapshot 검색을 SA 조회보다 먼저 적용하면 TTL GC 시나리오도 기존 규칙으로 흡수된다.

#### B-5. 0-row 결과와 overwrite 모드가 미정의다 (ICE-02)

- **문제**: Spark `partitionOverwriteMode`가 dynamic이면 SELECT 결과가 0 row일 때 교체되는 partition이 없어 테이블이 이전 상태로 남고(조용한 stale), static이면 PARTITION 절 없는 OVERWRITE가 **모든 partition을 비운다**([spark-writes](https://iceberg.apache.org/docs/latest/spark-writes/): Iceberg는 dynamic 권장). 초안 12.1은 “INSERT OVERWRITE 또는 replace”만 적고 모드·0-row 정책이 없다. 업무 WHERE 오류·DR lag로 0 row가 나오면 Full이 no-op가 되면서 snapshot이 없거나 빈 snapshot만 남아 13.2가 ABORTED_NO_COMMIT → 재시도 루프 또는 오판정. 0-row MERGE/Append의 빈 snapshot 생성 여부는 엔진/버전 의존(PoC 확인 항목).
- **권고**: JobSpec에 overwrite 모드 명시(Full = static 전체 교체/replace, partition 범위 Full = dynamic), Template에서 `spark.sql.sources.partitionOverwriteMode` 고정. 계약에 `expected_commit_count`(chunk 수)·snapshot summary에 `etl.chunk_seq`를 두고 13.2 판정을 “chunk(계약)당 snapshot 1개”로 13.4와 정렬. 0-row 정책: Full 0-row는 기본 거부(`allow_empty_full` 플래그), Append/Merge 0-row는 Spark Job이 extracted/written row 수를 보고하고 snapshot 없이 `FINALIZED(NO_DATA)`로 CAS 허용. 19.3에 “0 row 결과”, “빈 MERGE” 추가.

#### B-6. Full 테이블의 snapshot 보관 기간이 스토리지를 수십~백 배로 키운다 (ICE-01, SCL-09)

- **문제**: static INSERT OVERWRITE는 매 run 테이블 전체 데이터 파일을 새로 쓰고 이전 파일은 snapshot 만료까지 남는다([maintenance](https://iceberg.apache.org/docs/latest/maintenance/)). Full ~6,000 테이블, 1시간 주기 테이블을 Iceberg 기본 5일 보관으로 두면 **120배**, 7일이면 168배. 5.3/18은 commit 증거를 보존하기 위해 snapshot 자체를 길게 보관하라고 하는데, 증거와 데이터 파일 수명을 결합한 설계라 용량 계획이 성립하지 않는다. 초안에는 AIStor 용량 수치(Full snapshot, staging TTL × 일 추출량, Spark event log, OpenSearch)가 전혀 없다.
- **권고**: (1) commit 증거를 snapshot 생존과 분리 — FINALIZED 시점에 snapshot_id, sequence_number, summary 전체, manifest-list 경로, metadata location을 ExecutionContract에 영속화. (2) Full 테이블 retention은 retry window 수준(주기×2, `min-snapshots-to-keep 2`)으로 짧게, Append/Merge는 time-travel 요구에 따라 등급별. (3) 5.3/18 문장을 “증거 보관 기간 > reconciliation SLA, snapshot 보관은 테이블 등급별”로 수정. (4) §5에 Storage 용량 모델 절 추가(ΣT_full × (runs/일 × 보관일 + 1), staging E × TTL, event log·compute log·OpenSearch 일 증가량)하고 19.2에 AIStor 증가율(GB/일) 측정 추가.

### 5.C Oracle · Source 보호

#### C-1. INSERT_DT/UPDATE_DT는 commit 시각이 아니므로 overlap으로 late-commit 누락을 bound할 수 없다 (ORA-02)

- **문제**: row가 UPDATE_DT=10:00으로 stamp된 뒤 트랜잭션이 10:20에 commit되면, [10:00, 10:15) window를 10:16에 추출한 Run은 그 row를 못 보고 다음 window [10:10, 10:30)도 10:00을 포함하지 않아 **영구 누락**. 누락 한계는 “Source 애플리케이션의 최대 트랜잭션 열림 시간”인데 플랫폼은 알 수 없고, 배치성 업무(대량 UPDATE 후 commit, 야간 배치, 앱 서버 시계 skew)에서는 수십 분~수 시간이다. 초안은 commit-time과 event-time 구분을 어디에도 언급하지 않는다. 11.4의 AS OF SCN은 파티션 간 일관성 용도일 뿐 window predicate가 아니다. 7.1의 “`update_dt` 신뢰성” capability는 컬럼 자체의 신뢰성을 뜻하고 트랜잭션 열림 시간을 포함하지 않는다.
- **권고**:
  1. JobSpec에 `watermark_semantics: APPLICATION_TIME | COMMIT_SCN`. APPLICATION_TIME은 Source capability(7.1)에 `max_open_txn_seconds`를 DBA/업무가 등록한 경우에만 허용하고 overlap ≥ 그 값을 publish 검증(7.2-12) Gate로.
  2. COMMIT_SCN 모드: `ORA_ROWSCN > :low_scn AND ORA_ROWSCN <= :high_scn`(high_scn = standby 적용 SCN). ORA_ROWSCN은 commit SCN보다 작은 값을 절대 반환하지 않으므로 “누락 없음(false positive만)” 속성을 갖고 PK dedup으로 흡수 가능([ORA_ROWSCN](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/ORA_ROWSCN-Pseudocolumn.html)). **제약**: ORA_ROWSCN은 Flashback Query(`AS OF SCN`)와 한 쿼리에서 공존할 수 없고, pseudocolumn이라 인덱스를 타지 못해 범위 predicate가 full scan을 유발한다(ORA-07/08 부하와 충돌). ROWDEPENDENCIES 없이는 block 단위. 따라서 Merge 기본 후보가 아니라 PoC matrix 비교군으로 둔다.
  3. 어느 모드든 “정기 reconciliation”(넓은 window의 PK+UPDATE_DT만 추출해 Iceberg와 비교, repair MERGE)을 선택사항이 아닌 필수 Job으로 승격하고 Source 용량 모델에 포함(ORA-09). 19.4에 “late-commit 누락 탐지율/복구 시간” 지표 추가.

#### C-2. lease 회수는 Oracle 세션을 끝내지 않으며, weight 정의·heartbeat 주체가 미정이다 (CON-05, ORA-03)

- **문제**: Source S 한도 6. Job A가 4 token으로 executor 4개가 장시간 extract 중. A의 heartbeat 주체인 Run Pod가 GC pause/네트워크 분리로 TTL을 넘긴다(Spark driver는 정상). Control이 lease를 만료·회수해 pool에 4 token을 돌려놓고 Job B에 부여 → Oracle에는 A의 세션 4개가 그대로 살아 있고 B의 4개가 추가되어 **8 > 6**. fencing token은 Oracle이 검사할 수 없다. 즉 “회수”는 회계상 회수일 뿐 자원 회수가 아니다. 또 `numPartitions=1 ≈ connection 1`은 executor 세션만 센 표현이고, driver의 schema 해석·SCN 조회 세션(짧음), Spark task retry(`spark.task.maxFailures`)의 재연결, 11.5 direct 경로에서 다중 action 시 원천 쿼리 재실행이 빠져 있다. lease 해제 시점이 Dagster Run 종료인지 SA 종료인지도 미정이다(`free_slots_after_run_end_seconds`는 Run 종료 기준).
- **권고**:
  1. 2단계 회수: 만료 → `EXPIRING`(token 미반환) → Control이 SA를 직접 delete하고 driver/executor pod 부재를 K8s API로 확인 → `RECLAIMED` → 그때만 token 반환. 반환 전 재부여 절대 금지.
  2. heartbeat 주체를 Run Pod가 아니라 **Spark driver(또는 sidecar)**로 옮겨 세션을 실제로 쥔 프로세스의 생존과 lease를 묶는다.
  3. 관측 기반 fence: JDBC 연결에 `client_identifier/module/action`에 contract_id·attempt_no를 설정하고, Control이 token 부여 직전 `GV$SESSION` 집계(DBA 승인 읽기 전용 뷰)를 pool 회계와 비교해 초과 시 거부·알림. 이것이 Oracle에서 집행 가능한 유일한 fencing이다.
  4. `read_profile.query_timeout_seconds ≤ lease TTL − margin`을 같은 정책 테이블에서 파생. pool 한도를 절대 한도보다 낮게(회수 지연 여유).
  5. weight = numPartitions + driver_sessions(기본 1~2)로 정의하고 Phase 0에서 `V$SESSION` 실측으로 보정. lease 해제 조건은 “SA 종료 확인(driver pod terminal)”, Dagster pool은 보조로만. MVP는 numPartitions=1 고정이므로 위 프로토콜은 19.3 “lease 만료/회수” 장애 주입의 **합격 조건**으로 정의하고 가변 weight 확대 전 필수로 둔다.

#### C-3. Source 한도 강제가 Control Plane 한 겹뿐이다 (ORA-04)

- **문제**: 11.1 맨 위의 “Platform hard limit”은 실체(누가 어디서 강제)가 없다. 22-9는 승인 값일 뿐 DB 측 enforcement가 없다. Guard 우회 경로·lease 만료 후 Spark 생존·세션 계수 오차·22-10 direct 실행 허용 시나리오에서 lease는 깨질 수 있는 소프트 한도다. `query_timeout`은 클라이언트 측 cancel이라 네트워크 단절·driver OOM 시 Oracle 세션은 계속 실행된다. 19.4 “절대 한도 단 한 번도 초과 금지”는 **무엇으로 관측하는지** 정의가 없어 측정 불가능하다.
- **권고**: (1) Source별 ETL 전용 DB user + Profile(`SESSIONS_PER_USER` = envelope 한도 + 여유) + Resource Manager consumer group(`ACTIVE_SESS_POOL_P1`, `UTILIZATION_LIMIT`, `SWITCH_ELAPSED_TIME → CANCEL_SQL`)을 “Platform hard limit”의 실체로 정의하고 11.1 우선순위 맨 위에 “DB-enforced limit”를 추가([Resource Manager](https://docs.oracle.com/en/database/oracle/oracle-database/19/admin/managing-resources-with-oracle-database-resource-manager.html)). Resource Manager plan의 ADG standby 적용 여부는 22장 DBA 확정 항목. (2) standby에 ETL 전용 role-based service(role=PHYSICAL_STANDBY)를 만들어 ConnectionRevision은 그 service만 가리킨다(C-4와 연결). (3) 19.4 판정을 DB 측 관측(`V$SESSION`/`V$RSRC_SESSION_INFO` 샘플, ORA-02391 발생 건수)으로 정의하고, 초과 시 DB가 거부하는 것이 정상 동작임을 19.5에 명시. (4) Phase 3 wave 이관 기간(두 플랫폼 동시 운영)에도 합산 한도 규칙 확장.

#### C-4. switchover/failover 후 같은 descriptor가 primary를 가리켜도 감지하지 못한다 (ORA-10)

- **문제**: ConnectionRevision은 host/service descriptor hash로 고정된다. switchover가 일어나면 같은 host/service가 PRIMARY로 열리고 ETL은 descriptor를 바꾸지 않았으므로 “정책상 DR”이라 믿은 채 **실제 운영 primary를 읽는다** — 초안이 가장 피하려는 상황이 19.5 No-Go 검출 없이 발생한다. 19.3은 “DR outage”와 “endpoint 전환”만 다루고 역할 전환은 없다.
- **권고**: (1) Guard 또는 Spark driver 시작 직후 `SELECT database_role, open_mode FROM V$DATABASE`로 역할 검증, source_type=DR인데 `PHYSICAL STANDBY / READ ONLY WITH APPLY`가 아니면 즉시 중단 + Source 자동 Hold + Kafka alert(16.4에 “source role mismatch” 추가). (2) DBA와 standby 전용 role-based service(역할이 PHYSICAL_STANDBY일 때만 기동)를 만들어 switchover 시 연결 자체가 실패하도록. (3) Raw descriptor 검증이 ADDRESS_LIST의 모든 ADDRESS에 allowlist를 적용한다는 점을 명시하고 FAILOVER/LOAD_BALANCE 허용 여부 규정. (4) 19.3에 “switchover 후 실행” 시나리오 추가, 19.5 “Primary 자동 fallback”의 검출 방법을 (1)로 정의. (5) Wallet/TNS_ADMIN은 executor pod에도 마운트되어야 함을 10.1 템플릿 요구사항에 명시.

#### C-5. Source별 용량 제약 ΣD/C ≤ period를 어디서도 검산하지 않는다 (ORA-06, SCL-01)

- **문제**: 한 Source에 매시 정각 J개 Job이 묶여 각 D분 걸리면 max_concurrency C일 때 최소 ΣD/C분이 필요하다. 예: J=400, D=4분, C=10 → 160분 > 60분. 매 tick마다 queue가 단조 증가하고 freshness는 무한히 악화되지만 **19.5 No-Go 어디에도 걸리지 않는다.** Hold 해제 catch-up(500 Job, D=5, C=10 → 250분)은 정상 부하 위에 얹히고, 14.2 coalesce는 “Hold 중 놓친 회차”만 다루며 catch-up 진행 중 도착하는 NORMAL은 다루지 않는다. lease에 FIFO/우선순위가 없어 1시간 주기 Job이 24시간 주기 대형 Full에 계속 밀릴 수 있다. 운영자가 10,000번째 Job을 등록할 때 Source가 포화인지 알 수 없다.
- **권고**: (1) Control Plane에 Source별 capacity model을 1급 산출물로: 주기 band별 Σ(D_i × weight_i)/C_S = ρ를 계산해 `POST /v1/jobs/{id}/validate`에서 ρ > 0.7 경고, ρ > 1.0 거부 또는 DBA 승인. D는 Phase 0 baseline·15.2의 실측 처리량에서. (2) Full·Incremental 모두 “이전 occurrence 미완료 시 NORMAL 회차 처리(대기 vs coalesce)”를 13.4에 명시(A-5와 동일). (3) 정각 집중 해소: **논리 시각(정각 schedule)은 사용자 요구대로 유지**하고 cron을 분산하지 않는다. 부하 제어는 제출 지연이 아니라 **Source admission + 우선순위 queue**(4번)에서 한다 — 실제 실행만 Source gate에서 대기한다(v1.0의 staggered delay 안을 GPT 교차 리뷰에 따라 교체). (4) lease 획득에 우선순위/aging(짧은 주기·Critical 우선)과 `dagster/priority` 태그. (5) Hold 해제 API가 예상 catch-up 소요(N_hold × D/C)를 202 응답과 함께 반환하고 period를 넘으면 catch-up 중 NORMAL을 자동 coalesce. (6) 19.2에 “Source 포화 시나리오(C=1, ΣD > period)”와 PoC 합격 기준 “Source별 ρ와 queue 깊이가 7일간 발산하지 않음” 추가.

### 5.D 규모 · 용량 · SLO

#### D-1. Run Pod-per-Run 모델의 burst 자원·기동시간이 산정되지 않았고 run coordinator 기본값으로는 SLO가 불가능하다 (SCL-02, DAG-08)

- **문제**: Run 1건 = K8s Job + Run Pod + SparkApplication CR + driver Pod + executor Pod(+ConfigMap/Service) ≈ 6~8 객체 → 일 24만~32만 객체. Run Pod는 Spark가 끝날 때까지 **watch만 하는 유휴 Python 프로세스**인데 burst 시 500개가 동시에 존재하고 각 pod가 shard 정의(625~1,250 asset)를 다시 import한다. request 0.5 vCPU/1 GiB 가정이면 500 GiB·250 vCPU가 아무 계산도 하지 않는 pod에 묶인다(RSS는 가정값, PoC 실측 필요). `QueuedRunCoordinator` 기본값은 `max_concurrent_runs 10`, `dequeue_interval 5초`, `dequeue_use_threads false`(소스 확인) — 기본값 그대로면 500 burst는 50 wave로 직렬화되어 19.4 “queue→launch p95 2분”은 구조적으로 불가능하고, 500 이상으로 올리면 위 자원 문제가 현실화된다. run_monitoring `start_timeout_seconds` 기본 180초는 SLO p95 2분과 비슷해 burst 시 STARTING 실패가 날 수 있다. SLO의 측정 지점(daemon launch? Run Pod STARTED? SA 생성?)도 정의되지 않았다.
- **권고**: (1) 19.4에 측정 지점 정의: t0=QUEUED, t1=daemon launch_run, t2=Run Pod STARTED, t3=SA CR 생성, t4=driver RUNNING; p95 2분은 t0→t3. (2) 5.1에 dagster.yaml 기준값: `run_coordinator.max_concurrent_runs`(= 최대 동시 Spark 수 + 여유), `dequeue_use_threads: true`, `dequeue_num_workers`, `dequeue_interval_seconds`, Run Pod 리소스 최소(100m/256Mi), 노드 이미지 pre-pull. (3) PoC 필수 측정: Run Pod RSS/CPU(shard 로드 후), 이미지 크기, cold/warm start 분포, dequeue 처리량, kube-apiserver Job/Pod 생성률. (4) 비용 절감 대안을 PoC 비교 항목으로: 소형 Full master Job을 Source×주기 단위 multi-asset 1 run으로 묶기(1 asset = 1 job 원칙은 유지, 실행 단위만 묶음), “fire-and-observe”(submit 후 Run 종료 + sensor가 SA 완료 관찰 — Dagster run 상태와 Spark 상태 분리 트레이드오프 명시), `k8s_job_executor` 채택 금지 근거. (5) §5에 burst 500 기준 Run Pod + driver + executor 합산 vCPU/GiB 노드 용량표.

#### D-2. Job별 freshness/lateness SLO가 없다 (SCL-04)

- **문제**: 19.4의 세 단계(occurrence 2분 + queue 등록 5분 + launch 2분)를 합산하면 마지막 Job의 Spark 시작까지 p95 9분·p99 12분이고, 여기에 Source gate 대기·Spark 실행·검증·CAS가 더해진다. 1시간 주기 Job의 적재 완료가 정시+40분이어도 현재 표상 전부 “합격”이다. 반대로 Source gate 대기(정당한 보호) 때문에 queue→launch가 2분을 넘기면 **올바른 동작이 SLO 위반으로 집계**된다. 16.4 “delay/freshness breach” 이벤트의 임계값은 문서 어디에도 없다.
- **권고**: (1) Job class별 freshness SLO: lateness = FINALIZED − logical_scheduled_time, 예 1h 주기 p95 ≤ 20분, 24h p95 ≤ 60분(Phase 0 baseline으로 보정). (2) lateness를 플랫폼 지연 / 보호 지연(lease 대기) / 실행 / 검증으로 분해 기록. (3) “queue→launch” SLO는 lease 획득 후로 정의하거나 보호 대기를 제외. (4) breach 계산 주체(Dagster freshness 기능 vs Control 저빈도 lateness sensor — 원칙 5의 명시적 예외)를 정하고 16.4 임계값을 이 SLO로 고정. “기대 시각에 occurrence조차 없음”을 별도 알림 등급으로(OPS-10). (5) 19.5에 “ρ < 0.7인데 freshness SLO 미달”을 추가.

### 5.E 보안 · 운영 · 범위

#### E-1. Dagster OSS UI에는 인증·RBAC·감사가 없고 Guard는 Spark submit만 막는다 (OPS-01, DAG-05)

- **문제**: 10.2 “GraphQL이나 Dagster UI를 직접 사용해도 우회할 수 없다”는 **실행 생성 경로에 한해서만** 참이다. Guard를 거치지 않는 UI/GraphQL mutation: schedule/sensor 정지(해당 shard 수천 Job이 조용히 멈추고 Control은 Hold가 아니라 인지 못함 — Reconciler는 복구 이벤트 때만 동작), run terminate(lease·contract 상태가 흩어짐), run 삭제(contract의 dagster_run_id 증거 소실), asset wipe, 인스턴스 concurrency 편집, code location reload, FINALIZED 계약의 Re-execute. Dagster OSS에는 사용자 개념이 없어 **누가 했는지 기록이 남지 않는다**(인증·RBAC·audit은 Dagster+ 기능). 데이터 손실보다는 “조용한 지연”과 증거 소실이 실제 위험이다.
- **권고**: (a) `dagster-webserver --read-only`(CLI 옵션 확인됨: “all mutations such as launching runs and turning schedules on/off are turned off”)로 일반 사용자용 인스턴스와 플랫폼팀 전용 write 인스턴스를 분리. (b) write 인스턴스 앞에 SSO reverse proxy(oauth2-proxy 등)를 두고 GraphQL mutation 이름을 allowlist/denylist(정확한 mutation 명은 고정 버전에서 확인), proxy 로그를 감사 저장소로. (c) NetworkPolicy로 GraphQL 포트를 proxy 외 차단. (d) “schedule 정지는 Hold로만” 운영 규칙 + Reconciler의 “모든 grouped schedule RUNNING” 주기 점검(A-4). (e) Guard에 FINALIZED/ABORTED 계약 재실행 거부, RETRY는 실패 계약만 허용, `run_status_sensor(CANCELED)`로 UI terminate 감지(B-3). (f) 22.10을 “Dagster UI 노출 정책”으로 확장하고 10.2 문장을 “Spark submit과 Iceberg write는 우회 불가; 그 외 UI mutation은 Guard 범위 밖”으로 정정.

#### E-2. Control API 인증·권한·역할 분리와 Run Pod 신원이 없다 (OPS-02, CRT-02)

- **문제**: “승인자”(9.2), “명시적 승인”(14.1), “approved channel 승격”(8.2)이 반복되지만 역할(등록자/승인자/DBA/운영자/viewer), endpoint별 요구 역할, 같은 사람이 publish와 approve를 동시에 할 수 있는지(SoD) 정의가 없다. Run Pod(Guard)가 어떤 신원으로 Control API를 호출해 lease 획득·watermark CAS를 하는지가 없어 namespace 안의 임의 pod가 lease 해제나 CAS를 호출할 수 있고 AuditEvent에 actor가 비어 있다. Guard는 Run Pod 내부 client-side 검사라 Dagster 밖 경로(kubectl로 직접 CR 생성, ad-hoc asset, Guard 예외를 삼키는 고급 Template)는 막지 못한다.
- **권고**: (a) 역할 최소 집합(platform-admin, job-publisher, release-approver, dba-source-owner, operator, viewer)과 17장 endpoint별 요구 역할 표. publish↔approve·FORCE_STOP 요청↔승인의 2인 승인(SoD)은 **회사 정책 선택사항**이며 중앙 플랫폼팀만 운영하는 폐쇄망에서는 PoC blocker가 아니다 — 기술 요건은 actor 기록과 Run Pod 신원이 남아 있어 나중에 정책을 켤 수 있게 하는 것이다. (b) 사람은 사내 SSO(OIDC) JWT, 자동화는 서비스 계정 토큰. (c) Run Pod → Control API: projected ServiceAccount token(audience 지정) + TokenReview, contract 생성 시 발급한 contract-scoped 단기 토큰(11.2 lease fencing token과 통합)을 Guard가 제시해야 해당 contract의 lease/CAS만 허용. (d) AuditEvent에 actor, auth_method, source_ip, idempotency_key 필수. (e) Critical Source 한정 옵션: 서명 토큰을 SA CR annotation에 싣고 ValidatingAdmissionWebhook이 Control API로 Hold·lease 유효성을 확인해 CR 생성 자체를 거부(Hold 중 제출 불가를 K8s 수준에서 보장). 19.3에 “Guard 우회 시도(kubectl apply, Hold 중)” 추가.

#### E-3. Spark Pod에 자격증명을 전달·보호하는 방식이 없다 (OPS-03, CRT-09)

- **문제**: SecretRef의 실체(Vault? K8s Secret?), Driver/Executor 주입 시점·경로, 모든 pod가 모든 Source 자격증명을 읽을 수 있는지, JDBC URL/wallet 경로가 driver log·event log에 찍히는 문제, Polaris principal 모델(Job별? 플랫폼 단일? — Polaris는 catalog 수준 권한만 제공), Kafka publisher ACL이 없다. Credential rotation: 사내 규정상 주기적 비밀번호 교체가 있는데 Secret 반영 전후 불일치 상태에서 정시 burst가 오면 같은 ETL 계정으로 수백 회 로그인 실패 → Oracle DEFAULT profile 기준 **10회 실패에 계정 1일 잠김**([CREATE PROFILE](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/CREATE-PROFILE.html)) → 해당 Source 전체 정지. 11.1의 circuit breaker 조건은 열거되지 않았고 19.3에 인증 실패 주입이 없다.
- **권고**: (a) SecretRef = 외부 Secret 저장소 경로, Control이 contract 생성 시 Source별 K8s Secret(External Secrets Operator 동기화)을 SA spec에 참조. executor에도 wallet 마운트. (b) `spark.redaction.regex` 계열로 URL/password 마스킹(고정 버전에서 적용 범위 확인). (c) Polaris writer principal을 도메인/catalog 단위로 분리, AIStor 접근은 vended credential만(AIStor STS 호환 여부는 22장 확정), Run Pod는 read 전용 principal. (d) Kafka outbox publisher는 topic write ACL만. (e) CredentialRevision(ConnectionRevision 하위)으로 rotation을 모델링 — 실제 Spark namespace에서 로그인 테스트(VERIFIED) 전 ACTIVE 금지, contract에 credential revision 고정, SUPERSEDED revision으로 실행 중인 Run의 grace 기간과 REVOKED 시 강제 중단 여부 명시. (f) 인증 실패 코드(ORA-01017/28000/28001/28002)를 Source-level 즉시 circuit breaker + 자동 Hold 조건으로 명시(실패 1~2건에서 차단). 22장에 “현행 ETL 계정 profile 값 확인·상향” 추가, 19.3에 “비밀번호 교체 중 정시 burst” 추가.

#### E-4. schema drift 시 MVP 동작이 정의되지 않았다 (OPS-04)

- **문제**: 10,000 Oracle 테이블 중 일부는 매주 DDL이 바뀐다. 초안은 “schema drift workflow”를 Phase 4, “자동 승인”을 MVP 이후로 두었고 11.5 staging manifest의 schema hash만 있다. 기본 설정(`write.spark.accept-any-schema=false`, mergeSchema 미사용)에서는 drift가 대부분 loud failure로 나타나므로 “조용한 손상”은 제한적이지만(타입 호환 암묵 캐스팅에 한정), 실제 위험은 **검출 시점이 Oracle을 이미 읽은 뒤라 Source 예산 낭비 + 대량 실패 시 운영 경로 부재**다.
- **권고**: MVP 최소 동작을 13.1 앞단에 추가: (a) JobSpecVersion에 publish 시점 Source column list+type digest와 Target Iceberg schema id 고정. (b) Guard 단계에서 `ALL_TAB_COLUMNS`(저부하, 15.2 원칙과 양립) 조회로 digest 비교 → 불일치면 본문 쿼리 없이 `SCHEMA_DRIFT_DETECTED`로 종료 + Kafka 알림. (c) `accept-any-schema` off, mergeSchema 미사용을 명문화. (d) drift 분류표: 추가 컬럼(경고), NULLable 확대(허용), 삭제/타입 축소(차단). (e) 해결은 Wizard 재승인 → 새 JobSpecVersion → Target lease 하 schema change job → DataHub 재동기화. “자동 승인”만 Phase 4로.

#### E-5. Data Quality Check의 내용·실행 위치·실패 시 상태가 없다 (OPS-05, DOC-12)

- **문제**: 13.1 성공 경로의 “Data Quality Check”와 13.2 “DQ가 통과하면 FINALIZED”가 전부다. Source와의 row count 비교는 Oracle COUNT를 한 번 더 날려야 하므로 11장 Source 보호·15.2 “큰 테이블 COUNT(*) 금지”와 충돌한다. 6.2에 “commit은 관찰됐으나 DQ 실패” 상태가 없다 — Append에서 commit 후 DQ 실패 시 ABORTED_NO_COMMIT은 거짓이 되고, 운영자가 RETRY를 누르면 같은 window를 다시 append해 **19.5 “Append 이중 commit”**에 해당한다. 19.4 “PK 중복 0”의 측정 주체도 없다.
- **권고**: MVP DQ를 “Source 재조회 없는 검사”로 한정해 13.1에 명시: (1) extract 단계 row 수(또는 staging manifest) = snapshot summary `added-records`(Full은 `total-records`, `deleted-records` 함께 해석), (2) 대상 snapshot/partition 내 PK 유일성(Append/Merge 필수), (3) PK·watermark NOT NULL, (4) window 내 watermark max ≤ high, (5) Full: 이전 snapshot 대비 row 급감(−50%) 시 FINALIZED 보류, (6) Source COUNT는 `dq_source_count_allowed` 플래그가 있는 비Critical Source만, lease 1 token 소비로 모델링. 실행 위치는 Run Pod(Iceberg metadata만) + Spark job 말미(PK 검사), 결과를 commit evidence에 저장. 6.2에 `DQ_FAILED`(RECONCILIATION_REQUIRED 진입) 추가, **DQ 실패 시 RETRY 금지·REPLAY(MERGE repair/partition replace)만 허용**. DQ 결과를 Dagster `AssetCheckResult`로도 내보낼지 22장 결정(DAG-11).

#### E-6. 기존 10,000 Airflow Job의 이관 절차가 없다 (OPS-06)

- **문제**: Phase 3는 wave 순서만 있다. (1) 변환: 19.4 “신규 등록 median 10분”을 그대로 적용하면 10,000 × 10분 ≈ 1,667 operator-hours(자동 변환이 없다는 전제의 상한). HAflow Template/DAG 메타데이터 → JobSpec Draft converter와 커버리지 목표가 없다. (2) Shadow 비교 도구는 24개에만 정의. (3) Cutover: Airflow의 마지막 watermark를 ExecutionContract watermark로 seed하는 API(17장에 없음), Airflow schedule을 끄는 시점, 같은 Iceberg target에 두 플랫폼이 동시에 쓰지 않도록 13.3 lease에 Airflow writer를 포함하는 규칙(StarRocks에는 “동일 lease 불가 시 v1 제외” 규칙이 있으나 Airflow에는 없음)이 없다. 순서가 틀리면 Append 이중 commit 또는 window gap. (4) Job별 롤백(watermark 역인계) 절차 없음. 초안 23장이 Airflow 대체 결정을 PoC 이후로 명시하므로 PoC go 기준은 아니지만 **Phase 3 진입 조건**이다.
- **권고**: (a) “Phase 2.5 — 이관 도구”: converter(유형별 커버리지, 변환 불가 항목은 Wizard에 “확인 필요”), 지표 = 자동 변환률·변환 후 수정률. (b) Shadow 비교: 동일 window의 Airflow target vs 신규 target row count/PK hash diff를 Iceberg에서만 계산, 허용 차이 규칙을 유형별로. (c) Job별 cutover runbook: Airflow pause → 마지막 성공 run의 high watermark 확인 → `POST /v1/jobs/{id}/watermark:seed`(승인 필요, 17장 추가) → overlap 적용 첫 NORMAL → 검증 → DAG 삭제. 13.3 lease 목록에 “HAflow/Airflow 기존 writer” 추가 또는 StarRocks와 동일한 “동일 lease 불가 시 같은 target 병행 쓰기 금지” 명문화. (d) 롤백 runbook: Hold(Job) → 신규 watermark를 Airflow 변수로 역기록 → unpause. (e) 전담 인원 × 기간, wave당 Job 수 가정 명시.

#### E-7. Control PostgreSQL HA/DR과 Polaris/AIStor 장애 시 기대 동작이 없다 (OPS-09)

- **문제**: Control PG는 watermark·contract·lease·outbox의 유일한 권위(4장)인데 HA/PITR/RPO·RTO는 5.1의 Dagster PG에만 적혀 있다. 오래된 백업으로 복구되면 watermark가 과거로 돌아가 Append 이중 commit, 발급된 lease/fencing token 소실로 동시 쓰기 허용, Dagster PG에는 있는 run이 Control에는 없는 불일치가 생긴다. 두 DB를 다른 시점으로 복구했을 때의 재동기화 절차가 없다. Polaris/AIStor 장애는 19.3에 “주입한다”만 있고 기대 동작(Guard 사전 차단? 연속 실패 시 자동 Hold?)이 19.4/19.5에 없다(11.5 staging 재사용은 실행 중 Spark에 한정).
- **권고**: (a) 5장에 “Control PostgreSQL” 절: 동기 복제 HA, PITR, RPO ≤ 1분/RTO ≤ 30분, 복구 리허설 주기. (b) “Control PG 복구 후 Reconcile” runbook: Iceberg snapshot summary(etl.* 키)와 Dagster run tag로 contract/watermark를 재구성, 재구성 전까지 Global Hold. (c) Polaris/AIStor: Guard 사전 health check 실패 시 submit 차단(Source를 읽지 않음), 실행 중이면 staging 보존 후 ABORTED_NO_COMMIT, 연속 실패 시 자동 Global/Domain Hold(11.1 circuit breaker는 Source 전용이므로 별도 규정). (d) on-call·알림 등급·Runbook 목록은 별도 운영 설계 문서로 분리해도 되나 참조는 남긴다. (e) 플랫폼 전체 DR 범위 내/외를 22장에 확정.

#### E-8. MVP 범위에 첫 달 사고 대응 수단이 빠져 있다 (OPS-13, DOC-09)

- **문제**: 첫 달에 반드시 생기는 사건 — Template/mapping 오류로 잘못 적재된 window 재처리, Source 지연 데이터 재적재, Source DDL 변경 — 에 대해 MVP 수동 모드는 NORMAL/RETRY뿐이다. 8.2가 말하는 “repair backfill”을 수행할 공식 경로가 없어 운영자가 Dagster UI 비공식 재실행이나 수동 Spark로 contract/lease 체계를 첫 달부터 우회하게 된다(22.10과 직접 충돌). 14.4 “단일·다중 Job 모두 지원” vs 21 “복잡한 다중 Job Backfill UX만 이후”로 단일 Job Backfill의 MVP 포함 여부가 이중 해석된다. 21 MVP 목록의 “grouped schedule”, “target-table lease”, “Hold DRAIN/catch-up”은 Phase 1/2 산출물에 없고, 18장이 요구하는 compaction first-class Job이 MVP에 없어 MVP 기간 compaction 운영 방식이 공백이다.
- **권고**: 21장 MVP 포함 목록에 명시적으로 추가: Manual REPLAY(단일 window), 단일 Job BackfillPlan(API/CLI, UI 최소), Full RERUN_LATEST, schema drift 검출·차단(자동 승인 제외), run/event purge 도구(SCL-03). Phase × 기능 매트릭스를 만들어 20·21장을 그 매트릭스에서 파생. MVP 기간 compaction 운영 방식(기존 플랫폼 유지? lease 수동?)과 Rule-only Advisor 포함 여부 명시. DataHub 연동은 유지하되 producer 단일화만 확인.

#### E-9. Definition Bundle이 Code Location에 도달·활성화되는 프로토콜이 없다 (CRT-01, DOC-07)

- **문제**: 8.1 “Code Location은 그 Bundle만 읽는다”, 6.2 `COMPILED → VALIDATED → DEPLOYED → VERIFIED → ACTIVE`, 3장 “TP → DS” 화살표 하나. Bundle이 (a) 이미지에 포함(Dagster K8s 표준: 이미지 재빌드 + helm upgrade로 code location 롤링 재시작 — publish마다 shard 재기동, 3분 SLO와 양립 어려움)인지 (b) AIStor 등 외부에서 pull(OSS reload는 최신 state를 읽으므로 Manifest digest 검증이 어느 프로세스에서 실행되고 불일치 시 로드 실패→shard 전체 정지 vs 이전 bundle fallback 중 무엇인지)인지 선택이 없다. DEPLOYED 수행 주체(Adapter의 reload mutation? code server 재시작?), VERIFIED 판정 근거(code location이 실제 로드한 manifest digest를 누가 읽는가), code location Pod 재기동 시 ACTIVE 포인터 출처(Control API에서 받으면 Control API가 모든 기동의 hard dependency), rollback 경로가 없다. 3장 다이어그램에는 Guard→Control API 화살표가 없어 Guard가 로컬 검사로 읽히고, 10장 시퀀스에는 Guard 거부·lease 만료/FORCE_STOP 분기가 없으며 RunRequest “tags=contract_id” 외에 window/digest/descriptor hash/secret ref가 어느 매체로 전달되는지 미정이다.
- **권고**: (1) Bundle 저장소 = AIStor 불변 객체(bundle_digest 키), shard별 ACTIVE 포인터는 AIStor 포인터 객체 또는 K8s ConfigMap(Control API 비의존). (2) reload trigger = Adapter의 GraphQL reload mutation(또는 code server rolling restart), code location이 로드한 manifest digest를 Definitions metadata로 노출해 Adapter가 VERIFIED 판정. (3) 로드 실패 시 직전 ACTIVE bundle로 자동 fallback + 알림. (4) rollback = 포인터 변경 + 동일 reload 경로. (5) 6.2 각 전이에 “수행 주체 / 판정 근거 / 실패 시 상태” 3컬럼. (6) 3장에 Bundle 저장소 노드·pull/reload 화살표·G→C 화살표 추가, 10장 뒤에 실패 경로 시퀀스 3종(Guard reject, ambiguous commit, lease 만료/FORCE_STOP), “인터페이스 계약” 절 신설(run tags: contract_id만; Guard 응답: window/descriptor hash/secret ref; SA: env/ConfigMap/AIStor URI+digest; Spark→Iceberg: snapshot summary 키). publish를 정시 ±5분 밖으로 제한하는 tick blackout 규칙(DAG-07).

#### E-10. Guard 거부·lease 대기 시 Run의 동작과 우선순위·Control API 장애 정책이 없다 (CRT-04)

- **문제**: 정시 500 Run 중 한 Source(최대 동시 10)의 Job 200개가 Guard에 도달하면 190개는 lease를 얻지 못한다. 이때 (a) 즉시 FAILURE → 실패 190건·알림 폭주·retry 오작동, (b) Run Pod 안에서 대기 → idle pod 190개(D-1) + 대기 timeout 미정, (c) skip → Dagster에 skipped 상태가 없어 materialization 없는 SUCCESS. 셋 중 무엇인지, 대기 순서(logical time? Critical? 짧은 주기 우선?), 기아 방지, Control API 불가 시 fail-closed(500 run 전부 실패)인지가 전혀 없다. Dagster pool 기본 granularity는 op이고 취소/실패 시 slot이 자동 해제되지 않으며, Dagster pool과 Control lease가 둘 다 대기 지점이 되면 이중 큐가 생긴다(DAG-10).
- **권고**: (1) Source lease 결정을 Run 기동 전(queue 단계)으로 당긴다: Source별 pool을 `granularity: run`으로 쓰고 Control lease는 최종 확인용. pool 수 한계를 PoC 측정 항목에. (2) Guard 거부 사유별 Run 결과 표: HOLD → `SKIPPED_BY_HOLD` 태그 + materialization 없는 정상 종료(알림은 Hold 단위 집계), LEASE_BUSY → 즉시 종료 후 Control이 backoff 재제출(slot 점유 최소화), DIGEST_MISMATCH → FAILURE(재시도 금지), CONTROL_API_UNAVAILABLE → fail-closed + 지수 backoff 재큐 + 집계 알림 1건. (3) 대기열 규칙: lateness/period 비율, Critical 가중, Source별 round-robin, RunRequest에 `dagster/priority` 태그(공식 지원, 정수 문자열·높을수록 먼저). (4) 11.2에 `run_monitoring.enabled: true` 전제와 `run_monitoring.free_slots_after_run_end_seconds` 경로, `concurrency.pools.granularity: run` 명시. (5) 19.2에 “단일 Source 200 Job 동시 도달”, 19.4에 “Guard 거부 시 오탐 알림 0”.

#### E-11. 신규 Append/Merge Job의 초기 적재와 watermark 초기값이 없다 (CRT-05)

- **문제**: 신규 Merge Job(예: 수년치 MES EQP_EVENT)의 첫 NORMAL에서 low가 무엇인지 없다. low = 테이블 최초 시각이면 전체 테이블을 단일 window·numPartitions=1·query_timeout 1800초 안에 읽으려다 타임아웃 또는 DR full scan 부하. low = “지금”이면 과거가 영구 누락. 초기 적재가 수 시간~수 일 걸리는 동안 다음 tick의 처리도 없다. 초기 적재를 Backfill/catch-up 중 어느 메커니즘으로 하는지 미정이고 19.1 matrix의 Append-large/Merge-large 4개는 첫 실행이 곧 초기 적재다.
- **권고**: JobSpec에 `initial_load { mode: FULL_SNAPSHOT_THEN_INCREMENTAL | FROM_TIMESTAMP | NONE, start, chunk, cooldown }`. 초기 적재를 `INITIAL_LOAD` occurrence 종류(9.2에 추가)로 BackfillPlan 엔진에 위임해 chunk+cooldown+SourceSafetyEnvelope 적용. 완료 전 NORMAL tick은 occurrence만 만들고 `SUPERSEDED_BY_INITIAL_LOAD`로 닫는다. Wizard 12단계 Preview에 예상 row·시간·Source 부하와 DBA 승인 임계. 19.1에 “신규 Append-large 초기 적재” 케이스, 21 MVP에 단일 Job 초기 적재 포함.

#### E-12. Oracle→Spark→Iceberg 타입·시간대·문자집합 매핑 규범이 없다 (CRT-06)

- **문제**: Spark JDBC Oracle dialect 기본값([jdbc](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)): NUMBER[(p[,s])] → Decimal(p,s), p>38이면 소수부 절삭·38자리 초과 값은 `NUMERIC_VALUE_OUT_OF_RANGE` 실패(정밀도 미지정 NUMBER의 구체 매핑은 드라이버 버전에서 확인); DATE → TimestampType(`oracle.jdbc.mapDateToTimestamp=true` 기본)으로 Spark session/JVM 시간대에 따라 해석 — 컨테이너가 UTC면 `day(event_dt)` 파티션 경계가 KST 09:00에서 갈리고 dt 파생 컬럼·watermark 비교·Airflow shadow 비교가 모두 어긋난다; TIMESTAMP의 LTZ/NTZ 선택(`preferTimestampNTZ`)이 Iceberg timestamp vs timestamptz와 맞물리는데 미결; VARCHAR2(BYTE) 길이와 KO16MSWIN949→UTF-8 변환 시 길이 초과; CLOB/LONG/RAW; 9999-12-31 sentinel의 무의미한 미래 파티션. 이 규칙이 없으면 24개 PoC Job의 “차이 0” 판정은 Job마다 운영자의 수동 타입 수정에 의존한다.
- **권고**: 플랫폼 표준 타입 매핑표를 Template 수준에서 고정: 무정밀도 NUMBER → decimal(38,10) 기본 + dictionary 통계 기반 추천 + 범위 검증; DATE/TIMESTAMP → Iceberg timestamp(NTZ) 또는 timestamptz 중 하나를 전사 확정, `preferTimestampNTZ`·`spark.sql.session.timeZone=Asia/Seoul`·JVM TZ를 Runner 이미지에 고정; sentinel 날짜 규칙; VARCHAR2 BYTE semantics 여유. Wizard 2단계에서 위험 타입 경고. 19.1에 타입 경계 케이스(큰 NUMBER, 자정 근처 DATE, 한글 바이트 길이). 22장에 “Source NLS_CHARACTERSET·DB 시간대” 추가.

---

## 6. P2 — MVP 전 확정 (37건)

### 6.A 스케줄·상태·문서 정합

| ID | 절 | 문제 | 권고 |
|---|---|---|---|
| CRT-07 | 9.3 / 14.2 / 6.2 | occurrence 종결 disposition(COALESCED/SKIPPED_HOLD/CANCELLED/SUPERSEDED)이 없어 Reconciler 검사 범위가 Hold 구간과 겹치면 coalesce된 회차를 누락으로 재제출하고, 19.4 “중복/누락 0”의 기대 집합(분모)을 정의할 수 없음 | ExecutionOccurrence에 disposition 열 추가. Reconciler는 disposition 있는 occurrence를 충족으로 간주. 기대 집합 = cron 전개 − SKIPPED/COALESCED로 정의하고 PoC 판정 쿼리를 문서에 |
| CRT-08 | 12.2 / 7.3 | Append의 “PK 중복 검사”가 batch 내부인지 target 대비 anti-join인지 미정. Append에 overlap을 채택하면 target 대비 dedup 없이는 매 회차 중복, 하면 watermark 컬럼≠파티션 컬럼일 때 target 전체 스캔 | overlap 구간은 target의 [low−overlap, low) 파티션만 anti-join — watermark 컬럼이 파티션 컬럼(또는 변환 가능)인 경우만 Append 허용, 아니면 Merge 강제(Wizard validator). batch 내부 dedup 규칙을 JobSpec 필드로. overlap 필요 여부는 Source capability로 |
| DOC-02 | 1 / 4 / 6.2 / 11.2 / 14 | “자체 실행 상태 머신을 만들지 않는다” 선언은 타당하나 Dagster 이벤트→contract 전이 매핑표, read model 갱신 경로, DRAIN의 active 판정 기준, lease heartbeat 발신 주체가 미정의 | 1/4장 문구를 “Dagster run lifecycle을 권위로 복제하지 않는다; Control은 데이터 계약 상태와 자원 lease 상태만 소유”로 좁히고 6.2에 이벤트→전이→출처 표, 14.1에 DRAIN 완료 판정(scope의 target/source lease 모두 해제) |
| DOC-04 | 5.1 / 9.3 / 7.2 | 7.2가 허용하는 자유 cron이 9.3 “최소 1시간 주기” 전제와 정합하지 않고, daemon 장애 catch-up에 Hold 해제용 coalesce 규칙(원칙 6)이 적용되는지 미규정. 30분/5분 수치 출처 없음. 5.1 인용은 replica 제약만 뒷받침 | 7.2에서 cron을 최소 1시간 주기로 validator 검증하거나 9.3에 “catch-up 시에도 14.2 coalesce 적용” 명시. 수치 출처 각주. 5.1 인용 문장을 replica 제약으로 한정 |
| DOC-05 | 22.10 / 10.2 / 14.3 | 22.10 미결 질문을 10.2(“Critical 아니면 Guard가 Manual 계약 자동 생성”)와 14.3이 이미 결정. Guard 생성 계약의 모드·window 규칙 없음 | 22.10 삭제 후 10.2를 결정으로 승격하거나 옵션 A/B와 PoC 판단 기준 적시. Guard 생성 계약의 모드·window 규칙을 14.3에 |
| DOC-09 | 20 / 21 | Phase 2 목록·MVP 목록·“MVP 이후” 목록 불일치(E-8 참조). 7.2 3번 Advisor 단계가 필수로 오독될 수 있음 | Phase × 기능 매트릭스. Rule-only Advisor의 MVP 포함 여부 명시 |
| DOC-10 | 전체 | ADR 부재(특히 Dagster 네이티브 partitions/backfill 미사용 이유 — 4장 경계 원칙·23장 4번과 직접 충돌하므로 리뷰 보드가 반드시 제기), Control DB ERD(Occurrence:Contract:Dagster run:BackfillItem), 17장 오류 코드·재시도 가능성 표(409 계약 충돌/423 Hold/412 digest 불일치), Job cutover runbook | 4건 추가. 버전 가정(22.1)·Non-goals(1장)·리스크↔fault 대응(19.3/19.5)은 부분 충족 |
| DOC-12 | 13.1 / 6.2 | DQ 실패 시 상태 전이 공백(E-5에 병합) | E-5 참조 |
| CON-07 | 16.4 / 4 | Dagster만 아는 실패를 outbox로 반입하는 생산자 미정의(B-3에 병합) | B-3 참조 |

### 6.B 실행·Iceberg·Spark·Polaris

| ID | 절 | 문제 | 권고 |
|---|---|---|---|
| CON-08 | 13.2 / 13.1 | 증거 검색이 contract metadata가 있는 snapshot만 찾는 “양성 검색”이라 summary 누락(버전 조합·다른 thread SQL)을 “미commit”으로 오판해 retry. “상충 결과”의 정의 없음 | lease 획득 시점의 current snapshot을 `base_snapshot`으로 기록. 판정: base 이후 snapshot 0개 → ABORTED_NO_COMMIT(단 WRITER_FENCED 후); 1개 & 자기 contract → COMMIT_OBSERVED; 그 외 → RECONCILIATION_REQUIRED(자동 retry 금지). FINALIZE 시 parent 관계 검증으로 lease 우회 writer 탐지 |
| CRT-03 | 10 / 10.1 / 11.5 | compiled plan의 소비자(Spark Runner)의 정체(범용 plan interpreter vs Job별 코드), plan 포맷·버전·compiler↔image 호환 매트릭스, contract payload 전달 채널(CR에 descriptor를 넣으면 etcd 평문), Run Pod의 Polaris 읽기 principal, extract-once validation/transform 실행 단위 미정의 | “ETL Runner” Spark 앱을 명시적 구성요소로: 입력 = contract payload JSON(AIStor, CR에는 URI+digest만), 출력 = commit receipt JSON. plan schema version + 호환 매트릭스를 DefinitionRelease에. Run Pod용 Polaris read-only principal + pyiceberg. extract-once는 stage A/B 별도 SA |
| CRT-10 | 5.1 / 19.2 / 19.4 | Spark Operator 기본값(controller replicas 1, workers 10, workqueue bucket 500, webhook replicas 1·timeout 10s, submitter timeout 2m — chart 2.5 기준, 이전 버전은 controller 내부 spark-submit)이 정시 500 burst의 병목인데 측정·SLO 대상에 없음 | 19.2에 “SA 500건 동시 생성 → driver Running p95/p99”, 19.4에 “CR 생성→driver Running p95”. controller/webhook replica·workers·bucket을 PoC 변수로. lease 획득 시점을 driver 기동 이후로 재정의 검토. Operator 버전을 22.1에 |
| DAG-07 | 5.2 / 19.4 | grpc_server 방식에서 cold load 시간 = 해당 shard가 unavailable한 시간 = 그 shard 대상 tick이 실패하는 창(max_tick_retries 0). “10분” SLO가 tick 손실 창과 직결 | 정시 ±5분 tick blackout 배포 창, `max_tick_retries ≥ 1`, shard 교체 시 blue/green(새 location 기동 후 workspace 스왑), Factory의 lazy 정의 생성 |
| DAG-10 | 11.2 | pool 설명은 정확하나 `run_monitoring.enabled` 전제·`granularity`·이중 큐 누락(E-10에 병합) | E-10 참조 |
| ICE-03 | 13.2 / 12.2 / 22.6 | WAP publish 제약: cherrypick/publish_changes는 append·dynamic overwrite snapshot만 지원 → Full(static overwrite)·MERGE는 fast_forward만 가능(13.3 lease가 writer를 직렬화하므로 ancestor 조건은 충족, lease 우회 writer 있으면 실패). fast_forward는 새 snapshot을 만들지 않아 판정은 refs도 봐야 함. branch는 사전 생성·`write.wap.enabled` 필요 | 13.2 PoC 비교 항목에 위 제약 명시. ambiguous 판정 3단계: branch commit 없음 / branch commit 있고 main 미반영(fast_forward만 재시도, Oracle 재조회 불필요) / main 반영. Append Replay만 cherrypick 경로 |
| ICE-04 | 13.1 | `CommitMetadata.withCommitProperties`는 driver ThreadLocal·Callable 범위 — 한 Spark Job이 여러 SQL/chunk를 실행하면 같은 contract_id가 복수 snapshot에 붙어 13.2 “복수 commit→RECONCILIATION_REQUIRED”와 충돌. 다른 thread/async SQL은 누락. 19.4 “metadata 100%” 측정 수단 없음 | 모든 Iceberg commit을 단일 commit-wrapper로 통과시키고 `etl.contract_id, chunk_seq, statement_seq, writer_kind(ingest/maintenance/repair)` 기록. maintenance도 동일 wrapper. 13.2 판정을 “writer_kind=ingest & contract_id 일치 집합 = expected chunk 집합”으로. snapshots metadata table 스캔으로 측정 |
| ICE-05 | 13.3 / 18 | 단일 target-table lease에 compaction·manifest rewrite를 포함하면 Iceberg OCC(serializable isolation, `commit.retry.num-retries 4`, `use-starting-sequence-number` 기본 true, partial-progress)를 버리고, 대형 Append/Merge 테이블에서 compaction(수십 분)과 1시간 ingestion이 서로 밀어내 freshness 위반 또는 compaction 기아. 10k 테이블에서 수동 조정 불가 | lease를 3단계로(GPT 교차 리뷰 보정): `EXCLUSIVE_TABLE`(Full static replace, schema/partition 변경, broad Merge) / `PARTITION_OR_FILESET`(compaction·manifest rewrite — partial-progress, 정시 회피) / `APPEND`(Iceberg OCC + commit evidence만). StarRocks는 동일 fencing·evidence 지원을 확인할 때까지 `EXCLUSIVE_TABLE`. Full 테이블은 data compaction 제외. DataFrame overwrite에 `isolation-level=serializable` 기본 |
| ICE-07 | 12.4 / 7.3 | `day(event_dt)` hidden transform과 사용자 노출 dt/wt/mt/yt 파생 컬럼을 함께 두면 저장 중복 + `WHERE dt='…'` 조회 시 pruning 불가(dt는 partition 컬럼이 아님) + identity partition으로 만들면 evolution 시 새 컬럼 필요 | 둘 중 하나로 확정: (a) 원천 timestamp에 hidden transform만, dt/wt/mt/yt는 granularity 메타데이터로만; (b) Hive 호환 필요 시 identity partition(dt)만 쓰고 transform과 병행하지 않음. Preview SQL 단계에 대표 소비 쿼리의 pruning(scan files) 검증 |
| ICE-09 | 12.3 / 11.4 | MERGE cardinality 주장은 정확하나 UNION ALL 전략(B)은 INSERT_DT·UPDATE_DT 둘 다 window에 드는 row를 항상 2번 반환하므로 dedup이 상시 경로인데 tie-breaker(같은 UPDATE_DT 두 버전) 미정의. `write.merge.mode` 기본 copy-on-write라 Merge-large는 매 회 매치 파일 전체 재작성; merge-on-read면 compaction 동시성(ICE-05) 필수. “사전 검사”는 MERGE 자체 오류와 중복 | dedup 규칙 JobSpec 표준: `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY UPDATE_DT DESC NULLS LAST, <전체 컬럼 hash> DESC)=1`. 사전 검사는 `dedup 행수 = distinct pk 수` assert로 대체. JobSpec에 `merge_mode` 필드, 19.1 Merge-large에 CoW vs MoR 비교 |
| ICE-12 | 4 / 13.2 / 19.2 | Polaris 전제 미확인: principal/catalog role 모델(catalog 수준 권한만, run별 principal 비현실적), AIStor STS/AssumeRole 호환 여부(credential vending), Iceberg REST 500/502/504 = “commit state unknown”(`CommitStateUnknownException`은 파일을 삭제하지 않아 orphan 잔류), REST 스펙의 Idempotency-Key(UUIDv7) 지원 여부(버전 의존), 500 burst 동시 commit 부하 | 22장에 Polaris 버전의 Idempotency-Key 지원, AIStor STS 지원, principal/role 모델 추가. 13.2에 REST 5xx 경로를 ambiguous commit 대표 원인으로 명시, Spark Job이 예외 종류를 보고, orphan은 remove_orphan_files. 19.2에 Polaris commit p95/500 동시 commit, 19.3에 Polaris 5xx 주입 |
| CRT-02 | 10.2 / 5.1 / 3 | Guard 신뢰 경계(E-2에 병합) | E-2 참조 |
| DAG-05 | 10.2 | Dagster UI mutation(E-1에 병합) | E-1 참조 |

### 6.C Oracle

| ID | 절 | 문제 | 권고 |
|---|---|---|---|
| ORA-05 | 11.4 / 7.3 / 22-2 | AS OF SCN의 출처 미정의(standby `V$DATABASE.CURRENT_SCN`은 checkpoint SCN이라 적용 SCN보다 작다 — 이 값을 쓰면 적용된 것보다 과거를 읽음), 물리 standby의 undo는 primary 것이라 flashback 가능 범위가 primary `UNDO_RETENTION`/`RETENTION GUARANTEE`에 종속, 1800초 장기 쿼리·병렬 읽기의 ORA-01555, 12.1/14.4 “Flashback/Archive” 용어 모호(undo retention 내 Flashback Query vs primary에 별도 구성하는 Flashback Data Archive). ADG standby에서의 Flashback Query 지원 범위는 DBA 검증 | 계약에 고정할 SCN = 세션 가시 SCN(`DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER` 또는 `V$RECOVERY_PROGRESS`, ADG 정확한 함수는 DBA 확인). staging manifest의 SCN 출처를 이것으로 명시. Source capability에 retention 등록 + 예상 추출 시간 > retention×50%면 FLASHBACK_SCN 거부·chunk 강등. ORA-01555를 “재시도 금지 + chunk 축소” 오류 분류로. 12.1/14.4 용어 구분 |
| ORA-07 | 12.3 / 7.3 | `:low/:high` 바인드 표기를 쓰지만 Spark JDBC에는 바인드 옵션이 없고(query/prepareQuery/pushDownPredicate만) literal push-down. hard parse 우려는 40k/일 규모에선 미미하나, OracleDialect가 Timestamp를 `{ts '…'}`로 내려 DATE 컬럼과 비교 시 암시 변환(INTERNAL_FUNCTION)으로 인덱스·partition pruning이 무력화될 수 있음(문서 미확인, DBMS_XPLAN으로 확인). UPDATE_DT 미인덱스면 B안 두 번째 쿼리는 매시 full scan. B안 두 쿼리 SCN 불일치 시 같은 PK가 첫 쿼리에만 나타나고 뒤에 갱신되면 overlap에 의존. A/B 확정 주체 미정 | Template이 query 옵션으로 완성 SQL 생성, window 값은 컬럼 타입에 맞춘 `TO_DATE/TO_TIMESTAMP` 고정 형식, SQL 텍스트 형태 Job당 1개(plan baseline 가능). Wizard 2단계에서 UPDATE_DT 인덱스/파티션 확인 → 없으면 UNION_DEDUP 경고·인덱스 요청·Full 강등 선택. B안은 동일 AS OF SCN 필수. 실측 plan·읽기량을 Run metadata로 수집 |
| ORA-08 | 12.1 / 19.1 / 14.4 | Full 60%가 바뀌지 않은 데이터를 주기적으로 재추출. Full 계약에 Source 시점(SCN/applied_ts) 기록이 없어 snapshot이 어느 Source 시점을 대표하는지 설명 불가 → “설명되지 않은 row 차이 0” 판정 불가 | Full Job에 `change_detection: NONE | ROWCOUNT_MAXTS | TAB_MODIFICATIONS`(MAX(UPDATE_DT) 인덱스 있을 때, `DBA_TAB_MODIFICATIONS`는 flush 지연 확인 필요; COUNT(*)는 DBA 승인 테이블만) — 동일하면 `FINALIZED_NOOP`. snapshot summary에 `etl.source_scn`. Phase 0에서 Full-large의 주기당 변경 비율 측정 → Full→Merge 전환 후보. Read IO 기준을 Source·시간대별로 |
| ORA-09 | 12.2 / 12.3 / 7.3 | `NO_HARD_DELETE_CONFIRMED`는 등록 시점 선언일 뿐 이후 purge 배치 도입을 감지 못함. soft delete의 Iceberg 반영 규칙(flag 유지 vs DELETE) 없음. 정기 reconciliation의 Source 부하 예산이 용량 모델·14.4 Preview·18장에 없음 | `delete_semantics` enum: NONE_DECLARED(Critical은 publish 거부) / SOFT_DELETE(column, value, 반영 규칙) / PK_RECONCILE(주기, 창) / CDC_LATER. PK_RECONCILE은 PK(+ORA_ROWSCN)만 인덱스 스캔 → anti-join → 삭제 반영하는 first-class Asset으로, Source 용량 모델과 target lease 공유. 통계 기준 row count drift는 경고 수준, 확정은 PK_RECONCILE |
| CRT-09 | 7.1 / 6.2 / 11.1 | credential rotation·계정 잠금(E-3에 병합) | E-3 참조 |

### 6.D 운영·범위·규모

| ID | 절 | 문제 | 권고 |
|---|---|---|---|
| OPS-07 | 20 / 21 | 인원·역량(Dagster/Iceberg 숙련도, Java/Python 비율)·DBA 리드타임 가정이 없어 Phase 1(3~4주)·Phase 2(4주) 일정을 검증할 수 없음(7일 soak 자체는 Phase 1 안에 병렬 가능) | 20장 서두에 가정 표. Phase 종료 게이트를 19.5와 연결하고 일정 초과 시 범위 축소 우선순위(Wizard UI → YAML JobSpec + CLI validate/publish, 24 Job shadow → Full 8개, weighted lease → 고정 lease) 명시 |
| OPS-08 | 6 / 8.1 / 6.2 | “왜 Java인가” 근거 없음. JobSpec이 Java(Registry·validator·Wizard)와 Python(Asset Factory·Guard·Adapter·Bundle 로더)에서 모두 파싱·검증되고 COMPILED→VALIDATED는 Dagster Definitions 로드가 필요해 Python에서만 가능 → 스키마 변경마다 두 언어 동기화, 어긋나면 release FAILED 반복. Guard(Python)와 규칙 권위(Java) 분산 (판단 사항) | 옵션 A(Java 유지): JobSpec/Template/Contract를 JSON Schema(또는 protobuf) 단일 원본 + codegen, CI 동일 fixture 동치성 테스트, Python “compile service”가 VALIDATED 담당, “규칙 권위는 Java, Guard는 Control API 호출만” 원칙을 10.2에. 옵션 B(Python Control Plane). 6장에 선택 근거(팀 역량·사내 표준) 한 단락, 22장 추가 |
| OPS-10 | 16 / 16.4 | “기대 시각에 occurrence조차 없음”(schedule off, daemon 정지)을 누가 감지하는지 없음(원칙 5와의 관계). 비용 귀속 label(Job/부서) 없음. 로그 PII(업무 WHERE 바인드 값·row 샘플이 driver log/OpenSearch로) 기준 없음. Spark event log 용량 추정치 | JobSpec에 `freshness_slo`, Dagster freshness 기능 또는 Control 저빈도(5~15분) lateness sensor(원칙 5의 명시적 예외). Spark pod label에 job_id/source/owner_dept, Prometheus 집계는 dept/source 수준. 바인드 값·row 샘플 출력 금지, OpenSearch 보관·접근 권한 |
| OPS-11 | 15 | 테이블·컬럼명·comment·소유 부서(생산 공정 구조)의 외부 모델 전송 가능 여부에 대한 데이터 분류·승인 절차 없음. `gpt-5.6-sol`은 사내/외부 불명·버전 pin 없음(원칙 4와 불일치). Golden Set 선별 수치·크기·평가 합격선 없어 15.4 2단계 “통과” 판정 불가. auto-prefill의 automation bias 대책 | 15.2에 전송 가능/금지 필드 분류표와 보안 승인 절차. 허용 모델 목록(버전 pin), `model_id+version`을 AdvisorAnalysis에. Golden Set: 유형별 최소 n건, 선별 기준(최근 90일 실패율, DQ 위반 0, 부하 한도 내), 합격선(안전하지 않은 추천률 <1%, 핵심 필드 수정률 <20%). Wizard에 LLM 제안 diff 강조·고위험 항목 명시 확인 |
| OPS-12 | 8 / 17 / 5.1 | dev/stage/prod 승격 경로(Control Plane 이미지·Bundle·Spark image·Template), 환경별 Source/Polaris catalog, stage가 실제 DR을 읽을 때 envelope 공유, Dagster 업그레이드 절차(DB migration, daemon 단일 replica 공백→9.3 catch-up 연계, Adapter contract test 시점) 없음 | 환경 표(dev=synthetic, stage=DR read-only+제한 envelope, prod). envelope에 environment 차원. “Platform Release manifest”로 동일 digest 승격. 업그레이드 Runbook: pin → stage contract test → Global Hold(DRAIN) → DB migrate → Daemon/Webserver/Code Location 순차 → Run Pod 이미지 동기화 → Hold 해제 catch-up 검증 |
| SCL-03 | 5.3 / 19.2 | OSS에는 run/event log 자동 purge가 없음(retention 블록은 schedule/sensor tick만). 5.3이 archive/purge를 Runbook으로 유보했으나 7일 soak는 30/90일 bloat를 검증 못함. `delete_run`이 materialization 이력까지 지움. run당 event row 수·바이트는 가정값(60 row/1.5KB → 3.6GB/일 → 1.3TB/년, 인덱스 포함 2배) | soak 시작 전 ≥30일치 event_logs seed. 21 MVP에 purge 도구(delete_run 배치 또는 event_logs 파티셔닝+DROP PARTITION) 포함, 삭제 전 요약(status, duration, snapshot id)은 Control read model로. autovacuum·pg_repack Runbook. Spark watch 진행 로그는 compute log로만 |
| SCL-08 | 5.3 / 19.2 | K8s 객체 churn 수치(일 28만~32만 생성/삭제, steady-state ~14k, etcd write 수백만/일), Run Pod 500개의 개별 SA long-lived watch + resourceVersion 만료 동시 재접속, TTL 값(`ttlSecondsAfterFinished`, SA TTL) 미제시 | 5.3에 객체/일·steady-state·etcd 쓰기 표와 TTL 값. 10.1 adapter에 label selector 공유 list+watch 또는 폴링(15초) 선택지. 전용 namespace·etcd 모니터링(object count, db size, apiserver p99) 임계값 |
| SCL-09 | 11.5 / 12.1 / 18 / 16.2 | AIStor 용량 계획 수치 전무(B-6에 병합) | B-6 참조 |
| SCL-10 | 19.2 | stub Source가 “즉시 응답하는 빈 테이블”이면 lease·한도 초과 0건·Run Pod 동시 점유·Polaris commit 경합·AIStor IOPS가 측정되지 않음. 7일 soak 기록 항목이 추상적 | stub 요건: 동시 connection 카운터 + 상한 초과 시 ORA-00018 유사 거부, Phase 0 D 분포(lognormal)로 지연 주입, 테이블별 row 수 분포로 실제 Iceberg commit 수행, Source 30개 × C 한도 재현, 일부 Source 장애 시나리오. soak 기록: 시간대별 run 수/lateness 분해, Source별 ρ·lease 대기, Run Pod RSS/start, tick 소요·실패, dequeue 처리량, event_logs 증가·bloat·autovacuum, etcd 객체/db size/apiserver p99, Polaris commit latency·conflict retry, AIStor IOPS·용량, OpenSearch ingest. 합격 기준에 “7일간 추세선 기울기 0 수렴(큐 깊이, DB 증가율, lateness)”. stress에서 병목 순위표 |

---

## 7. P3 — 문서 정리·근거 보강 (21건)

| ID | 절 | 요지 | 조치 |
|---|---|---|---|
| CON-09 | 9.2 / 13.3 / 14.3 | target lease는 시간축 직렬화만 제공. REPLAY/BACKFILL window가 현재 watermark 이상 구간과 겹칠 때 허용 규칙 부재(PK 검사·repair 정책으로 데이터 중복은 막힘). Guard의 job window → target lease → source token 획득 순서 미명시 | job 단위 열린 window를 exclusion constraint(`job_id =, window &&`)로 채널 무관 배타. REPLAY/BACKFILL은 `window.high ≤ current watermark`일 때만. 고정 lock 순서 + bounded try-lock + 역순 해제 |
| CON-10 | 8.2 / 10.2 | 큐 대기 Run이 contract의 pinned digest로 실행됨은 이미 의도(10.2/8.2). Guard 불일치 시 pinned 우선임을 명문화하고, asset key/partition def 같은 실행 인터페이스 변경은 12.4처럼 release change로 취급해 열린 contract를 `SUPERSEDED_BY_RELEASE`로 마감 | 한 문장 명문화 + DefinitionRelease에 OPEN_CONTRACT_CHECK 게이트 |
| CON-11 | 9.3 | Dagster 설정값을 문서에 고정: `max_tick_retries ≥ 1`, `max_catchup_runs`(파티션 전용), `run_monitoring.start_timeout_seconds/max_runtime_seconds`. 연속 탐지기는 원칙 5와 충돌하므로 초안의 승격 경로 유지 | A-2/A-3의 stale 검사·heartbeat gap 트리거로 충족 |
| CON-12 | 13.1 / 6.1 | contract:Dagster run = 1:N인데 binding이 단수 | ExecutionAttempt(contract_id, attempt_no, dagster_run_id, spark_application_name, lease ids, base_snapshot, state). 판정을 “contract당 정확히 한 attempt가 commit”으로(B-1과 동일 뿌리) |
| CRT-11 | 16.4 / 7.3 / 7.2 | JobSpec에 owner/severity가 없어 Source 단위로만 라우팅 가능. outbox event 스키마·dedup key·failed↔recovered 짝 맞춤 규칙 없음 | JobSpec `ownership {team, contact, severity_class, quiet_hours}` + DataHub 동기화. CloudEvents 유사 스키마(event_id, type, schema_version, scope, severity, owner, dedup_key, correlation_id). Source 장애 시 Job 이벤트 억제·Source 단위 1건 |
| CRT-12 | 23 / 22 / 5.3 | 채택 판단에 비용 축 없음(run당 오버헤드, Full 재적재, staging 이중 저장, snapshot 보관) | 22장에 비용 모델 추가, Phase 0에서 현행 Airflow 비용을 같은 단위로 측정, 19.4에 “비용/run ≤ 현행 × k” 잠정 기준 |
| DAG-09 | 8.2 / 원칙 4 | K8sRunLauncher는 run 생성 시 저장된 `job_code_origin.container_image`로 launch하므로 큐 대기 run은 생성 시점 이미지 사용(8.2 기대와 부합). 잔여 이슈: RETRY는 새 run이라 현재 code location 이미지 → 14.3 “RETRY 기존 digest 고정”과 rollback 후 RETRY(결함 digest)가 충돌 | 8.2/14.3에 “rollback 후 RETRY는 REPLAY(새 계약, 현재 승인 digest)로 승격” 규칙, Guard 거부 시 Control 자동 재제출 |
| DAG-11 | 14.4 / 13.1 / 1 | Backfill/Hold를 Control에 두는 이유는 이미 기술됨. asset checks·freshness policy 재사용 여부만 미기재 | 13.1 DQ 결과를 `AssetCheckResult`로 내보낼지, freshness 표시를 Dagster에 위임할지(알림 원천은 Control) 22장 결정 |
| DAG-12 | 10.1 / 16.2 | Iceberg snapshot summary가 1차 증거 채널로 정의됨. Driver 산출 row/file 통계·DQ 결과를 Dagster materialization metadata로 옮기는 경로만 미정 | 10.1에 `open_pipes_session` + AIStor PipesMessageReader(또는 staging manifest)를 선택적 이중 증거 채널로 |
| DOC-06 | 6.1 / 8.1 / 8.2 | Occurrence:Contract 관계(1:1/1:N), DefinitionRelease/Bundle/Manifest/channel 관계와 approved channel의 물리적 실체, Critical의 판정 주체(Source 속성 vs Job 속성)·저장 위치 미정의 | 용어집 신설(shard/Code Location, safe high watermark 구성요소, lease vs pool 우선순위는 이미 정의됨) |
| DOC-08 | 19.4 / 8.1 | 19.4 항목과 19.5 No-Go의 1:1 대응 없음. “상위 10개 유형” 분류 기준 없음(60/20/20·주기는 입력값이므로 제외) | 19.4에 No-Go 여부 열 추가. 8.1 유형 분류표(유형명, Job 수, 대표 Job) 부록 |
| DOC-11 | 참고 자료 / 11.3 / 5.1 | Oracle 26 haovw URL(존재·V$DATAGUARD_STATS 포함)이 본문 어디에도 인용되지 않음. Oracle 인용 버전 19c/26 혼재. 5.1 인용은 replica 제약만 뒷받침 | 11.3 1번에 haovw 인용 연결. 22.2 확정 버전에 맞춰 통일. 5.1 문장 한정 |
| DOC-13 | 9.3 / 원칙 8 / 13.2 / 11.4 | “Reconciler”(누락 tick 복원), “RECONCILIATION_REQUIRED”(commit 판정), “정기 reconciliation”(데이터 대조)이 같은 어휘 — 16.4 알림 수신 운영자가 어느 절차를 밟을지 불명 | Schedule Gap Recovery / Commit Adjudication / Data Reconciliation Audit로 분리 명명 |
| ICE-08 | 18 / 5.3 | Template CREATE TABLE 기본 속성 미정: `write.metadata.delete-after-commit.enabled`(기본 false → metadata.json 무한 누적, 시간 주기 테이블 연 8,760개), `previous-versions-max`(기본 100), `history.expire.*` 등급별, WAP 테이블의 `max-ref-age`·branch RETAIN(기본 영구). maintenance run 수(테이블별 vs namespace batch)가 19.2 부하에 없음. `remove_orphan_files older_than`(기본 3일) > staging TTL + 최장 Spark 실행시간 규칙 | 18에 속성 표, namespace batch maintenance, 19.2에 maintenance 부하, 11.5/18에 orphan 규칙 한 줄 |
| ICE-10 | 11.2 / 11.1 | “1 slot ≈ 1 connection”은 executor 세션만 센 표현. driver의 schema 해석·SCN 조회 세션(짧음)을 weight에 반영(C-2에 병합). Registry MetadataSnapshot으로 `customSchema`(공식 옵션) 주입해 driver schema 조회 축소 검토 | C-2 참조 |
| ICE-11 | 11.5 / 13.2 / 12.2 | staging(extract 재사용)과 WAP(publish 원자성)는 목적이 다르므로 통합보다 역할 경계 명시. custom staging 유지 시 manifest 스키마·Parquet 포맷·schema hash 규칙(Spark StructType 정규화)·TTL 정리 Asset·Replay SCN 불일치 거부가 미정의 | PoC 비교에 “Iceberg landing 테이블(namespace raw_stage, contract_id partition)” 대안 추가. Phase 2 Shadow의 staging 공유도 같은 메커니즘 |
| ORA-11 | 19.4 / 19.5 | Oracle 측 SLO 행 없음: 정합성 비교 기준 시점(계약의 세션 가시 SCN, undo retention 밖은 비교 불가), Run당 최대 세션 ≤ lease weight(`V$SESSION` 샘플), ORA-02391/03172/01555 발생 0 또는 의도된 fail-fast만, apply lag p95·DEGRADED_CONFIDENCE 비율, Source별 ρ. 19.5에 “source role mismatch 감지 시 자동 Hold 미작동” | C-1~C-5 결과를 19.4 표에 반영 |
| SCL-06 | 5.2 / 19.4 | 측정 항목은 갖췄으나 publish 가시화 p95 3분의 단계별 예산(compile+validate ≤60초, 승격 ≤10초, shard reload ≤60초, webserver 갱신 ≤30초) 분해와 publish quiet window 정책 없음. reload 중 tick 실패는 다음 평가에서 복구되므로 지연에 그침 | 예산 분해 + quiet window(E-9의 tick blackout과 동일) |
| SCL-07 | 11.2 / 19.4 | §5에 burst 500 기준 Spark driver/executor 자원 합산표(driver 1vCPU/2GiB + executor 2vCPU/4GiB 가정 → 순간 1,500 vCPU/3TiB, 평균 가동률 ~28%)와 전 Source session 합계 vs DR `processes` 파라미터 비교 없음. “순간 20 session” 단정은 근거 약함(driver 세션은 순차) | 용량표 추가, 소형 Full master의 driver-only 실행 profile을 PoC 비교 항목으로, cron offset 분산 목표(500 → 5분 단위 분산 시 최대 120) |
| SCL-11 | 16.2 / 16.4 | `run_id` 제외만으로는 부족 — `job_id` label 자체가 10k × metric × status ≈ 100만 series. 16.4 “동일 Source 폭주 억제” 창/건수 임계값 없음 | Prometheus는 source/template/shard/mode 집계만, Job별 lateness는 Control read model(+Grafana PostgreSQL datasource). 동일 source_id 실패 60초 내 N≥10이면 Source-level 1건, 창 ≤30초 |
| SCL-12 | 5.1 / 9.3 | Daemon이 :55~:25 사이에 죽으면 복구 직후 30분 누락분(~833) + 정시 500 ≈ 1,300건이 동시에 queue — Burst 가정의 2.6배. `max_catchup_runs`는 grouped schedule에서 run 수 상한이 못 되고 실제 상한은 `max_concurrent_runs`(기본 10)와 pool | 9.3에 복구 직후 최대 동시 제출 상한과 재정렬 우선순위(짧은 주기·Critical). Full은 coalesce로 catch-up 수 축소. 19.3에 “정시 직전 daemon kill → 복구 후 동시 제출 수·start_timeout 실패·SLO 위반” 측정 |

---

## 8. 절별 수정 체크리스트

초안 저자가 절 순서대로 반영할 수 있도록 finding을 절에 매핑했다.

| 절 | 반영할 항목 |
|---|---|
| 1 결론 / 2 원칙 | 원칙 5 재서술(A-1), 1/4장 “상태 머신” 문구 범위 축소(DOC-02), 재사용/재구현 결정(DAG-11) |
| 3 논리 아키텍처 | Bundle 저장소 노드·pull/reload 화살표·G→C 화살표(E-9) |
| 4 책임표 | “Dagster run 종료 사실의 Control 반입 = run status sensor”(B-3) |
| 5.1 Dagster | dagster.yaml 기준값 표(§9), Control PostgreSQL 절(E-7), 노드 용량표(D-1), 재접속 정의 정정(B-2), 30분 수치 출처·인용 한정(DOC-04) |
| 5.2 shard | lazy 정의 생성 규약, tick blackout, blue/green(DAG-07, SCL-06) |
| 5.3 보관 | 증거/snapshot 분리(B-6), K8s 객체·TTL 표(SCL-08), SA TTL 하한(B-4), purge 도구(SCL-03), Storage 용량 모델(B-6) |
| 6.1/6.2 Aggregate·상태 | occurrence 키·pinned 속성(CON-01), 상태 순서·전이 주체(A-2), 종결 상태 5종(A-6, CRT-07), ExecutionAttempt(CON-12), DQ_FAILED(E-5), CredentialRevision(E-3), DefinitionRelease 전이 3컬럼(E-9), ERD(DOC-10) |
| 7.1 Source | 역할 검증·standby 전용 service·ADDRESS allowlist(C-4), `max_open_txn_seconds`·retention·NLS·시간대 capability(C-1, ORA-05, E-12), SecretRef 실체·rotation(E-3), Profile/Resource Manager(C-3) |
| 7.2/7.3 Wizard·JobSpec | `watermark_semantics`, `initial_load`, `delete_semantics` enum, `merge_mode`, overwrite 모드, `change_detection`, `ownership`, `freshness_slo`, 타입 매핑 경고, UPDATE_DT 인덱스 확인, 용량 Gate(C-5), cron 최소 주기 validator(DOC-04) |
| 8.1/8.2 Bundle·Release | 전달·활성화·fallback·rollback 프로토콜(E-9), rollback 후 RETRY→REPLAY(DAG-09), OPEN_CONTRACT_CHECK(CON-10), 환경 승격(OPS-12) |
| 9.1 정상 경로 | batch endpoint·timeout·fail-closed(A-1), schedule 이름·default_status(A-4), staggered delay(C-5) |
| 9.2 unique key | CON-01, 모드×키×충돌 표(A-7), CATCHUP/INITIAL_LOAD 채널(A-5, E-11) |
| 9.3 장애 복구 | catch-up 문구 정정·heartbeat gap 트리거(A-3), PLANNED stale 검사(A-2), 복구 직후 제출 상한(SCL-12), 용어 분리(DOC-13) |
| 10 / 10.1 / 10.2 | 실패 경로 시퀀스 3종·인터페이스 계약 절(E-9), attempt·SA 이름·spec 불변(B-1, B-4), Guard 첫 단계 증거 조회(B-2), chunk별 Guard(A-6), 거부 사유별 Run 결과 표(E-10), “우회 불가” 문장 정정(E-1), Runner 구성요소(CRT-03), Pipes 이중 증거(DAG-12) |
| 11.1~11.5 | DB-enforced limit(C-3), 2단계 회수·heartbeat 주체·weight 정의·V$SESSION fence(C-2), high 산출 규칙·DEGRADED_CONFIDENCE와 CAS(ORA-01), SCN 출처·ORA-01555(ORA-05), staging 스키마·landing 테이블 대안(ICE-11), pool 설정(E-10) |
| 12.1~12.4 | overwrite 모드·0-row(B-5), change_detection·source_scn(ORA-08), Append overlap/dedup 범위(CRT-08), dedup 규칙·merge_mode(ICE-09), literal/타입 변환(ORA-07), hard delete enum(ORA-09), partition 역할 확정(ICE-07) |
| 13.1~13.4 | DQ 정의(E-5), commit-wrapper·writer_kind(ICE-04), base_snapshot 구간 판정(CON-08), WRITER_FENCED(B-1), WAP 제약·3단계 판정(ICE-03), lease 분리(ICE-05), window 계산 시점·RETRY 유효성·COALESCED(A-5), Airflow writer(E-6) |
| 14.1~14.4 | chunk Guard·DRAIN/FORCE_STOP 프로토콜·SKIPPED_BY_HOLD(A-6), catch-up 소요 반환·자동 coalesce(C-5), 모드 표와 키 대응(A-7), 초기 적재(E-11) |
| 15 | 데이터 분류·모델 목록·Golden Set·diff UI(OPS-11), 타입 매핑 추천(E-12) |
| 16 | freshness 계산 주체·임계값(D-2, OPS-10), Prometheus label 정책·집계 규칙(SCL-11), 이벤트 스키마·ownership(CRT-11), source role mismatch·credential failure 이벤트(C-4, E-3), 마스킹·PII(E-3, OPS-10) |
| 17 Control API | batch occurrence, dagster-terminal-event, chunks:begin, watermark:seed, 역할 표, 오류 코드 표, Run Pod 인증(A-1, B-3, A-6, E-6, E-2, DOC-10) |
| 18 Maintenance | ingest-lease/maintenance 분리(ICE-05), 테이블 속성 표·namespace batch·orphan 규칙(ICE-08) |
| 19.1~19.5 | matrix 추가 케이스(초기 적재, 타입 경계, CoW/MoR, 0-row), stub 요건·soak 기록(SCL-10), 측정 지점·freshness·Oracle 행·No-Go 대응 열(D-1, D-2, ORA-11, DOC-08), fault 추가(republish 직후, switchover, 비밀번호 교체 중 burst, Guard 우회, Polaris 5xx, 단일 Source 200 Job, 정시 직전 daemon kill) |
| 20 / 21 | 가정 표·게이트·축소 우선순위(OPS-07), Phase 2.5 이관 도구·cutover/롤백 runbook(E-6), Phase×기능 매트릭스·MVP 추가 항목(E-8) |
| 22 확정 항목 | §10 참조 |
| 23 최종 판단 | 비용 축(CRT-12) |

---

## 9. dagster.yaml / 운영 설정 기준안 (초안에 없는 값)

PoC 배포 기준안(5.1)에 명시해야 할 값이다. 정확한 키 이름은 고정 버전의 문서에서 재확인한다.

| 영역 | 설정 | 근거 |
|---|---|---|
| Run coordinator | `max_concurrent_runs` = 최대 동시 Spark 수 + 여유(기본 10), `dequeue_use_threads: true`, `dequeue_num_workers`, `dequeue_interval_seconds` | D-1 |
| Run monitoring | `run_monitoring.enabled: true`, `start_timeout_seconds`(burst 고려), `cancel_timeout_seconds`, `free_slots_after_run_end_seconds`, `max_runtime_seconds` | B-2, B-3, E-10 |
| Run retries | `run_retries.enabled: true`, `retry_on_asset_or_op_failure: false`(crash만) | B-2 |
| Schedules | `max_tick_retries ≥ 1`, `use_threads/num_workers`, `DAGSTER_SCHEDULE_GRPC_TIMEOUT_SECONDS` 명시 | A-1 |
| Concurrency pools | `concurrency.pools.granularity: run`, Source별 pool | E-10 |
| Schedule 정의 | `default_status=RUNNING`, 결정론적 이름 | A-4 |
| Webserver | 일반 사용자 `--read-only` 인스턴스 + 플랫폼팀 write 인스턴스(SSO proxy) | E-1 |
| Retention | event log purge 도구(OSS 미제공), 온라인 보관 기간 | SCL-03 |

---

## 10. 22장 “채택 전에 확정할 항목”에 추가할 것

기존 10개 항목에 더해:

11. Occurrence 키 = (job_id, logical_scheduled_time), digest는 contract pinned 속성 (CON-01)
12. watermark high 산출 규칙(standby 적용 시점 기반)과 `DEGRADED_CONFIDENCE` 시 CAS 정책 (ORA-01)
13. AS OF SCN·applied SCN의 출처(세션 가시 SCN), Resource Manager의 ADG standby 적용 여부, 현행 ETL 계정 profile 값 (ORA-05, C-3, E-3)
14. `watermark_semantics`(APPLICATION_TIME/COMMIT_SCN)와 Source별 `max_open_txn_seconds` (C-1)
15. Source별 용량 모델(ΣD/C)과 publish Gate 임계(ρ 0.7/1.0) (C-5)
16. Bundle 전달·활성화·fallback 프로토콜과 ACTIVE 포인터 위치 (E-9)
17. Dagster UI 노출 정책(read-only 분리, proxy, 감사) — 22.10 확장 (E-1)
18. Control API 역할 모델·SoD·Run Pod 신원 (E-2)
19. Secret 저장소·주입 경로·Polaris principal 모델·AIStor STS 지원 여부·Polaris Idempotency-Key 지원 여부 (E-3, ICE-12)
20. 타입 매핑 표준(Iceberg timestamp vs timestamptz, session timezone), Source NLS_CHARACTERSET (E-12)
21. Full 테이블 snapshot 보관 등급과 증거 영속화 방식 (B-6)
22. MVP DQ 검사 집합과 DQ 실패 시 RETRY 금지·REPLAY 허용 규칙 (E-5)
23. schema drift MVP 동작과 drift 분류표 (E-4)
24. 기존 Job 이관 도구·watermark seed·cutover/롤백 runbook, Airflow writer의 lease 포함 여부 (E-6)
25. Control PostgreSQL HA/RPO/RTO, 플랫폼 전체 DR 범위 (E-7)
26. Java vs Python Control Plane 선택 근거와 JobSpec 스키마 단일 원본 방식 (OPS-08)
27. 팀 규모·역량·DBA 리드타임 가정과 Phase 종료 게이트 (OPS-07)
28. 비용 모델과 현행 대비 잠정 기준 (CRT-12)
29. asset checks·freshness policy 재사용 여부 (DAG-11)
30. Spark Operator 버전·제출 경로(submitter vs controller 내부)와 replica/worker 값 (CRT-10)

---

## 11. 권장 다음 단계

1. **초안 v1.1 개정**: §1.4 “① v1.1 선반영” 항목을 반영한다. 서로 얽혀 있으므로(attempt_no, 상태 종결, window 계산 시점, release 고정) 한 번에 고치는 편이 낫다 — 상태 머신을 `Occurrence(disposition) ⊃ Contract(pinned release/digest, window, current_attempt) ⊃ Attempt(run binding, SA name, base_snapshot, state)` 3층으로 다시 그리면 CON-01/02/03/04/06/12, ICE-06, DAG-03/04가 한 그림에 들어온다. batch endpoint·WAP·compaction 동시성 같은 성능/호환 가설은 v1.1 본문이 아니라 PoC 기준서(2번)로 보낸다.
2. **PoC 설계 문서 분리**: 19장을 별도 문서로 빼고 stub Source 요건(SCL-10), 측정 지점(D-1), freshness SLO(D-2), Oracle 행(ORA-11), 추가 fault 7종, No-Go 대응 열을 넣는다.
3. **DBA 확인 세션**: ORA-01/05(적용 SCN·세션 가시 SCN), C-3(Resource Manager on ADG, Profile), C-4(role-based service), E-3(계정 profile, rotation), C-1(`max_open_txn_seconds`)을 한 번에 확인한다 — 모두 초안이 아닌 DBA가 답해야 하는 질문이다.
4. **보안 설계 1장 추가**: E-1/E-2/E-3을 묶어 “신뢰 경계와 신원” 절을 신설한다. MVP가 실제 DR에 붙기 전(Phase 2 진입)이 데드라인이다.
5. **이관·MVP 범위 재정의**: E-6/E-8/OPS-07을 묶어 Phase 2.5와 Phase × 기능 매트릭스를 만든다. 일정은 팀 가정을 적은 뒤 조건부로 다시 쓴다.

초안의 결론(“Dagster-first를 채택 PoC로 진행하되 No-Go 통과 전에는 Airflow 대체를 결정하지 않는다”)은 그대로 유효하다. 이 리뷰는 그 PoC가 **무엇을 증명해야 하는지**를 더 정확하게 만들기 위한 것이다.
