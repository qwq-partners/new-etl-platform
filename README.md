# ETL Platform — 목표 아키텍처 · PoC 기준서 · 교차 리뷰 기록

Dagster OSS + 얇은 Java Control Plane(PostgreSQL) 기반 신규 ETL 플랫폼의 설계 문서와, 그 설계를 여섯 차례 교차 리뷰로 검증한 기록이다.

- **규모**: Job 약 10,000개 · Run 약 40,000건/일 · 정시 burst 약 500건
- **적재 경로**: Oracle physical standby(ADG) → Spark → Iceberg / Polaris
- **적재 모드 비중**: Full 60% · Append 20% · Merge 20%

---

## 1. 지금 어디에 있는가

| 단계 | 상태 |
|---|---|
| 목표 아키텍처 v1 → v1.2.3.1 | **완료**(다섯 차례 교차 리뷰 반영) |
| **제약 변경**: Oracle 원천 DBA 협조 불가 확정(2026-08-24) | v1.2.3.1이 전제하던 DBA 의존이 무효화됨 |
| Profile U(무권한) 재설계 범위 제안 | 작성 완료 · 6차 교차 리뷰 완료 · **v2.0 규격 동결은 NO-GO** |
| **G0-0 실측** | 산출물 준비 완료 · **한 번도 실행되지 않음** ← 지금 여기 |
| 감축 1차 | **완료**(2026-08-27) — 변경 이력 분리로 −16.2%. 나머지는 G0-0 이후 |
| DBA 권한 요청 방향 | **보류**(2026-08-27) — 받아도 순이익이 아니거나 요청서가 틀렸다 |
| A v2.0 / P v2.0 규범 개정 | G0-0 결과 확정 후 착수 |

**핵심 원칙**: 실측(G0-0) 전에는 규범 문서를 대규모로 고치지 않는다. 실측 하나가 여러 절의 상태·enum을 뒤집기 때문이다.

---

## 2. 문서 지도

### 2.1 목표 아키텍처 (A 계열)

| 파일 | 설명 |
|---|---|
| `etl-platform-target-architecture-v1.md` | 최초 초안 |
| `etl-platform-target-architecture-v1-review.md` | 그 초안에 대한 상세 리뷰(97건) |
| `…v1.1.md` → `…v1.2.md` → `…v1.2.1.md` → `…v1.2.2.md` → `…v1.2.3.md` | 교차 리뷰를 반영한 개정 계열 |
| **`…v1.2.3.1.md`** | **현행 최신 규범**(1,533줄 — 변경 이력 분리 후). DBA 협조를 전제하므로 Profile O로 보존 |

### 2.2 PoC 시험·합격 기준서 (P)

| 파일 | 설명 |
|---|---|
| **`etl-platform-poc-test-plan-v1.md`** | 8차(v1.2.3.1 정합, 649줄). FI 66건 · SC · H 가설 · 4층 판정 oracle · G0/G1/G2 게이트 |

### 2.3 교차 리뷰와 검토서

리뷰는 외부 리뷰어(Codex)가, 검토서는 이쪽에서 각 지적을 1차 출처로 재판정한 결과다.

| 회차 | 리뷰 | 검토서 |
|---|---|---|
| 1차 | `etl-platform-v1.2-codex-cross-review.md` | `…-assessment.md` |
| 2차 | `etl-platform-v1.2.1-codex-second-cross-review.md` | `…-second-review-assessment.md` |
| 3차 | `etl-platform-v1.2.2-codex-third-cross-review.md` | `…-third-review-assessment.md`(v3.1 — 4차 확인 반영) |
| 5차 | `etl-platform-v2.0-codex-cross-review.md` | `etl-platform-v2.0-codex-review-assessment.md` |
| 6차 | `etl-platform-v2.0-codex-review-assessment-recheck.md`(재검증) | — |

리뷰 요청서: `codex-cross-review-prompt.md`(1차) · `codex-cross-review-prompt-v2.0.md`(v2.0)

### 2.4 Profile U 재설계

| 파일 | 설명 |
|---|---|
| **`etl-platform-v2.0-unprivileged-redesign-scope.md`** | DBA 없는 세계의 재설계 범위 제안. 죽는 것 / 살아남는 무권한 수단 / 보증 축 재정의 / 결정 4건 |
| **`etl-platform-v2.0-capability-overlay.md`** | 이기종 원천 대응. 코어는 권한 0·최저 버전에서 성립하고 capability 는 원천별 측정 오버레이로 붙는다 |
| **`etl-platform-v2.0-simplification-decision.md`** | 감축 결정 기록. 무엇을 잘랐고 **무엇을 왜 안 잘랐는지** |
| **`etl-platform-v2.0-grant-request-verdict.md`** | DBA 권한 요청 방향 판정 — **보류**. 후보 37건 중 검증 9건이 전부 기각된 이유 |
| `CHANGELOG.md` | 아키텍처 변경 이력(규범 아님). 본문에서 분리 |

---

## 3. G0-0 실측 산출물

**실행 순서: A → B0 → B1 → C00 → C01~C09.** G0-0 결과가 A v2.0 / P v2.0의 여러 분기를 결정한다.

| 게이트 | 파일 | 대상 | 안전 등급 |
|---|---|---|---|
| **G0-0A** | `g0-0a-capability-inventory.sql` | 계정 권한·capability·**원천 이식성** 실측(78 probe) | 운영계 가능(대상 테이블 접촉 `ROWNUM=1` 3건) |
| **G0-0B0** | `g0-0b0-spark-smoke.py` | stock Spark JDBC 경로 관측 | 운영계 제한적(ROWNUM 제한) |
| **G0-0B1** | *(미구현)* | 커스텀 `JdbcConnectionProvider`가 schema·metadata·task 3경로를 덮는지 | — |
| **G0-0C00** | `g0-0c-fence-facts.sql` | fence 반례 fact collector | 운영계 제한적(`ACK_FULL_SCAN` 게이트) |
| **G0-0C01~C09** | `g0-0c-counterexamples/` | stateful counterexample harness (9종 구현 완료) | **폐기용 쓰기 가능 환경 전용** |

안내: `g0-0-probe-README.md` — 안전 규칙 · 실행법 · 결과 → 설계 분기표 · **이 프로브가 증명하지 못하는 것**
반례 harness 전용 안내: `g0-0c-counterexamples/README.md` — 환경변수 · 종료 코드 · 시나리오별 증거 형태

### 3.1 안전 규칙 (요약)

1. 읽기 전용 프로브는 DDL·DML·job 생성이 **한 줄도 없다**.
2. **잘못된 비밀번호를 절대 시도하지 않는다** — 계정 잠금은 전체 파이프라인 정지다.
3. 비밀번호를 **명령줄 인자에 넣지 않는다**(`/nolog` + stdin, 또는 환경변수).
4. 모든 판정은 실제 `SQLCODE`에서 나온다. 성공을 가정한 리터럴 출력이 없다.
5. C01~C09는 `environment_guard`가 통과하지 않으면 **한 줄도 실행하지 않는다**.

---

## 4. 이 저장소의 이력이 말해 주는 것

여섯 차례 교차 리뷰에서 반복해 잡힌 결함 유형은 하나다 — **문서로 확인되지 않은 것을 확인된 것처럼 쓴 문장**. 대표적인 예:

- `SYS_CONTEXT('USERENV','CON_DBID')` — 19c USERENV에 없는 속성(ORA-02003)
- `executionMetadata.runId` — Dagster GraphQL 스키마에 없는 입력 필드
- `sessionInitStatement` — Spark의 schema/metadata connection은 이것을 실행하지 않는다
- `AS OF SCN` — `SELECT`만으로는 불가(객체별 `FLASHBACK` 권한 필요)
- `MAX(watermark)` fence — 반개구간과 결합하면 tail 행이 지연·누락된다

그래서 이 저장소의 규칙은 다음과 같다.

> **확인하지 못한 것은 "미확인"이라고 쓴다. 오류 부재는 증거가 아니다. 0건 조건에는 양성 대조를 함께 둔다.**

---

## 5. 다음 작업

1. **G0-0A 실행** — 분기점 두 줄: 대상 테이블의 `FLASHBACK` 객체 권한, `versions.lock`의 Spark 버전
2. **G0-0B1 구현** — 커스텀 `JdbcConnectionProvider` tracer. 이것이 성립하지 않으면 Profile U의 세션 단언 모델 전체가 서지 않는다
3. **C01~C09 실행** — 폐기용 환경 확보 후
4. capability 확정 → **A v2.0 / P v2.0**(단일 core + ConnectionRevision capability overlay, v1.2.3.1은 archive)
