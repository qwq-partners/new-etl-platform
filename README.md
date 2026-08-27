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
| **제약 변경**(2026-08-24, 표현 통일 2026-08-27) | **정합성을 DBA 협조에 걸 수 없다.** 비critical 권한 요청은 가능하나 **현재 보류이며 절대 가정하지 않는다.** v1.2.3.1이 전제하던 DBA 의존이 무효화됨 |
| Profile U(무권한) 재설계 범위 제안 | 작성 완료 · 6차 교차 리뷰 완료 · **v2.0 규격 동결은 NO-GO** |
| **G0-0 실측** | **S1~S3 최초 실행 완료**(2026-08-27, profile `SANDBOX_CONTAINER`) · S4~S8 미실행 ← 지금 여기 |
| 감축 1차 | **완료**(2026-08-27) — 변경 이력 분리로 −16.2%. 나머지는 G0-0 이후 |
| DBA 권한 요청 방향 | **보류**(2026-08-27) — 받아도 순이익이 아니거나 요청서가 틀렸다 |
| 로컬 G0-0 실행 계획 | 작성 완료(2026-08-27) — S0~S8. **S2(B1 컴파일)가 최속 신호** |
| **S1~S3 실행 결과** | `g0-0-s1-s3-results.md` — B1 이 실제 Spark jar 3판본에 대고 빌드·배선됐다. 정정 2건 |
| **7차 교차 리뷰 판정** | **완료**(2026-08-27) — 리뷰 23건 중 **기각 0건** |
| **7차 리뷰 조치** | **코드 조치 0~5 완료**(2026-08-27) — 증거 봉투 fail-closed · 축 13개 재설계 · analyzer 판정 · 경로별 주입 하네스. 회귀 시험 **201건**. 남은 조치는 전부 Oracle 이 필요하다 |
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
| 7차 | `etl-platform-v2.0-codex-seventh-cross-review.md` | `etl-platform-v2.0-codex-seventh-review-assessment.md` — **P0 6 / P1 12 / P2 5 재판정. 기각 0건** |

리뷰 요청서: `codex-cross-review-prompt.md`(1차) · `codex-cross-review-prompt-v2.0.md`(v2.0)

### 2.4 Profile U 재설계

| 파일 | 설명 |
|---|---|
| **`etl-platform-v2.0-unprivileged-redesign-scope.md`** | DBA 없는 세계의 재설계 범위 제안. 죽는 것 / 살아남는 무권한 수단 / 보증 축 재정의 / 결정 4건 |
| **`etl-platform-v2.0-capability-overlay.md`** | 이기종 원천 대응. 코어는 권한 0·최저 버전에서 성립하고 capability 는 원천별 측정 오버레이로 붙는다. **§3 의 7축 표는 폐기** — 권위는 부록 A 와 `g0_axes.py` 다 |
| **`etl-platform-v2.0-simplification-decision.md`** | 감축 결정 기록. 무엇을 잘랐고 **무엇을 왜 안 잘랐는지** |
| **`etl-platform-v2.0-grant-request-verdict.md`** | DBA 권한 요청 방향 판정 — **보류**. 후보 37건 중 검증 9건이 전부 기각된 이유 |
| **`etl-platform-local-poc-plan.md`** | 로컬 WSL2 에서 G0-0 를 처음 돌리기 위한 실행 계획. **로컬이 증명하는 것/못하는 것 경계**가 핵심 |
| **`g0-0-s1-s3-results.md`** | **첫 실측 회차 기록**(S1·S2·S3). B1 컴파일·SPI 배선 확인, 판정기·증거 계약 첫 검증, 계획서 정정 2건, S4 이후가 막힌 이유 |
| `CHANGELOG.md` | 아키텍처 변경 이력(규범 아님). 본문에서 분리 |

---

## 3. G0-0 실측 산출물

**실행 순서: A → B0 → B1 → C00 → C01~C09.** G0-0 결과가 A v2.0 / P v2.0의 여러 분기를 결정한다.

| 게이트 | 파일 | 대상 | 안전 등급 |
|---|---|---|---|
| **G0-0A** | `g0-0a-capability-inventory.sql` | 계정 권한·capability·**원천 이식성** 실측(86 probe) | 운영계 가능(대상 테이블 접촉 `ROWNUM=1` 3건) |
| **G0-0B0** | `g0-0b0-spark-smoke.py` | stock Spark JDBC 경로 관측 | 운영계 제한적(ROWNUM 제한) |
| **G0-0B1** | `g0-0b1-connection-provider/` | 커스텀 `JdbcConnectionProvider`가 schema·task 경로를 덮는지 + fail-closed 성립 여부 | 운영계 제한적(`ROWNUM` 제한, 읽기 전용) |
| **G0-0C00** | `g0-0c-fence-facts.sql` | fence 반례 fact collector | 운영계 제한적(`ACK_FULL_SCAN` 게이트) |
| **G0-0C01~C09** | `g0-0c-counterexamples/` | stateful counterexample harness (9종 구현 완료) | **폐기용 쓰기 가능 환경 전용** |

**증거 계약**(2026-08-27 재설계 — 7차 리뷰 P0-02·03·04): `g0-0-evidence.schema.json` +
`g0-normalize.py` + `g0-child-contract.md` + `g0-run-child.sh`.

- record_type 은 **`g0_0_evidence`** 다. P §8.1 의 최종 `g0_evidence` 와 **이름이 다르다** —
  같은 이름으로 두 계약을 정의하던 것이 P0-04 였다. `gate_eligible` 은 schema 의 `const false` 다.
- 각 산출물은 `g0-run-child.sh` 로 실행해 **manifest 사이드카**를 남긴다. 실행 시점의
  `versions.lock` digest·종료 코드·산출물 해시가 거기 박히고, 집계기가 그것을 대조한다.
- 계약 위반·schema 위반은 경고가 아니라 **거부**다(exit 4, 최종 경로에 쓰지 않는다).
- **capability 축은 13축으로 재설계됐다**(`g0_axes.py` — 표 기반 pure function).
  `watermark_commit_bound` 를 복원했다. apply lag 와 `commit_time − watermark_value` 는
  독립인데 감축 과정에서 한 축으로 합쳐졌던 것이 P0-05 다.
- 회귀 시험: `g0-normalize-tests.py`(56) · `g0-axes-tests.py`(79) · `g0-b1-analyzer-tests.py`(40)
  · `g0-0b1-connection-provider/run-tests.sh`(26, Java) — **합계 201건**
**판본 고정**: `versions.lock` — 이 파일의 sha256 이 모든 증거에 `versions_lock_digest` 로 박힌다.
`UNSET` 이 남아 있으면 그 항목에 의존하는 측정은 미확정이다.

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

**S1~S3 은 끝났다**(`g0-0-s1-s3-results.md`). 그러나 7차 교차 리뷰가 **G0-0 실행 전에 닫아야 할 P0 6건**을
확정했다(`…-seventh-review-assessment.md`). 측정기를 고치는 것이 실측의 전제다 — 순서가 바뀌었다.

0. **P0 6건 수정** — 검토서 §5 의 조치 순서. **0~5 코드 조치 완료**(NLS 정정 / gate_eligible
   const / 증거 봉투 fail-closed / 축 재설계 / analyzer 판정 수정 / 경로별 주입 하네스).
   **남은 것은 전부 Oracle 이 있어야 한다** — P0-06(b)(`fail=all` 이 실제로 task 에 닿는가)는
   S6 에서 잰다
1. **Oracle 이 붙는 환경 확보** — S4~S8 이 전부 여기에 걸려 있다. **사용자 WSL2 에서 한다**
2. **G0-0A 실행** — 분기점 두 줄: 대상 테이블의 `FLASHBACK` 객체 권한, `versions.lock`의 Spark 버전
3. **G0-0B1 본실행** — 빌드·배선은 확인됐다. 남은 질문은 §1의 셋 그대로다:
   `TASK` 경로 커버리지 / 프리앰블 전면 적용 / fail-closed 성립. **셋 다 아직 미측정이다**
4. **C01~C09 실행** — 폐기용 환경 확보 후
5. capability 확정 → **A v2.0 / P v2.0**(단일 core + ConnectionRevision capability overlay, v1.2.3.1은 archive)
