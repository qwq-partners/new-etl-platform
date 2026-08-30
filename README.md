# ETL Platform — 목표 아키텍처 · PoC 기준서 · 교차 리뷰 기록

> **처음 이어받는 사람은 [`HANDOFF.md`](HANDOFF.md) 를 먼저 읽어라.** 이 문서는 무엇이 어디 있는지의
> 지도이고, `HANDOFF.md` 는 **왜 그렇게 돼 있는지**와 **무엇을 하면 안 되는지**를 담는다.

Dagster OSS + 얇은 Java Control Plane(PostgreSQL) 기반 신규 ETL 플랫폼의 설계 문서와 누적 교차 리뷰 기록이다.

- **규모**: Job 약 10,000개 · Run 약 40,000건/일 · 정시 burst 약 500건
- **적재 경로**: Oracle physical standby(ADG) → Spark → Iceberg / Polaris
- **적재 모드 비중**: Full 60% · Append 20% · Merge 20%

---

## 1. 지금 어디에 있는가

| 단계 | 상태 |
|---|---|
| 목표 아키텍처 v1 → v1.2.4 | **완료**(다섯 차례 교차 리뷰 + UI 도출 개정 요청 13건 반영) |
| **제약 변경**(2026-08-24, 표현 통일 2026-08-27) | **정합성을 DBA 협조에 걸 수 없다.** 비critical 권한 요청은 가능하나 **현재 보류이며 절대 가정하지 않는다.** v1.2.3.1이 전제하던 DBA 의존이 무효화됨 |
| Profile U(무권한) 재설계 범위 제안 | 작성 완료 · 6차 교차 리뷰 완료 · **v2.0 규격 동결은 NO-GO** |
| **G0-0 실측** | **원천에 대해서는 한 번도 실행되지 않았다.** Oracle 없는 회차로 S1~S3(하네스 빌드·SPI 배선)만 돌았다. M0 은 닫혔고 **M1 child 증거 계약이 남아 사내 원천 실행은 여전히 NO-GO** |
| 감축 1차 | **완료**(2026-08-27) — 변경 이력 분리로 −16.2%. 나머지는 G0-0 이후 |
| DBA 권한 요청 방향 | **보류 유지** — 승인 판정 후보 0건·28건 미검증. `FLASHBACK`의 순이익 부재라는 강한 결론은 8차 리뷰에서 재검토 대상으로 환원 |
| 로컬 G0-0 실행 계획 | 작성 완료(2026-08-27) — S0~S8. **S2(B1 컴파일)가 최속 신호** |
| **S1~S3 실행 결과** | `g0-0-s1-s3-results.md` — B1 이 실제 Spark jar 3판본(전체 배포판)에 대고 빌드·배선됐다. 정정 2건 · profile `SANDBOX_CONTAINER` |
| **B1 로컬 부분 검증**(2026-08-28) | 병행 회차. partial Maven classpath 에서 compile·SPI linkage 확인. **full distribution runtime·Oracle 경로·fail-closed 는 미실행** |
| **7차 교차 리뷰 판정·조치** | 판정 `…-seventh-review-assessment.md`(기각 0건) · 조치 `…-seventh-review-fixes.md`. 회귀 시험 **204건**. **8차 재검증 결과 P0 6건은 CLOSED 0 / PARTIAL 2 / OPEN 4** |
| **8차 교차 리뷰**(2026-08-30) | 완료 — 현 normalizer 결과 수용·B1 `PROVEN`·G0 PASS·v2.0 동결은 **NO-GO** |
| **8차 M0(실행 안전성)** | **완료**(2026-08-30) — 6건 처리, 회귀 41건 신설(`g0-m0-safety-tests.py`). 상세는 §3 |
| **8차 M1(child 증거 계약)** | 진행 중 ← **지금 여기** |
| A v2.0 / P v2.0 규범 개정 | M0/M1 수정 → raw G0-0 수집 → 축/composition 확정 후 착수 |

**핵심 원칙**: 실측 전 규범 문서를 대규모로 고치지는 않는다. 다만 **실측을 신뢰할 수 있게 만드는 실행 안전성과 증거 결속은 실측보다 먼저 고친다.**

---

## 2. 문서 지도

### 2.1 목표 아키텍처 (A 계열)

| 파일 | 설명 |
|---|---|
| `etl-platform-target-architecture-v1.md` | 최초 초안 |
| `etl-platform-target-architecture-v1-review.md` | 그 초안에 대한 상세 리뷰(97건) |
| `…v1.1.md` → `…v1.2.md` → `…v1.2.1.md` → `…v1.2.2.md` → `…v1.2.3.md` | 교차 리뷰를 반영한 개정 계열 |
| `…v1.2.3.1.md` | 8차 교차 리뷰가 검토한 판. **동결** — 그 리뷰의 인용이 가리키는 대상이므로 고치지 않는다 |
| **`…v1.2.4.md`** | **현행 최신 규범**. UI 정보구조에서 도출한 개정 요청 17건 중 13건 반영(`etl-platform-a-revision-request-ui.md`). 핵심은 **시각 컬럼 규약**(모든 `*_at` 은 `timestamptz` UTC — 세 시계 사이의 뺄셈이 선언 없이 이뤄지고 있었다)과 **운영자 조회 표면**(replica + `etl_ui` 뷰). DBA 협조를 전제하므로 Profile O로 보존 |

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
| 7차 | `etl-platform-v2.0-codex-seventh-cross-review.md` | `etl-platform-v2.0-codex-seventh-review-assessment.md` — **P0 6 / P1 12 / P2 5 재판정. 기각 0건.** 조치 내역은 `…-seventh-review-fixes.md`. **8차에서 종결 여부 재기각** |
| **8차** | **`etl-platform-v2.0-codex-eighth-cross-review.md`** | 후속 검토서 대기 — **CLOSED 0 / PARTIAL 2 / OPEN 4** |

리뷰 요청서: `codex-cross-review-prompt.md`(1차) · `codex-cross-review-prompt-v2.0.md`(v2.0) · `codex-cross-review-prompt-8th.md`(8차)

### 2.4 Profile U 재설계

| 파일 | 설명 |
|---|---|
| **`etl-platform-v2.0-unprivileged-redesign-scope.md`** | DBA 없는 세계의 재설계 범위 제안. 죽는 것 / 살아남는 무권한 수단 / 보증 축 재정의 / 결정 4건 |
| **`etl-platform-v2.0-capability-overlay.md`** | 이기종 원천 대응. 코어는 권한 0·최저 버전에서 성립하고 capability 는 원천별 측정 오버레이로 붙는다. **§3 의 7축 표는 폐기** — 권위는 부록 A 와 `g0_axes.py` 다 |
| **`etl-platform-v2.0-simplification-decision.md`** | 감축 결정 기록. 무엇을 잘랐고 **무엇을 왜 안 잘랐는지** |
| **`etl-platform-v2.0-grant-request-verdict.md`** | DBA 권한 요청 방향 — **보류 유지**. 8차 리뷰에서 일부 사실·실체 판정 재검토 요구 |
| `etl-platform-v2.0-grant-request-candidates.md` | 후보 37건 working reconstruction. 9건 검토·28건 미검증이며 journal/source 결속은 미완료 |
| **`etl-platform-local-poc-plan.md`** | 로컬 WSL2 에서 G0-0 를 처음 돌리기 위한 실행 계획. **로컬이 증명하는 것/못하는 것 경계**가 핵심 |
| **`g0-0-runbook.md`** | **실행 절차서.** S0~S8 을 명령 단위로. 흩어져 있던 절차를 한곳에 모았다 — **실행할 때는 이것을 편다** |
| **`g0-0-s1-s3-results.md`** | **첫 실측 회차 기록**(S1·S2·S3). B1 컴파일·SPI 배선 확인, 판정기·증거 계약 첫 검증, 계획서 정정 2건, S4 이후가 막힌 이유 |
| **`g0-child-contract.md`** | **child 산출물 계약.** manifest 사이드카 형식, 집계기 강제표, exit 0/3/4, RECON 회차와 증거 회차의 분리 |
| **`etl-platform-transfer-guide.md`** | **사내 반입 안내** — `git bundle` 오프라인 반입, 폐쇄망 의존물, 결과 반출, 변경관리 문답 |
| **`etl-platform-a-revision-request-ui.md`** | **A 개정 요청서 17건과 처리 결과.** UI 정보구조를 쓰다 발견한 A 자체의 공백. **P0 둘**: 조회 경로 선택(D-01)과 **`*_at` 필드 25개의 저장 시간대 미선언**(A-13). **2026-08-30 — 13건 반영(v1.2.4) · 1건 철회(A-20 은 22장 7번이 이미 덮는다) · 3건은 22장 확정 대상** |
| **`etl-platform-ui-information-architecture.md`** | **Control Plane UI 정보구조.** 화면 8개, Phase 0 표시 규칙, 두 UI 접합면, **§10 Advisor UX · §11 시각 표시 · §12 운영자 조회 · §13 목록 규모 · §14 인증·감사(계정 ID = 사번)**. **미결 7건 중 6건 해소 — 남은 것은 capability 표시 하나이고 G0-0 실측이 답한다.** **A 개정이 필요한 공백 17건**(§9.1)은 따로 세워 뒀다. **판본 사본을 뜨지 않는다** — 이 파일을 직접 고치고 이력은 git 이 남긴다(문서 머리의 결정 기록 표 참조)
| `CHANGELOG.md` | 아키텍처 변경 이력(규범 아님). 본문에서 분리 |

---

## 3. G0-0 실측 산출물

**현재는 아래 순서로 실행하지 않는다.** 8차 리뷰의 M0/M1을 먼저 닫은 뒤 A → B0 → B1 → C00 → C01~C09 순으로 raw evidence를 수집한다.

| 게이트 | 파일 | 대상 | 안전 등급 |
|---|---|---|---|
| **G0-0A** | `g0-0a-capability-inventory.sql` | 계정 권한·capability·원천 이식성 raw fact(87 probe) | **여전히 NO-GO** — M0 은 닫혔으나 **M1 child 계약이 남았다** |
| **G0-0B0** | `g0-0b0-spark-smoke.py` | stock Spark JDBC 경로 관측 | **M0 조치 완료**(2026-08-30) — `sys.exit(main())`·partition/session 상한·**대상 접촉 전 신원 preflight**. M1 이 남았다 |
| **G0-0B1** | `g0-0b1-connection-provider/` | provider의 schema·task 경로 + fail-closed | **현재 NO-GO** — `failclosed_task` 실행 불가·stack 추정 순환 판정 |
| **G0-0C00** | `g0-0c-fence-facts.sql` | fence fact collector | external completion wrapper + scan 승인 후 조건부 |
| **G0-0C01~C09** | `g0-0c-counterexamples/` | stateful counterexample harness | **M0 조치 완료** — `CE_ENV_ALLOWLIST`(패키지 **밖**) 없이는 실행되지 않으며, 그 검사가 **preflight 접속보다 먼저** 온다 |

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
- **실행 래퍼**: 모든 child 는 `g0-run-child.sh` 로 감싼다(manifest 사이드카). sqlplus 는
  `g0-sqlplus.sh` 로 — 비밀번호를 stdin 으로만 넘겨 manifest 에 남기지 않는다
- 회귀 시험: `g0-normalize-tests.py`(59) · `g0-axes-tests.py`(79) · `g0-b1-analyzer-tests.py`(40)
  · `g0-m0-safety-tests.py`(41) · `g0-0b1-connection-provider/run-tests.sh`(26, Java)
  — **합계 245건**

> **그렇다고 이 계약이 8차 리뷰의 M1 을 닫은 것은 아니다.** 8차는 `main` 판을 보고
> child 완결성·run/source/runtime 결속·`effective_value` floor 미보장을 지적했다.
> 위 재설계가 그 지적의 상당 부분을 앞서 다루지만, **닫혔다는 판정은 8차 반례를 이 구현에
> 직접 돌려 본 뒤에만 쓴다.** 그 전까지 현 normalizer 결과는 **판정 입력으로 수용하지 않는다.**

**판본 고정**: `versions.lock` — 이 파일의 sha256 이 모든 증거에 `versions_lock_digest` 로 박힌다.
`UNSET` 이 남아 있으면 그 항목에 의존하는 측정은 미확정이다.

안내: `g0-0-probe-README.md` — 안전 규칙 · 실행법 · 결과 → 설계 분기표 · **이 프로브가 증명하지 못하는 것**
반례 harness 전용 안내: `g0-0c-counterexamples/README.md` — 환경변수 · 종료 코드 · 시나리오별 증거 형태

### 3.1 안전 규칙 (요약)

1. A·B0·B1·C00은 원천 객체 DDL/DML·job 생성을 의도하지 않는다. A의 `ALTER SESSION`은 자기 세션에만 적용된다. **다만 read-only가 source-safe를 뜻하지는 않는다.**
2. **잘못된 비밀번호를 절대 시도하지 않는다** — 계정 잠금은 전체 파이프라인 정지다.
3. 비밀번호를 **명령줄 인자에 넣지 않는다**(`/nolog` + stdin, 또는 환경변수).
4. SQL capability probe는 실제 `SQLCODE`를 기록한다. B1·CE·normalizer 판정은 별도의 runtime·manifest·child binding이 필요하며 현재 그 계약이 미완료다.
5. C01~C09는 저장소 내부 `environment_guard`만 믿지 않는다. 사내 CMDB/환경 registry 등 **외부 disposable allowlist**가 승인한 환경에서만 실행한다.

---

## 4. 이 저장소의 이력이 말해 주는 것

누적 교차 리뷰에서 반복해 잡힌 결함 유형은 하나다 — **문서로 확인되지 않은 것을 확인된 것처럼 쓴 문장**. 대표적인 예:

- `SYS_CONTEXT('USERENV','CON_DBID')` — 19c USERENV에 없는 속성(ORA-02003)
- `executionMetadata.runId` — Dagster GraphQL 스키마에 없는 입력 필드
- `sessionInitStatement` — Spark의 schema/metadata connection은 이것을 실행하지 않는다
- Flashback Query — 대상 `SELECT`/`READ`와 object `FLASHBACK` 또는 system `FLASHBACK ANY TABLE`이 필요. 최소권한 정책에서는 object grant를 검토
- `MAX(watermark)` fence — 반개구간과 결합하면 tail 행이 지연·누락된다

그래서 이 저장소의 규칙은 다음과 같다.

> **확인하지 못한 것은 "미확인"이라고 쓴다. 오류 부재는 증거가 아니다. 0건 조건에는 양성 대조를 함께 둔다.**

---

## 5. 다음 작업

**S1~S3 은 끝났다**(`g0-0-s1-s3-results.md`). 7차 리뷰 조치 0~5 도 코드까지 끝났다.
그러나 **8차 교차 리뷰가 P0 6건을 CLOSED 0 / PARTIAL 2 / OPEN 4 로 재판정했다.**
사내 원천 실행은 그 전에 열지 않는다.

0. **8차 M0 실행 안전성** — wrapper `pipefail`/producer exit, B0 `sys.exit(main())`,
   partition/session cap, 대상 접촉 전 identity hard preflight, CE 외부 allowlist
1. **8차 M1 child evidence contract 재검증** — A/B0/B1/C00 별 run ID·source/profile·
   runtime/lock digest·start/end·exit·정확한 manifest. 이 브랜치의 `g0-child-contract.md`·
   `g0-run-child.sh` 가 이미 상당 부분을 구현한다 — **8차 반례를 그 구현에 돌려 판정한다**
2. **8차 M2 B1 재설계** — explicit `connectionProvider`, 실제 task-only scenario,
   stack guess 와 injection/PASS 분리
3. synthetic negative suite 에서 incomplete·mixed·stale evidence 가 전부 fail-closed 인지 확인
4. **Oracle 이 붙는 환경 확보** — S4~S8 이 전부 여기에 걸려 있다. **사용자 WSL2 에서 한다**
5. pinned 사내 환경에서 raw G0-0 수집 → capability composition 확정 →
   **A v2.0 / P v2.0**(단일 core + ConnectionRevision capability overlay, v1.2.3.1은 archive)
