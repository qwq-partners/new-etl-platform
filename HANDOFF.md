# 인수인계 — 이 저장소를 이어받는 사람(또는 LLM)에게

이 문서는 **대화 이력이 없는 상태에서 이 일을 이어받기 위한** 것이다. `README.md` 는 무엇이 어디 있는지의 지도이고, 이 문서는 **왜 그렇게 돼 있는지**와 **무엇을 하면 안 되는지**를 담는다. 후자가 더 중요하다 — 지도는 파일을 보면 복원되지만 판단의 근거는 그렇지 않다.

먼저 알아 둘 것: 이 설계는 **여덟 차례 교차 리뷰**를 거쳤고, 그때마다 잡힌 결함의 유형이 거의 같다. **문서로 확인되지 않은 것을 확인된 것처럼 쓴 문장.** 그래서 이 저장소의 규율은 §2 에 있고, 그것이 이 프로젝트에서 가장 값나가는 자산이다.

---

## 1. 한 장 요약

**무엇**: Dagster OSS + 얇은 Java Control Plane(PostgreSQL) 기반 ETL 플랫폼.
Oracle physical standby(ADG) → Spark → Iceberg/Polaris.
Job 약 10,000 / Run 약 40,000건·일 / 정시 burst 500 / Full 60%·Append 20%·Merge 20%.

**지금 어디**

| 단계 | 상태 |
|---|---|
| 목표 아키텍처 v1 → v1.2.3.1 | 완료. **단, DBA 협조를 전제하므로 Profile O 참고판으로 보존** |
| PoC 시험·합격 기준서 8차 | 완료(649줄) |
| Profile U(무권한) 재설계 범위 | 제안 완료 · **규격 동결은 NO-GO** |
| **8차 Codex 교차 리뷰** | 완료(2026-08-30) — 7차 P0 재판정 **CLOSED 0 / PARTIAL 2 / OPEN 4** |
| **G0-0 실측** | 원천 미실행. **9차 리뷰가 M0~M4 를 PARTIAL·OPEN 으로 재판정했다**(2026-08-31, 기각 0건) — 사내 원천 실행 **NO-GO**. 판정서 `etl-platform-v2.0-codex-ninth-review-assessment.md` |
| G0-0B1 로컬 부분 실측 | partial Maven classpath compile·SPI linkage만 확인. full Spark/Oracle runtime·fail-closed는 미실행 |
| A v2.0 / P v2.0 | raw G0-0 → 축/composition 확정 후 착수 |

**가장 중요한 사실 하나**: **저장소에 플랫폼 코드가 0줄이다.** Java 소스는 G0-0B1 tracer 3파일뿐이다. Control Plane·Guard·lease·Commit Adjudication·FI-01~66 은 설계 문서로만 존재하며 아직 시험 대상이 아니다.

---

## 2. 규율 — 이걸 지키지 않으면 이 저장소의 가치가 없다

> 1. **확인하지 못한 것은 "미확인"이라고 쓴다.**
> 2. **오류 부재는 증거가 아니다.** 0건 조건에는 양성 대조를 함께 둔다.
> 3. **측정하지 않은 숫자를 측정한 것처럼 쓰지 않는다.**
> 4. **검증 도구는 그것이 검증하는 대상보다 엄격해야 한다.**

4번은 7차 리뷰에서 배웠다. 규율을 강제하려고 만든 `g0-normalize.py` 가 정작 **미확정을 확정으로 바꾸고 있었다.** 도구가 통과시키면 그게 곧 거짓 안심이 된다.

### 이미 저지른 위반 — 같은 실수를 반복하지 마라

| 무엇 | 어떻게 틀렸나 |
|---|---|
| `c_expected` 를 **세 번** 틀림 | 56 → 78 → 86 → 87. 매번 `grep` 이 어떤 호출 형태를 놓쳤다(`p_stmt  (` 처럼 공백이 낀 것). 이 값이 틀리면 첫 실행에서 `manifest_ok=false` 가 떠 **측정 결과 전체가 폐기된다** |
| 수정을 안 하고 "수정했다"고 기록 | 치환 패턴이 안 맞아 `AssertionError` 가 났는데 검토서에는 "확정·수정" 으로 적고 커밋했다. **코드는 그대로, 기록만 바뀐 상태**였다 |
| 없는 능력을 있다고 표시 | `AS OF` 만 되고 `READ ONLY` 는 실패한 입력에 `READ_ONLY_TXN` 을 줬다. **상위가 실패했다고 하위로 내리면 안 된다** — 하위도 실패했을 수 있다 |
| 클래스패스 결함을 측정으로 오인 | "Oracle URL 을 claim 하는 provider 1개" 라는 출력이 나왔으나, 부분 클래스패스 탓에 Spark 내장 provider 가 로드조차 안 된 것이었다. 폐기했다 |
| `COUNT(*)` 가 되는 것을 "읽을 수 있다"로 | `view.v_database` 는 `SELECT COUNT(*)` 일 뿐 `CURRENT_SCN` 을 읽지 않는데 SCN 원점으로 썼다 |
| 404 를 "없음"으로 읽음 | fine-grained PAT 은 selected repositories 밖 저장소에 **404** 를 준다. 없는 게 아니라 **안 보이는** 것이다 |

---

## 3. 제약 — 문서에 다 안 적혀 있던 것들

이 제약들은 사용자가 대화에서 준 것이고 시간순으로 바뀌었다. **최신 상태만 유효하다.**

1. **정합성을 DBA 협조에 걸 수 없다.** (2026-08-24)
   처음엔 "DBA 권한을 받을 수 없다"였고, 뒤에 "critical 하지 않은 권한은 ETL 계정에 요청 가능"으로 완화됐다. 그러나 **권한 요청 방향은 2026-08-27 판정으로 보류**다. 어떤 설계도 권한을 가정하지 않는다.
2. **DB 에 부하·악영향 금지.** **일부 원천은 생산라인과 밀접하다.** DB 가 흔들리면 물리적 생산이 멈춘다. "아마 괜찮다"는 근거가 아니고, 어떤 자원을 얼마나 쓰는지 **기전으로** 설명해야 한다.
3. **원천 Oracle 은 버전·옵션·charset 이 제각각이다.** 그래서 코어는 **권한 0 · 최저 버전(11.2)** 에서 성립해야 하고, 능력은 원천별 측정 오버레이로 붙는다.
4. **실제 PoC 는 사내 환경에서 진행한다.** 원천 접속 정보·셋업도 사내에서 한다.
5. **설계가 장황하다는 지적이 있었고 타당했다.** 다만 감축은 G0-0 실측 이후다 — 측정 없이 자르면 자른 근거가 추측이다.

---

## 4. 결정 기록 — 특히 "왜 안 했나"

자른 이유는 남지만 **안 자른 이유는 잊힌다.** 그래서 이쪽을 적는다.

| 결정 | 이유 |
|---|---|
| **`FLASHBACK` 권한 요청 보류 유지** | 승인 판정 후보가 0건이고 28건이 미검증이므로 지금 요청하지 않는다. **근거는 2026-08-30(M4-4)에 정정됐다** — '받아도 순이익이 아니다'가 아니라 **'G0·object 수·DDL·LOB·retention·Spark 전파를 실증하기 전에는 활성화·요청하지 않는다'**. passive grant(primary 에서 `GRANT` 1회 — 일회성 metadata·redo·audit·invalidation 영향이며 **정확한 양은 미측정**)와 runtime 활성화(undo·DDL·standby 부하, primary `UNDO_RETENTION` 상향 요구)는 **비용이 다르므로 분리해 센다**. 이전 판이 세지 않은 이득: 같은 `AS OF TIMESTAMP` 리터럴은 connection 에 매이지 않아 **여러 물리 connection 을 같은 anchor 에 묶는다** — 현 코어(`SET TRANSACTION READ ONLY`, connection scope)는 그것을 낼 수 없다. timestamp mapping 은 ±3초가 아니라 **최대 3초 이전**이다 |
| **`ZERO_GAP` 계열 24.6% 를 지금 안 자름** | 도달 불가로 보이지만 **아직 측정하지 않았다.** 지우면 되살릴 때 잃는다 |
| **감축안 중 가장 공격적인 것(58%)을 채택 안 함** | 3명이 독립 심사해 **전원 최하위**를 줬다. `ORA-01555` 분기를 `AS OF SCN` 계열로 묶어 삭제하려 했는데, **`AS OF SCN` 이 사라져도 `ORA-01555` 는 사라지지 않는다** |
| **overlay 9축 재설계를 미룸** | 지금 확정하면 또 측정 없이 규격을 짜는 것이다 |
| **로컬에서 ADG 를 구성하지 않음** | Oracle Free/XE 는 Data Guard 가 **N**(라이선스 문서 Table 1-5). `ORA-03172` 를 로컬에서 재현할 수 없다 |
| **`CE_STANDBY_DSN` 을 설정하지 않음** | 두 번째 컨테이너를 standby 인 척 물리면 `standby_verified=true` 라는 **거짓**이 생긴다. 가드를 끄는 것보다 나쁘다 |
| **저장소를 public 으로 함** | **사내에서 다운로드·접근하기 쉽도록** 한 결정이다(2026-08-31, 사용자). 이전 판은 "생산라인" 29건·규모 수치·미해결 결함 목록을 이유로 private 유지를 적었으나 **그 제약은 철회됐다.** 자격증명은 저장소에 없다 |

---

## 5. 파일 지도 — 무엇을 볼 때 어디로

| 알고 싶은 것 | 볼 곳 |
|---|---|
| 전체 현황·문서 목록 | `README.md` |
| **규율 위반 사례와 처리** | `etl-platform-v2.0-codex-seventh-review-assessment.md` |
| **현재 차단점·8차 재판정** | `etl-platform-v2.0-codex-eighth-cross-review.md` (**우선 읽을 것**) |
| 무엇을 왜 안 잘랐나 | `etl-platform-v2.0-simplification-decision.md` (§6 에 미해결 공백) |
| 권한 요청을 왜 접었나 | `etl-platform-v2.0-grant-request-verdict.md` + 원자료 `…-candidates.md`(37건) |
| 이기종 원천 대응 | `etl-platform-v2.0-capability-overlay.md` (**동결 불가**) |
| 현행 규범(참고판) | `etl-platform-target-architecture-v1.2.3.1.md` (1,533줄) |
| 시험·합격 기준 | `etl-platform-poc-test-plan-v1.md` (649줄) |
| 로컬에서 뭘 증명할 수 있나 | `etl-platform-local-poc-plan.md` (**§1 증거 등급 H/D/X 가 핵심**) |
| 사내로 어떻게 가져가나 | `etl-platform-transfer-guide.md` |
| 프로브 실행법 | `g0-0-probe-README.md` |
| 증거 계약 | `g0-0-evidence.schema.json` + `g0-normalize.py` |
| 판본 고정 | `versions.lock` (**`UNSET` 17건 — 채우기 전엔 그 항목 의존 측정이 미확정**) |

**G0-0 산출물 실행 순서**(9차 조치 1~7 전에는 실행 금지): A → B0 → **B1** → C00 → C01~C09 → `g0-normalize.py`

---

## 6. 하지 말 것

1. **G0-0 실측 전에 A/P 규범을 대규모로 고치지 마라.** 단, 측정기의 M0 실행 안전성과 M1 증거 결속은 실측 전에 반드시 고친다.
2. **C01~C09를 저장소 내부 `environment_guard`만 믿고 실행하지 마라.** guard를 유지하는 것과 별개로 외부 disposable-environment allowlist가 필요하다.
3. **로컬 결과를 사내 설계 근거로 쓰지 마라.** `g0-normalize.py --profile LOCAL_WSL` 은 증거에 "설계 근거가 아니다"를 박는다. **`--profile CORP_POC` 로 바꿔 우회하지 마라.**
4. **틀린 비밀번호를 시도하지 마라.** 계정 잠금은 전체 파이프라인 정지다. 반입 직후 손으로 접속을 시험할 때가 가장 위험하다.
5. **`ACK_FULL_SCAN=Y` 를 승인 없이 켜지 마라.** C00 의 기본값은 대상 테이블 질의 0건이다.
6. **`suite.yaml` 을 패키지 밖으로 옮기지 마라.** `artifact_hash` 가 디렉터리 전체를 순회한다.
7. **evidence 최상위에 새 필드를 넣지 마라.** `additionalProperties: false` 다.
8. **`pass=true` 를 설계 통과로 읽지 마라.** 그건 "하네스가 끝까지 돌았다"이다. `counterexample_reproduced`·`mitigation_failed` 가 있으면 그게 설계 결함이다.

---

## 7. 열려 있는 것

### 설계에 실재하는 공백 (G0-0 이후 A v2.0 에서 답할 것)

- **fence 의 전달 가능성이 소멸했다.** SCN 은 DB 단위 전역이라 모니터 세션 1개가 읽은 값을 executor 세션들이 공유할 수 있었다. Profile U 에는 그 전역 원점이 없다
- **`STANDBY_MAX_DATA_DELAY` 의 `D` 는 세션 속성인데 모니터 세션은 Source 당 1개다** — freshness SLO 가 다른 Job 이 한 세션을 공유할 수 없다
- **`safety_lag`·`clock_skew` 의 조달 경로가 없다.** 기준서는 "DBA 승인값"으로 규정하는데 그 경로가 사라졌고, 이 값을 재는 probe 도 없다
- **`ORA-03172` 자기증폭 루프 대책이 규범에 0건.** `lag ↑ → 세션 사망 → 재시도 → 재독 → lag ↑`. 정시 burst 500건이 같은 `next_eligible_at` 을 받으면 동기화된 재제출이 반복된다. `jitter`·breaker 규정이 A·P 어디에도 없다
- **`watermark_commit_bound` 를 재는 probe 가 없다.** apply lag 과 독립인 축인데 G0-0 이 못 잰다

### 7차 리뷰 미반영 3건

P1-07(LOB RETENTION 분리) · P1-09 일부(C00 end sentinel·manifest) · P1-03(전체 SQLCODE taxonomy 표).
8차 판정은 P1-03·07은 raw 수집까지 제한적으로 미룰 수 있으나 semantic normalization/publish 전에는 필수,
P1-09 child contract는 normalizer 수용 전에 필수라고 구분했다.

### 8차 리뷰 완료 — 현재 P0

상세는 `etl-platform-v2.0-codex-eighth-cross-review.md`를 본다. 핵심은 다음이다.

- B1 `failclosed_task`가 argparse choice에 없어 Spark 시작 전에 종료
- 같은 `Trace.classify()` 추정이 injection actuator와 proof 양쪽을 제어하는 순환 판정
- B0 안전 상한 위반이 오류를 출력하면서 process exit 0
- README의 `producer | tee`가 producer exit를 잃음
- 얕은 synthetic child payload로 A/B0/B1/C00/CE 모두 `MEASURED` 가능
- child run/source/profile/runtime/lock binding과 stale/effective floor 미구현
- A의 identity mismatch가 target 접촉 전 hard stop이 아니며 `ROWNUM`은 I/O 하드 상한이 아님

---

## 8. 다음에 할 일

1. ~~**M0 실행 안전성**~~ — 완료(2026-08-30). 6건 처리, 회귀 `g0-m0-safety-tests.py` 51건
2. ~~**M1 child evidence contract**~~ — 완료(2026-08-30). child schema 4종 · `source_id`/`harness_digest`/start·end · 회차 집합 검사 · run 별 불변 경로
3. ~~**M2 B1 재작성**~~ — 완료(2026-08-30). explicit `connectionProvider` · 선언된 phase 로 injection 구동(스택 추정과 actuator 분리) · schema/task/metadata 독립 시나리오 · terminal token·business SQL 0·trace completeness
4. ~~**M3 normalizer**~~ — 완료(2026-08-30). schema 통과 산출물만 집계 · SQLCODE taxonomy + probe별 typed predicate · `effective_value` floor 실동작 · `not_covered` 를 최종 계약과의 차집합으로 · 최종 게이트(`g0_final_gate.py`) 분리 · current 포인터 무효화
5. ~~**M4 사실·규범 문서 정정**~~ — 코드까지 갔으나 **9차에서 PARTIAL 로 재판정**. Oracle·Spark 사실 정정 7건은 전부 맞다고 확인됐고, 과대 문구("딕셔너리 row 1건" 등)와 stale runbook 이 남았다
6. **9차 조치 1~11** ← **지금 여기**. 판정서 §7 이 권위다. 중심은 개별 결함이 아니라 **시험의 경계를 producer 뒤로 미는 것** — 조치 1(실물 `run.sh` 통합 시험)과 조치 2(runbook dry-run 시험)가 그 성질을 만든다
7. 그 뒤 **M5a→M5b→M5c→M5d→M5e** 순으로 단계별 해제(9차 리뷰 §7). "사내 원천 실행" 을 한 단계로 두지 않는다
8. 측정 분포로 capability 축/composition을 확정하고 **A v2.0 / P v2.0** 착수

> **9차의 한 줄.** 8차는 *"고쳤다고 쓰기 전에 실행했는가"* 를 물었고, 9차는 **그 질문을 시험에도
> 해야 한다**고 답했다. 회귀 397건은 재현되지만 그중 B1 관련 72건은 실물 producer 를 건너뛴다 —
> `InjectionMatrix` 는 `shouldFail()` 을 순수 함수로 시험하고, analyzer 시험은 producer 가 만들지
> **않는** 토큰을 합성해 넣는다. 그래서 `PROVEN` 이 도달 불가능한 채로 둘 다 통과했다.
---

## 9. LLM 에게

이 저장소를 읽고 작업할 때:

- **문서가 "미확인"이라고 쓴 것을 확정으로 바꾸지 마라.** 그게 이 프로젝트가 여덟 번 잡은 결함이다.
- **숫자를 인용하기 전에 실물에서 다시 세라.** `c_expected` 는 세 번 틀렸다. `probe 87개`, `UNSET 17건` 같은 값도 파일이 바뀌면 달라진다.
- **"고쳤다"고 쓰기 전에 실제로 바뀌었는지 확인하라.** 치환이 실패해도 스크립트는 조용히 끝날 수 있다.
- **제안을 받으면 반증부터 시도하라.** 이 저장소의 리뷰는 전부 "기본 입장은 이 지적이 틀렸다"로 시작한다. 그렇게 해서 살아남은 것만 확정이다.
- 모르면 **모른다고 쓰고 무엇을 확인해야 확정되는지 적어라.** 그게 여기서는 감점이 아니라 요구사항이다.
