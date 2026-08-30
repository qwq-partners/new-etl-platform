# 8차 교차 리뷰 요청

- 요청일: 2026-08-28
- 기준 커밋: `9cba9209409d67df13405593e970fe76c2987366` (`main`)
- 저장소: `git@github.com:qwq-partners/new-etl-platform.git` (private)
- 로컬: `/mnt/c/Users/user/Documents/Codex/2026-08-22/new-chat/outputs`
- 리뷰 결과: `etl-platform-v2.0-codex-eighth-cross-review.md`

---

## 1. 7차 리뷰 이후 무엇이 바뀌었나

7차 리뷰(`etl-platform-v2.0-codex-seventh-cross-review.md`)의 **P0 6건을 전부 확정**하고 수정했다. 판정과 조치는 `etl-platform-v2.0-codex-seventh-review-assessment.md` 에 있다.

7차의 핵심 지적을 그대로 인정한다.

> 가장 위험한 것은 Oracle 사실의 미확정 자체가 아니라, **미확정을 확정값으로 바꾸는 코드가 이미 있다는 것**이다.

| 무엇 | 이전 | 이후 |
|---|---|---|
| `derive_axes` 등급 승격 | 하위 probe 가 실패해도 하위 등급을 줬다 | 등급마다 **자기 양성 근거**를 요구. 기능 부재(`ABSENCE_ORA`)만 `NONE`, 나머지는 `UNDETERMINED` |
| `lag_visibility` | 관측과 강제를 한 축에 합침 | `lag_observation` / `lag_admission` 분리. `ACCEPTED_UNVERIFIED`(ALTER 수락만) vs `ENFORCED`(`ORA-03172` 양성 대조) |
| `bound_kind` | `lag_visibility` 로 대체(오류) | **철회 취소.** `watermark_commit_bound` 축 신설, 값은 항상 `UNDETERMINED`(재는 probe 가 없다는 사실을 드러내려고) |
| 불완전 산출물 | 한 줄 입력도 `MEASURED`, exit 0 | 최소 내용 요구 → `PARTIAL`/`FAILED`, exit 3 |
| 스키마 검증 | 파일 쓴 뒤 stderr 만, exit 0 | **쓰기 전에** 검증. 위반이면 `.invalid` 에만, exit 4 |
| 증거 계약 | `g0_evidence` 이름으로 두 계약 충돌 | `g0_0_evidence` / `scope: CAPABILITY_INVENTORY` / **`gate_eligible: false` 고정** |
| 측정 대상 결속 | `target_owner/table/wm_column` 미기록 | `target.identity` probe 신설 + 스키마 필수 |
| B1 `MIXED` | 1건이 SCHEMA·TASK 양쪽 게이트 통과 | 이중 계수 제거 |
| B1 fail-closed | `fail=all` 이 schema 에서 죽어도 "task 도 통과" | 경로별 주입(`fail=schema\|task`) + `failclosed_task` 회차 |
| `derived_from` | 의도한 probe 목록 | **실제 읽은 것만.** 나머지는 `considered_but_not_used` |
| 권한 판정 원자료 | 없음(재현 불가) | 후보 37건 전체 + 기각 9건 논증 전문 복원 |

P1/P2 17건 중 **12건 수정 · 2건 부분 · 3건 미반영**(P1-07 LOB RETENTION 분리 / P1-09 일부 C00 sentinel·manifest / P1-03 전체 SQLCODE taxonomy).

---

## 2. 반드시 전제할 것

1. **정합성을 DBA 협조에 걸 수 없다.** critical 하지 않은 권한은 ETL 계정에 요청 가능하나 그 요청은 **현재 보류**이며 어떤 설계도 그것을 가정하지 않는다.
2. **DB 에 부하·악영향 금지.** 일부 원천은 **생산라인과 밀접**하다.
3. 원천 Oracle 은 **버전·옵션·charset 이 제각각**이다.
4. 코어는 **권한 0 · 최저 버전(11.2)** 에서 성립해야 한다.
5. **G0-0 은 원천에 대해 한 번도 실행되지 않았다.** 아래 §3 의 로컬 실측을 원천 사실로 읽지 마라.
6. **저장소에 플랫폼 코드가 0줄이다.** Control Plane·Guard·lease·Commit Adjudication 은 아직 시험 대상이 아니다.
7. 실제 PoC 는 사내에서 진행한다. 지금은 리뷰 단계다.

---

## 3. 이번에 처음 생긴 실측 (로컬, 부분)

`evidence/g0-0b1-local-s2s3.json` — **profile=LOCAL_WSL. 하네스 동작 확인용이며 설계 근거가 아니다.**

Spark 4.2.0 / Scala 2.13.16 / JDK 21.0.11, **Maven 아티팩트 부분 클래스패스**(전체 배포판 아님):

- SPI 시그니처 일치(`javap` 확인). `modifiesSecurityContext` 가 4.2.0 에서는 `abstract` 다
- `build.sh` exit 0, 바이트코드 major=61(Java 17)
- ServiceLoader 가 tracer 발견, `canHandle` 이 oracle 만 true
- `disabledJdbcConnProviderList=basic` 필요성을 **바이트코드로** 확인 — `BasicConnectionProvider.canHandle = (keytab==null ‖ principal==null)` → 비-Kerberos 에서 true, `SecureConnectionProvider` 계열은 false. 즉 Basic + 우리 것 **2개**가 claim

**폐기한 관측 하나**: 중간에 "Oracle URL claim provider 1개" 가 나왔으나 부분 클래스패스 탓에 Spark 내장 provider 가 로드조차 안 된 결과였다. 측정이 아니므로 폐기했다.

---

## 4. 특히 봐 주었으면 하는 것

### 4.1 수정이 진짜로 닫혔는가

7차 P0 6건에 대해 **같은 반례를 다시 구성해** 뚫리는지 봐 달라. 특히:

- `derive_axes` 에 아직 **양성 근거 없이 승격되는 조합**이 남아 있는가
- `coverage` 최소 내용 기준(B0 4건·S 계열 2그룹 / B1 `by_path`+`preamble_ok_by_path` / C00 값 나온 fact / CE scenario ≥ 9)이 **우회 가능한가**
- `gate_eligible: false` 와 exit 0/3/4 가 실제로 G0 PASS 오독을 막는가

### 4.2 새로 만든 것의 결함

- `etl-platform-v2.0-grant-request-candidates.md` — 원자료 복원이 **판정을 재현 가능하게 만들었는가**. 28건 미검증 표시가 충분한가
- 경로별 주입(`Preamble.apply(conn, path)`) — `path` 는 `Trace.classify()` 의 **스택 추정**이다. 추정으로 주입 대상을 정하는 것이 타당한가
- `effective_value` vs `value` 분리가 `stale` 처리에 실제로 쓰이는가(지금은 같은 값을 넣는다)

### 4.3 아직 안 한 것이 정말 미뤄도 되는가

- overlay 9축 재설계를 **G0-0 실측 이후로** 미룬 판단
- child artifact 별 개별 스키마(`run_id`·sentinel·manifest·runtime digest) 부재
- P1-07 / P1-09 일부 / P1-03

### 4.4 로컬 PoC 계획

`etl-platform-local-poc-plan.md` — 특히 **§1 증거 등급 H/D/X 의 배정이 정확한가**. 로컬 결과가 사내 설계 근거로 샐 경로가 남아 있는가.

---

## 5. 이 저장소의 규율 (이 기준으로 봐 달라)

> 확인하지 못한 것은 "미확인"이라고 쓴다.
> 오류 부재는 증거가 아니다. 0건 조건에는 양성 대조를 함께 둔다.
> 측정하지 않은 숫자를 측정한 것처럼 쓰지 않는다.
> **검증 도구는 그것이 검증하는 대상보다 엄격해야 한다.**

마지막 줄은 7차 리뷰에서 배운 것이다. 이번 판이 그것을 지키는지 봐 달라.

---

## 6. 읽는 순서

```
README.md                                        현재 상태표·문서 지도
etl-platform-v2.0-codex-seventh-review-assessment.md   7차 처리 내역 (여기부터)
g0-normalize.py + g0-0-evidence.schema.json      가장 많이 고친 곳
etl-platform-v2.0-capability-overlay.md          동결 불가 · 축 분해
etl-platform-v2.0-grant-request-candidates.md    복원한 원자료
etl-platform-local-poc-plan.md                   로컬 계획 · 증거 등급
g0-0b1-connection-provider/                      경로별 주입 · 판정 로직
g0-0a-capability-inventory.sql                   87 probe
versions.lock                                    판본 고정 (UNSET 17건)
```
