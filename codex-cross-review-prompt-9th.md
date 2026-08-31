# 9차 교차 리뷰 요청

- 요청일: 2026-08-30
- 기준 커밋: `94a555dd80a192bd7ec12c861aa94ea29824abf5`
- 브랜치: `claude/continue-previous-session-15bm9o` (PR #1, draft)
- 저장소: `git@github.com:qwq-partners/new-etl-platform.git` (private)
- 리뷰 결과 파일명(제안): `etl-platform-v2.0-codex-ninth-cross-review.md`

---

## 1. 이번 회차의 한 문장

**8차 리뷰가 낸 최소 수정 순서 M0~M4 를 전부 처리했다. M5(사내 원천 실행)는 아직이다.**

8차는 P0 6건을 `CLOSED 0 / PARTIAL 2 / OPEN 4` 로 재판정하고 M0~M5 를 순서로 지정했다.
이번 요청은 **M0~M4 가 실제로 닫혔는지**를 같은 반례로 다시 뚫어 달라는 것이다.

| M | 무엇 | 상태 | 회귀 |
|---|---|---|---|
| M0 | 실행 안전성 6건 | 완료 | `g0-m0-safety-tests.py` 51건 |
| M1 | child evidence contract 4건 | 완료 | `g0-normalize-tests.py` |
| M2 | B1 재작성 5건 | 완료 | `InjectionMatrix.java` 29 + `g0-b1-analyzer-tests.py` 43 |
| M3 | normalizer 5건 | 완료 | `g0-normalize-tests.py` 147 + `g0-axes-tests.py` 127 |
| M4 | 사실·규범 문서 정정 5건 + §10 문서 상태 오류 6건 | 완료 | — (문서) |
| **M5** | **사내 원천에서 raw G0-0 수집** | **미실행** | — |

합계 회귀 **397건**(Python 368 + Java 29). 전부 통과.

**숫자를 그대로 믿지 말아 달라.** 8차가 지적한 것 중 가장 아팠던 것이 "매트릭스 26건 통과"가
사실이면서도 아무것도 보증하지 않던 경우였다(§3.4). 397 도 같은 방식으로 무의미할 수 있다.

---

## 2. 반드시 전제할 것 (8차와 동일)

1. **정합성을 DBA 협조에 걸 수 없다.** 비critical 권한 요청은 가능하나 **현재 보류**이며 어떤
   설계도 그것을 가정하지 않는다.
2. **DB 에 부하·악영향 금지.** 일부 원천은 **생산라인과 밀접**하다.
3. 원천 Oracle 은 **버전·옵션·charset 이 제각각**이다.
4. 코어는 **권한 0 · 최저 버전(11.2)** 에서 성립해야 한다.
5. **G0-0 은 사내 원천에 대해, 그리고 full sequence 로는 한 번도 실행되지 않았다.**
   저장소의 로컬·샌드박스 실측을 원천 사실로 읽지 마라.
6. **저장소에 플랫폼 코드가 0줄이다.** Control Plane·Guard·lease·Commit Adjudication 은
   아직 시험 대상이 아니다.
7. 실제 PoC 는 사내에서 진행한다. 지금은 리뷰 단계다.

---

## 3. M 별로 무엇을 바꿨나 — 그리고 **어디를 의심해야 하는가**

각 항목에 **내가 스스로 의심하는 지점**을 같이 적는다. 그쪽을 먼저 봐 달라.

### 3.1 M0 — 실행 안전성

- wrapper `set -euo pipefail` + producer exit/sentinel 별도 보존
- B0 `sys.exit(main())` — 오류를 찍고 exit 0 으로 끝나던 것
- B0/B1 `MAX_PARTITIONS=8` · `MAX_CONCURRENT_SESSIONS=12` 하드 상한
- **target 접촉 전 identity preflight** — `DUAL` 만 읽는 S-1 단계를 어떤 `.load()` 보다 앞에
  두고, `--expect-db-unique-name` 불일치면 즉시 종료
- C01~C09 **패키지 밖** `CE_ENV_ALLOWLIST` 필수. 그 검사가 **preflight 접속보다 먼저** 온다

> **의심 지점.** `MAX_PARTITIONS=8` 은 근거 없는 숫자다. "행 상한은 connection 수의 상한이
> 아니다"라는 8차 지적은 받았지만, 8이 안전한 값이라는 근거는 없다 — 원천 세션 budget 을
> 모르기 때문이다. 이것이 **가짜 안전감**을 주는가?

### 3.2 M1 — child evidence contract

- `g0-child-schemas/` A·B0·B1·C00 개별 스키마. **집계 전에** 검증
- manifest 에 `source_id`(원천 `DB_UNIQUE_NAME`) · `harness_digest`(하네스 코드 digest —
  `versions.lock` 은 실행 판본이지 코드가 아니다) · `started_at`/`ended_at`
- `check_run_set()` — duplicate/unknown/**concatenated** run 거부.
  각 child 는 자기 manifest 와 일관되므로 개별 검사로는 안 잡히는 것을 겨냥
- run 별 불변 산출물 경로(경로에 `RUN_ID` 필수, 덮어쓰기 금지)

> **의심 지점.** `harness_digest` 는 11개 하네스 파일의 해시다. **그 목록이 하드코딩**이라
> 파일을 새로 추가하면 digest 에 안 들어간다. 목록 자체의 digest 를 넣어야 하지 않는가?

### 3.3 M2 — B1 재작성

- 명시 `.option("connectionProvider", "g0b1tracer")`. 전역 비활성화는 진단 fallback 으로 강등
- **주입 대상을 driver 가 선언한 phase 로 정한다.** driver 가 step 전에 phase 파일에 이름을
  쓰고 provider 는 그 값만 읽는다. `Trace.classify(stack)` 의 스택 추정은 `path_guess` 로
  추적에 남지만 **어떤 판정 술어에도 들어가지 않는다**(8차 §2 P0-06-2 의 순환 판정)
- `--scenario {full,schema_only,task_only,metadata_only}` 독립 실행
- fail-closed PASS 조건: 회차별 `injection_applied>0` ∧ terminal token ∧
  `EXPECTED_FAILURE_OBSERVED` ∧ `rows_read_total==0` ∧ `trace_complete`

> **의심 지점 둘.**
> (a) phase 파일은 **driver JVM 의 로컬 파일**이다. executor 가 다른 노드에 있으면
> `declaredPhase()` 가 무엇을 읽는가? 지금은 `local[*]` 전제이고, 그 전제가 깨지는 순간
> task 경로 주입은 다시 성립하지 않는다. **이 한계가 문서에 충분히 적혀 있는가?**
> (b) `trace_complete` 는 shutdown hook 이 쓴 sentinel 로 판정한다. JVM 이 `SIGKILL` 되면
> 그 hook 이 안 돈다 — 그때 `MEASUREMENT_FAILED` 로 가는 것이 맞는가, 아니면 이 판정 자체가
> 관측할 수 없는 것을 관측한다고 주장하는가?

### 3.4 M3 — normalizer

- **child schema 를 통과한 산출물만 집계 입력이 된다.** 이전 판은 집계 뒤 `coverage` 만
  `FAILED` 로 덮었고 본문(`account_privs`·`capability_axes`·`source`·`fence_facts`)은 남았다
- **SQLCODE taxonomy 확장.** `NONE` 을 지지하는 것은 `UNSUPPORTED` 하나뿐이고
  `DENIED`·`WRONG_TARGET`·`PROBE_BUG`·`AMBIGUOUS`·`GRAMMAR`·`EMPTY`·`TRANSIENT` 는 전부
  `UNDETERMINED`. 뜻이 probe 마다 다른 코드(ORA-00904)는 **그 뜻을 아는 probe 가
  `absent_codes` 로 선언**한다. probe 별 typed predicate(행·값·문법·양성 대조)도 표로 분리
- **`effective_value` 가 실제로 floor 로 내려간다.** 사유 7종, `g0_axes.FLOOR_REASONS` 가 권위.
  floor 는 값을 **내리기만** 한다(`UNDETERMINED` 를 `sql_dialect` 의 floor `11G` 로 올리지 않는다).
  요약·판정이 `value` 가 아니라 `effective_value` 를 읽는다
- `not_covered` 를 `g0-final-contract.json`(P §8.1 항목 13개)과의 **차집합으로 자동 검사**.
  최종 게이트 입구는 `g0_final_gate.py` — `record_type != g0_evidence` 무조건 거절,
  최종 aggregator 는 실물이 없어 `NotImplementedError`
- 최종 레코드도 run 별 불변 경로. `current` 포인터는 **거부된 회차에서 `INVALIDATED` 로 덮인다**
- exit 3 은 **측정 완결성만** 말한다. capability 등급은 `outcome.capability` 로 분리

> **의심 지점 셋.**
> (a) **TTL 30일은 근거가 없다.** `freshness.basis = OPERATOR_DECLARED_TTL` 로 그 사실을
> 적기는 했지만, 근거 없는 값을 적어 두면 다음 사람이 그것을 기준으로 읽는다.
> 차라리 TTL 미선언(전 축 floor)을 기본으로 두는 것이 맞지 않는가?
> (b) `PROFILE_NOT_AUTHORITATIVE` floor 를 **내가 추가했다.** 8차 §6 의 불변식 목록에는
> "profile/runtime/source **mismatch**" 만 있고 "비권위 profile" 은 없다. 과한 확장인가,
> 아니면 레코드가 스스로 "설계 근거가 아니다"라고 적는 것과 정합한가?
> (c) `covered` 항목의 `present:false` 를 **위반이 아니라 사실**로 뒀다. 거짓 주장만 막고
> 실행 범위는 강제하지 않는다는 판단인데, 이게 빠져나갈 구멍인가?

### 3.5 M4 — 사실·규범 문서 정정

Oracle 사실 3건은 **1차 출처로 재확인**했다(`docs.oracle.com` 은 egress 차단이라
검색 경유로 원문 문구를 확인).

| 정정 | 이전 | 지금 |
|---|---|---|
| `AS OF TIMESTAMP` 오차 | "±3초 근삿값" | **최대 3초 이전** — 방향이 있고 미래로 가지 않는다 |
| `SCN_TO_TIMESTAMP` 와의 관계 | 섞어 씀 | 다른 문장이다. 후자는 상한도 방향 보장도 아니다 |
| `ORA-08181` | "같은 SCN 재사용이 이 오류의 표적" | **아니다.** SCN 이 유효 범위 **밖**일 때. 그 상황의 실제 코드는 `ORA-01555`/`ORA-01466` |
| `ORA-08180` | 어디에도 없음 | timestamp 매핑 실패의 코드. `AS OF TIMESTAMP` 채택 시 A §11.4 목록에 추가 필요 |
| grant 보류 근거 | "받아도 순이익이 아니다" | "**실증 전에는 활성화·요청하지 않는다**" — 순이익 부재를 단정할 측정이 0건 |
| passive vs runtime | 한 덩어리 | 분리. primary 로 가는 채널은 **runtime 쪽**에서 열린다 |
| 빠져 있던 이득 | — | 같은 timestamp 리터럴 → **cross-connection 공통 anchor**. 현 코어는 못 낸다 |
| 권한 규칙 | "object FLASHBACK" | `READ\|SELECT ∧ (object FLASHBACK \| FLASHBACK ANY TABLE)` |
| Spark provider | 전역 비활성화 필수 | `connectionProvider` 옵션이 주 수단. 전역은 진단 fallback |
| overlay 자기모순 | §3.1 철회 ↔ §7 확정, §5 가 없는 축 사용, "7축"인데 9행 | 넷 다 정정 |
| 7차 assessment 조치 5 | ✅ 완료 | **철회.** `failclosed_task` 는 argparse choices 에 없어 실행조차 안 됐다 |

> **의심 지점.** M4-1 의 "공통 anchor 이득"은 **논증이지 측정이 아니다.** 같은 리터럴이
> 모든 물리 connection 에 실제로 실리는지는 G0-0B1 이 답할 문제이고 아직 안 쟀다.
> 이것을 verdict 문서에 이득으로 적은 것이 **다시 "미확정을 확정으로 바꾸는" 패턴인가?**
> 나는 "가능성이지 실측이 아니다"라고 병기했지만, 8차가 지적한 그 병이 이런 모양으로 온다.

---

## 4. 특히 봐 주었으면 하는 것

### 4.1 M0~M4 가 진짜로 닫혔는가 — 같은 반례로

8차 §12 "필수 회귀 시험" 목록을 그대로 다시 구성해 뚫어 달라.

```
missing/duplicate/unknown probe ID
summary 복수 · concatenated runs
summary 존재 + end sentinel 부재 + child exit nonzero
old log/new lock · A/B source swap · profile relabel
query_ok=true + row 없음 / null / malformed value
unsupported/denied/empty/transient/wrong-target/probe-bug SQLCODE
stale high · new transient · expiry
invalid rerun 뒤 old final 을 current 로 읽지 않음
G0-0 record 를 final G0 gate 에 입력하면 항상 거부
```

각각 회귀 시험으로 옮겼다고 **주장**한다. 그 시험이 **실제로 그 성질을 시험하는지** 봐 달라 —
"픽스처가 현실과 다르면 아무것도 시험하지 않는다"가 M1 작업 중 실제로 걸린 문제였다.

### 4.2 이번에 새로 만든 것의 결함

| 새 파일 | 무엇 | 무엇을 의심하는가 |
|---|---|---|
| `g0-final-contract.json` | P §8.1 항목 13개 | 항목 목록이 맞는가. `executed_at` 을 `children[*].measured_at` 으로 덮었다고 한 것이 타당한가 |
| `g0_final_gate.py` | 최종 게이트 입구 | 세 검사(`record_type`·`gate_eligible`·항목 존재)로 충분한가. **미구현을 `NotImplementedError` 로 적는 것**이 정직한가, 아니면 회피인가 |
| `g0_axes.PROBE_SPEC` | probe별 typed predicate | `absent_codes` 배정이 Oracle 사실에 맞는가. 특히 `feat.standard_hash_sha256` 의 ORA-00904, `alter.STANDBY_MAX_DATA_DELAY.*` 의 ORA-02248 |
| `g0_axes.apply_floors` | floor 실동작 | 사유 7종이 8차 §6 불변식을 **덮는가**. 빠진 것이 있는가 |

### 4.3 아직 안 한 것이 정말 미뤄도 되는가

- **A §22 미결 3건**(A-14·A-22·A-23) — 조직 결정 대상으로 뒀다
- **UI IA §9-6**(capability 표시) — G0-0 실측이 답한다고 뒀다
- **§14.4 사번 결속 검증 3건** — 사내 IdP 가 사번 claim 을 내는가 / Dagster proxy access log 에
  사번을 실을 수 있는가 / HR 이 사번을 재사용하는가. **셋 다 확인 못 했고 설계는 그것을 전제한다**
- **8차 §8.4 누수 경로 4번** — 로컬 B1 증거에 raw `javap`/ServiceLoader 출력과 artifact hash 가
  없다. S2·S3 회차를 `g0-run-child.sh` 로 감싸야 닫힌다. **안 했다**
- **P1-07**(LOB RETENTION 분리) — 8차 §7 은 "Flashback/LOB 활성화 전 필수"로 뒀고 활성화하지
  않았으므로 미뤘다. 이 논리가 성립하는가

### 4.4 M5 를 열어도 되는가 — **이번 회차의 실질 질문**

M0~M4 를 닫은 목적은 하나다. **사내 원천에서 G0-0 을 돌려도 되는가.**

봐 주었으면 하는 것:

1. 생산라인과 밀접한 원천에 대해 **지금 하네스가 안전한가.** 특히 A 의 대상 접촉 횟수,
   B0 의 partition/session 상한, C00 의 full-scan 계열
2. 실행 순서(`g0-0-runbook.md`)가 **운영자가 그대로 따라가면 되는 상태인가**
3. 실행 후 나올 증거가 **사후에 재판정 가능한가** — raw 보존·결속·거부 경로
4. 여전히 남은 **NO-GO 사유**가 있는가

---

## 5. 이 저장소의 규율 (이 기준으로 봐 달라)

> 확인하지 못한 것은 "미확인"이라고 쓴다.
> 오류 부재는 증거가 아니다. 0건 조건에는 양성 대조를 함께 둔다.
> 측정하지 않은 숫자를 측정한 것처럼 쓰지 않는다.
> **검증 도구는 그것이 검증하는 대상보다 엄격해야 한다.**
> **'고쳤다'고 쓰기 전에 그 경로를 한 번이라도 실행했는지 묻는다.** ← 8차에서 배운 것

마지막 줄이 8차의 가장 아픈 지적이었다. 7차 assessment 가 조치 5 를 ✅ 로 적었는데
그 코드는 **argparse choices 에 값이 없어 실행조차 되지 않았다**. 이번 판이 같은 병을
반복하는지 봐 달라 — 특히 §3 의 "의심 지점"들이 그 후보다.

---

## 6. 읽는 순서

```
README.md                                     현재 상태표·문서 지도 (§3 증거 계약이 핵심)
HANDOFF.md                                    다음에 할 일 · 반복된 실수 목록
etl-platform-v2.0-codex-eighth-cross-review.md   8차 원본 (M0~M5 의 출처)

── M3 이 가장 많이 바꾼 곳 ──
g0-normalize.py                               집계 순서 · floor · 포인터 · outcome
g0_axes.py                                    SQLCODE taxonomy · PROBE_SPEC · apply_floors
g0-0-evidence.schema.json                     axis 필수 필드 · outcome · freshness · covered
g0-final-contract.json + g0_final_gate.py     최종 계약과 게이트 입구 (신규)

── M2 ──
g0-0b1-connection-provider/                   README · Preamble.java · Trace.java · analyze-trace.py

── M4 가 고친 문서 ──
etl-platform-v2.0-grant-request-verdict.md    §1·§3 전면 개정
etl-platform-v2.0-capability-overlay.md       머리말 정정 블록 · §5 · §7 · §8-2 · A.3 · A.4
etl-platform-local-poc-plan.md                §1 H/D/X — D 의 이전 술어, X 의 사유 분류
etl-platform-v2.0-codex-seventh-review-assessment.md  조치 5 철회 블록

── 규범 ──
etl-platform-target-architecture-v1.2.4.md    A 현행 (§11.4 ORA 코드 정정)
etl-platform-poc-test-plan-v1.md              P §8.1 이 최종 g0_evidence 의 권위
etl-platform-ui-information-architecture.md   Control Plane UI 정보구조 (판본 없음, 현행 하나)

── 실행 ──
g0-0-runbook.md                               절차서. M5 를 여는 문
g0-0a-capability-inventory.sql                87 probe
versions.lock                                 판본 고정
```

---

## 7. 회귀 시험 돌리는 법

```bash
python3 g0-normalize-tests.py      # 147
python3 g0-axes-tests.py           # 127
python3 g0-b1-analyzer-tests.py    #  43
python3 g0-m0-safety-tests.py      #  51

cd g0-0b1-connection-provider
SPARK_HOME=<spark 배포판> bash build.sh && SPARK_HOME=<...> bash run-tests.sh   # 29 (Java)
```

`run-tests.sh` 는 `src/` 가 `build/classes` 보다 새로우면 **exit 2 로 거부**한다.
M2 작업 중 stale class 로 시험이 통과하는 일이 실제로 있었다.
