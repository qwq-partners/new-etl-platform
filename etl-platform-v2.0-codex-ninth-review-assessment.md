# 9차 Codex 교차 리뷰 — 판정서

- 판정일: 2026-08-31
- 대상: `etl-platform-v2.0-codex-ninth-cross-review.md` (리뷰일 2026-08-31, tip `d82e3d0`)
- 판정 기준 커밋: `8d7e713`
- 판정 방법: 각 지적을 **코드 실물(줄 번호)과 직접 실행**으로 재현. 기본 입장은 "이 지적이 틀렸다"

---

## 0. 한 문장

**기각 0건이다. NO-GO 판정을 수용한다.**

| 구간 | 건수 | 확정 | 부분 확정 | 기각 |
|---|---:|---:|---:|---:|
| P0 | 7 | **7** | 0 | **0** |
| P1 | 7 | **7** | 0 | **0** |
| P2 | 6 | 6 | 0 | 0 |
| §5.2 stale 문구 | 4 | 4 | 0 | 0 |
| **합계** | **24** | **24** | 0 | **0** |

그리고 **판정 과정에서 리뷰가 짚지 않은 결함 3건을 더 찾았다**(§5).

### 0.1 M 별 재판정 수용

| M | 8차 이후 자체 주장 | 9차 재판정 | 수용 |
|---|---|---|---|
| M0 | 완료 | PARTIAL | ✅ |
| M1 | 완료 | **OPEN** | ✅ |
| M2 | 완료 | **OPEN** | ✅ |
| M3 | 완료 | PARTIAL | ✅ |
| M4 | 완료 | PARTIAL | ✅ |
| M5 실행 | "남은 것은 실행뿐" | **NO-GO** | ✅ — 그 상태 표시를 철회한다 |

### 0.2 이번 리뷰의 성격

7차는 *"문서는 고쳤는데 코드가 안 고쳐졌다"* 를 잡았다.
8차는 *"코드는 고쳤는데 그 경로를 한 번도 실행하지 않았다"* 를 잡았다.
**9차는 *"실행 경로를 고쳤다고 쓰고, 그 고침을 검증하는 시험도 만들었는데, 그 시험이 실물 producer 를 우회한다"* 를 잡았다.**

세 번 다 같은 축 위에 있다 — **주장과 관측 사이의 거리**. 이번에는 그 거리가 시험 안으로 들어왔다.

---

## 1. 무엇을 직접 재현했나

리뷰의 397/397 재현은 신뢰한다(같은 명령으로 같은 수가 나온다). 판정은 **그 숫자가 아니라 반례**로 했다.

| 지적 | 재현 방법 | 결과 |
|---|---|---|
| P0-01 | probe 3건 + `summary{expected:86,emitted:86}` 로 정규화기 실행 | `MEASURED`·위반 0건 — **확정** |
| P0-02 | 같은 회차에 manifest `source_id=TESTSTBY`, 서버 `DB_UNIQUE_NAME=ETLSTB` | 위반 0건 — **확정** |
| P0-03 | `run.sh` · `run-g0-0b1.py` · `Trace.java` · `analyze-trace.py` 의 run 식별자 대조 | 세 이름이 다르다 — **확정** |
| P0-04 | `est_sessions = partitions + 1`, `partitions ≤ 8` | 최대 9 < 12, 분기 도달 불가 — **확정** |
| P0-05 | `g0-0a-capability-inventory.sql` 의 identity probe 와 target touch 순서 | 기록만 하고 계속 진행 — **확정** |
| P0-06 | runbook 명령과 wrapper·CLI 요구사항 대조 | 8건 중 6건 직접 확인 — **확정** |
| P1-02 | `analyze-trace.py:71-72` | `trace_complete = bool(ends)` — **확정** |
| P1-04 | 쓰레기 문자열로 채운 레코드를 `admit()` 에 투입 | **승인됨** — 확정, 그리고 리뷰보다 나쁘다(§5-1) |
| P2-1 | A SQL 의 `&TARGET_OWNER..&TARGET_TABLE` 출현 수 | 5건. 주석은 "한 건뿐" — **확정** |

---

## 2. P0 7건

### P0-01 A 완결성 검사가 선언된 probe 집합을 검증하지 않는다 — **확정**

실행 결과가 그대로 증거다.

```
입력: probe 3건 + {"probe_summary":{"expected":86,"emitted":86,"manifest_ok":true}} + sentinel
결과: coverage.g0_0a = MEASURED
      contract_violations = []
      exit 3 (다른 child 미실행 때문이며 A 때문이 아니다)
```

`cov_a()` 는 sentinel·중복·summary 개수·`manifest_ok` 만 본다. 다음을 **하나도** 보지 않는다.

- `len(parsed_probes)` vs `summary.emitted`
- `summary.expected` vs SQL 의 `c_expected = 87`(`g0-0a-capability-inventory.sql:58`)
- 빠진 ID · 알 수 없는 ID

**같은 파일의 `cov_b0` 와 `cov_c00` 은 그 검사를 한다.** `cov_b0` 는 `expected_steps` 와 `emitted_steps` 의 차집합을 보고, `cov_c00` 은 `expected_probe_ids` 를 id 집합으로 대조한다. **A 만 안 한다.** 87 probe 를 내는 가장 큰 child 가 가장 약한 검사를 받고 있었다.

> **이 반례를 저장소가 스스로 고정해 뒀다.** `full_fixture()` 의 A 로그는 probe 3건이고 summary 는 86 이다. 그리고 그 fixture 는 **내가 M3 에서 손댔다** — `nls.characterset` 을 추가하면서 probe 가 3건뿐이고 summary 가 86 이라는 것을 보고도 넘어갔다. 픽스처가 현실과 다르면 아무것도 시험하지 않는다는 것을 M1 에서 배웠는데, 같은 자리에서 다시 놓쳤다.

### P0-02 source 와 profile 이 자가 신고값에 머문다 — **확정**(두 갈래 모두)

**원천 신원.** 한 레코드 안에 모순된 두 원천이 들어가고 위반이 0건이다.

```
children.g0_0a.source_id     = TESTSTBY   (manifest — 운영자가 G0_SOURCE_ID 로 신고)
record.source.db_unique_name = ETLSTB     (A probe — 서버가 스스로 밝힌 값)
contract_violations = []
```

`check_child` 는 manifest `source_id` 를 `--source-id` 인자·다른 child 와만 대조한다. **서버가 말한 값과는 대조하지 않는다.** child 들이 같은 거짓 이름을 공유하는 것은 같은 원천에서 나왔다는 증명이 아니다.

M1-2 의 목적이 *"어느 원천에서 잰 값인지 말하게 한다"* 였는데, 레코드가 두 원천을 동시에 말하면서 통과한다. **M1 을 OPEN 으로 되돌리는 것이 맞다.**

**실행 profile.** `PROFILE_NOT_AUTHORITATIVE` floor 는 `LOCAL_WSL`·`SANDBOX_CONTAINER` 에만 걸린다(`g0-normalize.py:761-764`). caller 가 `PROFILE=CORP_POC` 를 고르면 floor 를 우회한다. 8차의 profile relabel 반례가 **그대로 살아 있다** — 내가 M3 에서 추가한 floor 는 그 반례를 막는 것이 아니라 **정직하게 신고한 회차만** 막고 있었다.

### P0-03 B1 `PROVEN` 이 실물 launcher 에서 도달 불가능하다 — **확정. 이번 리뷰의 최대 결함**

run 식별자가 **세 가지 이름으로 갈려 있다.**

| 어디 | 무엇 | `failclosed_task` 회차의 값 |
|---|---|---|
| `run.sh:83` | `-Dg0b1.run` (system property) | `failclosed_task` |
| `run.sh:83` | python `--mode` | `failclosed` |
| `run-g0-0b1.py:138` | phase 파일 `g0-0b1-phase-{a.mode}.txt` | `g0-0b1-phase-failclosed.txt` |
| `Trace.java:67` | phase 파일 `g0-0b1-phase-{run()}.txt` | `g0-0b1-phase-failclosed_task.txt` |
| `run-g0-0b1.py:276` | terminal token `"run": a.mode` | `failclosed` |
| `analyze-trace.py:220` | 요구하는 run 이름 | `failclosed_task` |

**driver 가 쓰는 파일과 provider 가 읽는 파일의 이름이 다르다.** 결과는 두 회차에서 서로 다르다 —
리뷰는 둘을 함께 묶었지만 실패 양상이 갈리고, **그 차이가 왜 아무도 못 알아챘는지를 설명한다.**

| 회차 | 주입 | 판정 |
|---|---|---|
| `coverage` | — | `run == mode == coverage` 라 **우연히 맞는다.** 그래서 정상으로 보였다 |
| `failclosed_schema` | `fail=all` → phase 와 무관하게 **주입은 걸린다** | terminal token 이름이 달라 `terminal_token_present=false` |
| `failclosed_task` | `fail=phase` + `fail.phase=partitioned_count` vs `declaredPhase()="UNDECLARED"` → **주입이 아예 안 걸린다** | 같은 이유로 판정 불가 |

`run_proven()` 은 `injection_applied>0 ∧ terminal_token_present ∧ …` 를 요구하므로 **두 회차 모두 영원히 `NOT_PROVEN`** 이다.

**왜 시험이 못 잡았나.** 이것이 핵심이다.

- `InjectionMatrix.java` 29건 — `Preamble.shouldFail(declaredPhase)` 를 **순수 함수로** 시험한다. phase 가 어디서 오는지는 시험 범위 밖이다.
- `g0-b1-analyzer-tests.py` 43건 — producer 가 만들지 **않는** `failclosed_schema`/`failclosed_task` 토큰을 직접 합성해 판정기에 넣는다.

두 시험 다 통과하면서 **그 사이의 배선은 아무도 안 본다.** 리뷰 §1.3 이 정확하다.

> **이것은 8차가 잡은 것과 같은 병이다.** 8차: `failclosed_task` 가 argparse choices 에 없어 실행조차 안 됐다. 9차: 실행은 되는데 이름이 달라 판정에 닿지 않는다. **M2 의 존재 이유가 fail-closed 를 증명 가능하게 만드는 것이었는데, 실물 producer 는 그 증명을 만들 수 없다.** M2 를 OPEN 으로 되돌린다.

추가 지적 넷도 전부 확정이다 — `task_only` 도 먼저 `.schema` 를 부른다(`run-g0-0b1.py:206-220`), task 귀속이 topology 증거가 아니라 시나리오 이름에 의존한다, phase 파일이 driver 로컬이라 원격 executor 는 못 본다(내가 9차 요청서 §3.3 에 스스로 적은 의심 지점이다), schema 실패와 task 실패가 분리되지 않는다.

### P0-04 원천 보호 한계가 행 수 제한에 치우쳐 있다 — **확정**

산수로 끝난다.

```python
est_sessions = a.partitions + 1        # g0-0b0-spark-smoke.py:98
if est_sessions > MAX_CONCURRENT_SESSIONS:   # 12
```

`partitions ≤ 8` 이 이미 강제되므로 `est_sessions ≤ 9`. **이 분기는 어떤 정상 입력에서도 도달하지 않는다.** 안전 검사처럼 보이지만 죽은 코드다.

나는 9차 요청서 §3.1 에서 *"`MAX_PARTITIONS=8` 은 근거 없는 숫자다"* 라고 스스로 의심했다. **그런데 그 옆의 12 가 아예 안 걸린다는 것은 못 봤다.** 의심을 적는 것과 실제로 값을 넣어 보는 것은 다르다.

그리고 더 근본적인 지적을 수용한다 — **한 Spark read 의 partition 상한은 source 전체의 안전 budget 이 아니다.** 여러 job 이 같은 원천에 동시에 붙으면 이 상한은 아무것도 보장하지 않는다. 시간·I/O·statement deadline·retry budget 도 없다.

### P0-05 A identity probe 가 대상 접촉을 막지 않는다 — **확정**

`p_scalar('userenv.DB_UNIQUE_NAME', …, '&EXPECT_DBUNAME')` 의 세 번째 인자는 **기대값과 비교해 `value_mismatch` 로 기록**할 뿐이고 블록은 계속 돈다. 그 뒤 `after_D.touch_target`(`:134`)가 대상 테이블을 읽는다. 확인된 target touch 는 **5건**이다.

M0-4 로 preflight 를 넣은 것은 **B0(`g0-0b0-spark-smoke.py`)** 였고, **A(SQL)에는 넣지 않았다.** 87 probe 를 내는 쪽이 그대로 남았다. 생산라인 밀접 원천에서 기록은 차단이 아니다.

### P0-06 runbook 을 그대로 실행할 수 없다 — **확정**

8건 중 6건을 직접 확인했다.

| # | 지적 | 확인 |
|---|---|---|
| 1 | `G0_SOURCE_ID` 미export | ✅ wrapper `:41-47` 가 없으면 exit 2 |
| 2 | 정의되지 않은 `$DB_UNIQUE_NAME`(`:322`) | ✅ **M4 에서 내가 넣었다**(§5-2) |
| 3 | B0 에 `--expect-db-unique-name` 없음 | ✅ M0-4 가 필수로 만든 인자 |
| 4 | B1 artifact 경로에 `RUN_ID` 없음(`:249`) | ✅ M1-4 wrapper 가 거부 |
| 5 | CE artifact 경로에 `RUN_ID` 없음(`:291`) | ✅ 같은 이유 |
| 6 | `CE_ENV_ALLOWLIST` 없음 | ✅ M0-5 가 필수로 만든 값 |
| 7 | `CE_DOC_PATH` 가 `v1.2.3.1`(`:287`) | ✅ 현행은 v1.2.4 |
| 8 | A SQL 내부 `DEFINE` 이 앞선 값을 덮음 | 미확인 — SQL 실행이 필요하다 |

**패턴이 하나다.** 1·3·4·5·6 은 전부 **M0·M1 이 새로 만든 요구사항**이고, 절차서는 그 요구사항이 생기기 전에 쓰였다. 계약을 강하게 만들면서 그 계약을 쓰는 문서를 같이 고치지 않았다.

> *"이 문서만 따라가면 된다"* 고 쓴 문서가 **문서대로 하면 실패한다.** 운영자 실수가 아니라 문서 결함이며, M5 의 직접 차단 사유라는 판정에 동의한다.

### P0-07 CE 증거와 corporate source 증거를 같은 회차로 묶을 수 없다 — **확정**

`check_run_set()` 은 child 들의 `source_id` 가 갈리면 거부한다(M1-3). CE 는 폐기용 writable primary 에서 돌고 A/B0/B1/C00 은 사내 standby 를 본다. 따라서 한 회차에 넣으면 **CE 가 거짓 source ID 를 신고하거나 집계가 거부되거나** 둘 중 하나다.

M1-3 을 만들 때 "이어 붙인 회차는 하나의 회차가 아니다"를 강제했는데, **CE 가 원래 다른 환경의 것이라는 사실을 계약이 표현할 방법이 없다.** `environment_scope` 도입이 맞다.

외부 allowlist 가 "저장소 밖에 있다"만 증명한다는 지적도 맞다. 승인 주체·소유권·서명은 증명하지 않는다.

---

## 3. P1 7건 — 전부 확정

| # | 지적 | 판정 | 근거 |
|---|---|---|---|
| P1-01 | `harness_digest` 가 behavior surface 를 덮지 못한다 | **확정** | `g0-run-child.sh:82-87` 이 11개 파일을 하드코딩. provider Java source·`build.sh`·child schema·`g0-final-contract.json`·CE scenario 코드가 전부 빠져 있다. 내가 9차 요청서 §3.2 에 의심 지점으로 적었고, 리뷰가 **어느 파일이 빠졌는지**까지 열거해 확정했다 |
| P1-02 | trace 완결성이 "하나의 `trace_end`" 로 축약된다 | **확정** | `analyze-trace.py:71-72` — `trace_complete = bool(ends)`. driver 만 정상 종료해도 전체가 complete 다. 요청서 §3.3(b) 의 의심 지점이 이 형태로 확정됐다 |
| P1-03 | 기본 TTL 30일은 측정하지 않은 정책값 | **확정** | `g0-normalize.py:73`. `basis` 에 "operator-declared" 라고 적어 두는 것과 **운영자가 선언하지 않아도 자동 적용되는 것**은 다르다. 내 의심 지점(§3.4-a)에 대한 답이며 리뷰 쪽이 맞다 |
| P1-04 | final gate 가 forged record 를 승인한다 | **확정 + 리뷰보다 나쁘다** | §5-1 참조 |
| P1-05 | B0 preflight 가 후속 physical connection 을 결속하지 못한다 | **확정** | 첫 DUAL connection 의 신원으로 나머지를 대표할 수 없다. pool·TNS failover 가 있으면 특히 |
| P1-06 | `PROFILE_NOT_AUTHORITATIVE` floor 는 유지할 가치가 있다 | **확정 — 내 의심 지점에 대한 답** | 요청서 §3.4(b)에서 "과한 확장인가" 물었다. 답은 "방향은 맞고 문제는 `CORP_POC` 자가선언"이다. **floor 를 지우지 않는다** |
| P1-07 | `covered.present=false` 는 사실 표현으로 허용 가능 | **확정 — 내 의심 지점에 대한 답** | 요청서 §3.4(c)의 답. 다만 "complete" 조건은 exact required set 통과일 때만 참이어야 한다는 단서를 수용한다(P0-01 과 같은 뿌리) |

---

## 4. P2 · stale 문구

### P2 6건 — 전부 수용

1. A SQL `:133` 주석 "대상 테이블 접촉은 ROWNUM = 1 **한 건뿐**" — 실제 5건. **확정**
2. `MAX_CONCURRENT_SESSIONS` 도달 불가 분기 제거 또는 정책식으로 교체 — P0-04 와 함께
3. CE "PASS" 가 실행 완결인지 mitigation 성공인지 이름 분리
4. `g0-final-contract.json` 의 표시 이름(`hash_vector_result (V-01~V-16)`)과 실제 field name 분리 — P1-04 와 같은 뿌리(§5-1)
5. `executed_at` 을 임의 child 의 `measured_at` 으로 만족시키지 않는다
6. `covered` 에서 raw field 존재와 semantic validation 분리

### §5.2 stale 문구 4건 — 전부 수용

| 문구 | 어디 | 정정 방향 |
|---|---|---|
| "딕셔너리 row 1건" | verdict `§3-3`, HANDOFF | **측정하지 않은 수치다.** "일회성 metadata·redo·audit·invalidation 영향, 정확한 양 미측정"으로 낮춘다 |
| 공통 anchor 를 "이득"으로 | verdict `§3-0`, overlay `§8-2` | **"검증할 잠재 이득"이지 확인된 이득이 아니다.** 내가 9차 요청서 §3.5 에서 스스로 의심한 그 항목이고, 리뷰가 확정했다 |
| README 의 "7축 표" | `README.md:80` | 현행 13축과 맞지 않는다. M4-5 에서 overlay 본문은 고치고 **README 의 참조는 안 고쳤다** |
| runbook 의 `v1.2.3.1` pointer·H/D/X 설명 | runbook | M4 정정 뒤에도 동기화되지 않았다 |

---

## 5. 리뷰가 짚지 않은 것 — 판정 중 추가로 찾았다

### 5-1. `admit()` 은 계약의 `where` 를 **쓰지 않는다** — P1-04 보다 나쁘다

리뷰는 "`admit()` 이 값의 schema·digest 를 안 본다"고 했다. 맞지만 그 앞에 더 기본적인 것이 있다.

```python
missing = [it for it in contract_items(c) if not resolve(record, it)]   # g0_final_gate.py:106
```

**item 이름을 경로로 해석한다.** 그런데 계약 파일은 항목마다 `where` 를 따로 갖고 있고, G0-0 정규화기의 `covered` 는 그 `where` 를 쓴다.

| 항목 | `admit()` 이 찾는 경로 | 계약의 `where` |
|---|---|---|
| `executed_at` | `record["executed_at"]` | `children[*].measured_at` |
| `oracle_env.nls_characterset` | `record["oracle_env"]["nls_characterset"]` | `source.characterset` |

**같은 계약 파일을 두 소비자가 다른 규칙으로 읽는다.** M3-4 에서 계약과 게이트를 분리하며 만든 결함이고, `where` 필드는 게이트에서 죽은 데이터다.

실증 — 전부 쓰레기 문자열인 레코드가 **승인된다.**

```
{"record_type":"g0_evidence","gate_eligible":true,
 "g0_report_id":"쓰레기","executed_at":"언젠가","versions_lock_digest":"digest아님",
 "oracle_env":{...}, "ddl_digest":"FORGED", …}
  → admitted = True, reasons = []
```

### 5-2. P0-06 의 `$DB_UNIQUE_NAME` 은 **M4 에서 내가 새로 만든 결함이다**

리뷰는 "정의되지 않은 변수"라고만 적었다. 사실은 그보다 나쁘다 — **그 줄은 M4 커밋(`94a555d`)이 추가한 것**이다. runbook 에 `--source-id "$DB_UNIQUE_NAME"` 을 넣으면서 그 변수를 export 하는 줄은 넣지 않았다.

**같은 커밋의 9차 요청서에 내가 이렇게 썼다** — *"'고쳤다'고 쓰기 전에 그 경로를 한 번이라도 실행했는지 묻는다."* 그 문장을 쓰면서 바로 위에서 한 번도 안 돌려 본 명령을 절차서에 넣었다.

### 5-3. P0-03 이 왜 `coverage` 회차에서는 안 드러나는가

리뷰는 세 회차를 함께 묶었다. 실제로는 **`coverage` 만 `run == mode` 라 우연히 맞는다.** 그래서 배선을 눈으로 훑을 때 정상으로 보인다. 이 우연이 없었다면 첫 실행에서 바로 드러났을 결함이다.

**교훈**: 식별자가 여러 이름으로 갈릴 때, 그중 하나라도 우연히 일치하는 경우가 있으면 그것이 나머지를 가린다. run identity 를 **하나의 필드**로 만들어야 하는 이유가 이것이다.

---

## 6. 근본 원인 셋

리뷰 §10 의 세 뿌리를 그대로 수용하되, **왜 시험이 못 잡았는가**를 한 줄 더 붙인다.

| 뿌리 | 무엇 | 왜 397건이 못 잡았나 |
|---|---|---|
| **증거 신원** | 누가·어디서·어느 원천에 대해 실행했는가가 서버 사실에 결속되지 않는다 | 픽스처가 그 불일치를 **정상 입력으로** 갖고 있었다(`TESTSTBY` vs `ETLSTB`) |
| **실행 종단** | 실물 producer 가 판정기가 기대하는 artifact 를 만들지 못한다 | 시험이 producer 를 건너뛰고 **판정기에 직접 합성 입력을 넣는다** |
| **원천 보호** | 반환 행 수와 단일 run partition 상한이 source 전체 안전을 보장하지 않는다 | 시험이 인자 검증만 보고 **동시 실행·시간·I/O 를 모델링하지 않는다** |

세 줄이 같은 말을 한다 — **시험의 경계가 producer 앞에서 끝난다.** 이번 조치의 중심은 개별 결함이 아니라 그 경계를 producer 뒤로 미는 것이다.

---

## 7. 조치 순서

리뷰 §8 을 따르되 **순서를 하나 바꾼다** — P0-06(runbook dry-run)을 P0-03 과 함께 맨 앞에 둔다. 둘 다 "실물을 한 번 돌리면 즉시 드러났을 것"이고, 그 통합 시험이 서면 나머지 조치의 회귀를 잡아 준다.

| # | 조치 | 겨냥 | 새 시험 |
|---|---|---|---|
| **1** | B1 run identity 를 **하나의 필드**로 통일 | P0-03 · 5-3 | **실물 `run.sh` 통합 시험**(fake spark-submit 으로 종단 배선 검증) |
| **2** | runbook 명령 정정 + **clean shell dry-run 시험** | P0-06 · 5-2 | runbook 의 모든 명령을 파싱해 필수 env·인자·경로 규칙 검사 |
| **3** | A **87-ID exact manifest** 검증 | P0-01 · P1-07 | 삭제·추가·개수 불일치·거짓 summary 4반례 |
| **4** | profile attestation + 서버 신원 결속 | P0-02 · P1-06 | source_id↔`DB_UNIQUE_NAME` 불일치, `CORP_POC` 재라벨 |
| **5** | harness manifest 를 versioned 선언으로 | P1-01 | 미선언 파일 추가 시 실패 |
| **6** | source safety envelope · identity fail-fast | P0-04 · P0-05 · P1-05 · P2-2 | 기본 partitions=1, 도달 가능한 세션 상한, A SQL 차단 |
| **7** | corporate / CE evidence scope 분리 | P0-07 | scope 혼합 거부 |
| **8** | TTL 기본값 미선언으로 | P1-03 | 선언 없으면 전 축 floor |
| **9** | final gate **hard-disabled** + `where` 사용 | P1-04 · 5-1 · P2-4 | forged record 무조건 거부 |
| **10** | trace 완결성을 stream 별로 | P1-02 | executor trace 하나만 잘려도 `MEASUREMENT_FAILED` |
| **11** | M4 stale 문구·runbook pointer 동기화 | §5.2 · P2-1 | — |

**조치 1·2 가 이번 회차의 중심이다.** 나머지는 개별 결함이지만 이 둘은 *"시험의 경계를 producer 뒤로 민다"* 는 성질을 만든다.

### 즉시 철회하는 상태 표시

- README·HANDOFF 의 **"남은 것은 M5 — 실제로 돌리는 것"** → 철회. 리뷰 §7 의 M5a~M5e 로 대체한다
- PR #1 본문의 같은 문장 → 갱신
- **M1·M2 를 "완료"에서 되돌린다.** M0·M3·M4 는 PARTIAL 로 표시한다

---

## 8. 재리뷰 합격 기준 13건 — 대응표

리뷰 §9 의 13건을 **그대로 회귀 시험으로 옮긴다.** "실물 producer 를 통해" 가 조건이므로, 합성 fixture 로 대체하지 않는다.

| # | 반례 | 조치 |
|---:|---|---|
| 1 | A required probe 하나 삭제 → 집계 전 exit 4 | 3 |
| 2 | A unknown probe 추가 → exit 4 | 3 |
| 3 | A 실제 3건 + summary 87 → exit 4 | 3 |
| 4 | manifest source ≠ 서버 `DB_UNIQUE_NAME` → target touch 전 종료 | 4 · 6 |
| 5 | WSL 에서 `PROFILE=CORP_POC` 재라벨 → 거부 또는 floor | 4 |
| 6 | behavior file 추가/변경 → digest 변화 또는 unlisted 실패 | 5 |
| 7 | 실물 `run.sh` 가 두 token 을 만들고 analyzer 가 인식 | **1** |
| 8 | task 회차가 schema connection 에서 실패 → TASK proven 금지 | 1 |
| 9 | executor trace 하나만 sentinel 없음 → 전체 `MEASUREMENT_FAILED` | 10 |
| 10 | runbook 전 명령 clean dry-run → 누락 0건 | **2** |
| 11 | 두 동시 run 이 session budget 초과 → 둘째 거부 | 6 |
| 12 | final gate 에 forged record → 무조건 거부 | 9 |
| 13 | corporate A/B 와 CE record 혼합 → scope 위반 거부 | 7 |

---

## 9. 이번 판정에서 유지하는 것

리뷰 §10 이 "헛되지 않았다"고 한 것들을 그대로 유지한다. 되돌리지 않는다.

- 종료 코드 보존과 `sys.exit(main())`
- child schema 4종과 manifest 결속의 **형태**(내용 검사는 강화한다)
- SQLCODE taxonomy 와 probe 별 typed predicate
- `value` / `effective_value` 분리와 floor 가 값을 내리기만 하는 성질
- G0-0 / final G0 record type 분리와 `gate_eligible = const false`
- `PROFILE_NOT_AUTHORITATIVE` floor (P1-06 — 지우지 않는다)
- M4 의 Oracle·Spark 사실 정정 7건 (§5.1 이 전부 맞다고 확인)

**축 taxonomy·파생 방향은 GO 다.** 문제는 파생식이 아니라 그 앞의 입력 결속이며, 조치 3·4 가 그것을 닫는다.

---

## 10. 한 줄

> 8차는 *"고쳤다고 쓰기 전에 실행했는가"* 를 물었다.
> 9차는 **그 질문을 시험에도 해야 한다**고 답했다 — 판정기를 시험하는 것과 그 판정기가 받을 입력을 만드는 쪽을 시험하는 것은 다르다.
