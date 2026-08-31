# 10차 교차 리뷰 요청

- 요청일: 2026-08-31 (문서 정정 2026-09-01)
- 브랜치: `main` — **최신을 봐 달라**
- **코드 기준 커밋: `1b4c48e0a7c2b6893aaff2c248bedf4d21af1be2`**
  (조치 1~11 의 마지막 코드 커밋. 그 뒤 커밋은 이 요청서와 문서 정정뿐이며
  회귀 576건은 이 커밋에서 측정한 값이다)
- 저장소: `git@github.com:qwq-partners/new-etl-platform.git` — **9차 때와 달리 public 이다**
  (사내 반입 편의를 위한 결정, 2026-08-31). 읽기에 인증이 필요 없다
- 리뷰 결과 파일명(제안): `etl-platform-v2.0-codex-tenth-cross-review.md`

---

## 1. 이번 회차의 한 문장

**9차가 낸 조치 1~11 을 전부 처리했다. 그러나 그것은 여전히 로컬에서만 확인됐고,
9차의 핵심 지적(시험의 경계가 producer 앞에서 끝난다)이 진짜로 닫혔는지는 사내
원천에서 한 번도 확인되지 않았다.**

9차는 P0 7건 · P1 7건을 전부 확정하고 조치 순서 1~11 을 지정했으며 M5 를 NO-GO 로
판정했다. 이번 요청은 그 조치들이 **실제로 그 결함을 닫았는지**, 그리고 **M5a 를 열어도
되는지**를 묻는 것이다.

| 조치 | 무엇 | 커밋 | 누가 |
|---|---|---|---|
| 1 | B1 run 식별자 통일 + 실물 `run.sh` 종단 배선 시험 | `27f0310` | 이전 세션 |
| 2 | 절차서 정정 + dry-run lint | `c894f5e` | 이전 세션 |
| 3 | A 87-ID exact manifest 검증 | `f4b843d` | 이전 세션 |
| 4 | 원천 신원 결속 + profile attestation | `ede9d42` | 이전 세션 |
| 5 | harness digest 를 versioned 선언으로 | `3eb4d2b` | 이전 세션 |
| 6 | 원천 안전 봉투 · 신원 fail-fast | `77243c9` | 이전 세션 |
| 7 | corporate / CE 증거 scope 분리 | `893370e` | 이전 세션 |
| **8** | **TTL 기본값을 미선언으로** | `b7ffd3e` | 이번 세션 |
| **9** | **final gate hard-disabled + `where` 사용** | `e40d1f6` | 이번 세션 |
| **10** | **trace 완결성을 stream 별로** | `f3624da` (+ `1b4c48e`) | 이번 세션 |
| **11** | **M4 stale 문구 정정** | `b8d8c75` | 이번 세션 |

회귀 **576건**(Python 547 + Java 29). 전부 통과.

**두 가지를 먼저 밝힌다. 이 문서에서 가장 중요한 부분이다.**

1. **조치 1~7 은 이번 세션이 하지 않았다.** 나는 그 코드를 읽고 회귀가 통과하는 것을
   봤을 뿐, 그것이 겨냥한 결함을 **재현해 보지 않았다.** 8~11 은 재현부터 했다
   (§3 에 재현 결과를 적었다). 이 차이를 감안해 읽어 달라 — 조치 1~7 에 대한 내 서술은
   커밋과 코드에서 읽은 것이지 실행해서 확인한 것이 아니다.
2. **숫자를 그대로 믿지 말아 달라.** 397 → 576 은 그 자체로 아무것도 보증하지 않는다.
   9차가 가장 아프게 지적한 것이 "회귀 397건은 재현되지만 그중 B1 관련 72건은 실물
   producer 를 건너뛴다" 였다. 576 도 같은 방식으로 무의미할 수 있다.

---

## 2. 반드시 전제할 것 (9차와 동일)

1. **정합성을 DBA 협조에 걸 수 없다.** 비critical 권한 요청은 가능하나 **현재 보류**이며
   어떤 설계도 그것을 가정하지 않는다.
2. **DB 에 부하·악영향 금지.** 일부 원천은 **생산라인과 밀접**하다.
3. 원천 Oracle 은 **버전·옵션·charset 이 제각각**이다.
4. 코어는 **권한 0 · 최저 버전(11.2)** 에서 성립해야 한다.
5. **G0-0 은 사내 원천에 대해, 그리고 full sequence 로는 한 번도 실행되지 않았다.**
6. **저장소에 플랫폼 코드가 0줄이다.** Control Plane·Guard·lease·Commit Adjudication 은
   아직 시험 대상이 아니다.
7. 실제 PoC 는 사내에서 진행한다. 지금은 리뷰 단계다.

### 이번 회차의 실행 환경 (증거 등급에 영향)

로컬 WSL2 에 개발환경을 세웠다. **Oracle 은 없다.**

| 항목 | 값 | 확인 방법 |
|---|---|---|
| Spark | `spark-4.2.0-bin-hadoop3`, Scala 2.13.18 | tarball sha512 가 `versions.lock` 과 일치 |
| ojdbc | `ojdbc11-23.9.0.25.07.jar` | sha256 이 `versions.lock` 과 일치 |
| JDK | 21.0.12 (lock 은 21.0.10) | `java -version` |
| Python | 3.12.3 (lock 은 3.11.15) | `python3 -V` |
| Oracle | **없음** | — |

**`versions.lock` 을 갱신하지 않았다.** `evidence_binding.rule` 이 "lock 이 바뀌면 이전
증거는 그 판본의 것" 이라고 규정하므로, 갱신하면 기존 S1~S3 증거의 결속이 끊긴다.
따라서 이 회차에서 돈 것은 **증거가 아니라 회귀 시험**이며, 어떤 증거 레코드도 만들지
않았다. 그 판단이 옳은지도 봐 달라.

---

## 3. 조치 8~11 — 무엇을 바꿨고 **어디를 의심하는가**

조치 1~7 은 §1 의 이유로 여기서 다루지 않는다. 각 항목에 **재현 결과**와
**내가 스스로 의심하는 지점**을 같이 적는다.

### 3.1 조치 8 — TTL 기본값을 미선언으로 (P1-03)

**재현.** `--capability-ttl-days` 를 주지 않으면 30일이 조용히 적용되고 레코드에
`freshness.basis: OPERATOR_DECLARED_TTL` · `ttl_seconds: 2592000` 이 박힌다. 운영자는
선언한 적이 없다.

**바꾼 것.** 기본값을 0(미선언)으로. floor 기계(`g0_axes.apply_floors`)는 이미 있었고
결함은 기본값 하나였다. 부수로 `freshness.note` 를 basis 별로 갈랐다 — 미선언인데
"운영자가 선언한 상한이다" 를 박고 있었다.

**의심 지점.**

- **runbook·probe-README 의 명령에 `--capability-ttl-days 30` 을 리터럴로 넣었다.**
  빼면 문서가 "아무 축도 확정하지 못하는 명령"을 가르치게 되어서다. 그런데 이것이
  **복사-붙여넣기로 30 을 사실상 기본값으로 되돌리는 것 아닌가.** P1-03 이 가르는 선이
  *자동 적용 대 선언*이라면 명령줄에 보이는 값은 선언이 맞다고 봤는데, 이 논리가
  성립하는가. 필수 환경변수로 묶는 쪽이 옳았는가.
- 30 이라는 숫자 자체는 여전히 측정 근거가 없다. 문서에 "실행자의 선언" 이라고 적는
  것으로 충분한가.

### 3.2 조치 9 — final gate hard-disabled + `where` 사용 (P1-04 · §5-1 · P2-4)

**재현.** 판정 §5-1 의 위조 레코드를 그대로 넣으니 `admitted = True, reasons = []`.
전부 쓰레기 문자열인데 승인된다.

**바꾼 것.**

- `GATE_OPEN = False`. 닫힌 동안 **어떤 레코드도** 승인하지 않는다. 반환은
  `GATE_OPEN and not reasons` 곱이다. 사유는 닫힌 채로도 전부 모은다.
- 경로 권위를 계약의 `where` 하나로. `item` 을 경로로 해석하던 것을 끊었다.
  `where` 가 null 인 항목은 위치 미정이며 어떤 레코드로도 충족되지 않는다.
- P2-4: `item`(식별자) / `label`(표시 이름) / `where`(위치) 3분할.
  `hash_vector_result (V-01~V-16)` → item `hash_vector_result`.
- 증거 스키마의 `not_covered` enum 을 계약 식별자와 동기화하고
  `schema_version` 2.2.0 → 2.3.0.

**의심 지점. 이번 회차에서 가장 크게 의심하는 곳이다.**

- **값 검증(schema·digest)을 만들지 않고 문을 닫았다.** 근거는 "최종 레코드 실물이
  없는데 검증기를 쓰면 나중에 실물을 검증기에 맞추게 된다"이고, 이는
  `aggregate()` 를 `NotImplementedError` 로 둔 것과 같은 논리다. 9차는 그 논리를
  받아들였다. **그러나 이번 것은 문 전체를 닫는 결정이라 무게가 다르다.**
  이것이 정직한 판단인가, 아니면 P1-04 를 안 고치고 회피한 것인가.
- **닫힌 문은 검증되지 않는다.** `GATE_OPEN` 이 False 인 동안 `where` 해석·항목 대조는
  실행되지만 그 결과가 판정을 바꾸지 않는다. 문을 여는 날 그 경로들이 처음으로
  의미를 갖는데, 그때 이 회귀가 무엇을 보증하는가.
- **`schema_version` 을 올린 것이 맞는가.** 계약 항목 식별자가 바뀌었으니 스키마가
  바뀐 것이라고 봤다. 이 enum 으로 만들어진 증거 레코드는 아직 없다.
  올리지 말았어야 하는가, 아니면 더 크게 올렸어야 하는가.
- **`where: null` 이 9건 남아 있다.** 위치를 G0-1~G0-5 실물이 생길 때 정한다고 뒀는데,
  그때 누군가 실물 없이 `where` 만 채워 넣는 경로가 열려 있지 않은가.

### 3.3 조치 10 — trace 완결성을 stream 별로 (P1-02)

**재현. 판정서 문구보다 나빴다.** 9차는 "driver 만 정상 종료해도 전체가 complete 다"
라고 적었는데, 실제로는 executor 추적이 **통째로 없어져도**
`trace_complete: true · verdict.coverage: PROVEN · exit 0` 이 나온다.
미완결을 NOT_PROVEN 으로 흘리는 것도 아니고 그냥 통과였다.

**바꾼 것.** 완결성을 stream(= 파일 = JVM) 별로. 하나라도 미완결이면 회차 전체가
`MEASUREMENT_FAILED` · exit 5 이고 회차 분석으로 넘어가지 않는다. `trace_streams` 와
`verdict.incomplete_streams` 에 어느 파일인지 적는다. 추가로 `lines_written + 1`
대조로 줄 유실을 잡는다.

**요청서를 쓰다 내 검사의 구멍을 찾았다**(`1b4c48e`). 첫 판은 `lines_written` 대조를
'모자란' 쪽으로만 했고, 물리 6줄에 `lines_written: 1` 인 파일이 complete 로 통과했다.
줄이 많다는 것은 이 회차가 쓰지 않은 줄이 섞였다는 뜻이다 — `Trace.file()` 이
`CREATE, APPEND` 로 열므로 디렉터리 재사용·JVM 이름 충돌·이전 회차 잔존이 그 경로다.
양쪽으로 고치고 [15] 를 세웠다.

**의심 지점.**

- **파일이 아예 없는 executor 는 보이지 않는다.** 완결성은 "존재하는 파일이 끝까지
  쓰였는가"만 답한다. tracer 가 로드조차 안 된 executor 는 stream 이 0개이므로
  미완결로 잡히지 않는다. 기대 executor 수를 아는 주체가 Spark 쪽이라 판정기가
  단독으로 닫을 수 없다고 봤는데, **이것이 P1-02 의 남은 절반 아닌가.**
- `lines_written + 1` 불변식이 정말 항상 성립하는가. `LINES` 는 `line()` 에서
  증가하고 sentinel 은 `rawLine()` 으로 세지 않는다는 코드 독해에 기대고 있다.
  다른 경로로 파일에 쓰는 곳이 있는가.
- 하네스 픽스처를 `_realistic_counts()` 로 보정했다. **보정 없이는 새 검사가 기존
  픽스처 전부를 미완결로 잡았다** — 즉 기존 픽스처가 tracer 가 낼 수 없는 조합이었다.
  보정이 옳은 방향인가, 아니면 시험을 통과시키려고 픽스처를 손본 것인가.

### 3.4 조치 11 — M4 stale 문구 정정 (§5.2 · P2-1)

**§5.2 4건을 하나씩 확인했더니 2건은 이미 정정돼 있었다.** 언제 고쳐졌는지 git 으로
추적했고, 둘의 사정이 다르다.

- **README 의 "7축 표" — 판정서를 쓴 커밋이 같은 커밋에서 고쳤다.** 판정서 커밋
  `144e290` 시점의 `README.md:81` 은 이미 "§3 의 축 표(9행)는 폐기 — 권위는 부록 A 의
  13축" 이며, `git log -S` 로 보면 **그 문자열이 바로 그 커밋에서 도입됐다.**
  즉 §5.2 는 같은 커밋이 이미 닫은 항목을 미해결로 적었다. overlay §3 의 실제 행 수도
  9행으로 일치하므로 "7축" 이라는 표현 자체가 그 시점의 저장소와 맞지 않았다.
  **판정서도 stale 문구를 갖는다** — §5.2 가 겨냥한 병을 §5.2 자신이 앓았다.
- **runbook 의 `v1.2.3.1` pointer — 정상 경로였다.** 판정서 시점에는 실제로
  `v1.2.3.1` 이었고 **조치 2(`c894f5e`, 절차서 정정)가 `v1.2.4` 로 바꿨다.**
  H/D/X 설명은 runbook 에 없고 `etl-platform-local-poc-plan.md` §1 을 가리키는
  pointer 뿐이며 그 §1 은 현행 H/D/X 를 유지하므로 stale 이 아니다.

고친 것:

- **P2-1.** A SQL 머리말이 대상 테이블 접촉을 "세 건 + 한 건"(4건)이라 적었는데 실제
  5건이다. 빠진 것이 `txn.select_inside` 이고 **그것만 `ROWNUM <= 10` 이라 최대 10행**
  이다 — 빠뜨린 한 건이 가장 많이 읽는 문장이었다.
- **공통 anchor.** verdict §3-0 본문은 이미 "가능성이지 실측이 아니다"라고 적고
  있었으나 제목과 인용처 4곳이 따라오지 않았다. 전부 "잠재 이득 · 검증 전"으로.
- **미측정 수치.** candidates 의 "수백 바이트" 를 "양은 측정하지 않았다"로.
- 드리프트 재발 방지: `g0-m0-safety-tests.py` 가 A SQL 을 파싱해 머리말 목록·건수·
  문장별 상한·합계를 실제 문장과 대조한다(6건).

**의심 지점.** 그 대조가 정규식으로 주석을 읽는다. 주석 형식이 조금만 달라져도 조용히
빈 집합을 비교할 수 있어 "접촉 문장이 실제로 있다"는 비공허 검사를 함께 뒀는데,
그것으로 충분한가.

---

## 4. 특히 봐 주었으면 하는 것

### 4.1 §8 재리뷰 합격 기준 13건 — 대응표

9차 §8 이 "실물 producer 를 통해" 를 조건으로 걸었다. 각 항목에 회귀를 붙였다고
**주장**한다. 그 시험이 실제로 그 성질을 시험하는지 봐 달라.

| # | 반례 | 조치 | 회귀 | 재현 확인 |
|---:|---|---:|---|---|
| 1 | A required probe 삭제 → 집계 전 exit 4 | 3 | `g0-normalize-tests` [42~] | 이전 세션 |
| 2 | A unknown probe 추가 → exit 4 | 3 | 〃 | 이전 세션 |
| 3 | A 실제 3건 + summary 87 → exit 4 | 3 | 〃 | 이전 세션 |
| 4 | manifest source ≠ `DB_UNIQUE_NAME` → target touch 전 종료 | 4·6 | `g0-m0-safety-tests` [11] · `g0-normalize-tests` | 이전 세션 |
| 5 | WSL 에서 `PROFILE=CORP_POC` 재라벨 → 거부/floor | 4 | `g0-normalize-tests` | 이전 세션 |
| 6 | behavior file 추가/변경 → digest 변화 또는 unlisted 실패 | 5 | `g0-normalize-tests` [54] | 이전 세션 |
| 7 | 실물 `run.sh` 가 두 token 생성 → analyzer 인식 | 1 | `g0-b1-wiring-tests` | 이전 세션 |
| 8 | task 회차가 schema connection 에서 실패 → TASK proven 금지 | 1 | `run-tests.sh`(Java) | 이전 세션 |
| 9 | executor trace 하나만 sentinel 없음 → 전체 `MEASUREMENT_FAILED` | 10 | `g0-b1-analyzer-tests` [12~15] | **이번 세션** |
| 10 | runbook 전 명령 clean dry-run → 누락 0건 | 2 | `g0-runbook-lint` 19건 | 이전 세션 |
| 11 | 두 동시 run 이 session budget 초과 → 둘째 거부 | 6 | `g0-m0-safety-tests` [10] | 이전 세션 |
| 12 | final gate 에 forged record → 무조건 거부 | 9 | `g0-normalize-tests` [64~66] | **이번 세션** |
| 13 | corporate A/B 와 CE record 혼합 → scope 위반 거부 | 7 | `g0-normalize-tests` | 이전 세션 |

**9번과 12번만 이번 세션이 결함을 재현한 뒤 고쳤다.** 나머지 11건은 회귀가 통과하는
것만 봤다. 9차가 "픽스처가 현실과 다르면 아무것도 시험하지 않는다"를 M1 에서 실제로
겪었으므로, 그 11건이 같은 병인지 봐 달라.

### 4.2 이번에 새로 만든 것의 결함

| 무엇 | 어디 | 무엇을 의심하는가 |
|---|---|---|
| `GATE_OPEN` | `g0_final_gate.py` | 문을 닫는 것이 P1-04 의 조치인가 회피인가. 여는 조건 넷이 충분한가 |
| `item_locations()` | 〃 | `where` 를 유일 권위로 둔 것이 `covered` 계산(정규화기)과 어긋나지 않는가 |
| `item`/`label`/`where` 3분할 | `g0-final-contract.json` | 식별자 변경이 증거 스키마 enum 과만 얽혀 있는가. 놓친 소비자가 있는가 |
| `stream_status()` | `analyze-trace.py` | 파일 0개인 executor 를 못 잡는 것이 허용 가능한 한계인가 |
| `_realistic_counts()` | `g0-b1-analyzer-tests.py` | 픽스처 보정이 시험을 약화시키는가 |
| A SQL 접촉 목록 대조 | `g0-m0-safety-tests.py` | 정규식 파싱이 조용히 무력화될 경로 |

### 4.3 아직 안 한 것이 정말 미뤄도 되는가

9차 요청서 §4.3 의 목록은 **대부분 그대로다.** 이번 회차가 건드리지 않았다.

- **A §22 미결 3건**(A-14·A-22·A-23) — 조직 결정 대상
- **UI IA §9-6**(capability 표시) — G0-0 실측이 답한다고 뒀다
- **§14.4 사번 결속 검증 3건** — 셋 다 확인 못 했고 설계는 그것을 전제한다
- **8차 §8.4 누수 경로 4번** — 로컬 B1 증거에 raw `javap`/ServiceLoader 출력과
  artifact hash 가 없다. **여전히 안 했다**
- **P1-07**(LOB RETENTION 분리) · **P1-03 전체 SQLCODE taxonomy 표** — 8차 §7 이
  "semantic normalization/publish 전 필수"로 뒀고 아직 그 단계가 아니다
- **최종 G0 aggregator** — `NotImplementedError` 그대로. 조치 9 로 게이트까지 닫혔다

**9차 §7 "즉시 철회하는 상태 표시" 는 세 항목 모두 닫혔다.**

| 항목 | 상태 |
|---|---|
| README·HANDOFF 의 "남은 것은 M5 — 실제로 돌리는 것" | 철회 (조치 1~7) |
| M1·M2 를 "완료"에서 되돌리고 M0·M3·M4 를 PARTIAL 로 | 반영 (조치 1~7) |
| **PR #1 본문의 같은 문장** | **철회 (2026-09-01)** |

PR #1 은 이미 merged 이므로 **원문을 지우지 않고 머리에 정정 블록을 얹었다** — 이
저장소가 `etl-platform-v2.0-grant-request-verdict.md` 머리말에서 쓰는 방식과 같다.
병합 시점(2026-08-31)의 기록은 그대로 남고, 정정 블록이 현재 권위를 `README.md` 와
`HANDOFF.md` 로 가리킨다. 병합 이력(merge commit `31ea8217`)은 건드리지 않았다.

**이 조치는 요청서 초안을 쓴 뒤에 이뤄졌다.** 초안에는 "토큰이 읽기 전용이라 못
고쳤고 문서 상태 오류가 한 곳 남아 있다"고 적혀 있었다. 그 문장이 사실이 아니게
되었으므로 이 절을 고쳤다 — 남겨 두면 그것 자체가 stale 문구가 된다.

### 4.4 M5a 를 열어도 되는가 — **이번 회차의 실질 질문**

9차는 "사내 원천 실행" 을 한 단계로 두지 말고 M5a~M5e 로 쪼개라고 했다.
조치 1~11 을 다 했다는 것이 M5 GO 를 뜻하지 않는다고 README·HANDOFF 에 적어 뒀다.

봐 주었으면 하는 것:

1. **M5a 의 경계가 어디인가.** 9차 §7 이 M5a~M5e 를 이름만 주고 내용은 주지 않았다.
   무엇까지가 M5a 이고 그것을 통과했다는 판정은 무엇으로 하는가.
2. **생산라인과 밀접한 원천에 지금 하네스가 안전한가.** 조치 6 의 안전 봉투와
   조치 11 이 정정한 A 접촉 5건(최대 14행)을 함께 봐 달라. 특히 `txn.select_inside`
   가 최대 10행을 읽는데 이것이 봉투 안에 제대로 세어져 있는가.
3. **실행 순서가 운영자가 그대로 따라가면 되는 상태인가.** 조치 2 의 dry-run lint 가
   19건인데, lint 가 통과한다는 것과 사람이 따라갈 수 있다는 것은 다르다.
4. **여전히 남은 NO-GO 사유가 있는가.**

---

## 5. 이 저장소의 규율 (이 기준으로 봐 달라)

> 확인하지 못한 것은 "미확인"이라고 쓴다.
> 오류 부재는 증거가 아니다. 0건 조건에는 양성 대조를 함께 둔다.
> 측정하지 않은 숫자를 측정한 것처럼 쓰지 않는다.
> 검증 도구는 그것이 검증하는 대상보다 엄격해야 한다.
> '고쳤다'고 쓰기 전에 그 경로를 한 번이라도 실행했는지 묻는다.
> **그 질문을 시험에도 한다.** ← 9차에서 배운 것

이번 세션이 조치 8~11 에서 지킨 방식을 적는다. 이 방식 자체가 충분한지도 봐 달라.

1. **고치기 전에 결함을 재현한다.** 조치 8~10 은 전부 재현부터 했고, 조치 9·10 은
   판정서 문구보다 나쁜 상태였다.
2. **고친 뒤 음성 대조를 돌린다.** 새 시험을 그대로 두고 **구현만** 되돌려 그 시험이
   실제로 실패하는지 본다. 조치 8~11 전부에 대해 했고, 결과를 커밋 메시지에 적었다.
3. **양성 대조를 함께 둔다.** [63]·[13] 이 그것이다 — 없으면 "거부한다"는 검사가
   무엇을 넣어도 실패하는 공허한 검사가 된다.

**그럼에도 조치 10 의 구멍은 이 세 단계를 다 통과한 뒤에 남아 있었다.**
요청서를 쓰며 다시 읽다가 찾았다. 한 번의 재현·음성 대조로는 부족하다는 증거다.

---

## 6. 읽는 순서

```
README.md                                     현재 상태표 · 문서 지도
HANDOFF.md                                    §2 규율 · §6 하지 말 것 · §8 다음에 할 일
etl-platform-v2.0-codex-ninth-cross-review.md    9차 원본 (조치 1~11 의 출처)
etl-platform-v2.0-codex-ninth-review-assessment.md  9차 판정서 §7(조치 순서)·§8(기준 13건)

── 이번 세션이 고친 곳 (재현 → 조치 → 음성 대조) ──
g0-normalize.py                               조치 8 (TTL 기본값 · freshness.note)
g0_final_gate.py                              조치 9 (GATE_OPEN · item_locations)
g0-final-contract.json                        조치 9 (item/label/where 3분할)
g0-0-evidence.schema.json                     조치 9 (not_covered enum · schema_version 2.3.0)
g0-0b1-connection-provider/analyze-trace.py   조치 10 (stream_status · 줄 수 양방향 대조)
g0-0a-capability-inventory.sql                조치 11 (접촉 5건 목록)

── 이전 세션이 고친 곳 (이번 세션은 실행 확인만) ──
g0-0b1-connection-provider/run.sh · run-g0-0b1.py   조치 1
g0-0-runbook.md + g0-runbook-lint.py                조치 2
g0-0a-probe-manifest.py + g0-child-schemas/         조치 3
g0-source-envelope.json + g0_source_envelope.py     조치 6
g0-harness-manifest.py + g0-harness-manifest.json   조치 5

── 규범 ──
etl-platform-target-architecture-v1.2.4.md    A 현행
etl-platform-poc-test-plan-v1.md              P §8.1 이 최종 g0_evidence 의 권위
etl-platform-v2.0-capability-overlay.md       부록 A 13축이 권위 (§3 9행 표는 폐기)

── 실행 ──
g0-0-runbook.md                               절차서. M5a 를 여는 문
versions.lock                                 판본 고정 (이 회차에서 갱신하지 않았다)
```

---

## 7. 회귀 시험 돌리는 법

```bash
python3 g0-normalize-tests.py                          # 235
python3 g0-axes-tests.py                               # 127
python3 g0-b1-analyzer-tests.py                        #  65
python3 g0-m0-safety-tests.py                          #  80
python3 g0-runbook-lint.py                             #  19
python3 g0-0b1-connection-provider/g0-b1-wiring-tests.py  #  21
                                                       # ── Python 547

cd g0-0b1-connection-provider
SPARK_HOME=<spark 배포판> bash build.sh
SPARK_HOME=<spark 배포판> bash run-tests.sh            #  29 (Java)
```

`run-tests.sh` 는 `src/` 가 `build/classes` 보다 새로우면 **exit 2 로 거부**한다.
이번 세션에서도 두 번 걸렸다(git 브랜치 조작이 mtime 을 갱신). 내용 변경은 없었고
재빌드 후 통과했다 — 가드가 보수적으로 동작하는 쪽이라 그대로 뒀다.

`python3 g0-harness-manifest.py` 로 harness digest 를 확인할 수 있다.
기준 커밋에서 `87aff5bb…` 이며 이번 세션에서 다섯 번 바뀌었다
(`0a774f86` → `3190d299` → `3daa0531` → `c7430de1` → `8819c789` → `87aff5bb`).
`*.md` 는 excluded_globs 이므로 이 요청서는 digest 를 바꾸지 않는다.
