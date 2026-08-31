# G0-0 child 산출물 계약

> **2026-08-30 — 8차 교차 리뷰 M1 반영.** 계약이 넷 강해졌다.
>
> | M1 | 무엇 | 어디서 강제하는가 |
> |---|---|---|
> | M1-1 | **child 별 개별 스키마** — `g0-child-schemas/` 넷 | 집계기가 **집계 전에** 검증. 어기면 exit 4 |
> | M1-2 | manifest 에 **`source_id`·`harness_digest`·`started_at`/`ended_at`** 추가 | 래퍼가 기록, 집계기가 요구 |
> | M1-3 | **회차 집합 검사** — child 들이 서로 다른 회차·원천·판본·하네스면 거부 | `check_run_set()` |
> | M1-4 | **run 별 불변 산출물 경로** — 경로에 `RUN_ID` 필수, 기존 파일 덮어쓰기 금지 | 래퍼가 생성 시, 집계기가 사후 |
>
> **2026-08-30 — 8차 M3 반영.** 집계기 쪽이 셋 강해졌다(§3 표 · §4 · §4.1).
>
> | M3 | 무엇 |
> |---|---|
> | M3-1 | child schema 를 어긴 산출물은 **본문 집계에 들어가지 않는다** — coverage 만 덮던 것을 고쳤다 |
> | M3-3 | `effective_value` 가 실제로 floor 로 내려간다. 판정·publish 는 그 값만 읽는다 |
> | M3-5 | 최종 레코드도 run 별 불변 경로. `current` 포인터는 **거부 시 INVALIDATED 로 덮인다** |
>
> **왜 `harness_digest` 가 따로 필요한가.** `versions.lock` 은 **실행 판본**(Spark·JDK·ojdbc)이지
> 하네스 코드가 아니다. 프로브 SQL 이나 판정기를 고쳐도 lock digest 는 그대로다 — 그러면
> **서로 다른 코드로 잰 값이 같은 판본으로 묶인다.**
>
> **2026-08-31(9차 조치 5) — 그 digest 가 코드의 일부만 덮고 있었다.** 래퍼가 11개 파일을
> 하드코딩했고 provider Java source·ServiceLoader 등록·`build.sh`·child schema 4종·
> final contract·gate·CE 시나리오가 전부 빠져 있었다. 빠진 파일을 바꿔도 digest 는 그대로였다.
> 지금은 `g0-harness-manifest.json` 이 **저장소의 모든 파일**(미커밋 포함)을
> `harness`(digest 대상) · `tooling`(아님) · `excluded_globs`(문서 등, 이유 명시)로 선언하고,
> 검사기가 미선언 파일을 찾으면 **digest 를 내주지 않는다** — 그런 digest 는 "이 코드로 쟀다"
> 를 말하지 못하기 때문이다. harness 45건이다(이전 11건).
>
> **M1-3 이 잡는 것이 개별 검사로는 안 잡힌다.** child 마다 자기 manifest 와 일관되면
> child 단위 검사는 전부 통과한다. 그런데 그 child 들이 서로 다른 회차의 것이면
> **이어 붙인 회차는 하나의 회차가 아니다.** 그래서 집합을 따로 본다.
>
> 실행에 필요한 것 하나가 늘었다 — `G0_SOURCE_ID`(대상 원천의 `DB_UNIQUE_NAME`).
> 없으면 래퍼가 exit 2 다.

> 7차 교차 리뷰 **P0-02 / P0-03** 의 조치. 각 산출물이 **자기 실행 시점에** 신원·판본·범위를
> 스스로 기록하게 하고, 집계기가 그것을 대조하게 한다.
>
> 이 계약이 없던 동안 무슨 일이 가능했는지가 이 문서의 존재 이유다.

---

## 1. 무엇이 문제였나

집계기(`g0-normalize.py`)가 다음을 전부 통과시켰다.

| 입력 | 통과한 판정 |
|---|---|
| B0 로그에 JSON 한 줄 | `MEASURED` |
| B1 증거가 `{"verdict":{"coverage":"PROVEN"}}` 뿐 | `MEASURED` |
| C00 로그에 `fence.summary` 한 줄 | `MEASURED` |
| CE 증거가 `suite_verdict.pass=true` + scenario 0개 | `MEASURED` |
| 시나리오가 통과 모양을 찍고 exit 1 로 죽음 | suite PASS 후보 |
| 어제 로그 + 오늘 `versions.lock` | 정규화 성공, 아무 경고 없음 |

마지막 줄은 가설이 아니다. **2026-08-27 S1~S3 회차에서 실제로 그렇게 했다** — B1 산출물을 만든
뒤에 `versions.lock` 을 채우고 정규화했는데 도구가 아무 이의도 제기하지 않았다.

원인은 하나다. **산출물이 자기가 언제·어디서·어느 판본으로 실행됐는지 말하지 않는다.**
집계 시점의 파일을 해시해 붙일 뿐이었다.

---

## 2. 계약

각 child 실행은 **사이드카 manifest 파일 하나**를 함께 남긴다. 산출물 본문이 아니라 별도 파일인
이유는 G0-0A 가 `sqlplus` 스풀이라 자기 종료 코드를 본문에 적을 수 없기 때문이다. 래퍼가 적는다.

```
<산출물>.manifest.json
```

### 2.1 형식

```json
{
  "schema_version": "1.0.0",
  "record_type": "g0_child_manifest",
  "child": "G0_0A | G0_0B0 | G0_0B1 | G0_0C00 | G0_0C_SUITE",
  "run_id": "RUN-2026-08-27-01",
  "profile": "LOCAL_WSL | CORP_POC | SANDBOX_CONTAINER",
  "started_at": "2026-08-27T04:00:00+00:00",
  "ended_at":   "2026-08-27T04:03:11+00:00",
  "exit_code": 0,
  "versions_lock_digest": "<실행 시점 versions.lock 의 sha256>",
  "artifact": { "path": "...", "sha256": "...", "lines": 123 },
  "runtime": { "host_kind": "...", "uname": "...", "java": "...", "python": "..." },
  "command": ["...", "..."]
}
```

`versions_lock_digest` 는 **child 실행 시점**의 값이다. 집계 시점 값이 아니다. 둘이 다르면
집계기가 거부한다 — 그것이 이 필드의 유일한 목적이다.

### 2.2 만드는 법

```bash
./g0-run-child.sh G0_0B1 RUN-2026-08-27-01 LOCAL_WSL g0-0b1-evidence.json -- \
    ./run.sh "$URL" ETL_PROBE ETL_PROBE.G0_TARGET FREE PRIMARY none
```

래퍼가 하는 일은 넷뿐이다.

1. 실행 **전에** `versions.lock` 을 해시한다
2. 명령을 돌리고 **종료 코드를 포착한다**
3. 산출물을 해시한다
4. manifest 를 쓴다

**해석하지 않는다.** 판정은 집계기가 한다.

### 2.1 A probe 목록의 권위 (9차 조치 3)

`g0-child-schemas/g0-0a-probe-manifest.json` 이다. **손으로 적지 않는다** —
`g0-0a-probe-manifest.py` 가 `g0-0a-capability-inventory.sql` 에서 생성하고, 회귀 시험이
둘이 어긋났는지 확인한다. SQL 을 고치고 재생성하지 않으면 시험이 실패한다.

목록을 생성하는 이유는 하나다. 이 저장소는 `c_expected` 를 **세 번 틀렸다**(56 → 78 → 86 → 87).
매번 `grep` 이 어떤 호출 형태를 놓쳤다. 사람이 세는 숫자는 또 틀린다.

9차 P0-01 이 잡은 것: `cov_a()` 가 이 대조를 안 해서 **probe 3건 + `summary{expected:86,
emitted:86}` 이 `MEASURED`** 가 됐다. `cov_b0` 는 step 차집합을, `cov_c00` 은 probe id 집합을
보는데 **A 만 안 봤다** — 87 probe 를 내는 가장 큰 child 가 가장 약한 검사를 받고 있었다.
그리고 그 반례가 시험 픽스처에 **양성 대조로 박혀 있었다.**

---

## 3. 집계기가 강제하는 것

`g0-normalize.py` 는 각 child 에 대해 다음을 확인하고, 하나라도 어긋나면 **그 child 를 `FAILED`
로 두거나 집계 자체를 거부**한다.

| 검사 | 어긋나면 |
|---|---|
| manifest 가 있는가 | child `FAILED` (계약 미준수) |
| `artifact.sha256` 이 지금 파일의 해시와 같은가 | **집계 거부**(exit 4) — 산출물이 실행 후 바뀌었다 |
| `exit_code == 0` 인가 | child `FAILED` |
| `versions_lock_digest` 가 child 들 사이에서 같은가 | **집계 거부**(exit 4) |
| 집계 시점 `versions.lock` 해시와 같은가 | **집계 거부**(exit 4) |
| `profile` 이 child 들 사이에서 같은가 | **집계 거부**(exit 4) |
| `run_id` 가 child 들 사이에서 같은가 | **집계 거부**(exit 4) |
| source identity(A 가 밝힌 것)가 다른 child 와 모순되지 않는가 | **집계 거부**(exit 4) |
| manifest 의 `source_id` 가 **서버가 밝힌 `DB_UNIQUE_NAME`** 과 같은가 | **집계 거부**(exit 4) — 9차 조치 4 |
| 선언된 `profile` 이 래퍼가 **관측한 `env_kind`** 와 모순되지 않는가 | **집계 거부**(exit 4) — 9차 조치 4 |
| 산출물이 자기 `expected` 를 채웠는가 | child `PARTIAL` 또는 `FAILED` |
| child schema(`g0-child-schemas/`)를 지키는가 | **집계 거부**(exit 4) — 게다가 그 child 는 **본문 집계에 들어가지도 않는다**(8차 M3-1) |

**M3-1 이 고친 순서.** 이전 판은 먼저 전부 집계하고 그 다음 `coverage` 만 `FAILED` 로 덮었다.
그러면 coverage 는 정직해지지만 `account_privs`·`capability_axes`·`source`·`fence_facts` 는
**schema 를 어긴 파일에서 뽑힌 값 그대로** 레코드에 남는다. 형태가 계약과 다른 산출물을 파싱한
결과를 싣고서 "이 레코드는 무엇을 세었는가" 를 말할 수 없다. 지금은 통과한 산출물만 집계
입력이 되고, 제외한 child 는 `warnings` 에 남는다.

### 3.0 원천 신원과 profile 은 **신고값이 아니다** (9차 조치 4)

9차 P0-02 가 잡은 것 둘. 둘 다 "운영자가 적은 값을 그대로 믿었다" 는 같은 뿌리다.

**① `source_id` 를 서버가 밝힌 값과 대조하지 않았다.** 그래서 한 레코드 안에
`children.g0_0a.source_id = TESTSTBY` 와 `source.db_unique_name = ETLSTB` 가 공존하고
위반이 0건이었다. M1-2 는 manifest ↔ `--source-id` ↔ 다른 child 만 봤는데,
**child 들이 같은 거짓 이름을 공유하는 것은 같은 원천에서 나왔다는 증명이 아니다.**
지금은 A 의 `userenv.DB_UNIQUE_NAME` 과 대조한다. 서버 신원을 못 읽은 회차는 위반이 아니라
**미확인**이고, 그 사실이 축의 `SOURCE_IDENTITY_UNVERIFIED` floor 사유가 된다.

**② `profile` 이 caller 가 고르는 label 이었다.** WSL 에서 `PROFILE=CORP_POC` 로 재라벨하면
`PROFILE_NOT_AUTHORITATIVE` floor 를 우회했다 — 그 floor 는 **정직하게 신고한 회차만** 막고
있었다. 지금은 래퍼가 `env_kind` 를 **관측해** manifest 에 남기고, 집계기가 모순을 거부한다.

**판정은 비대칭이다.** 완전한 attestation(승인된 launcher·서명)은 이 저장소 범위 밖이므로,
할 수 있는 것은 **알려진 거짓말을 막는 것**뿐이다.

| 관측 `env_kind` | 뜻 |
|---|---|
| `wsl` · `container` | 그 환경에서 `CORP_POC` 선언은 **거짓이다** → 거부 |
| `host` | `CORP_POC` 를 **입증하지 않는다.** cgroup v2 컨테이너도 여기로 온다 → 반증만 안 될 뿐 |
| `UNRECORDED` | 래퍼가 기록하지 않았다 → **관측 못 한 것을 통과로 두지 않는다** → 거부 |

`CORP_POC` 의 실질적 근거는 fingerprint 가 아니라 **①의 서버 신원 대조**다.

### 3.1 `expected` — "한 줄이면 통과"를 막는 것

manifest 만으로는 B0 한 줄이 `MEASURED` 가 되는 것을 막지 못한다. 각 child 는 **몇 개를 낼
예정인지**를 산출물 본문에 남겨야 하고, 집계기가 그것과 실제 개수를 대조한다.

| child | 완결 조건 |
|---|---|
| `G0_0A` | `probe_run_end` sentinel ∧ `manifest_ok=true` ∧ probe id 중복 0 ∧ `probe_summary` 정확히 1개 ∧ **probe 집합이 계약과 정확히 같음**(9차 조치 3 — 빠짐 0 · 알 수 없는 것 0) ∧ `expected == emitted == 계약 건수 == 실제 파싱 수` |
| `G0_0B0` | `S*_summary` sentinel ∧ 선언된 step 이 전부 출력됨 |
| `G0_0B1` | `verdict` ∧ `by_path` ∧ `preamble_ok_by_path` ∧ `runs_seen` 이 **모두** 있고, `runs_seen` 에 `coverage` 와 **`failclosed` 로 시작하는 회차**가 하나 이상 있음(조치 5 로 `failclosed_schema`·`failclosed_task` 로 갈렸다) |
| `G0_0C00` | `fence.summary` ∧ 선언된 probe 가 전부 출력됨(skipped 도 출력이다) |
| `G0_0C_SUITE` | `scenarios` 길이 ≥ 1 ∧ suite 가 요구한 `required` 시나리오가 전부 있음 ∧ 각 시나리오의 child returncode 가 0 |

---

## 4. 종료 코드

| 코드 | 뜻 | 산출물 |
|---|---|---|
| **0** | 유효한 레코드를 썼다 | 최종 경로에 씀 |
| **3** | 불완전 — child 중 `NOT_RUN`/`PARTIAL` 이 있다 | 최종 경로에 씀. `completeness: INCOMPLETE` 로 표시 |
| **4** | **무효** — 계약 위반 또는 schema 위반 | **최종 경로에 쓰지 않는다.** `<out>.rejected.json` 으로만 남긴다 |

**exit 0 은 "G0-0 을 완주했다"이지 "G0 PASS"가 아니다.** 레코드의 `gate_eligible` 은 항상
`false` 이며 그것은 schema 의 `const` 다 — 도구가 그 값을 바꿀 방법이 없다. 그리고
`g0_final_gate.admit` 이 `record_type != g0_evidence` 를 **무조건** 거절하므로,
`gate_eligible` 을 손으로 고쳐도 최종 게이트에는 들어가지 못한다(8차 M3-4).

**exit 3 은 측정 완결성만 말한다**(8차 M3). "capability 를 못 정했다" 는 여기 섞지 않고
레코드의 `outcome` 이 네 값으로 따로 낸다 — `process`(계약·schema 통과 여부) ·
`measurement`(다섯 child 완결 여부) · `capability`(13축 `effective_value` 중 확정값 수) ·
`final_gate`(언제나 `REJECTED_BY_CONTRACT`). 측정이 완결돼도 capability 가 `UNGRADED` 일 수
있으며 그것은 실패가 아니라 결과다.

### 4.1 회차 산출물과 current 포인터 (8차 M3-5)

`--out` 은 **경로에 `RUN_ID` 가 있어야 하고 기존 파일을 덮지 않는다** — child 산출물에
적용한 M1-4 를 최종 레코드에도 적용한 것이다. 고정 이름 하나를 계속 덮어쓰면 그 이름은
여러 회차의 별칭이 된다.

`--out` 옆(또는 `--current` 위치)에 포인터 파일을 쓴다. **소비자는 회차 파일이 아니라 이
포인터를 읽는다.**

| `status` | 언제 | 뜻 |
|---|---|---|
| `VALID` | exit 0/3 | `path`·`sha256` 이 가리키는 회차가 현재 유효하다 |
| `INVALIDATED` | exit 4 | 마지막 정규화가 거부됐다. **이전 회차를 현재로 읽지 마라** |

거부된 회차에서도 포인터를 반드시 쓴다는 것이 요점이다. 이전 판은 거부 시 최종 경로를
건드리지 않았고, 그러면 이전 회차 파일이 그대로 남아 소비자에게는 여전히 '현재' 로 보였다 —
무효한 재실행이 있었다는 사실은 어디에도 나타나지 않았다.

---

## 4.1 이 계약이 강제하는 실행 순서 — 회차를 둘로 나눠라

`versions_lock_digest` 를 실행 시점에 박고 집계 시점과 대조하므로, **lock 을 child 실행 뒤에
고치면 집계가 통째로 거부된다.** 그런데 lock 의 `oracle:` 항목은 Oracle 에 붙어 봐야 안다.

그래서 회차를 나눈다.

| 회차 | 무엇 | 산출물의 지위 |
|---|---|---|
| **탐색(RECON)** | Spark 설치 · B1 빌드 · Oracle 기동 · 전제물 확보 | **증거가 아니다.** 판본을 알아내는 것이 목적 |
| ↓ | **여기서 `versions.lock` 을 확정한다** | digest 고정 |
| **증거(RUN-…)** | A · C00 · B1 · B0 · CE → 정규화 | `run_id` 하나로 묶인 증거 |

lock 을 중간에 고쳐야 하면 **증거 회차를 처음부터 다시 돈다.** 이미 만든 증거는 그 판본의
것이며 새 판본의 근거로 재사용하지 않는다 — `versions.lock` 머리말의 `evidence_binding.rule`
이 그렇게 규정한다.

실행 절차는 `g0-0-runbook.md` 가 단계별로 안내한다.

## 4.2 비밀번호는 argv 에 넣지 않는다 — 계약이 그것을 기록하기 때문이다

manifest 는 `command` 를 남긴다. 그래서 다음처럼 쓰면 **비밀번호가 manifest 파일에 남는다.**

```bash
# ✗ 하지 마라
g0-run-child.sh G0_0A … -- bash -c "sqlplus -S /nolog <<EOF
CONNECT $ORA_USER/$ORA_PW@$DSN
…"
```

`g0-sqlplus.sh` 를 쓴다 — 비밀번호를 **stdin 으로만** 넘긴다.

```bash
# ✓
export ORA_USER=… ORA_DSN=//host:1521/svc      # ORA_PW 는 환경변수
g0-run-child.sh G0_0A "$RUN_ID" "$PROFILE" out.log -- ./g0-sqlplus.sh probe.sql out.log
```

래퍼가 `user/pw@host` 형태를 `<redacted>` 로 바꾸긴 하지만 **그것은 심층 방어이지 해결책이
아니다.** 2026-08-27 runbook 초안이 위 ✗ 형태였고 실제 실행에서 비밀번호가 남는 것을 확인했다.

---

## 5. 이 계약이 닫지 못하는 것

- **child 가 거짓말하는 경우.** manifest 는 래퍼가 쓰지만 산출물 본문은 child 가 쓴다.
  악의적 위조를 막는 장치가 아니다. 이 계약이 막는 것은 **사고와 착각**이다 — 잘못된 순서,
  옛 로그 재사용, 부분 실행의 완주 오인.
- **source 가 실제로 그 DB 인가.** A 의 `SYS_CONTEXT` 값을 그대로 믿는다. 서버가 스스로
  밝힌 신원이므로 운영자 신고값보다는 낫지만, 대상을 잘못 지정한 실행은 그 자체로 일관돼 보인다.
- **측정의 타당성.** 계약은 "잰 것이 무엇인지"를 묶을 뿐 "잘 쟀는지"를 보지 않는다.
  그쪽은 축 파생기(조치 3)와 B1 하네스(조치 5)의 몫이다.
