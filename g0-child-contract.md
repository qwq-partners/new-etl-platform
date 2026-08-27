# G0-0 child 산출물 계약

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
| 산출물이 자기 `expected` 를 채웠는가 | child `PARTIAL` 또는 `FAILED` |

### 3.1 `expected` — "한 줄이면 통과"를 막는 것

manifest 만으로는 B0 한 줄이 `MEASURED` 가 되는 것을 막지 못한다. 각 child 는 **몇 개를 낼
예정인지**를 산출물 본문에 남겨야 하고, 집계기가 그것과 실제 개수를 대조한다.

| child | 완결 조건 |
|---|---|
| `G0_0A` | `probe_run_end` sentinel ∧ `manifest_ok=true` ∧ `emitted == expected` ∧ probe id 중복 0 ∧ `probe_summary` 정확히 1개 |
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
`false` 이며 그것은 schema 의 `const` 다 — 도구가 그 값을 바꿀 방법이 없다.

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
