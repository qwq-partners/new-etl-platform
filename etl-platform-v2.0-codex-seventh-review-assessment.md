# 7차 교차 리뷰 검토서

- 검토 대상: `etl-platform-v2.0-codex-seventh-cross-review.md` (408줄, 요청 기준판 `538ec31`)
- 판정일: 2026-08-27
- 방법: 각 지적을 **실제 코드로 재현**하고 1차 출처와 대조

---

## 0. 총평 — **P0 6건 전부 확정. 조건부 동의가 아니라 전면 동의다.**

이 리뷰의 핵심 문장이 정확하다.

> **현재 가장 위험한 것은 Oracle 사실의 미확정 자체가 아니다. 미확정을 확정값으로 바꾸는 코드가 이미 있다는 것이다.**

내가 만든 `g0-normalize.py` 가 이 저장소의 규율("확인하지 못한 것은 미확인")을 **실행물에서 위반하고 있었다.** 문서로는 그 규율을 여섯 번 지켜 놓고, 그것을 강제하려고 만든 도구가 정반대로 동작했다.

| 지적 | 판정 | 재현 방법 |
|---|---|---|
| P0-01 `derive_axes` 가 반대 증거에도 승격 | **확정** | 반례 실행 — 아래 §1 |
| P0-02 불완전·조작 산출물을 `MEASURED` 로 | **확정** | 한 줄짜리 입력 4종 전부 `MEASURED`, exit 0 |
| P0-03 증거가 측정 대상·시각·판본에 안 묶임 | **확정** | `target_owner/table/wm_column` 을 한 번도 채우지 않음 |
| P0-04 `g0_evidence` 두 계약 이름 충돌 | **확정** | P §8.1 의 5개 필수 필드를 스키마가 **거부**(`additionalProperties:false`) |
| P0-05 overlay 가 독립 축을 합침 | **확정** | `max_commit_minus_watermark_seconds` 가 A 에 7회 — apply lag 과 독립 |
| P0-06 B1 이 task-path fail-closed 없이 `PROVEN` | **확정** | `MIXED` 1건이 SCHEMA·TASK 양쪽 게이트를 통과 |

---

## 1. 재현한 반례 두 개

리뷰가 제시한 정적 반례를 그대로 돌렸다.

**P0-01 ①** — `AS OF` 는 되지만 SCN 원점이 없고 `READ ONLY` 트랜잭션은 **실패**한 입력

```
as_of_timestamp.target = ok,  dbms_flashback.get_scn = fail,  view.v_database = fail
txn.set_read_only = fail,     txn.select_inside = fail
  수정 전: snapshot_read = READ_ONLY_TXN   ← 실패한 능력을 있다고 표시
  수정 후: snapshot_read = AS_OF_TIMESTAMP
```

**P0-01 ②** — `V$DATAGUARD_STATS` 가 읽히지만 행이 0건

```
  수정 전: lag_visibility = DG_STATS       ← 잴 수 없는데 잴 수 있다고 표시
  수정 후: lag_observation = UNDETERMINED
```

`view.v_database` 는 `SELECT COUNT(*) FROM v$database` 일 뿐 `CURRENT_SCN` 을 읽지 않는다. **SCN 원점이 아니다.** 내가 그것을 SCN 원점으로 쓴 것이 오류의 뿌리였다.

---

## 2. 수정한 것

### 2.1 `derive_axes` — 등급마다 자기 양성 근거를 요구한다

- **상위가 실패했다고 하위로 내리지 않는다.** 하위도 실패했을 수 있다.
- 실패가 **기능 부재**(`ORA-00942`/`01031`/`00900`/`06550` 등)로 확인될 때만 `NONE`. 원인 불명·타임아웃·probe 부재는 `UNDETERMINED`.
- `AS_OF_TIMESTAMP` 를 별도 값으로 신설(SCN 원점 없이 `AS OF` 만 되는 경우).
- `READ_ONLY_TXN` 은 재발행 `ORA-01453` 양성 대조 유무를 note 에 남긴다 — 없으면 "잠정" 이라고 적는다.
- **`lag_visibility` 를 `lag_observation` / `lag_admission` 두 축으로 분리.** `ACCEPTED_UNVERIFIED` 를 신설해, `ALTER` 가 수락된 것과 `ORA-03172` 양성 대조로 강제가 확인된 것을 구분한다.
- **`watermark_commit_bound` 축 신설 — 값은 항상 `UNDETERMINED`.** 이 축을 재는 probe 가 G0-0 에 **없다**는 사실을 증거에 드러내기 위해서다. 없는 것을 없다고 적는 것이 이 도구의 일이다.

### 2.2 coverage — 한 줄짜리를 `MEASURED` 로 만들지 않는다

| 입력 | 수정 전 | 수정 후 |
|---|---|---|
| B0 probe 1줄 | `MEASURED` | `PARTIAL` — S 계열 step 2개·probe 4건 미만 |
| B1 `verdict` 만 | `MEASURED` | `PARTIAL` — `by_path`·`preamble_ok_by_path` 없음 |
| C00 `summary` 만 | `MEASURED` | `PARTIAL` — 값이 나온 fence fact 0건 |
| CE `pass=true`, scenario 0개 | `MEASURED` | **`FAILED`** |

### 2.3 검증을 **쓰기 전에** 한다

수정 전에는 파일을 쓴 뒤 stderr 에만 출력하고 exit 0 이었다. 이제 계약 위반이면 `.invalid` 경로에만 쓰고 **최종 경로에는 쓰지 않으며 exit 4** 다.

종료 코드: `0` 유효·완결 / `3` 유효하나 불완전 / `4` 계약 위반.

### 2.4 계약 분리 (P0-04)

`record_type: g0_0_evidence`, `scope: CAPABILITY_INVENTORY`, **`gate_eligible: false` 고정**, 파일명 `g0-0-evidence.schema.json`. P §8.1 의 최종 `g0_evidence` 는 G0-1~G0-5 aggregator 만 생성한다. **이 레코드의 exit 0 을 G0 PASS 로 해석할 수 없다**고 스키마 설명에 박았다.

`executed_at`(측정 시각) 과 `normalized_at`(정규화 시각) 을 분리했다.

### 2.5 측정 대상 결속 (P0-03)

G0-0A 에 `target.identity` probe 를 신설해 `OWNER.TABLE#WM_COLUMN` 을 증거에 남긴다(`c_expected` 86 → 87). 스키마에서 `source.target_owner/target_table/wm_column` 을 **필수**로 만들었다 — 없으면 테이블 A 의 `ROWDEPENDENCIES` 결과를 테이블 B 에 적용하는 것을 막지 못한다.

### 2.6 B1 (P0-06)

- **`MIXED` 를 양쪽으로 세지 않는다.** 한 건이 SCHEMA·TASK 를 동시에 만족시키던 것을 제거. `MIXED` 는 사람이 `raw_stack` 을 보고 재분류할 대상이다.
- **fail-closed 는 TASK 경로 도달을 요구한다.** `fail=all` 은 첫 provider 호출(schema)에서 즉시 던지므로 task connection 이 열리지 않을 수 있다. 그 상태를 "task 도 fail-closed" 로 읽으면 안 된다 → `TASK_PATH_NOT_REACHED` 를 신설하고 blocking 에 넣었다.
- `NLS_NUMERIC_CHARACTERS` 를 `'. '` → `'.,'` 로 정정(A·G0-0A 와 일치).

### 2.7 overlay (P0-05)

**동결 불가**를 문서 머리에 명시하고 §3.1 에 축 분해를 기록했다. 가장 중요한 정정은 **`bound_kind` 철회를 취소**한 것이다 — apply lag 과 `commit_time − watermark_value` 는 독립이고, **lag=0 이어도** 오래된 `UPDATE_DT` 를 가진 트랜잭션이 늦게 commit 하면 overlap 밖 누락이 생긴다.

---

## 3. 회귀 확인

```
P0-01 ①  snapshot_read       READ_ONLY_TXN → AS_OF_TIMESTAMP
P0-01 ②  lag                 DG_STATS      → UNDETERMINED
P0-02    한 줄 입력 4종        전부 MEASURED/exit 0 → PARTIAL·FAILED/exit 3
P0-02    계약 위반            파일 씀/exit 0 → .invalid 만/exit 4
P0-06    MIXED 1건            PROVEN → NOT_PROVEN (blocking: 경로 커버리지)
P0-06    TASK 미도달           PROVEN → TASK_PATH_NOT_REACHED
정상 경로 (SCHEMA+TASK 관측, failclosed 도 TASK 실패 관측)  → PROVEN / exit 0
```

Java 3파일 재컴파일 통과(실제 Spark 4.2.0 jar).

---

## 4. 아직 하지 않은 것

리뷰가 요구한 것 중 **이번에 하지 않은 것**을 남긴다. 조용히 넘기지 않는다.

1. **child artifact 별 개별 스키마** — 리뷰는 A/B0/B1/C00/C-suite 각각에 `run_id`·시작/종료 sentinel·manifest·exit code·runtime digest 를 요구한다. 지금은 aggregator 쪽만 강화했다. child 가 스스로 기록하게 만드는 것은 각 산출물 수정이 필요하다.
2. **경로별 주입**(`fail=schema|task|metadata`) — `TASK_PATH_NOT_REACHED` 로 **탐지**는 하게 됐지만, 그 상태를 **해소**하려면 주입점을 경로별로 나눠야 한다.
3. **overlay 9축 재설계** — 분해는 기록했으나 최종 축 목록과 composition function 은 G0-0 실측 후로 미뤘다. 지금 확정하면 또 측정 없이 규격을 짜는 것이다.
4. **CE runner 의 child `returncode` 검사** — 리뷰 P0-02 말미 지적. 미반영.
5. **P1/P2 항목** — 아직 검토하지 않았다.

---

## 5. 이 리뷰에서 배운 것

여섯 차례 리뷰가 잡아 온 결함 유형은 "문서로 확인되지 않은 것을 확인된 것처럼 쓴 문장" 이었다. 이번 것은 한 단계 더 나간다 — **그 규율을 강제하려고 만든 도구가 규율을 위반했다.**

검증 도구는 그것이 검증하는 대상보다 엄격해야 한다. 그러지 못하면 도구의 통과가 곧 거짓 안심이 된다.
