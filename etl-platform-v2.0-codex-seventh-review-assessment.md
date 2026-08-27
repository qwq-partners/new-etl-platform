# 7차 교차 리뷰 검토서 — 각 지적의 재판정

- 검토일: 2026-08-27
- 대상 리뷰: `etl-platform-v2.0-codex-seventh-cross-review.md` (P0 6건 · P1 12건 · P2 5건 · 초점 4건)
- 방법: **저장소 코드 실물 대조**(줄 번호까지 열어 확인) + Oracle·Spark 1차 출처 + 2026-08-27 S1~S3 실측(`g0-0-s1-s3-results.md`)
- 판정 등급: **확정**(수용, 고친다) / **부분 확정**(사실은 맞으나 범위·경중을 좁힌다) / **기각** / **보류**(측정 전에는 판정 불가)

---

## 0. 총평 — 이 리뷰는 대부분 맞다

**P0 6건 중 6건, P1 12건 중 11건, P2 5건 중 5건이 확정이다.** 기각은 0건, 부분 확정이 4건이다.

이 리뷰의 지적은 앞선 여섯 차례와 성격이 다르다. 이전 리뷰들은 **문서가 사실이 아닌 것을 사실처럼 썼다**를 잡았다. 이번 리뷰가 잡은 것은 그 한 겹 아래다.

> **문서는 경계를 옳게 썼는데, 그 경계를 강제해야 할 코드가 강제하지 않는다.**

리뷰 §0 의 이 문장이 정확하다.

> 현재 가장 위험한 것은 Oracle 사실의 미확정 자체가 아니다. **미확정을 확정값으로 바꾸는 코드가 이미 있다는 것**이다.

특히 아프게 맞는 것이 **P0-05** 다. `bound_kind`(watermark commit bound)를 `lag_visibility` 로 대체한 것은 이 저장소가 **자기가 이미 발견한 사실을 감축 과정에서 잃어버린** 사례다. README §4 는 여섯 차례 리뷰의 대표 결함 다섯 개를 열거하면서 마지막 줄에 이렇게 적어 두었다.

> `MAX(watermark)` fence — 반개구간과 결합하면 tail 행이 지연·누락된다

apply lag 와 `commit_time − watermark_value` 가 독립이라는 것이 정확히 그 줄의 내용이다. 그것을 알고 있으면서 두 축을 합쳤다. **감축은 중복을 지우는 작업이어야 하는데 독립 성질을 지웠다.**

### 판정 요약

| 구간 | 확정 | 부분 확정 | 기각 | 보류 |
|---|---|---|---|---|
| P0 (6) | 5 | 1 | 0 | 0 |
| P1 (12) | 11 | 1 | 0 | 0 |
| P2 (5) | 5 | 0 | 0 | 0 |
| 초점 4건 | 2 | 2 | 0 | 0 |

---

## 1. P0 재판정

### P0-01 `derive_axes()` 승격 오류 — **확정**

세 주장을 각각 코드로 확인했다. **셋 다 사실이다.**

**(1) `view.v_database` 성공이 `AS_OF_SCN` 승격 근거가 된다.**

```python
# g0-normalize.py:89-96
asof, scn1, scn2 = P.get("as_of_timestamp.target"), P.get("dbms_flashback.get_scn"), P.get("view.v_database")
elif ok(asof) and (ok(scn1) or ok(scn2)):
    put("snapshot_read", "AS_OF_SCN", used)
```

그런데 그 probe 의 실물은 이렇다.

```sql
-- g0-0a-capability-inventory.sql:269
p_scalar('view.v_database', q'[SELECT TO_CHAR(COUNT(*)) FROM v$database WHERE ROWNUM = 1]');
```

**`COUNT(*)` 다. SCN 을 읽지 않는다.** `V$DATABASE` 에 한 행이 있는지만 본다. 이것으로 `AS_OF_SCN` 을 주는 것은 근거가 없다.

더 나쁜 것이 있다. **같은 파일에 실제 값을 읽는 probe 가 따로 있다.**

```sql
-- g0-0a-capability-inventory.sql:201
p_scalar('v$database', q'[SELECT db_unique_name||'|'||database_role FROM v$database]');
```

`view.v_database`(접근 가능한가)와 `v$database`(값이 무엇인가)는 서로 다른 질문이고 둘 다 수집된다. 파생기가 **접근 가능성 probe 를 값 probe 처럼 썼다.**

`dbms_flashback.get_scn`(`:142`)은 SCN 을 실제로 읽는다. 그러나 그것도 **SCN 획득 가능 ≠ 그 SCN 으로 대상 object 를 `AS OF SCN` 조회 가능**이다. 리뷰가 요구한 composite probe 가 없다는 지적이 맞다.

**(2) `AS OF TIMESTAMP` 만 성공하면 `READ_ONLY_TXN` 이 된다.**

```python
# g0-normalize.py:97-99
elif ok(asof):
    put("snapshot_read", "READ_ONLY_TXN", used,
        "AS OF 는 되지만 SCN 원점이 없어 AS OF TIMESTAMP(±3초 근삿값)뿐이다 — AS_OF_SCN 으로 올리지 않는다")
```

`txn.set_read_only` 와 `txn.select_inside` 가 **둘 다 실패해도** 이 분기를 탄다. note 는 정직한데 **값이 거짓말을 한다.** 축의 값은 기계가 읽는 것이고 note 는 사람이 읽는 것이다. 기계가 읽는 쪽이 틀렸다.

이것은 단순한 등급 오류가 아니다. `READ_ONLY_TXN` 과 `AS OF TIMESTAMP` 는 **snapshot 의 scope 가 다르다.** 전자는 한 connection 의 transaction snapshot, 후자는 SQL literal 이라 connection 과 무관하다. 값을 바꿔 붙이면 Spark 다중 connection 에서 성립하는지가 정반대로 뒤집힌다.

**(3) ORA-01453 양성 대조를 파생기가 읽지 않는다.**

probe 는 존재한다.

```sql
-- g0-0a-capability-inventory.sql:217-218
-- 두 번째 SET TRANSACTION이 ORA-01453이면 첫 트랜잭션이 실제로 열려 있었다는 양성 증거다.
p_stmt('txn.set_read_only.reissue', 'SET TRANSACTION READ ONLY');
```

`derive_axes()` 의 `used` 목록에 이 id 가 없다. **양성 대조를 SQL 은 수집하는데 파생기가 버린다.** 이 저장소의 규칙 "0건 조건에는 양성 대조를 함께 둔다"를 파생기가 위반한다.

**같은 오류가 `lag_visibility` 에도 있다.**

```python
# g0-normalize.py:129-134
dg, d = P.get("view.v_dataguard_stats"), P.get("alter.STANDBY_MAX_DATA_DELAY.D")
elif ok(dg): put("lag_visibility", "DG_STATS", used)
elif ok(d):  put("lag_visibility", "MAX_DELAY_ONLY", used, ...)
```

```sql
-- :268
p_scalar('view.v_dataguard_stats', q'[SELECT TO_CHAR(COUNT(*)) FROM v$dataguard_stats WHERE ROWNUM = 1]');
```

`DG_STATS` 는 "lag 값을 안다"는 뜻인데 근거는 `COUNT(*)` 다. **row 도 값도 `DATUM_TIME` 도 읽지 않는다.** `MAX_DELAY_ENFORCED` 쪽도 `ALTER SESSION` 이 수락되기만 하면 되고, overlay 문서 자신이 요구한 ORA-03172 양성 대조(§4)를 파생기가 확인하지 않는다.

**조치**: 리뷰의 필수 정정 6항을 그대로 채택한다. 다만 `AS_OF_TIMESTAMP` 를 별도 값으로 두려면 축 enum 자체를 바꿔야 하므로 **P0-05 의 축 재설계와 한 묶음으로 처리한다.**

---

### P0-02 불완전·조작 산출물이 `MEASURED` 가 된다 — **확정**

리뷰의 6행 표를 한 줄씩 코드로 확인했다. **6행 전부 사실이다.**

| 리뷰 주장 | 코드 | 확인 |
|---|---|---|
| B0 한 줄이면 `MEASURED` | `cov_b0 = {"status": "MEASURED" if b0 else "FAILED", ...}` (`:250`) | ✔ 파싱 가능한 줄이 1개면 통과 |
| B1 이 `{"verdict":{"coverage":"PROVEN"}}` 뿐이어도 `MEASURED` | `"MEASURED" if v.get("coverage") == "PROVEN" else "PARTIAL"` (`:263`) | ✔ 다른 필드를 보지 않는다 |
| C00 summary 한 줄이면 `MEASURED` | `fence = {r["probe"]: r for r in recs ...}` → `"MEASURED" if fence` (`:270-274`) | ✔ `fence.summary` 도 `probe` 키를 가진다(`g0-0c-fence-facts.sql:122`) |
| CE 가 `pass=true` + scenario 0개면 `MEASURED` | `"MEASURED" if v.get("pass") else "PARTIAL"` (`:285`) | ✔ `scenarios` 길이를 보지 않는다 |
| A summary 없어도 축은 확정값 가능 | `cov_a = PARTIAL` 이지만 `P` 는 비지 않으므로 `derive_axes(P)` 가 확정값 산출 (`:234-237`, `:300`) | ✔ coverage 와 축 판정이 연동되지 않는다 |
| schema 위반이어도 exit 0 가능 | 파일을 **먼저 쓰고**(`:307`) 검증은 stderr 출력만(`:325-328`), 종료는 `return 0 if not und else 3`(`:338`) | ✔ 모든 축이 확정값이면 schema 위반이어도 exit 0 |

마지막 행은 특히 나쁘다. **schema 위반이 곧 "계약을 못 지켰다"인데 그 사실이 종료 코드에 반영되지 않고, 무효한 레코드가 최종 경로에 이미 쓰여 있다.** 뒤이어 그 파일을 읽는 도구는 위반 사실을 알 방법이 없다.

**CE runner 의 returncode 미검사도 확정이다.**

```python
# g0-0c-counterexamples/runner.py:284
out = subprocess.run(cmd, capture_output=True, text=True, timeout=per, ...)
```

이후 `out.stdout` 만 파싱하고 **`out.returncode` 를 한 번도 읽지 않는다.** 시나리오가 통과 모양의 `SCENARIO_RESULT` 를 찍은 뒤 exit 1 로 죽어도 suite PASS 후보가 된다.

**조치**: 리뷰의 필수 정정 5항 채택. 우선순위 1위다 — 이것이 열려 있으면 다른 모든 수정의 결과를 신뢰할 수 없다.

---

### P0-03 증거가 대상·시각·판본에 묶이지 않는다 — **확정**

`source` 는 optional 이고 그 안의 필드도 전부 optional 이다(`g0-evidence.schema.json:21-30`). 그리고 normalizer 가 채우는 것은 5개뿐이다.

```python
# g0-normalize.py:239-245
for k, pid in (("db_unique_name", ...), ("database_role", ...), ("instance_name", ...),
               ("oracle_version", ...), ("characterset", ...)):
```

**`target_owner`·`target_table`·`wm_column` 은 schema 에 자리가 있는데 한 번도 채워지지 않는다.** 그런데 overlay §5 는 `snapshot_read`·`row_change_scn`·`wm_granularity` 세 축이 **테이블 단위**라고 규정한다. 테이블 식별자 없이 테이블 단위 축을 저장하고 있는 것이다. `ROWDEPENDENCIES=ENABLED` 인 테이블 A 의 `ROW_LEVEL` 을 테이블 B 에 적용하는 것을 증거 형식이 막지 못한다.

**판본 binding 이 사후 부착이라는 지적도 확정이다.** `lock_digest = sha(lock)`(`:262`)는 **정규화를 실행하는 시점의** `versions.lock` 을 해시한다. B0/B1/C 산출물이 실제로 어떤 lock 아래 실행됐는지와 대조하지 않는다. `executed_at` 도 `datetime.now()`(`:292`)이므로 probe 시각이 아니라 정규화 시각이다.

**이번 회차가 이것을 실증했다.** 2026-08-27 S3 회차에서 나는 B1 산출물을 만든 **뒤에** `versions.lock` 을 채우고, 그 다음 정규화를 돌렸다. 도구는 아무 이의도 제기하지 않았다. 리뷰가 말한 "old log 를 새 lock 과 함께 정규화할 수 있다"가 가설이 아니라 **이미 일어난 일**이다.

**조치**: 리뷰의 필수 정정 5항 채택. `measured_at`(child 기록) / `normalized_at`(aggregator 기록) 분리를 포함한다.

---

### P0-04 `g0_evidence` 한 이름에 두 계약 — **확정**

P §8.1 이 요구하는 최종 G0 레코드 필드를 원문에서 확인했다.

> `g0_report_id` · `executed_at` · `versions_lock_digest` · `ddl_digest` · `verdict_sql_digest` · `canonical_hash_spec_digest` · `hash_vector_result` · `submission_path_result` · `source_kind` · `oracle_env{nls_characterset, nls_nchar_characterset, max_string_size}`

현재 schema 는 `record_type: {"const": "g0_evidence"}`, `schema_version: {"const": "1.0.0"}`, `additionalProperties: false` 다. **즉 같은 이름·같은 판본을 쓰면서 위 7개 필드를 전부 거부한다.** G0-0 부분 레코드만 표현할 수 있고 최종 G0 레코드는 오히려 schema-invalid 다.

schema 의 `description` 이 "G0-0 범위로 구현한 것"이라고 밝히고 있지만, **`record_type` 이 같으면 기계는 둘을 구분할 수 없다.** 리뷰가 §2.3 에서 말한 대로, 경계가 prose 에만 있고 record type·schema condition·aggregator·exit code 네 곳 중 어디에도 없다.

`not_covered` 가 다섯 항만 열거하고 `source_kind`·`oracle_env{nls_nchar_characterset, max_string_size}`·G0-5(동일 lock 실증)를 빠뜨렸다는 지적도 사실이다(`g0-normalize.py:31-42`). 그리고 `not_covered` 는 `minItems:1` 에 자유 텍스트라 **dummy 한 항이면 통과**한다.

> **이 검토서의 자기 정정(2026-08-27).** 조치 3 착수 시 probe 목록을 전수 확인하다가 위 항목의
> 근거를 내가 잘못 적었다는 것을 발견했다. 처음에는 `oracle_env.nls_nchar_characterset` 을
> "G0-0A 에 NCHAR charset probe 가 없다"로 적었는데 **틀렸다** —
> `nls.nchar_characterset`(`:234`)과 `v$parameter.max_string`(`:203`)이 둘 다 있다.
> 빠진 것은 probe 가 아니라 **그 값을 최종 `oracle_env` 로 조립하는 단계**이며 그것이 G0-1~5
> aggregator 소관이다. 두 값은 이제 `source` 에 담는다. 리뷰의 지적(그 항목이 `not_covered` 에
> 없었다) 자체는 그대로 유효하다 — 이유만 바뀐다.

**조치**: `g0_0_evidence` 를 별도 record type·별도 schema 로 분리한다. 항상 `gate_eligible=false`, `scope=CAPABILITY_INVENTORY` 를 고정 필드로 둔다. `not_covered` 를 자유 텍스트에서 고정 enum 집합으로 바꾼다.

> **이번 회차와의 관계**: 나는 2026-08-27 에 이 schema 의 `profile` enum 에 `SANDBOX_CONTAINER` 를 더했다. 그 변경은 이 P0 과 **충돌하지 않지만 해소하지도 않는다.** 분리 작업 시 새 `g0_0_evidence` schema 로 그대로 옮긴다.

---

### P0-05 overlay 가 독립인 축을 합쳤다 — **확정. 이 리뷰의 최중요 지적이다**

overlay §7 의 대체표를 원문에서 확인했다.

> | `bound_kind = ENFORCED / OBSERVED` + `bound_evidence` | `lag_visibility` 축 | `ENFORCED` 의 유일한 근거인 `SYNC_COMMIT_GUARD` 가 DBA 장치 등록이라 도달 불가 |

**대체 사유는 맞고 대체 자체는 틀렸다.** `ENFORCED` 를 얻을 수 없다는 판단은 옳다(DBA 협조 불가). 그러나 그것은 **`bound_kind` 의 값이 `OBSERVED` 로 내려간다**는 뜻이지 **축이 사라진다**는 뜻이 아니다.

두 축이 독립인 이유를 이 저장소는 이미 알고 있다.

- **apply lag** = redo 가 standby 에 적용되기까지의 지연. `V$DATAGUARD_STATS` 가 재는 것
- **watermark commit bound** = `commit_time − watermark_value`. 애플리케이션이 `UPDATE_DT` 를 쓴 시각과 그 트랜잭션이 commit 된 시각의 차

apply lag 가 **0 이어도** 오래된 `UPDATE_DT` 를 든 장기 트랜잭션이 뒤늦게 commit 하면 반개구간 fence 밖으로 그 행이 빠진다. lag 를 아무리 정밀하게 재도 이 누락은 보이지 않는다. README §4 의 마지막 줄이 정확히 이 현상이다.

> `MAX(watermark)` fence — 반개구간과 결합하면 tail 행이 지연·누락된다

즉 이 결함은 외부 리뷰어가 새로 발견한 것이 아니라 **저장소가 스스로 발견해 놓고 감축 과정에서 지운 것**이다. 감축 결정서(`etl-platform-v2.0-simplification-decision.md`)가 "무엇을 왜 안 잘랐는지"를 남긴 문서인데, **잘라서는 안 될 것을 자른 사례**가 여기 있다.

나머지 세 축의 병합 지적도 확인했다.

| 축 | 합쳐진 것 | 확인 |
|---|---|---|
| `snapshot_read` | object FLASHBACK 권한 · SCN 원점 · ADG 지원 · Spark connection 전파 · snapshot scope | ✔ P0-01 의 분기가 이 다섯을 한 값으로 접는다 |
| `lag_visibility` | lag **관측**(`DG_STATS`)과 lag **admission 강제**(`MAX_DELAY`) | ✔ overlay §4 스스로 "측정이 아니라 강제"라고 쓰면서 한 축의 두 등급으로 뒀다. 둘은 **동시에 참일 수 있다** — 우열 관계가 아니다 |
| `row_hash` | SHA-256 함수 존재 vs cross-engine canonical row 비교 능력 | ✔ 함수 하나의 가용성으로 `SHA256` 을 준다 |
| `charset_class` | 한 DB 의 charset vs 두 source 간 comparable 여부 | ✔ `AL32UTF8` 하나로 `cross_source_comparable` 을 주장한다 |

**조치**: 리뷰의 9축 분리표를 채택하되 **이번에 곧바로 동결하지 않는다.** 축을 두 번 바꾸는 것이 한 번 늦게 바꾸는 것보다 나쁘다. 순서는 §5 를 따른다.

---

### P0-06 B1 이 task-path fail-closed 없이 `PROVEN` — **부분 확정**

**두 층으로 나눠 판정한다. 리뷰가 이 둘을 한 항목에 담았는데 근거의 강도가 다르다.**

**(a) 판정기가 그 상황에서 `YES` 를 준다 — 코드로 확정.**

```python
# analyze-trace.py:139-152
broken = [r for r in fc if r.get("status") in ("FAIL_CLOSED_BROKEN", "FAIL_CLOSED_PARTIAL")]
...
"answer": "NO" if broken else "YES",
```

failclosed 회차의 **`TASK` trace 가 0건이어도** 모든 step 이 실패하기만 하면 `status = EXPECTED_FAILURE_OBSERVED` → `broken` 이 비어 → `YES`. **task 경로가 fail-closed 인지 확인하지 않고 fail-closed 를 통과시킨다.** 확정이다.

**(b) `fail=all` 이 실제로 task 경로에 도달하지 못한다 — 논리는 타당하나 미실측.**

`Preamble.java:41-46` 이 어떤 connection 이든 첫 호출에서 던지는 것은 사실이다. schema resolution 이 먼저 일어나므로 task 에 도달하지 못할 개연이 높다. 그러나 **이것은 아직 관측되지 않았다.** 2026-08-27 S3 회차는 Oracle 이 없어 `failclosed` 회차를 태우지 못했다. 반증도 확증도 없다.

두 층을 섞으면 안 된다. **(a)는 지금 고칠 코드 결함이고 (b)는 S6 에서 잴 사실이다.** (a)를 고치면 (b)가 참이든 거짓이든 판정이 정직해진다 — 그것이 (a)를 먼저 고쳐야 하는 이유다.

**추가 지적 다섯 개 중 넷은 확정, 하나는 이번 회차가 답했다.**

| 추가 지적 | 판정 |
|---|---|
| `MIXED` 한 건을 SCHEMA·TASK 양쪽으로 센다 | **확정.** `seen_schema = SCHEMA + MIXED`, `seen_task = TASK + MIXED`(`:103-104`). 한 건이 두 경로 관측으로 계상된다 |
| DSv2 `METADATA` 경로를 유발하지 않는데 README 첫 문장은 "모든 물리 connection" | **확정.** README §7 의 제외 고지는 있으나 normalizer 에 scope 구분이 없다 |
| `Preamble` 이 `SET TRANSACTION READ ONLY` 를 실행하지 않으므로 B1 통과는 snapshot capability 증거가 아니다 | **확정.** `Preamble.java` 전문에 그 문장이 없다 |
| **프리앰블이 `NLS_NUMERIC_CHARACTERS = '. '` 를 쓴다. A 는 `'.,'` 를 요구한다** | **확정. 즉시 수정 대상** — 아래 별도 |
| `SET TRANSACTION` 은 트랜잭션의 첫 statement 여야 한다 | **확정.** 향후 READ ONLY 추가 시의 설계 제약으로 채택 |

**`NLS_NUMERIC_CHARACTERS` 불일치는 저장소 전체에서 B1 두 곳만 다르다.** 실물 대조 결과는 이렇다.

```
'.,'  ← g0-0a-capability-inventory.sql:135     (G0-0A)
'.,'  ← g0-0b0-spark-smoke.py:116              (G0-0B0)
'.,'  ← etl-platform-target-architecture-v1.2.3.1.md:836   (A 규범)
'.,'  ← etl-platform-poc-test-plan-v1.md:212   (P — NUMBER 실행 규격 ⑥ 로케일 차단)
'. '  ← g0-0b1-connection-provider/.../Preamble.java:94     ← 여기
'. '  ← g0-0b1-connection-provider/run-g0-0b1.py:77         ← 여기
```

`'. '` 는 Oracle 이 받는 유효한 값이다(그룹 구분자를 공백으로). 그래서 **실행은 되고 조용히 다르다.** P §3.2 NUMBER ⑥ 은 canonical hash 의 결정론을 위해 `'.,'` 를 고정하라고 규정하므로, B1 이 재는 세션은 규범이 규정한 세션이 아니다. **B1 통과가 규범 세션의 성립을 시사하지 못한다.**

이것은 축 재설계를 기다릴 필요가 없는 2문자 수정이다. **이 검토서와 함께 고친다.**

---

## 2. P1 재판정

| ID | 판정 | 근거 (코드 실물) |
|---|---|---|
| P1-01 | **확정** | `capability_axes` 가 `required` 목록에 없다(`schema:7-8`). 값은 `additionalProperties` 로 자유 key. per-axis 는 `additionalProperties:false` + `value/derived_from/note` 뿐이라 overlay §6-3 이 요구한 `measured_at` 과 §6-5 의 `stale` 을 **저장할 자리가 없다.** 문서가 요구하는 것을 계약이 금지한다 |
| P1-02 | **확정** | overlay §6-5 "유효기간이 지난 capability 는 **이전 값을 유지**하되 `stale = true`". 만료된 고등급을 안전성 판정에 계속 쓰면 fail-closed 가 아니다. 표시용 prior value 와 effective capability 를 분리해야 한다 |
| P1-03 | **확정** | `sql_dialect` 는 `feat.fetch_first` 하나의 성패로 `11G`/`12C_PLUS` 를 가른다(`:167-169`). ORA-03135(연결 단절)·ORA-01031(권한)도 전부 `11G` 로 강등된다. **transient error 와 기능 부재를 구분하는 taxonomy 가 어디에도 없다** |
| P1-04 | **확정** | overlay §5 의 두 층 표에 `row_hash` 가 없다. DB 단위인지 테이블 단위인지 미정의 |
| P1-05 | **확정** | `charset_class` 는 `nls.characterset` 하나로 결정된다(`:170-176`). NCHAR charset 은 보지 않는다. P §8.1 이 `nls_nchar_characterset` 을 별도로 요구하는 이유(STRING 경로가 `TO_NCHAR` 경유)와 정면으로 어긋난다 |
| P1-06 | **확정** | `derived_from` 이 **의도한** probe 목록이다. `snapshot_read` 의 `used` 5개 중 `asof` 분기에서 실제로 읽는 것은 3개뿐이고, `wm_granularity` 의 `used` 에 든 `feat.interval_ns_successor`·`feat.timestamp9_precision` 은 **어느 분기에서도 쓰이지 않는다**(`:143-160`) |
| P1-07 | **확정** | LOB history 와 ordinary undo 를 하나의 boolean 으로 합친다. BasicFiles 의 `PCTVERSION/RETENTION` 과 SecureFiles 의 `MAX/MIN/AUTO/NONE` 은 성질이 다르며 1차 출처가 그렇게 규정한다 |
| P1-08 | **확정** | `g0-0b0-spark-smoke.py:203-205` — `except Exception as e: emit("S4.init_query_timeout", True, ...)`. **어떤 예외든 `ok=True`** 다. `DBMS_SESSION.SLEEP` 부재(ORA-00904)·권한 오류도 "timeout 보호 성공"으로 기록된다. note 가 "elapsed 가 5초 근처면"이라며 판단을 사람에게 미루지만 **값은 이미 True 로 확정돼 있다** |
| P1-09 | **확정** | C00 는 end sentinel·expected manifest 가 없고, `fence.summary` 한 줄이면 normalizer 가 `MEASURED` 로 만든다(P0-02 표와 동일 경로) |
| P1-10 | **부분 확정** | `artifact_hash(root, exclude={suite.yaml, out})`(`runner.py:485`). **제외 자체는 옳다** — 계산한 해시를 그 파일에 적으면 순환이 된다(코드 주석 `:218` 이 그 이유를 정확히 적어 두었다). 그러나 **suite config digest 를 별도로 남기지 않는 것**이 결함이다. 필수 scenario·budget·pass rule 이 어떤 코드 해시에도 묶이지 않는다. 리뷰의 최소 정정("code digest 와 suite config digest 를 **별도로** 기록")이 정확한 처방이다 |
| P1-11 | **확정** | 판정서가 스스로 인정한다 — "37건 중 28건은 검증되지 않았다"(`grant-request-verdict.md:13`). 9건의 판정 원자료·1차 출처 URL·검증 일시가 저장소에 없어 재현 불가 |
| P1-12 | **확정** | `README.md:16` "DBA 협조 불가 확정" vs `simplification-decision.md:48` "critical 하지 않은 권한은 ETL 계정에 요청 가능". **두 문장이 같은 저장소에서 다른 제약을 말한다.** 리뷰의 통일안("정합성 전제 불가; 비critical 요청은 가능하나 현재 보류·절대 가정하지 않음")을 채택한다 |

---

## 3. P2 재판정 — 5건 전부 확정

| 지적 | 판정 |
|---|---|
| `g0-0-probe-README.md:65` "기본 설정이면 산출물이 0건" | **확정.** SQL 은 skipped 레코드를 낸다(`g0-0c-fence-facts.sql:59,93,109`) + `fence.summary`(`:122`). "**대상 table query** 0건"으로 고친다 |
| B0 `--probe-rows` / B1 `--limit` 상한 검증 없음 | **확정.** 둘 다 `type=int` 뿐이다(`b0:50`, `b1:37`). 0·음수·과대값이 그대로 들어간다. production-safe 라벨을 유지하려면 hard maximum 이 필요하다 |
| `COUNTEREXAMPLE_REPRODUCED`·`MITIGATION_FAIL` 도 suite `pass=true` | **확정.** 하네스 완주라는 뜻으로는 맞지만 설계 PASS 로 오독된다. `execution_complete` 와 `mitigation_holds` 분리 채택 |
| `versions.lock` 의 `UNSET` 검사가 문자열 검색 | **확정 — 이번 회차에서 실제로 관측했다.** `elif "UNSET" in lock.read_text(...)`(`g0-normalize.py:265`). 파일 8행 주석 "**UNSET 은 빈칸이 아니라 판정이다**" 자체에 `UNSET` 이 있어 **모든 값을 채워도 경고가 사라지지 않는다.** 2026-08-27 정규화 실행에서 그 경고가 그대로 떴다 |
| artifact `sha256` 형식·`lines >= 0` 미제한 | **확정.** `sha256: {"type":"string"}`, `lines: {"type":"integer"}` 뿐이다(`schema:78-81`). `versions_lock_digest` 는 `^[0-9a-f]{64}$` 를 거는데 artifact digest 는 안 건다 — 같은 계약 안에서 엄격도가 다르다 |

---

## 4. 초점 4건에 대한 재판정

### 4.1 capability 축과 `derive_axes` — **확정(동결 불가에 동의)**

리뷰가 좋은 점으로 든 네 가지는 사실이다(기능 직접 probe 우선 / 입력 부재 시 `UNDETERMINED` / SHA-256 `abc` 양성 벡터 / raw digest 보존). 필수 보완 6항도 전부 타당하다.

`wm_granularity` 구현 오류는 **직접 확인했고 리뷰보다 심각하다.**

```python
# g0-normalize.py:151-152
g = {9: "NS", 6: "US", 3: "MS", 0: "SEC"}.get(scale if scale is not None else 6, "US")
```

`TIMESTAMP(2)` → `dict.get(2, "US")` → **`US`**. 실제 최소 단위는 10ms 다. `TIMESTAMP(8)` 도 `US` 로 표시되지만 실제는 10ns 다. **successor(M) 계산이 틀어진다 — 이것이 CE01(typed successor)이 겨누는 바로 그 지점이다.** 반개구간 seal 에서 successor 를 잘못 잡으면 tail 행을 잃거나 중복 적재한다.

```python
# :155-156
elif dt == "NUMBER" and scale is not None and scale >= 0:
    put("wm_granularity", "SEC", used, f"NUMBER(*,{scale}) — 고정 scale 이라 successor 가 정의된다")
```

**`NUMBER(10,2)` 가 `SEC` 이 된다.** note 는 "successor 가 정의된다"고만 말하는데 값은 시간 단위를 주장한다. NUMBER watermark 는 애초에 시간이 아닐 수 있다(시퀀스·SCN·논리 버전). **등급 enum 이 아니라 `datatype` + exact scale + min step 으로 저장해야 한다**는 리뷰 처방이 옳다.

`row_hash=SHA256` 이 너무 강하다는 지적도 확정이다. G0-0A 가 증명하는 것은 `STANDARD_HASH` 한 함수의 가용성이고, 행 내용 비교 능력은 NULL framing·NFC·NUMBER/TIMESTAMP 직렬화·LOB 경로를 포함해 **G0-3 V-01~V-16 을 통과한 뒤**에 나온다.

### 4.2 권한 보류와 FLASHBACK 논증 — **부분 확정. 여기서 리뷰가 우리를 유리하게 정정한다**

**"지금 요청서를 내지 않는다"에 대한 동의는 그대로 받는다.** 이 결론은 유지된다.

**"FLASHBACK 은 순이익이 아니다"라는 일반화의 부분 기각도 수용한다.** 다만 리뷰가 든 사실 정정 넷을 각각 재판정하면 경중이 갈린다.

| 리뷰의 사실 정정 | 재판정 |
|---|---|
| **`±3초` 표현이 틀렸다** — timestamp 는 지정 시각보다 **이전**으로 매핑될 수 있다 | **확정. 그리고 이 정정은 우리에게 유리하다.** 판정서 `:34` 가 "`SMON_SCN_TIME` 기반 **±3초 근삿값**"이라고 썼다. `±` 는 미래 방향 오차가 있다는 뜻으로 읽힌다. 실제로 매핑이 **이전**으로만 간다면 반개구간 fence 에서는 **안전 방향**이다 — 더 오래된 snapshot 은 누락이 아니라 중복을 만든다(중복은 멱등 적재가 흡수한다). **저장소가 스스로를 과잉 보수적으로 깎았다** |
| **ORA-08181 은 "같은 SCN 재사용"이 아니라 valid bounds 밖** | **확정.** 판정서 `:37` 이 "무효 SCN. 재시도에 같은 SCN 을 재사용하는 설계가 정확히 이 오류의 표적"이라고 썼다. 같은 SCN 재사용 자체는 오류가 아니다. 오래된 SCN 의 주 위험은 undo 부족(ORA-01555)이고, ORA-08181 은 SCN 이 유효 범위를 벗어난 경우다. **두 오류를 하나로 합쳤다** |
| **ORA-01466 에 index rebuild 를 일반 트리거로 단정하지 말라** | **부분 확정.** 판정서 `:36` 은 "파티션 롤링·인덱스 재구성이 그대로 트리거"라고 썼다. 파티션 DROP/ADD 는 테이블 정의 변경이라 **맞다**. `ALTER INDEX ... REBUILD` 는 테이블 정의를 바꾸지 않으므로 **과장이다**. 절반만 수용한다 |
| **ABMR 은 FLASHBACK grant 고유 비용이 아니다** | **확정.** 판정서 `:25` 가 "primary 로 가는 경로는 없다"의 반례로 ABMR 을 든다. ABMR 은 **FLASHBACK 유무와 무관하게** standby read 면 발생 가능하다. FLASHBACK 요청의 비용으로 계상하면 안 된다. 다만 "primary 로 가는 경로가 있다"는 원 주장 자체는 여전히 참이다 — **비용의 귀속처만 틀렸다** |

즉 판정서의 **결론(보류)은 살아남고 논증 넷 중 셋은 정정이 필요하다.** 이것은 판정서를 약화시키는 것이 아니라 **판정서를 더 정직하게 만든다** — 부풀린 반대 근거를 걷어내고도 "지금 요청서를 내지 않는다"가 성립한다면 그 결론이 더 강하다.

리뷰가 권고한 상태값 `NO_SUBMIT_PENDING_G0`(임시 hold)와 opt-in 조건, 37행 matrix 를 채택한다.

### 4.3 `G0-0 != G0` 경계 — **확정(문서상 확인 / 실행상 미폐쇄)**

리뷰가 확인한 좋은 경계 네 가지는 사실이다. 그리고 그것이 prose 에만 있다는 지적도 사실이다(P0-02·03·04 가 그 증거다).

리뷰가 제시한 합격식을 그대로 채택한다.

```text
G0-0 completed  => capability inventory artifact only
G0 PASS         => G0-1 && G0-2 && G0-3 && G0-4 && same_lock(G0-5)
G0-0 completed  != G0 PASS
```

**네 곳(record type · schema condition · aggregator · exit code)에서 동일하게 강제한다**는 요구가 이 항목의 핵심이다.

### 4.4 `simplification-decision.md` §6 의 공백 — **부분 확정**

**"고유 공백은 다섯 개"라는 지적(`safety_lag/clock_skew` 중복)은 확정이다.**

다섯 공백에 대한 리뷰의 답 다섯 개도 전부 타당하며 채택한다. 특히 두 번째("monitor 의 D 를 executor 에 전달하지 않는다 — D 는 모든 physical data connection 에 각각 설정한다")는 **문제 위치를 옮긴 정정**이라 가치가 크다. 전달 메커니즘을 설계하려던 방향 자체가 불필요했다.

**추가로 제시한 공백 다섯 개 중 넷은 확정, 하나는 범위를 좁힌다.**

| 추가 공백 | 판정 |
|---|---|
| watermark commit bound 가 lag 축으로 대체됐다 | **확정.** P0-05 와 동일 사안 — 복원·분리한다 |
| Full 60% 의 snapshot contract 가 없다 | **확정.** `SINGLE_CONNECTION_ATOMIC` / `PARALLEL_NONATOMIC` 선택·SLO·표시가 필요하다 |
| 무권한 source protection 의 실체가 없다 | **확정.** `GV$SESSION` 없이 Control 이 보는 것은 자기 token 과 client close 뿐이다. `SELF_LIMITED_ONLY` 로 정직하게 명명한다 |
| capability drift 가 진행 중 계약·과거 증거에 어떻게 적용되는지 없다 | **확정.** overlay §6-4 가 드리프트를 말하지만 **in-flight Run 규칙이 없다.** revision pin·effective downgrade·신규 Hold·in-flight continue/fence 가 필요하다 |
| 보증이 낮아진 뒤 전환 가치가 명시되지 않았다 | **부분 확정.** 공백인 것은 맞으나 이것은 §6(감축 공백)의 항목이라기보다 **README §1 의 상위 판단**이다. 리뷰 §4 의 네 조건을 그쪽에 넣는다 |

리뷰 §4 의 Dagster 전환 판정(네 조건)은 타당하며 **PoC 합격 조건으로 P v2.0 에 옮긴다.**

---

## 5. 조치 순서

리뷰 §6 의 8단계를 대체로 채택하되 **두 곳을 바꾼다.**

| # | 조치 | 리뷰와 다른 점 |
|---|---|---|
| **0** ✅ | **`NLS_NUMERIC_CHARACTERS` `'. '` → `'.,'`** (`Preamble.java`, `run-g0-0b1.py`) | **완료**(2026-08-27). 2문자 수정이고 다른 어느 것도 기다릴 필요가 없었다 |
| 1 ✅ | 현재 normalizer 를 gate producer 로 쓰지 않는다 | **완료** — `gate_eligible` 을 schema 의 `const false` 로 박았다. 도구가 그 값을 바꿀 방법이 없다 |
| 2 ✅ | P0-02~04 — evidence envelope · child manifest · binding 을 fail-closed 로 | **완료**(2026-08-27). `g0-0-evidence.schema.json` 신설·구 스키마 삭제, `g0-child-contract.md` + `g0-run-child.sh`, strict aggregator 재작성, exit 0/3/4 분리, CE returncode·config digest. 회귀 시험 `g0-normalize-tests.py` 40건 통과 |
| 3 ✅ | P0-01·05 — 축 모델을 표 기반 pure function 으로 재작성 + §5.1 반례 자동화 | **완료**(2026-08-27). `g0_axes.py` 신설 — 7축 → **13축**. `watermark_commit_bound` 복원, 접근 가능성/값 probe 분리, 양성 대조 요구, 실패 taxonomy(transient≠NONE), binding 없으면 테이블 축 UNDETERMINED, 합성 축 분리. overlay §3 표는 폐기하고 부록 A 로 대체. 반례 시험 79건 통과 |
| 4 | P0-06(a) — analyzer 의 fail-closed·`MIXED` 판정 수정 | **(a)만 먼저.** (b)는 S6 에서 잰다 |
| 5 | B1 을 path-specific fail injection 하네스로 확장 후 pinned Spark 에서 실행 | **build·SPI 배선은 2026-08-27 에 이미 했다**(3판본). 남은 것은 하네스 확장과 **Oracle 이 있는 환경** |
| 6 | 가장 덜 민감한 사내 DR source 에서 A → B0 → B1 | 그대로. C00 full-scan 계열은 별도 승인 전 미실행 |
| 7 | C01~C09 는 폐기 가능한 writable Oracle 에서만 | 그대로. child returncode·suite config digest 수정 후 |
| 8 | overlay distribution 확인 후 A/P v2.0 개정 | 그대로 |
| 9 | 37행 matrix 완성 후 object-set 별 optional FLASHBACK ROI 재판정 | 그대로 |

리뷰 §9 가 지목한 "다음 산출물 네 개"에 동의한다. 다만 **0번(2문자 수정)이 그 앞에 있다.**

---

## 6. 이 리뷰가 놓친 것 — 2026-08-27 S1~S3 실측이 더한 사실

리뷰는 `538ec31` 을 기준판으로 하고 **"G0-0 은 한 번도 실행되지 않았다"** 를 명시적 한계로 선언했다(리뷰 머리말). 그 전제는 검토 시점에 옳았고, 그 뒤 S1·S2·S3 이 실행됐다(`g0-0-s1-s3-results.md`). 리뷰의 판정을 뒤집는 것은 없지만 **더할 사실이 셋 있다.**

**(1) 리뷰 §7 이 요구한 검증을 이미 했다.** 리뷰는 1차 출처로 이렇게 적었다.

> Spark `JdbcConnectionProvider` 는 `DeveloperApi`/`Unstable` 이며 provider 선택은 pinned runtime 에서 검증해야 한다

2026-08-27 회차가 정확히 그 검증이다. Spark 4.2.0/2.13.18 · 3.5.9/2.12.18 · 3.5.9/2.13.8 세 판본에서 build · ServiceLoader 적재 · provider selection 을 실측했다. 세 판본 모두 통과했다. **리뷰 §6 권고 순서 5번의 절반이 끝나 있다.**

**(2) provider 미선택이 Spark 4.2.0 에서 진단 불가능하다 — 리뷰가 다루지 않은 새 사실.**

`disabledJdbcConnProviderList` 를 빠뜨리면 `ConnectionProviderBase.create` 가 `IllegalArgumentException("… more than one connection provider was found …")` 를 던진다(v4.2.0 소스 확인 + 직접 호출로 실증). 그런데 **Spark 4.2.0 은 `JdbcUtils.classifyException` 으로 그것을 `[FAILED_JDBC.CONNECTION] … Couldn't connect to the database` 로 덮는다.** Spark 3.5.9 는 원문을 그대로 노출한다.

이것은 리뷰 P0-06 의 "경로별 실패를 구분하라"와 같은 계열의 문제이며, **B1 하네스를 확장할 때 판정 입력을 예외 메시지로 삼으면 안 되는 이유**다. 판정은 추적 라인 수로 해야 한다.

**(3) `basic` 하나만 끄는 것으로는 부족한 경우가 있다 — 리뷰가 다루지 않은 새 사실.**

Spark 3.5.9·4.2.0 모두 내장 `OracleConnectionProvider`(name = `oracle`)를 싣는다. `canHandle` 이 정확히 배타적이다.

```scala
SecureConnectionProvider  canHandle = keytab != null && principal != null && driverClass 일치
BasicConnectionProvider   canHandle = keytab == null || principal == null
```

Kerberos 원천이면 후보가 `{Oracle, ours}` 가 되어 `basic` 을 꺼도 여전히 2개다 — 같은 예외가 난다. `basic,oracle` 이어야 한다. **사내 원천의 인증 방식이 정해지기 전에는 이 conf 값을 규범에 고정할 수 없다.**

---

## 7. 결론

리뷰 §9 의 마지막 진단을 그대로 받는다.

> 이번 결함도 아키텍처가 과도해서 생긴 것이 아니라, **설명용 경계를 실행 계약으로 옮기는 마지막 단계가 덜 닫힌 것**에 가깝다.

여섯 차례 리뷰가 잡은 것이 "문서가 확인되지 않은 것을 확인된 것처럼 썼다"였다면, 일곱 번째가 잡은 것은 **"문서는 고쳤는데 코드가 안 고쳐졌다"** 다. 같은 병의 다음 단계다.

한 가지만 이 검토서가 리뷰에 덧붙인다. **P0-05 는 새로 발견된 결함이 아니라 되찾아야 할 결함이다.** `MAX(watermark)` fence 의 tail 누락은 README §4 에 이미 적혀 있었고, 감축 과정에서 축을 합치며 지웠다. 감축 결정서의 부제가 "무엇을 잘랐고 **무엇을 왜 안 잘랐는지**"인데, 이제 세 번째 칸이 필요하다 — **잘라서는 안 됐던 것.**
