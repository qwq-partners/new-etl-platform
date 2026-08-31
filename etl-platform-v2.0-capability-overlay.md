# Capability 오버레이 규격 (v2.0 **초안 — 동결 불가**)

> **2026-08-27 7차 교차 리뷰 판정: 이 문서는 동결할 수 없다(NO-GO).**
> 방향(단일 privilege-zero core + 원천별 overlay)은 GO 이나, §3 의 축 표가 **독립인 성질을 합치고**
> 조합 규칙이 없다. 아래 §3.1 이 그 분해를 기록한다. G0-0 실측 후 이 표를 대체한다.

> ## 2026-08-30 — 8차 교차 리뷰 M4-5: 이 문서가 스스로와 어긋나던 곳
>
> 8차 리뷰 §2 P0-05 가 이 문서를 "스스로 모순된다"고 판정했다. 네 건이었고 전부 **같은 원인**이다 —
> 7차 조치가 §3·§3.1·부록 A 만 고치고 §5·§7·§8 을 그대로 뒀다. 그래서 폐기한 축 이름이
> 문서 뒤쪽에서 계속 살아 있었다.
>
> | 어긋남 | 지금 |
> |---|---|
> | 머리말은 "**7축**", §3 표는 **9행** | 세지 않고 썼다. §3 표는 9행이고, 현행 축은 **13개**(부록 A)다. 머리말에서 숫자를 뺐다 |
> | §3.1 은 `bound_kind` **복원**, §7 은 `lag_visibility` 로 **대체** | §7 표를 정정했다. **복원이 확정**이고 대체는 철회다 |
> | §3·§4 는 `lag_observation`/`lag_admission`, §5 는 없는 축 `lag_visibility` | §5 표를 현행 축 이름으로 바꿨다 |
> | §3.1 은 composition **미정**, §7 은 대체 관계를 **확정형**으로 | §7 머리말에 "이 표는 이력이다"를 붙였다 |
>
> **읽는 순서**: 현행 정의는 **부록 A** 와 `g0_axes.py` 의 `AXIS_SPEC` 뿐이다. §3·§5·§7 은
> 이력이며, 어긋나면 부록 A 가 이긴다. 그 둘이 어긋나지 않는지는 `g0-axes-tests.py` 가 검사한다.

원천 Oracle 이 버전·옵션·권한 면에서 제각각이라는 전제에서, **하나의 코어 + 원천별 측정 오버레이**로 대응하는 규격이다. 이 문서는 짧게 유지한다 — 길어지면 그 자체가 문제다.

---

## 1. 원칙 세 줄

1. **코어는 권한 0 · 최저 버전에서 성립한다.** 그 위에서만 오버레이를 얹는다.
2. **권한은 정합성의 전제가 아니다.** 비용·지연을 줄이는 선택지일 뿐이다. 권한이 회수돼도 정합성은 유지되고 등급만 내려간다.
3. **강등은 반드시 보인다.** 조용한 강등은 결함으로 취급한다. 데이터 계약에 표시되지 않는 강등은 없다.

> 2번이 왜 중요한가: `GRANT FLASHBACK` 은 **객체 단위**다. Job 10,000개를 온보딩할 때마다 권한 요청이 붙고, 회수되면 보증이 조용히 바뀐다. 정합성을 여기에 걸면 조직적으로 확장되지 않는다.

---

## 2. 바닥(floor) — 아무것도 가정하지 않을 때 남는 것

Oracle **11.2 physical standby(ADG)** + ETL 계정에 **대상 테이블 SELECT 만** 있는 상태.

| 쓸 수 있는 것 | 비고 |
|---|---|
| `SELECT`, `ROWNUM` | `FETCH FIRST` 는 12.1+ 이므로 코어는 `ROWNUM` 만 쓴다 |
| `SET TRANSACTION READ ONLY` | 그 **connection 안에서만** 일관된 읽기 |
| `SYS_CONTEXT('USERENV', …)` | 신원·역할 확인 |
| `ALTER SESSION SET STANDBY_MAX_DATA_DELAY` | 11.1+ ADG. **측정이 아니라 강제**다(§4 참고) |
| `ORA_HASH` | 32비트. 대조용 해시로는 **못 쓴다** |
| `ALL_*` / `USER_*`, `NLS_DATABASE_PARAMETERS`, `PRODUCT_COMPONENT_VERSION` | 권한 불필요 |

**바닥에서 낼 수 있는 보증**: `upsert_consistency = BEST_EFFORT`, `snapshot_scope = CONNECTION`, `delete_consistency = NONE`(PK 대조 없이는 삭제를 볼 수 없다).

---

## 3. Capability 축

> ## ⚠ 2026-08-27 — 이 절의 축 표(9행)는 **폐기됐다**
>
> **2026-08-30 — 이 표를 "7축"이라고 부르던 것도 틀렸다.** 아래 표는 9행이고, 그것을 대체한
> 부록 A 는 원자 10 + 합성 3 = **13축**이다. 세지 않고 쓴 숫자였다(8차 M4-5).
>
> 7차 교차 리뷰 **P0-05** 가 이 축들이 독립 성질을 합쳤다고 판정했고, 재판정에서 **확정**했다
> (`etl-platform-v2.0-codex-seventh-review-assessment.md` §1). 가장 큰 오류는 §7 의 대체표가
> `bound_kind`(watermark commit bound)를 `lag_visibility` 로 **대체**한 것이다.
>
> **apply lag 와 `commit_time − watermark_value` 는 독립이다.** apply lag 가 0 이어도 오래된
> `UPDATE_DT` 를 든 장기 트랜잭션이 뒤늦게 commit 하면 반개구간 fence 밖으로 그 행이 빠진다.
> 이것은 이 저장소가 이미 알던 사실이다 — README §4 의 마지막 줄이 그 현상이다.
> **감축은 중복을 지우는 작업이어야 하는데 독립 성질을 지웠다.**
>
> 아래 표는 **이력으로 보존**한다. 실제로 도는 정의는 **부록 A** 와 `g0_axes.py` 의
> `AXIS_SPEC` 이며, 그 두 개가 어긋나지 않는지는 `g0-axes-tests.py` 가 검사한다.

측정은 `g0-0a-capability-inventory.sql` §8 이 한다. **버전으로 기능을 추정하지 않고 기능을 직접 실행해 `SQLCODE` 로 판정한다.**

| 축 | 값(높은 순) | 측정 probe | 무엇을 가르는가 | 강등 시 계약 표시 |
|---|---|---|---|---|
| `snapshot_read` | `AS_OF_SCN` → `READ_ONLY_TXN` → `NONE` | `as_of_timestamp.target` + SCN 원점(`dbms_flashback.get_scn` \| `view.v_database`), `txn.set_read_only`+`txn.select_inside` | 한 회차의 여러 chunk 가 **같은 시점 이미지**인가 | `snapshot_scope = JOB / CONNECTION / CHUNK` |
| `row_hash` | `SHA256` → `NONE` | `feat.standard_hash_sha256` | 행 단위 내용 대조 가능 여부 | `reconcile_depth = ROW_HASH / PK_AND_COUNT` |
| `row_change_scn` | `ROW_LEVEL` → `BLOCK_LEVEL` → `NONE` | `feat.rowdependencies_target`, `feat.ora_rowscn_target` | `ORA_ROWSCN` 기반 변경 탐지 | `change_detection = ROWSCN / WATERMARK_ONLY` |
| `lag_observation` | `DG_STATS` → `NONE` | `view.v_dataguard_stats` | lag 을 **잴 수 있는가** | `freshness_evidence = MEASURED` |
| `lag_admission` | `ENFORCED` → `ACCEPTED_UNVERIFIED` → `NONE` | `alter.STANDBY_MAX_DATA_DELAY.D` + `max_delay_zero.touch_target`(ORA-03172 양성 대조) | lag 초과를 **막는가** | `freshness_evidence = ENFORCED` |
| `watermark_commit_bound` | `ENFORCED` → `OBSERVED` → `NONE` | **측정 probe 없음 — 항상 `UNDETERMINED`** | `commit_time − watermark_value` 의 상한 | `upsert_consistency` 등급 |
| `wm_granularity` | `NS` → `US` → `MS` → `SEC` → `UNDEFINED` | **`wm_column.type_facts`**(결정자) + `feat.interval_ns_successor`·`feat.timestamp9_precision`(구문 지원 확인용) | `typed_successor` 반개구간 fence 성립 여부 | `fence_mode = HALF_OPEN / OVERLAP_ONLY` |
| `sql_dialect` | `12C_PLUS` → `11G` | `feat.fetch_first` | 생성 SQL 의 구문 집합 | (내부용, 계약 노출 없음) |
| `charset_class` | `AL32UTF8` → `OTHER` | `nls.characterset`, `nls.comp`, `nls.sort` | **원천 간** 해시 정본화 비교 가능성 | `cross_source_comparable` |

### 축별 강등이 실제로 뜻하는 것

- **`snapshot_read = NONE`** — Spark 가 `numPartitions>1` 로 읽으면 partition 마다 다른 connection·다른 시점이다. partition key 가 가변이면 중복·누락이 난다(CE06 이 겨냥). 완화는 단일 connection 강제뿐이고 그건 처리량을 깎는다.
- **`row_hash = NONE`** — 11g 에 `STANDARD_HASH` 가 없고 `DBMS_CRYPTO` 도 못 받으면 행 내용 대조가 불가능하다. `ORA_HASH` 는 32비트라 충돌률상 대체재가 아니다. Reconciliation 은 건수+PK 로 내려가고, **내용만 바뀐 행은 탐지 대상에서 빠진다**(CE09 가 겨냥).
- **`row_change_scn = BLOCK_LEVEL`** — `ROWDEPENDENCIES` 는 **테이블 생성 시점에 결정되고 사후 변경이 불가능하다.** 원천이 안 켰으면 영원히 못 쓴다. 블록 단위 SCN 은 과탐만 내고 누락은 없지만, delayed block cleanout 때문에 **상한만** 보장한다.
- **`wm_granularity = UNDEFINED`** — 무제약 `NUMBER` · `BINARY_DOUBLE` 은 고정 granularity 가 없어 `successor(M) == M` 이 된다(CE01 이 겨냥). 이 타입은 반개구간 seal 대상에서 제외하고 overlap 재적재로만 처리한다.

---

## 3.1 축 분해 — 7차 리뷰 P0-05 반영

기존 7축은 다음을 합치고 있었다. **합친 축은 증명하지 않은 능력을 표시한다.**

| 잘못 합친 것 | 왜 독립인가 |
|---|---|
| `lag_visibility` = 관측 + 강제 | 우열 관계가 아니라 **동시에 존재 가능한 두 성질**이다. 잴 수 있으나 못 막을 수도, 막지만 못 잴 수도 있다 |
| `bound_kind` → `lag_visibility` 대체 | **가장 큰 오류.** apply lag 과 `commit_time − watermark_value` 는 독립이다. **lag=0 이어도** 오래된 `UPDATE_DT` 를 가진 트랜잭션이 늦게 commit 하면 overlap 밖 누락이 생긴다. A 가 `max_commit_minus_watermark_seconds`(7회 등장)를 따로 둔 이유가 이것이다 |
| `snapshot_read` | object FLASHBACK 권한 / SCN 원점 / ADG 지원 / **Spark connection 전파** / snapshot scope 를 한 값에 담았다 |
| `row_hash` | SHA-256 함수 존재 ≠ cross-engine canonical row hash 검증(G0-3) |
| `charset_class` | 한 DB 의 charset ≠ 두 원천 간 comparable 여부 |

→ **`bound_kind`/`bound_evidence` 를 되살린다.** `lag_visibility` 로 대체한 것은 철회한다.
→ 최종 축 목록과 조합 함수(composition)는 G0-0 실측 후 확정한다. 예: `snapshot_scope = JOB` 은
   공통 anchor + 전 object coverage + 전 data connection 적용이 **모두** 참일 때만 나온다.

---

## 4. `lag_observation` 과 `lag_admission` 은 성질이 다르다

- `lag_observation = DG_STATS` — `V$DATAGUARD_STATS` 에서 lag 을 **측정**한다.
  단, `COUNT(*)` 가 된다는 것과 **값·`DATUM_TIME` 을 해석할 수 있다는 것은 다르다.
- `lag_admission = ACCEPTED_UNVERIFIED` — `ALTER SESSION SET STANDBY_MAX_DATA_DELAY` 가 수락됐을 뿐이다.
  값을 알려 주지 않고, 임계 초과 시 `ORA-03172` 로 **쿼리를 실패시킬 뿐**이다.
- `lag_admission = ENFORCED` — lag 이 큰 시간대에 **`ORA-03172` 를 최소 1회 확보**했을 때만.
  오류 부재는 강제가 걸린다는 증거가 아니다.

---

## 5. 측정 단위는 여러 층이다

> **2026-08-30 정정(8차 M4-5).** 이 표는 폐기된 축 이름(`lag_visibility`·`snapshot_read`·
> `wm_granularity`·`charset_class`)으로 쓰여 있었다. 특히 `lag_visibility` 는 §3.1 이 **없는 축**
> 이라고 못 박은 이름이다 — 같은 문서 안에서 한쪽은 폐기를 선언하고 다른 쪽은 그것을 쓰고 있었다.
> 현행 축 이름과 판정 단위로 바꾼다. **권위는 부록 A 와 `g0_axes.py` 의 `AXIS_SPEC` 이다.**

층은 둘이 아니라 넷이다. `g0-0-evidence.schema.json` 의 `axis.scope` enum 이 그것을 형태로 강제한다.

| 층(`scope`) | 축 | 왜 |
|---|---|---|
| **DB** | `lag_observation`, `hash_function`, `sql_dialect`, `db_charset` | 원천 DB 하나당 한 번 재면 된다 |
| **ACCOUNT** | `snapshot_anchor` | 같은 DB 라도 계정이 다르면 원점을 얻는 수단이 다르다 |
| **CONNECTION** | `lag_admission` | 세션 속성이라 connection 마다 걸어야 하고, 한 번 걸렸다고 다음 connection 이 걸리는 것이 아니다 |
| **TABLE / COLUMN** | `snapshot_object_coverage`, `row_change_scn`(ROWDEPENDENCIES), `watermark_commit_bound`, `wm_successor`(컬럼 타입) | 같은 DB 안에서도 테이블·컬럼마다 다르다 |
| **RUNTIME**(합성) | `snapshot_scope`, `canonical_row_compare`, `cross_source_comparable` | 위 축들의 조합. 입력이 하나라도 미확정이면 미확정이다 |

Job 은 `(source_db, table)` 쌍의 capability 를 상속한다. **DB 단위 축이 좋아도 테이블 단위 축이
나쁘면 그 Job 은 나쁜 쪽을 따른다.**

**층이 다르면 결속 대상도 다르다.** TABLE/COLUMN 축은 `binding`(`db_identity`·`owner`·`object`)이
없으면 확정값을 내지 못한다(부록 A.3-4). 테이블 A 에서 잰 값이 테이블 B 에 적용되는 것을
형식이 막는다.

---

## 6. 강등·드리프트 규칙

1. **fail-closed** — 정합성을 지킬 수 없는 조합은 publish 를 거부한다. (예: `wm_granularity = UNDEFINED` 인데 반개구간 fence 를 선언한 Job)
2. **degrade + disclose** — 지킬 수는 있으나 등급이 내려가는 경우, 거부하지 않고 계약 표시를 내린다. 사용자에게는 publish 전에 미리보기로 보여 준다.
3. **capability 는 값이 아니라 측정이다.** 모든 값에 `measured_at` 과 측정에 쓴 probe id 를 함께 저장한다.
4. **드리프트** — 재측정에서 값이 내려가면, 그 원천을 쓰는 ACTIVE Job 의 계약 표시를 갱신하고 경보한다. 값이 올라가는 것은 자동 반영하지 않는다(새 publish 에서만 반영). 올라간 등급을 자동 적용하면 **아무도 검토하지 않은 보증**이 생긴다.
5. **`stale`** — 유효기간이 지난 capability 는 이전 값을 유지하되 `stale = true` 로 표시한다. 값을 낙관적으로 올리지 않는다.

---

## 7. v1.2.3.1 에서 무엇을 대체하는가

> **2026-08-30 정정(8차 M4-5).** 이 표는 **제안이지 확정이 아니다.** §3.1 은 composition 을
> "G0-0 실측 후 확정"이라고 두는데 이 표는 대체 관계를 확정형으로 적고 있었다. 게다가 2행은
> §3.1 이 명시적으로 **철회한** 대체(`bound_kind` → `lag_visibility`)를 그대로 싣고 있었다 —
> 같은 문서가 한쪽에서 철회하고 다른 쪽에서 주장했다. 아래는 그 정정판이며, **어느 행도 아직
> A v1.2.4 를 실제로 대체하지 않았다.** A 는 그대로 유효하다.

| v1.2.4 | v2.0 **제안** | 상태 |
|---|---|---|
| `ZERO_GAP` / `BEST_EFFORT` 2등급 | 축별 등급(부록 A) | **제안.** 등급 하나가 여러 성질을 뭉뚱그렸다는 진단은 유지. 대체 여부는 실측 후 |
| `bound_kind = ENFORCED / OBSERVED` + `bound_evidence` | **대체하지 않는다 — 축으로 존치**(`watermark_commit_bound`) | **철회 확정.** 이전 판은 이것을 `lag_visibility` 로 대체했다. apply lag 과 `commit_time − watermark_value` 는 독립이며, `ENFORCED` 가 Profile U 에서 도달 불가라는 것은 **값이 `OBSERVED` 로 내려갈 이유이지 축을 지울 이유가 아니다**(§3.1) |
| `cutoff` 3종(`APPLICATION_TIMESTAMP_WITH_OVERLAP` / `STANDBY_VISIBLE_SCN` / `CDC_OFFSET`) | `snapshot_anchor` + `snapshot_object_coverage` + `wm_successor` | **제안.** `STANDBY_VISIBLE_SCN` 은 `AS OF SCN` 전제라 anchor 축에 흡수되지만, **anchor 를 얻는 것과 대상 전체에 통하는 것은 다른 축**이라 1:1 대체가 아니다 |
| `zero_gap_verified`, `ZERO_GAP_INVALIDATED` 자동 HOLD | §6 드리프트 규칙 | **제안.** 축이 여럿이면 단일 무효화 플래그가 성립하지 않는다는 진단은 유지 |

**아직 정하지 않은 것**: 위 대체가 `JobSpec` 입력 필드를 늘리는가. v1.2.3.1 의 설계 목표는 "JobSpec 입력 증가 0"이었고, 그 목표는 유지할 만하다 — 축은 **측정에서 파생**되므로 사용자가 입력할 것이 아니다. 다만 `fence_mode` 처럼 사용자가 고를 여지가 있는 축이 있는지는 G0-0 결과를 보고 정한다.

---

## 8. 다음에 확정할 것

1. **G0-0A 를 원천 3~5개에 돌려** 축 값의 실제 분포를 본다. 값이 다 같으면 오버레이가 과설계이고, 제각각이면 이 규격이 필요하다는 증거가 된다.
2. ~~`snapshot_read` 축에서 `AS_OF_SCN` 을 실제로 쓸 것인지 결정한다.~~
   **보류 유지(2026-08-27) · 근거 정정(2026-08-30, 8차 M4-1·M4-2·M4-4).**

   `FLASHBACK` 객체 권한만으로는 SCN 출처가 없어 `AS OF TIMESTAMP` 뿐이다. 그 오차는
   **`±3초` 가 아니라 최대 3초 이전**이다 — 방향이 있고 미래로는 가지 않는다. 새 실패 모드는
   `ORA-01466`(fence 이후 DDL)·**`ORA-08180`**(시각을 매핑표의 SCN 에 맞추지 못함)·
   `ORA-01555`(undo 부족)이며, **`ORA-08181` 은 여기가 아니다** — 그것은 공급된 SCN 이 유효
   범위 **밖**일 때이고 대표 사례는 다른 DB 의 SCN 이다. 그리고 undo 부족의 Oracle 권고 대책이
   `primary 의 UNDO_RETENTION 상향`이라 **생산 primary 로 부하가 도달하는 채널**이 열린다 —
   단 그것은 **overlay 를 실제로 쓸 때(runtime)** 생기지, grant 를 받아 두는 것(passive)만으로
   생기지 않는다.

   **이전 판이 세지 않은 잠재 이득 하나**(검증 전이며 확인된 이득이 아니다 — 9차 §5.2):
   같은 timestamp 리터럴을 모든 partition 쿼리에 bind 하면
   여러 물리 connection 이 **같은 flashback anchor** 에 묶인다. 현 코어의 `SET TRANSACTION
   READ ONLY` 는 connection scope 라 그것을 낼 수 없다. 3초 이전 매핑은 anchor **정밀도** 문제
   이지 공통 anchor 가 사라지는 문제가 아니다. 그래도 `snapshot_scope = JOB` 은 공통 anchor ∧
   전 object coverage ∧ 전 connection 전파가 **모두** 참일 때만 나온다.

   근거는 `etl-platform-v2.0-grant-request-verdict.md` §3(2026-08-30 개정판).
   → **코어는 그대로 connection scope 하나로 간다.** 결론은 바뀌지 않았고 근거만 정확해졌다.
3. `row_hash = NONE` 인 원천이 실제로 있는지 확인한다. 있으면 Reconciliation 규격을 두 갈래로 써야 한다.

---

# 부록 A — 축 재설계 (2026-08-27, 7차 리뷰 P0-01·P0-05 조치)

**권위는 이 표와 `g0_axes.py` 의 `AXIS_SPEC` 이다.** 위 §3·§5·§7 의 표는 이력이다.

## A.1 원자 축 — probe 에서 직접

| 축 | 값 | 판정 단위 | 무엇을 가르는가 |
|---|---|---|---|
| `snapshot_anchor` | `SCN` / `TIMESTAMP` / `NONE` | ACCOUNT | 고정 시점 원점을 **얻을 수 있는가**. 얻은 원점으로 대상을 읽을 수 있는지는 다음 축이 답한다 |
| `snapshot_object_coverage` | `ALL` / `PARTIAL` / `NONE` | TABLE(→ object-set) | extract object-set **전체**에 그 원점이 통하는가. **G0-0 에서는 `ALL` 이 나올 수 없다** |
| `lag_observation` | `DG_STATS` / `NONE` | DB | lag **값을 읽을 수 있는가**. 뷰 접근 가능성이 아니라 값 해석을 요구한다 |
| `lag_admission` | `MAX_DELAY_ENFORCED` / `NONE` | CONNECTION | 임계 초과 시 서버가 **거절하는가**. ORA-03172 양성 대조를 요구한다 |
| `watermark_commit_bound` | `ENFORCED` / `OBSERVED` / `NONE` | COLUMN | **복원된 축.** `commit_time − watermark_value` 의 상한을 무엇으로 보증하는가 |
| `row_change_scn` | `ROW_LEVEL` / `BLOCK_LEVEL` / `NONE` | TABLE | `ORA_ROWSCN` 입도 |
| `wm_successor` | `TIMESTAMP(n)` / `DATE` / `NUMBER(scale=n)` / `UNDEFINED` | COLUMN | **등급이 아니라 datatype·exact scale·min step** |
| `hash_function` | `SHA256` / `NONE` | DB | 함수 하나의 가용성. **행 비교 능력이 아니다** |
| `sql_dialect` | `12C_PLUS` / `11G` | DB | 방언 하한 |
| `db_charset` | 관측값 그대로 | DB | **등급이 아니다.** 비교 가능성은 합성 축이 답한다 |

## A.2 합성 축 — 다른 축에서

| 축 | 값 | 성립 조건 |
|---|---|---|
| `snapshot_scope` | `JOB` / `CONNECTION` / `STATEMENT` / `NONE` | `JOB` 은 공통 anchor ∧ `object_coverage=ALL` ∧ 모든 data connection 적용이 **모두** 참일 때만 |
| `canonical_row_compare` | `VECTORS_PROVEN` / `PARTIAL` / `NONE` | **`VECTORS_PROVEN` 은 G0-3 V-01~V-16 통과 전에 절대 나오지 않는다** |
| `cross_source_comparable` | `YES` / `NO` | 원천이 둘 이상 측정된 뒤에만. G0-0 은 하나만 잰다 |

**합성 축은 입력이 하나라도 `UNDETERMINED` 면 `UNDETERMINED` 다.**

## A.3 판정 규율 넷

1. **접근 가능성 probe 를 값 probe 로 쓰지 않는다.** `SELECT COUNT(*) FROM v$database` 성공이
   `AS_OF_SCN` 승격 근거였던 것이 대표 사례다. 같은 파일에 값을 읽는 `v$database` 가 따로 있다.
2. **승격에는 양성 대조를 요구한다.** `ALTER SESSION` 이 수락되는 것과 임계 초과 시 거절되는 것은
   다른 사실이다. 오류 부재는 증거가 아니다.
3. **`NONE` 과 `UNDETERMINED` 를 구분한다.** ORA-03135(연결 단절)·ORA-01555(undo 부족)는
   기능 부재가 아니다.
   **2026-08-30 확장(8차 M3-2)** — taxonomy 가 셋에서 열하나로 늘었다.
   `NONE` 을 지지하는 것은 **`UNSUPPORTED` 하나뿐**이며(`g0_axes.NEGATIVE_EVIDENCE`),
   `DENIED`(ORA-01031)·`WRONG_TARGET`(ORA-00942)·`PROBE_BUG`·`AMBIGUOUS`·`GRAMMAR`·
   `EMPTY`·`TRANSIENT`·`UNKNOWN`·`ABSENT` 는 전부 `UNDETERMINED` 로 간다.
   뜻이 probe 마다 다른 코드(ORA-00904 는 함수 부재일 수도, 컬럼 오타일 수도 있다)는
   전역에서 판정하지 않고 **그 뜻을 아는 probe 가 `PROBE_SPEC[...]['absent_codes']` 로 선언**한다.
   그리고 행 유무·값 유무·값 문법·양성 대조를 `query_ok` 하나로 뭉뚱그리지 않는다(typed predicate).
4. **묶이지 않은 확정값을 만들지 않는다.** 테이블·컬럼 단위 축은 `binding`
   (`db_identity`·`owner`·`object`)이 없으면 `UNDETERMINED` 다. 테이블 A 의 결과가 테이블 B 에
   적용되는 것을 형식이 막는다.

## A.4 `stale` 규칙 정정 (P1-02)

§6-5 는 "유효기간이 지난 capability 는 **이전 값을 유지**하되 `stale = true`" 라고 했다.
**안전성 판정에 만료된 고등급을 계속 쓰면 fail-closed 가 아니다.** 표시용 이전 값과 실행에 쓰는
값을 분리한다 — 축 레코드의 `value`(감사용 이전 값)와 `effective_value`(실행용)가 그것이며,
`stale=true` 면 `effective_value` 는 그 축의 floor 로 내려간다.

> **2026-08-30 — 이 정정이 코드에 실제로 들어갔다(8차 M3-3).** 그 전까지는 **규격에만 있었다** —
> 구현은 모든 축에 `effective_value = value` 를 넣었고 요약도 `value` 를 읽었다. 8차 §6 이 그것을
> 지적했다. 지금은 `g0_axes.apply_floors` 가 파생의 마지막 단계이고, floor 사유는 일곱이다 —
> `CHILD_NOT_MEASURED` · `UNBOUND` · `STALE` · `NO_FRESHNESS_BASIS` · `SOURCE_UNVERIFIED` ·
> `PROFILE_NOT_AUTHORITATIVE` · `COMPOSITE_INPUT_FLOORED`. 뜻은 `g0_axes.FLOOR_REASONS` 가 권위다.
>
> **floor 는 값을 내리기만 한다.** `UNDETERMINED` 는 더 내려갈 곳이 없으므로 그대로 둔다 —
> `sql_dialect` 의 floor 가 `11G` 라고 해서 `UNDETERMINED` 를 `11G` 로 만들면 그것은 floor 가
> 아니라 승격이다. 이미 floor 보다 약한 값도 그대로 둔다.
>
> **TTL 의 근거도 함께 적는다.** 현재 기본 30일은 **운영자 선언값**이며 측정 분포에서 나온 값이
> 아니다(레코드의 `freshness.basis = OPERATOR_DECLARED_TTL`). TTL 이 선언되지 않으면 신선도를
> 판정할 수 없으므로 **모든 확정값이 floor 로 내려간다** — 모르는 것을 신선하다고 가정하지 않는다.
> 실제 유효기간은 §8-1 의 원천 3~5개 분포를 본 뒤에 정한다.
