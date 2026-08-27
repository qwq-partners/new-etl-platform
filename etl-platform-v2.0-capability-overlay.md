# Capability 오버레이 규격 (v2.0 **초안 — 동결 불가**)

> **2026-08-27 7차 교차 리뷰 판정: 이 문서는 동결할 수 없다(NO-GO).**
> 방향(단일 privilege-zero core + 원천별 overlay)은 GO 이나, §3 의 7축이 **독립인 성질을 합치고**
> 조합 규칙이 없다. 아래 §3.1 이 그 분해를 기록한다. G0-0 실측 후 이 표를 대체한다.

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

## 5. 측정 단위는 두 층이다

| 층 | 축 | 왜 |
|---|---|---|
| **DB 단위** | `lag_visibility`, `sql_dialect`, `charset_class`, 버전 | 원천 DB 하나당 한 번 재면 된다 |
| **테이블 단위** | `snapshot_read`(FLASHBACK), `row_change_scn`(ROWDEPENDENCIES), `wm_granularity`(컬럼 타입) | 같은 DB 안에서도 테이블마다 다르다 |

Job 은 `(source_db, table)` 쌍의 capability 를 상속한다. **DB 단위 축이 좋아도 테이블 단위 축이 나쁘면 그 Job 은 나쁜 쪽을 따른다.**

---

## 6. 강등·드리프트 규칙

1. **fail-closed** — 정합성을 지킬 수 없는 조합은 publish 를 거부한다. (예: `wm_granularity = UNDEFINED` 인데 반개구간 fence 를 선언한 Job)
2. **degrade + disclose** — 지킬 수는 있으나 등급이 내려가는 경우, 거부하지 않고 계약 표시를 내린다. 사용자에게는 publish 전에 미리보기로 보여 준다.
3. **capability 는 값이 아니라 측정이다.** 모든 값에 `measured_at` 과 측정에 쓴 probe id 를 함께 저장한다.
4. **드리프트** — 재측정에서 값이 내려가면, 그 원천을 쓰는 ACTIVE Job 의 계약 표시를 갱신하고 경보한다. 값이 올라가는 것은 자동 반영하지 않는다(새 publish 에서만 반영). 올라간 등급을 자동 적용하면 **아무도 검토하지 않은 보증**이 생긴다.
5. **`stale`** — 유효기간이 지난 capability 는 이전 값을 유지하되 `stale = true` 로 표시한다. 값을 낙관적으로 올리지 않는다.

---

## 7. v1.2.3.1 에서 무엇을 대체하는가

| v1.2.3.1 | v2.0 | 이유 |
|---|---|---|
| `ZERO_GAP` / `BEST_EFFORT` 2등급 | 축별 등급 (§3 표) | 등급 하나가 여러 성질을 뭉뚱그렸다. 원천이 제각각이면 뭉치는 순간 못 쓴다 |
| `bound_kind = ENFORCED / OBSERVED` + `bound_evidence` | `lag_visibility` 축 | `ENFORCED` 의 유일한 근거인 `SYNC_COMMIT_GUARD` 가 DBA 장치 등록이라 도달 불가 |
| `cutoff` 3종(`APPLICATION_TIMESTAMP_WITH_OVERLAP` / `STANDBY_VISIBLE_SCN` / `CDC_OFFSET`) | `snapshot_read` + `wm_granularity` 축 | `STANDBY_VISIBLE_SCN` 은 `AS OF SCN` 전제라 `snapshot_read` 축에 흡수된다 |
| `zero_gap_verified`, `ZERO_GAP_INVALIDATED` 자동 HOLD | §6 드리프트 규칙 | 축이 여럿이면 단일 무효화 플래그가 성립하지 않는다 |

**아직 정하지 않은 것**: 위 대체가 `JobSpec` 입력 필드를 늘리는가. v1.2.3.1 의 설계 목표는 "JobSpec 입력 증가 0"이었고, 그 목표는 유지할 만하다 — 축은 **측정에서 파생**되므로 사용자가 입력할 것이 아니다. 다만 `fence_mode` 처럼 사용자가 고를 여지가 있는 축이 있는지는 G0-0 결과를 보고 정한다.

---

## 8. 다음에 확정할 것

1. **G0-0A 를 원천 3~5개에 돌려** 축 값의 실제 분포를 본다. 값이 다 같으면 오버레이가 과설계이고, 제각각이면 이 규격이 필요하다는 증거가 된다.
2. ~~`snapshot_read` 축에서 `AS_OF_SCN` 을 실제로 쓸 것인지 결정한다.~~ **결정됨(2026-08-27): 보류.**
   `FLASHBACK` 객체 권한만으로는 SCN 출처가 없어 `AS OF TIMESTAMP`(±3초 근삿값)뿐이고,
   `ORA-01466`·`ORA-08181` 이라는 새 실패 모드가 생기며, undo 부족의 Oracle 권고 대책이
   `primary 의 UNDO_RETENTION 상향`이라 **생산 primary 로 부하가 도달하는 채널**이 열린다.
   근거는 `etl-platform-v2.0-grant-request-verdict.md` §3.
   → 코어는 `READ_ONLY_TXN` 하나로 간다.
3. `row_hash = NONE` 인 원천이 실제로 있는지 확인한다. 있으면 Reconciliation 규격을 두 갈래로 써야 한다.
