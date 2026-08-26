# ETL Platform — 무권한(DBA 비협조) 재설계 범위 제안

- 작성일: 2026-08-24
- 대상: `etl-platform-target-architecture-v1.2.3.1.md`(A, 1,739줄) · `etl-platform-poc-test-plan-v1.md` 8차(P, 650줄)
- 계기: **Oracle 원천 DBA 협조를 받을 수 없다**(사용자 확정). 현 설계는 DBA를 깊게 전제하므로 보장 등급·fence·검증 oracle을 다시 짜야 한다.
- 방법: Oracle 19c 공식 문서로 "권한 없는 읽기 전용 계정이 실제로 할 수 있는 것"을 23항 확정 → A·P의 DBA 의존 요소 전수 인벤토리 → 대체 설계·등급 재정의.
- 이 문서의 지위: **범위 제안이며 규범이 아니다.** §9의 결정 4건을 승인받은 뒤 A·P를 개정한다.

---

## 1. 결론과 결정 요청

**한 줄: 이것은 같은 설계의 하향이 아니라 다른 제어 모델이다.** 현행은 *플랫폼이 원천 지표를 읽고 판단한다*(read-then-decide)였고, 새 것은 *세션이 제약을 선언하고 서버가 거부한다*(assert-and-let-the-server-refuse)다. 후자는 **freshness·role·identity 세 축에서 오히려 더 강하고**, 완전성·부하 제어·증거 세 축에서 확실히 약하다. 그 비대칭을 문서가 정직하게 적어야 나머지가 신뢰를 얻는다.

**결정 요청**

| # | 결정 | 권고 | 영향 |
|---|---|---|---|
| D1 | `ZERO_GAP` 등급을 **삭제**한다 | **삭제** | 세 갈래(`SYNC_COMMIT_GUARD`·`STANDBY_VISIBLE_SCN`·C1)가 **독립적으로** 막혀 조건부 존치는 반증 불가능한 참을 하나 더 만든다. 복원 조건은 부록 R에 비선택 값으로 보존 |
| D2 | 보증 축을 **5축**으로 재편(§6) | 채택 | DB가 강제하는 것(ⓐ)·플랫폼이 탐지·복구하는 것(ⓑ)·아무도 보장하지 않고 공시만 하는 것(ⓒ)을 한 이름에 섞지 않는다 |
| D3 | 버전을 **v2.0**으로, v1.2.3.1은 **Profile O로 보존** | 채택 | enum 값 삭제·게이트 폐지는 소비자 계약과 PoC 판정식을 동시에 깨는 breaking change다. DBA 협조가 생기면 Profile O로 되돌아간다 |
| D4 | 부록 W(협상용 5건)를 **문서에 남기되 전제로 쓰지 않는다** | 채택 | 본문 어느 절도 W를 참조하지 않는다. 참조가 생기는 순간 전제가 되고, 그것이 v1.2.3.1이 무너진 방식이다 |

---

## 2. 확정한 사실 — 권한 없는 계정으로 무엇이 되는가

전부 Oracle 19c 1차 출처 확인. "아마 될 것"은 §8 미확인 목록으로 분리했다.

### 2.1 살아남는 것 (무권한)

| 수단 | 근거 | 쓰임 |
|---|---|---|
| `SYS_CONTEXT('USERENV', …)` 14속성 — `DATABASE_ROLE`·`DB_UNIQUE_NAME`·`CON_NAME`·`CON_ID`·`SERVICE_NAME`·`SERVER_HOST`·`INSTANCE_NAME`·`ISDBA`·`IS_DG_ROLLING_UPGRADE` 등 | SQL Language Reference — 권한 전제 없음, 속성별 게이트 없음 | **identity·role fence 전체를 대체**. `V$DATABASE`·`V$CONTAINERS` 불요, `etl_assert_standby()` PL/SQL 불요 |
| **`ALTER SESSION SET STANDBY_MAX_DATA_DELAY = N`** | SQL Ref(ALTER SESSION 전제: "You do not need any privileges to perform the other operations") + DG Concepts ch.10 — 초과 시 **ORA-3172** | **DB가 강제하는 fail-closed staleness 한도.** 현행 `V$DATAGUARD_STATS` read-then-act 경쟁을 제거 |
| **`ALTER SESSION SYNC WITH PRIMARY`** | SQL Ref — "blocks until redo apply has applied all redo data received by the standby at the time the statement is issued", 미충족 시 **ORA-3173** | **완전성 배리어**. 삭제된 `SYNC_COMMIT_GUARD`의 무권한 근사치(단, SYNC transport·SYNCHRONIZED·real-time apply가 전제) |
| **`SET TRANSACTION READ ONLY`** | SQL Ref — 권한 전제 없음, "transaction-level read consistency… see only changes committed before the transaction began" | **다중 문장·다중 테이블 일관 읽기**. `AS OF SCN`의 무권한 대체 |
| `STANDARD_HASH(expr, 'SHA256')` + `RAWTOHEX`/`HEXTORAW` | SQL Ref — 내장 함수, 권한 전제 없음 | canonical hash를 **플랫폼 생성 SQL 텍스트**로 서버 측 산출. `ETL_CANON` PL/SQL 불요, `UTL_RAW`도 불요 |
| `ORA_ROWSCN`·`SCN_TO_TIMESTAMP` | SQL Ref | `commit − watermark` **표본 수집**(상한이 아니라 관측치) |
| `ALL_*`·`USER_*` 뷰 | Reference — "USER_ 뷰는 특별 권한 불요", ALL_은 접근 가능 객체 범위 | schema drift·자기 권한 감사 |
| `DBMS_APPLICATION_INFO`·`DBMS_SESSION.SET_IDENTIFIER` | PL/SQL Packages | 세션 태깅(단, **읽어서 대조할 `GV$SESSION`이 없다**) |

### 2.2 죽는 것

| 죽는 요소 | 사유(1차 출처) |
|---|---|
| `V$`/`GV$` 전부 — `V$DATABASE`·`V$CONTAINERS`·`V$TRANSACTION`·`V$DATAGUARD_STATS`·`GV$SESSION` | Reference: "only user SYS or anyone with SYSDBA privilege has access to the dynamic performance tables". CDB에서는 grant 후에도 `ALTER USER … CONTAINER_DATA` 추가 필요 |
| `DBA_*` 전부 — `DBA_2PC_PENDING`(C1)·profile·unified audit·`DBA_PROFILES` | Reference: "queried only by users with SYSDBA … SELECT ANY DICTIONARY … SELECT_CATALOG_ROLE" |
| **`AS OF SCN` / `AS OF TIMESTAMP`** | Development Guide §20.2.5: "grant FLASHBACK and either READ or SELECT privileges on those objects" — **SELECT만으로는 불가** |
| `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER` | 같은 절: "grant the EXECUTE privilege on DBMS_FLASHBACK" |
| `DBMS_CRYPTO`(LOB 해시) | PUBLIC EXECUTE 아님 |
| `ETL_HEARTBEAT` 테이블·갱신 job / 트랜잭션 나이 kill job / `SYNC_COMMIT_GUARD` / `etl_assert_standby()` / `ETL_CANON` | 전부 원천 **DDL + 스케줄러 작업** |
| profile 증거(`RESOURCE_LIMIT`·`SESSIONS_PER_USER`·`FAILED_LOGIN_ATTEMPTS`·`PASSWORD_ROLLOVER_TIME`) / undo·LOB RETENTION | 관리자 DDL, 조회도 `DBA_*` |
| **원천 측 admission control 일체** | 무권한 자기 제한 수단이 존재하지 않음(Resource Manager는 `ADMINISTER_RESOURCE_MANAGER` 필요) |
| DG 시험 인스턴스 + 장애 주입 → **G2 게이트 전체** | 운영 협조 |

---

## 3. 새 제어 모델

```
[현행]  플랫폼이 V$ 지표를 읽는다 → 플랫폼이 판단한다 → 진행/거부       (read-then-decide)
        └ 경쟁 구조이고, 이제는 읽기 자체가 불가능하다

[v2.0]  세션이 접속 직후 제약을 선언한다 → 서버가 위반 시 거부한다 → 회차 실패  (assert-and-refuse)
        └ 플랫폼 버그로 우회되지 않는다. 증거는 "서버가 발신한 오류 코드"다
```

**세션 프리앰블**(모든 추출·감사 connection의 `sessionInitStatement`, 실패 = connection 실패):

1. `ALTER SESSION SET STANDBY_MAX_DATA_DELAY = <D>` — staleness 한도 선언(초과 시 ORA-3172)
2. `SYS_CONTEXT` 단언 익명 블록 — `DATABASE_ROLE = 'PHYSICAL STANDBY'` ∧ `DB_UNIQUE_NAME`·`CON_NAME`·`SERVICE_NAME` 일치 ∧ `ISDBA = 'FALSE'` ∧ `IS_DG_ROLLING_UPGRADE <> 'TRUE'`, 불일치면 `RAISE_APPLICATION_ERROR`
3. NLS 고정 — `NLS_NUMERIC_CHARACTERS`·`NLS_DATE_FORMAT`·`NLS_TIMESTAMP_FORMAT`·`NLS_TIMESTAMP_TZ_FORMAT`·`TIME_ZONE='+00:00'`(canonical hash 정합 요건)
4. (해당 Job) `SET TRANSACTION READ ONLY` — 트랜잭션 스냅샷
5. (배치 경계 Job) `ALTER SESSION SYNC WITH PRIMARY` — 완전성 배리어

**증거 등급 재정의**: **1급 = 서버 발신 오류 코드와 그 부재**(ORA-3172/3173/1555/1017/28000/1722), **2급 = 플랫폼 자기 계측**. 1급 증거가 존재할 수 없는 항목(`session_limit_evidence` 등)은 "증거 없음"으로 표기하고 판정에서 뺀다.

---

## 4. 대체 설계 요지

### 4.1 fence / window 상한

세 후보를 비교한 결과 **(ii)를 집행기, (iii)을 시각 원점, (i)을 하향 전용 캡**으로 조합한다.

```
t0   = SELECT SYSTIMESTAMP FROM <대상 테이블> WHERE ROWNUM = 1   -- DUAL이 아니라 데이터 블록을 읽는 쿼리
high = min( t0 − D − safety_lag ,  MAX(watermark_column) )       -- 두 항 모두 하향 전용
```

- `D`는 프리앰블 1이 선언한 값이며, 같은 세션이 `t0`를 읽는 순간 apply lag ≤ D가 **DB에 의해 보장**된다.
- `MAX(watermark)`는 **high를 낮추는 방향으로만** 참여시킨다 — 미래 일자 오염 row가 high를 앞으로 밀어 조용한 누락을 만드는 경로가 사라진다. 인덱스 있는 컬럼에서만 활성화하고, 원천 유휴 시 정체는 **알림**으로 다루되 window를 강제 전진시키지 않는다.
- `safety_lag`는 DBA 승인값이 아니라 **근거 없는 플랫폼 보수 상수**임을 문서에 그대로 적는다.

### 4.2 읽기 일관성 — 병렬도와의 교환이 새로 생긴다

`SET TRANSACTION READ ONLY`는 **단일 세션** 트랜잭션 스냅샷이다. Spark JDBC `numPartitions > 1`이면 세션이 N개이고 스냅샷도 N개이며 시작 시점이 각각 다르다. FLASHBACK 없이 이 N개를 한 시점으로 묶을 방법은 없다.

> **`snapshot_scope`**: `TRANSACTION_SNAPSHOT`(단일 세션) / `PER_STATEMENT`(병렬) — **JobSpec Wizard가 이 교환을 운영자에게 보여야 한다.** 현행 A에는 이 교환이 없다(`AS OF SCN`이 공짜로 해결해 주고 있었다).

### 4.3 나머지

| 요소 | 대체 |
|---|---|
| identity | 이름 tuple 확장(`DB_NAME`·`DB_UNIQUE_NAME`·`DB_DOMAIN`·`CDB_NAME`·`CON_NAME`·`CON_ID`·`SERVICE_NAME`·`INSTANCE_NAME`·`SERVER_HOST`). **`DBID`·`CON_UID`·`GUID`·`RESETLOGS_CHANGE#`가 USERENV에 없으므로 이름을 재사용한 clone은 구분 불가** — A 6.1의 "clone은 어떤 경로로도 통과하지 못한다"는 **삭제해야 하는 문장**이다 |
| role | `SYS_CONTEXT DATABASE_ROLE` 단언 + **chunk 경계마다 재단언**. `SNAPSHOT STANDBY`까지 구분되어 현행보다 강하다 |
| 세션 한도 | Control lease + JDBC 풀만. **원천 집행 0** — "원천이 ETL 부하로부터 보호된다"는 주장을 **삭제**하고 `source_admission_control = SELF_LIMITED_ONLY`로 공시 |
| canonical hash | canonicalization을 **플랫폼 생성 SQL 텍스트**로 옮기고 `STANDARD_HASH(…,'SHA256')`로 산출. LOB·객체 타입만 서버 측 해시 불가 → **완전성 oracle(PK 차집합)과 내용 oracle(행 해시)을 분리**해 LOB 보유 Job도 완전성 축은 유지(현행 "LOB 보유 Job은 표본 탈락"은 과잉 폐기) |
| credential 안전 | `FAILED_LOGIN_ATTEMPTS`를 모르므로 breaker 임계를 **매우 낮게(2회)** 고정하고 잔여 위험을 리스크 대장에 등재. 잠기면 10k Job이 전부 정지한다 |

---

## 5. 증명할 수 없게 되는 명제

| # | 증명 불가 | 대체 관측 | 한계 |
|---|---|---|---|
| U-1 | "apply lag가 X초 이내였다" | `STANDBY_MAX_DATA_DELAY` 선언 + ORA-3172 미발생 | **값이 아니라 술어만.** 게다가 *미발생은 fence가 걸렸다는 증거가 아니다* → **양성 대조(작은 값으로 ORA-3172 1회 유발) 필수** |
| U-2 | "commit − watermark ≤ B" | `SCN_TO_TIMESTAMP(ORA_ROWSCN) − UPDATE_DT` 표본 | 블록 단위라 **과대**(보수 방향), 상한이 아니라 **관측 최댓값** |
| U-3 | "세션 절대 한도를 안 넘었다" | 풀 계수 + 태깅 | 플랫폼이 만든 세션만 센다. **한도 자체가 자기 선언값** |
| U-4 | "in-doubt 분산 트랜잭션 없음(C1)" | **없음** | 관측·반증 모두 불가 → "알려진 미관측 위험"으로 이동 |
| U-6 | "`AS OF SCN` truth와 일치" | 안정 구간 비교(`UPDATE_DT < 감사시각 − S`) | 과거로 되감을 수 없어 **스냅샷 비교 → 안정 구간 비교로 강등** |
| U-7 | "잔존 세션 0이므로 `RELEASED`" | 커넥션 close + 유예 | 문제의 본질이 "close했는데 서버에 남는 경우"라 클라이언트 관측이 정의상 못 본다 → **관측 기반 → 시간 기반 추정** |
| U-9 | "clone/재구축이 아니다" | 이름 tuple | **이름을 재사용한 복제본은 구분 불가** |

(전체 11건은 개정판 부록 U에 수록)

---

## 6. 보증 축 재정의 (D2)

| 축 | 값 | 성질 |
|---|---|---|
| **`source_staleness`** (신설) | `SYNC_BARRIER` / `DB_ENFORCED_MAX_DELAY(N)` / `UNBOUNDED` | **DB 강제** — 현행보다 **강함** |
| **`snapshot_scope`** (신설) | `TRANSACTION_SNAPSHOT` / `PER_STATEMENT` | **DB 강제** |
| **`upsert_consistency`** (값 교체) | `DETECT_AND_REPAIR` / `BEST_EFFORT` — **`ZERO_GAP` 삭제** | 플랫폼 탐지·복구 |
| **`delete_consistency`** + `delete_lag_slo_seconds` | `SYNC` / `BOUNDED_LAG` / `NONE` — 변경 없음 | 플랫폼 탐지·복구 |
| **`source_admission_control`** (신설, 공시 전용) | `SELF_LIMITED_ONLY` 고정 | **공시** |

**소비자 노출 문구(초안)**

> **정합성: 탐지·복구형 · 신선도: 원천 대비 최대 300초(DB 강제) · 삭제 반영: 최대 6시간**
>
> 이 테이블의 변경분이 **빠짐없이 들어온다는 보장은 없습니다. 빠진 것을 찾아내 메우는 보장이 있습니다.** 신선도 한도는 플랫폼이 측정해 판단한 값이 아니라 세션이 Oracle에 선언한 값이며, 초과 시 **Oracle이 쿼리를 거부(ORA-3172)해 회차가 실패**합니다.
>
> **이 표시가 뜻하지 않는 것**: 원천 DB는 이 파이프라인의 읽기 부하를 제한하지 않습니다 · 플랫폼은 실제 지연 수치를 볼 수 없습니다 · 이름이 같은 복제본으로 교체된 경우를 구분하지 못합니다 · LOB 등 일부 컬럼은 내용 대조 대상이 아닙니다(행 존재 여부는 대조합니다).

---

## 7. 게이트

- **G0**에 **G0-0 「계정 권한 실측」 신설(최우선)** — 현행 G0에는 계정 권한 확인 항목이 아예 없다. **`FLASHBACK` 객체 권한과 `DBMS_FLASHBACK` EXECUTE를 이미 갖고 있는지**가 문서 전체의 분기점이다("SELECT만"은 진술이지 실측이 아니다).
- **G2(Oracle ZERO_GAP Go) → G2′(Oracle Read-Path Go)**, 판정 단위 Source별 → **ConnectionRevision별**. 산출 필드 `zero_gap_verified` → **`read_path_verified`**(등급을 올리는 필드가 아니라 접속 경로 성립 확인). 6항: 권한 실측 / **ORA-3172 양성 대조(실패 시 FAIL)** / mid-query 평가 관측(미확정도 결과) / read-only 트랜잭션 수명·ORA-01555 / canonical hash 실 Oracle 벡터 / identity·role 단언 대조.
- **G1**은 실질 영향 없음. 단 "이 PoC가 증명하는 것은 **플랫폼 제어면이고 원천 보장이 아니다**"를 §1.1에 명시하고, 풀이 유일한 choke point임을 시험하는 SC 1건을 신설한다.
- **즉시 No-Go 개정**: 4번(세션 절대 한도) → "플랫폼 풀 선언 상한 초과 ≥ 1"로 재정의(자체 계측으로 판정 가능·양성 증거 가능) / 6번(watermark gap, ZERO_GAP) → 모집단 공집합이므로 **삭제하고 "감사 미탐지 누락 ≥ 1"로 대체**(target 측 인공 결손 주입으로 양성 증거 확보) / 5번(primary fallback) → 검출 근거만 `SYS_CONTEXT`로 교체.

---

## 8. 새로 생기는 미확인 목록 (G0-0에서 확정)

⒜ ADG standby에서 `SET TRANSACTION READ ONLY` 가부와 최대 수명 · ⒝ `STANDBY_MAX_DATA_DELAY`의 **mid-query 평가 여부**(문서에 기술 없음 — 설계는 보수적으로 **시작 시점 평가만 가정**) · ⒞ primary 세션에서의 무시 여부 · ⒟ ADG에서 `ORA_ROWSCN`·`SCN_TO_TIMESTAMP` 가부 · ⒠ `UTL_RAW`·`SESSION_PRIVS`·`NLS_DATABASE_PARAMETERS`·`ALL_TAB_COLUMNS`·`DECOMPOSE`/`COMPOSE`의 무권한 접근 · ⒡ **현행 계정이 이미 가진 권한**(가장 먼저 확인할 두 줄: `FLASHBACK` 객체 권한, `DBMS_FLASHBACK` EXECUTE).

---

## 9. 문서 개정 범위

**버전(D3)**: **A v2.0 / P v2.0**(P를 "9차"가 아니라 v2.0으로 짝지어 올린다). v1.2.3.1은 **Profile O(Oracle-cooperative)**로 동결 보존, 신규는 **Profile U(Unprivileged)**.

**A 개정 대상**: 6.1(SourceCapability 대폭 교체·`db_identity` 재정의·무효화 규칙 개명) · 6.2 · 7.1 · 7.2(cutoff에서 `STANDBY_VISIBLE_SCN` 삭제, `ZERO_GAP` 삭제) · 7.3 · 10.2(Guard 6·8번 재작성, `STALENESS_BOUND_REJECTED` 신설) · **11.1(4계층 → 3계층, 최상위 DB 집행 계층 삭제)** · 11.2 · **11.3 전면 재작성(「Session Assertion Fence와 staleness 한도」)** · 11.4(`AS OF SCN` → `SET TRANSACTION READ ONLY`) · 11.5 · 12.2 · 12.3(비교 A를 안정 구간 비교로, 완전성/내용 oracle 분리) · 13.4 · 16.2·16.4 · **17장(rule 2종 삭제·`computed_minimum` 재정의)** · 19장 · 20장 · 21장 · **22장(2·3·4·9·11·16·18·23번 삭제 또는 부록 W 이동)** · 부록 W·R·U 신설.

**P 개정 대상**: §1.1~1.3 · §2.1(**G0-0 신설**) · §2.2(DG 시험 인스턴스 삭제) · §2.3(stub이 재현할 대상이 **V$ 뷰가 아니라 오류 코드**로 바뀐다) · §2.4·2.5 · §3.1·3.2 · §3.4(**삭제** FI-41g·41e·44·44e / **신설** FI-52 ORA-3172 양성 대조·FI-53 SYNC WITH PRIMARY·FI-54 read-only 수명·FI-55 target 인공 결손 → 감사·repair·FI-56 풀 상한 드리프트) · §5 · §5.1(4층 → 3.5층) · §6 · §7 · §8.1 · §8.3 · §8.8.

---

## 10. 부록 W — 협상용(설계 전제 아님)

> 본문 어느 절도 이 부록을 참조하지 않는다. 설계는 **다섯 가지가 전부 거부되어도 그대로 성립**하며 그 상태가 기준선이다.

| 순위 | 요청 | 성격 | 되살리는 것 |
|---|---|---|---|
| 1 | `GRANT FLASHBACK ON <owner>.<table> TO <etl_user>;`(열거된 테이블만) | 권한 | `AS OF TIMESTAMP` → **다중 세션 공통 시점** → 병렬도·일관성 교환 해소, 비교 A가 시점 비교로 복귀. **단 `ZERO_GAP`은 돌아오지 않는다** |
| 2 | ETL 계정 profile 3값 **통보**(`SESSIONS_PER_USER`·`RESOURCE_LIMIT`·`FAILED_LOGIN_ATTEMPTS`) | **정보 통보** | `pool_cap < 절대 한도` 불변식의 우변, breaker 상한식의 우변 — **계정 잠금 예방**(잠기면 10k Job 정지) |
| 3 | standby DG 구성 사실 **통보**(SYNC transport 여부·보호 모드·real-time apply) | **정보 통보** | `SYNC WITH PRIMARY` 사용 가부 → 마감 배치에 `SYNC_BARRIER` |
| 4 | `GRANT EXECUTE ON SYS.DBMS_FLASHBACK` | 권한 | 1번과 결합해야 실효(단독 가치 낮음) |
| 5 | 세션 태그별 동시 최대값 주 1회 회신 | **정보 통보(단방향)** | U-3의 유일한 외부 검증 |

**딱 셋이면 1·2·3.** 2·3은 권한이 아니라 값 통보라 조직적으로 통과 가능성이 가장 높고 예방 가치가 크다.

---

## 11. 다음 단계

1. **D1~D4 승인** → A v2.0 / P v2.0 개정 착수(현행 빌드 파이프라인 그대로: 그룹 병렬 드래프트 → 판정 → 앵커 빌드 → 다렌즈 검증).
2. 개정과 **병행 가능**: G0-0 프로브 스크립트(권한 실측 20여 항) — 실 계정으로 1회 실행하면 §8 미확인 대부분이 확정되고, 그 결과가 v2.0의 여러 분기를 없앤다. **이것이 지금 가장 값싼 확실성**이다.
3. 그 뒤 G0 executable appendix → G1 → G2′.
