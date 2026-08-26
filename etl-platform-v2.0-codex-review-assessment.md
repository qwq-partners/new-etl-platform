# Codex v2.0 교차 리뷰 검토서

- 검토일: 2026-08-25
- 검토 대상: `etl-platform-v2.0-codex-cross-review.md`(493줄, SHA-256 `888CC1D7…D808`) — fence 공격 13건 + 신규 결함 26건 + 사실표 23건 + 결정 4건 + §6 가치 + §7 필수 정정
- 대조: 내 제안서 `etl-platform-v2.0-unprivileged-redesign-scope.md`(S, 195줄) · `etl-platform-target-architecture-v1.2.3.1.md`(A, 1,738줄) · `etl-platform-poc-test-plan-v1.md` 8차(P, 649줄)
- 방법: Spark 4.2.0/3.5.9 **소스 직접 대조**(7항) + Oracle 19c 1차 출처(9항) → 결함 33건·결정 19건 판정 → 회의적 재검토로 확정
- 판정: **결함 33건 = 확인 16 · 부분 16 · 기각 1**, 결정·절 19건은 대체로 동의(개별 조정 있음)

---

## 1. 총평

**리뷰의 결정표에 전부 동의한다.** Profile U 방향 타당 / v2.0 동결 NO-GO / D1 GO / D2 조건부 / D3 v2.0 GO·**두 규범 fork NO-GO** / D4 조건부 / **G0-0을 A·P 개정보다 먼저**. 특히 마지막 순서 역전은 내 제안서 §11의 "개정과 병행 가능"이 틀렸다 — 실측 하나가 여러 절의 상태·enum을 뒤집는다.

**그리고 리뷰가 과소평가한 곳이 두 군데 있다.** 소스를 직접 읽으니 리뷰보다 나빴다.

1. **F-04 — 내 fence 공식은 v1.2.2에서 이미 고친 오류로의 회귀다.** A:1016은 `high = min(T_lb, SYSTIMESTAMP_standby) − safety_lag`이고 `T_lb`는 **primary 시계 witness**다. A:104·A:416이 "`fence_ts`는 standby wall-clock이라 하한으로 쓰지 않는다"를 명문화해 두었는데, **S:89가 그 `T_lb` 항을 통째로 삭제했다.** 남은 두 항은 순수 standby 시계와 `MAX(watermark)`뿐이다. standby 시계가 δ만큼 빠르면 그 구간의 미적용 row가 window에 들어가고 CAS가 넘어간다 → 조용한 영구 누락. **그리고 나는 그 삭제를 문서 어디에도 적지 않았다.** 리뷰의 P0는 오히려 과소다.
2. **NEW-04 — Spark 4.2.0에서는 lease 밖 세션이 리뷰가 말한 것보다 많다.** `grep -rn sessionInitStatement` 결과 Spark 4.2.0·3.5.9 전체에서 그 심볼은 `JDBCOptions`(파싱)와 `JDBCRDD.compute()` **단 한 곳**에만 있다. schema 경로(`JDBCRelation.getSchema` → `JDBCRDD.resolveTable` → `JdbcUtils.withConnection`)에는 **0회**다. 게다가 4.2.0의 `JDBCRDD.scanTable`은 `JDBCDatabaseMetadata.fromJDBCConnectionFactory`를 **eager 평가**해 driver 측 물리 connection을 **하나 더** 연다(3.5.9에는 없는 경로). 그 블록은 `catch NonFatal → logWarning`으로 **모든 예외를 삼키므로**, 거기에 preamble을 태워도 `RAISE_APPLICATION_ERROR`가 fail-closed로 작동하지 않는다.
   → v1 read 1회당 **preamble 없는 driver 측 물리 connection 최소 2개**, DSv2 pushdown이면 3개 이상. 이들은 `driver.connect`로 직접 열려 **Control lease를 전혀 거치지 않는다.** 500 burst면 **1,000~1,500 세션이 lease 밖**이다. 리뷰가 "budget 우회는 미확인"이라 한 부분은 **미확인이 아니라 확인**이다.

---

## 2. 소스로 확정한 사실

### 2.1 Spark (v4.2.0 · v3.5.9 대조)

| # | 사실 | 근거 |
|---|---|---|
| S1 | `sessionInitStatement`는 `prepareStatement(문자열 1개)` → `execute()` **정확히 1회**. 분할·`;` 파싱·loop 없음 | `JDBCRDD.scala` L328-339(4.2.0) / L266-277(3.5.9), 바이트 동일 |
| S2 | 따라서 세미콜론 나열은 Oracle에서 `ORA-00911`로 **전체 파스 오류**. Spark 공식 문서의 유일한 예시가 익명 블록이다 | `docs/sql-data-sources-jdbc.md` L220-225 |
| S3 | 익명 블록은 `EXECUTE IMMEDIATE` 순차 실행이라 **원자적이지 않다** — 중간 실패 시 앞 단계 `ALTER SESSION`은 적용된 채 남는다 | 동 |
| S4 | **schema/metadata 경로는 preamble을 실행하지 않는다** | 심볼이 `JDBCOptions`·`JDBCRDD.compute` 2곳뿐 |
| S5 | 4.2.0은 scan마다 `JDBCDatabaseMetadata` connection을 **추가로** 연다. 그 경로는 예외를 삼킨다 | `JDBCRDD.scala` L185, `JDBCDatabaseMetadata.scala` L83·L94-97 |
| S6 | task마다 **connection 1개를 열고 completion에서 닫는다.** 캐시·풀·재사용 자료구조가 **소스에 없다** | `JDBCRDD.scala` L278-322, `JdbcDialects.scala` L228-239 |
| S7 | `customSchema`는 `resolveTable`을 **무조건 먼저 호출한 뒤** overlay한다 — 원격 schema read를 없애지 못한다 | `JDBCRelation.scala` L245-252 |

### 2.2 Oracle — 리뷰가 내 제안서를 정정한 것 중 채택

- **`USER_RESOURCE_LIMITS`·`USER_PASSWORD_LIMITS`·`USER_USERS`는 무권한으로 읽힌다.** 내가 "profile 증거는 죽는다"고 단정한 것은 **틀렸다.** 다만 `LIMIT`은 리터럴 `'DEFAULT'`/`'UNLIMITED'`를 돌려줄 수 있고, 값이 있어도 **실제 강제(`RESOURCE_LIMIT=TRUE`)를 증명하지 못한다** → `ASSIGNED_LIMIT_EVIDENCE`로만 기록.
- **`SYNC WITH PRIMARY`는 호출 시점에 standby가 *이미 수신한* redo까지만** 기다린다 → `RECEIVED_REDO_APPLY_BARRIER`로 개명. 내 "완전성 배리어" 표현은 과장이었다.
- `V$`/`DBA_*`의 "전면 사망"도 과했다 — **기본 설치에서 막힐 뿐 direct grant 가능성을 사전 단정할 수 없다.** 실측 전에 없다고 쓰면 안 된다.
- 오류 코드 정규 표기는 `ORA-03172`·`ORA-03173`.

---

## 3. fence 13건 판정

| ID | 판정 | 요지 |
|---|---|---|
| F-01 tail trap | **부분(P1)** | `low`가 **포함** 경계이므로 기본형은 영구 누락이 아니라 **1회차 지연**. 단 `MAX`가 정지하면(동률 반복·최종 batch 뒤 유휴) **영구가 되고**, 그때 `high ≤ low` → `FINALIZED_NO_DATA`가 "데이터 없음"과 구분되지 않아 조용하다 |
| F-02 late commit | **확인(P0)** | 예방 불가. DBA/CDC/commit-watermark bound 없이는 닫히지 않는다 |
| F-03 bounded Audit 공백 | **확인(P0)** | S:144 "빠진 것을 찾아내 메우는 보장이 있습니다"는 **거짓** |
| F-04 clock domain | **확인(P0, 리뷰보다 무겁다)** | §1의 `T_lb` 삭제 회귀 |
| F-05 empty/NULL | **부분** | empty bootstrap=P1(즉시 드러남) / **NULL watermark row 영구 제외=P0**(적재·Audit 양쪽에서 사라지는 유일 클래스, 리뷰는 등급을 안 매겼다) |
| F-06 두 watermark | **부분** | Merge의 `INSERT_DT OR UPDATE_DT`에 단일 `MAX`는 부족 |
| F-07 witness query | **확인(논거 교체)** | `t0` 자체가 삭제 대상 |
| F-08 same-session 미구현 | **확인(P0)** | Control probe 결과를 executor에 상속시킬 수 없다 |
| F-09 N snapshot | **부분** | 확인하되 partition key 가변성 조건 명시 필요 |
| F-10 SYNC 과장 | **부분** | 개명은 채택. 근거는 교체(`ALTER SESSION`은 암시적 commit이 없다) |
| F-11 D ≠ freshness | **확인(P1)** | S:141 "원천 대비 최대 300초"는 **admission predicate**이지 publication freshness가 아니다 |
| F-12 probe 부하 | **부분(P2→P1 상향)** | 부하가 아니라 **Guard 트랜잭션 임계경로 + Control 모니터 세션 미계수**가 문제 |
| F-13 유휴 정지 | **부분** | `NO_SOURCE_PROGRESS` 결과·backoff 필요 |

**리뷰의 `(watermark, stable_pk)` keyset cursor 대안은 기각한다** — A의 `numrange` window·GiST exclusion·`ceil((high−low)/max_chunk_span)` chunk 산식·literal pushdown pruning을 전부 다시 써야 하는데, 아래 한 줄이 같은 결함을 그것들을 하나도 건드리지 않고 닫는다.

---

## 4. 세 묶음으로 수렴한다

리뷰의 26건은 사실상 **세 개의 뿌리**다. 각각을 하나의 조치로 닫는 것이 핵심이다.

### 4.1 fence — 한 식으로 F-01·F-04·F-13·NEW-01·NEW-08을 동시에 닫는다

새 상수를 발명하지 않고 **A의 기존 필드 `fence_time_witness`가 Profile U에서 어떤 값으로 축퇴하는지**만 정의한다.

```
fence_time_witness = SCN_TO_TIMESTAMP  (G0-0 ⓑ에서 ADG 가용 확인 시)
    high = min( T_lb , MAX(wm) + ulp(type) )        -- standby 시계는 하향 캡으로만
fence_time_witness = NONE
    high = MAX(wm) + ulp(type)                      -- 시각 항 완전 제거 → clock domain 문제 소멸
```

- `ulp`는 선언 타입의 최소 표현 단위이며 **무권한으로 읽는 `ALL_TAB_COLUMNS`에서 파생**한다(DATE→1s, `TIMESTAMP(n)`→10⁻ⁿs, `NUMBER(p,s)`→10⁻ˢ). 리뷰의 "임의 `+epsilon`은 안전하지 않다"는 **임의값에 대해서만** 옳다.
- **대가를 같은 자리에 적는다**: ulp seal은 동률 late commit을 `overlap`으로 이관하는데, overlap의 충분조건 좌변은 S:119 U-2가 **"상한이 아니라 관측 최댓값"이라고 스스로 증명 불가로 선언한 값**이다. "overlap이 이미 담당한다"는 말은 과장이며, 정확히는 "담당 주체가 옮겨가고 그 충분성은 Profile U에서 증명되지 않는다".
- 미래일자 오염 row는 cutoff가 아니라 **필터**로만 배제하고 `FUTURE_DATED_OUTLIER_DEFERRED`로 계수·경보한다(누락이 아니라 적재 지연이 되도록).

### 4.2 Spark — 구현물 **하나**로 수렴한다

NEW-03·04·05·10·18·19는 전부 같은 곳을 가리킨다. **커스텀 `JdbcConnectionProvider`**(`connectionProvider` 옵션)가 `JdbcDialects.createConnectionFactory` 경로를 통과하므로 **schema·metadata·task 3경로를 stock Spark 패치 없이 전부 덮는** 유일한 수단이다. 요구사항: preamble 단일 블록 실행 · **순서 5→4**(`SET TRANSACTION READ ONLY`는 트랜잭션의 첫 문장이어야 하므로 `SYNC WITH PRIMARY`를 그 뒤에 두면 ORA-01453 — **내 S:74-75의 4→5 순서가 틀렸다**) · 전용 timeout · autoCommit 제어 · 버전 pin.

> **이것을 5~6개 작업으로 쪼개면 각각이 부분 해법이 되어 전부 실패한다.** 하나의 산출물로 계획해야 한다.
> 잔여: `JDBCDatabaseMetadata`의 예외 흡수(fail-open) 한 경로는 이 구현으로도 닫히지 않는다 → 리스크 대장 등재.
> 부수 효과: `prepareQuery`는 schema·data 양쪽에 prepend되지만 **같은 statement의 접두사**라 `ALTER SESSION`을 실을 수 없다 — 누군가 반드시 시도하므로 **규격에 명시적으로 부정 기재**한다.

### 4.3 탐지 — 세 축으로 쪼개면 리뷰의 파국 시나리오가 사라진다

NEW-02·15·24는 한 뿌리다: **유입(window)·탐지(Audit)·삭제 대조(`PK_RECONCILE`)가 전부 watermark 컬럼으로 색인돼 있다.** watermark를 우회하는 두 사건(늦은 commit·hard delete)은 정의상 세 경로에서 **동시에** 사라진다. 어떤 문구 수정으로도 복구되지 않는다 — 탐지 모집단 중 **하나 이상을 다른 축으로 색인**해야 한다.

다만 리뷰의 처방(전 컬럼 hash census)은 과잉이고, 그것이 리뷰 자신의 NEW-24(Audit이 가장 큰 부하가 된다) 파국을 만든다. 셋으로 쪼개면 두 자릿수 싸다.

| 사건 | 색인 축 | 비용 |
|---|---|---|
| late commit | `ORA_ROWSCN` 스윕 | 낮음(ADG 가부는 G0-0 ⓑ15) |
| hard delete | **PK-only 전수 anti-join** | 중간(값 비교 없음) |
| 값 오염 | 표본 해시 | 조절 가능 |

---

## 5. 리뷰가 틀리거나 과장한 곳 — A·P에 반영 금지

사실표 9의 오귀속 · 사실표 5의 표기 역전 · §7.2의 패스워드 한도 `UNKNOWN` 처리 · NEW-22의 `ALL_LOBS`→Flashback 과장 · NEW-21 clone 갈래(S:106·124·146에 **3중으로 이미 명시**돼 있다) · NEW-24 "미산정" 진단(A:1124·1127·1581이 반증하며 인과도 역전) · `(watermark, stable_pk)` keyset cursor · D2-4의 `SYNC` 삭제 · `PER_CONNECTION` 유령 enum.

---

## 6. G0-0 프로브에 반영한 것

전달한 프로브(`g0-0-probe.sql` 67 probe · `g0-0-probe-spark.py` · README)에 판정이 요구한 항목을 채웠다.

- `USER_USERS.ACCOUNT_STATUS` — `LOCKED(TIMED)` 관측 가능성 = credential breaker의 **관측 입력**
- `USER_RESOURCE_LIMITS`의 **리터럴 `DEFAULT` 반환 여부**를 별도 probe로(숫자로 해석 안 되면 할당값조차 미확정)
- **watermark 컬럼 타입 사실**(`DATA_TYPE`·`DATA_SCALE`·`DATA_PRECISION`·`NULLABLE`·`CHAR_LENGTH`) — §4.1의 `ulp` 파생과 NULL 정책의 근거
- `SET TRANSACTION READ ONLY` **재실행 → ORA-01453** 확인(트랜잭션이 실제로 열려 있었다는 양성 증거)
- 데이터 사실 3종(`rows_at_max_wm`·`null_wm_rows`·`future_wm_rows`)으로 F-01·F-04·F-05를 **이 테이블의 사실**로 전환

---

## 7. 다음 단계

1. **G0-0 실행** — 문서 개정보다 먼저. 특히 두 줄이 분기점이다: 대상 테이블의 `FLASHBACK` 객체 권한, `versions.lock`의 Spark 버전(4.2.0 이상/미만에 따라 lease 밖 connection 수와 NEW-10의 구멍 범위가 갈린다).
2. **커스텀 `JdbcConnectionProvider` spike** — §4.2. G0-0과 병행 가능하며, 이것이 성립하지 않으면 Profile U의 세션 단언 모델 전체가 성립하지 않는다.
3. 그 결과로 capability 목록을 확정한 뒤 **A v2.0 / P v2.0**(단일 core + ConnectionRevision capability overlay, v1.2.3.1은 archive) 개정.
