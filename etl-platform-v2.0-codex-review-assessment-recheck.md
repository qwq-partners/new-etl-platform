# ETL Platform v2.0 검토서 재검증

- 검토일: 2026-08-25
- 대상: `etl-platform-v2.0-codex-review-assessment.md` 131줄, SHA-256 `00B42E2465E03018A02B3F781157A7E9372F171E048846A5DA095B7A756F9BF5`
- 함께 검증한 실행물:
  - `g0-0-probe.sql` 253줄, SHA-256 `E9A1CB00D353BC6AA3224A2D7C4B88E9F62F1085374F42BAA7729A307CBDFD64`
  - `g0-0-probe-spark.py` 208줄, SHA-256 `67F1D31084F362E6068B687CEEE8EAD54A1D28DA5634CABDC27E0EAF1AFAF926`
  - `g0-0-probe-README.md` 114줄, SHA-256 `9F0EE7375AF781CF39B40061AE5298BB8C500A56DB139C406305AD029E467F85`
- 방법: A/S/P 문서 교차 대조, Oracle 19c·Spark 4.2.0 1차 출처 검증, fence 반례 구성, Spark connection 생명주기 산정, 탐지 coverage matrix, probe 정적 검사

---

## 0. 최종 판정

**검토서 전체는 PARTIAL ACCEPT다.** 큰 방향은 옳지만, 아래 세 주장을 현재 형태로 A v2.0/P v2.0에 옮기면 안 된다.

| 쟁점 | 판정 | 정확한 결론 |
|---|---|---|
| Profile U 방향·D1 GO | **동의** | 권한 실측 후 capability overlay를 확정하는 방향이 타당하다 |
| v2.0 동결 | **NO-GO 유지** | fence·Spark session assertion·탐지 보증이 아직 닫히지 않았다 |
| G0-0 선행 | **조건부 동의** | 문서 개정보다 먼저 해야 하지만, 현재 probe 실행은 **NO-GO**다. 먼저 probe를 수정·검증해야 한다 |
| Profile O/U 이중 규범 | **NO-GO 동의** | 단일 core와 ConnectionRevision capability overlay가 맞다 |
| fence가 A에서 회귀했다 | **동의** | S가 heartbeat 기반 `T_lb`를 제거한 의미를 충분히 공시하지 않았다 |
| 새 fence 식이 5건을 닫는다 | **기각** | 같은 snapshot에서 보이는 MAX 동률 행의 tail seal만 조건부로 닫는다. F-13과 late commit은 남는다 |
| 500 burst에서 1,000~1,500 session이 동시에 lease 밖 | **기각** | connection 생성 경로 수와 동시 peak를 혼동했다. preamble 우회는 확인되지만 budget 우회와 동시 수는 실측 전 미확정이다 |
| custom `JdbcConnectionProvider` 하나로 Spark 결함 종결 | **부분** | 3개 connection 경로를 가로챌 수 있는 핵심 spike다. 그러나 예외 흡수, job-wide snapshot, 취소·linger, sublease 반환까지 혼자 해결하지 못한다 |
| ROWSCN + PK census + sample hash | **조건부 채택** | 명시적으로 약한 BEST_EFFORT tier로는 유효하다. full census와 같은 보증은 아니다 |
| 분리 탐지가 100배 저렴 | **기각** | 같은 coverage horizon에서 비용을 재지 않았다. 현재 값은 `UNMEASURED`다 |

즉, 다음 순서는 `현재 probe 즉시 실행`이 아니라 다음과 같아야 한다.

1. G0-0 probe의 사실 오류와 부하 위험을 수정한다.
2. 안전한 capability inventory와 위험한 data/path 실험을 분리한다.
3. 모든 TNS endpoint에서 capability inventory를 실행한다.
4. disposable Oracle에서 fence counterexample와 provider spike를 수행한다.
5. 결과가 나온 뒤 A v2.0/P v2.0을 개정한다.

---

## 1. Fence 재판정

### 1.1 회귀 진단은 맞지만 표현은 좁혀야 한다

A:1016의 incremental high 식에는 heartbeat 기반 primary-clock witness `T_lb`가 있었다. S:89는 이를 제거하고 standby clock과 `MAX(watermark)`만 남겼다. standby clock을 completeness 하한으로 쓰지 않는다는 A의 원칙과 충돌하므로 **F-04 P0 회귀**라는 결론은 맞다.

다만 “삭제를 문서 어디에도 적지 않았다”는 표현은 과하다. S는 heartbeat 상실과 `safety_lag` 근거 상실을 공시했다. 정확한 결함은 다음이다.

> `T_lb` 및 `SCN_TO_TIMESTAMP` 갈래를 삭제했다는 사실과, 그 결과 high가 standby wall-clock domain에 종속된다는 안전성 변화가 변경 이력에 명시되지 않았다.

또한 `T_lb`라는 이름은 **heartbeat 갈래에만** 써야 한다. Oracle은 `SCN_TO_TIMESTAMP` 결과를 approximate timestamp, 통상 정밀도 3초라고만 정의하며 방향성 있는 하한이나 최대 오차를 보장하지 않는다. 따라서 이 값은 `T_approx`이지 `T_lb`가 아니다. [Oracle SCN_TO_TIMESTAMP](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SCN_TO_TIMESTAMP.html)

### 1.2 `MAX(wm) + ulp(type)`가 실제로 보장하는 것

`ulp`보다 `typed_successor`라는 이름이 정확하다. 다음 전제가 모두 성립할 때만 아래의 좁은 보장을 얻는다.

1. `M = MAX(eligible non-null watermark)`와 실제 추출이 **동일한 source snapshot Q**를 본다.
2. future-dated outlier는 `M`에서 제외되며, cursor가 그 row를 넘어가지 않고 후속 재평가한다.
3. `successor(M)`이 Oracle 비교 → OJDBC → Spark canonical value → Control 저장소를 lossless round-trip한다.
4. `successor(M) > M`이며 overflow가 없다.
5. 두 watermark Merge라면 각 축에 독립적인 seal이 있다.

이때만 `[low, successor(M))`은 Q에서 보이는 `low ≤ wm ≤ M` 행, 즉 현재 MAX 동률 행을 후보 집합에 넣는다.

Profile U에서 MAX probe와 Spark task들이 서로 다른 physical connection·snapshot을 쓰면 전제 1부터 성립하지 않는다. 따라서 이 식을 곧바로 completeness 보장으로 승격할 수 없다.

### 1.3 닫히지 않는 late-commit 반례

`TIMESTAMP(0)`, `successor(M)=M+1초`라고 하자.

1. 실행 R0 snapshot에서 보이는 `MAX=M=12:00:00`이다.
2. transaction X의 watermark도 M이지만 아직 commit되지 않았다.
3. R0는 `high=12:00:01`로 visible row를 적재하고 CAS하여 `low=12:00:01`로 전진한다.
4. 그 뒤 X가 commit된다.
5. 다음 schedule에서도 source MAX는 M이므로 `high=low=12:00:01`이다.
6. 기존 A 규칙은 `high ≤ low`에서 Spark를 제출하지 않고 `FINALIZED_NO_DATA`로 끝낸다.

따라서 overlap query 자체가 실행되지 않는다. X는 source에 더 큰 watermark가 나중에 생기거나 별도 reconciliation이 실행되지 않으면 영구 누락된다. “동률 late commit을 overlap으로 이관한다”는 말은 **source progress가 다시 생길 때만** 조건부로 맞다.

결함별 정확한 폐쇄 상태는 다음과 같다.

| 결함 | 새 식의 효과 |
|---|---|
| F-01 / NEW-01 | 동일 snapshot에서 이미 visible한 tail 동률 행만 조건부 폐쇄 |
| F-04 | standby OS clock 의존은 제거 가능. 단 `SCN_TO_TIMESTAMP`는 hard lower bound가 아님 |
| F-13 | **미폐쇄**. idle/failure 구분, `NO_SOURCE_PROGRESS`, backoff, freshness 의미가 별도로 필요 |
| NEW-08 | raw MAX 사용 시 future outlier가 cursor를 크게 전진시킬 수 있어 오히려 악화. `eligible_max`와 quarantine/re-evaluation 필요 |
| F-02 | commit 이후 늦게 보이는 old/equal watermark는 여전히 미폐쇄 |

### 1.4 typed successor 허용 범위

최소 허용안은 allowlist다.

- `DATE`: 1초 successor를 end-to-end round-trip으로 실증한다.
- `TIMESTAMP(n)`: Oracle 선언 정밀도가 아니라 **전체 canonical pipeline이 보존하는 정밀도**를 기준으로 한다. 하위 계층이 microsecond까지만 보존하면 그보다 작은 successor는 금지한다.
- `NUMBER(p,s)`: 고정 precision/scale이 있고 `10^-s`가 lossless일 때만 허용한다.
- unconstrained `NUMBER`, `BINARY_FLOAT`, `BINARY_DOUBLE`, timezone 변형, 최대 표현값/overflow는 검증 전 거부한다.
- `successor(M) ≤ M`, round-trip 불일치, overflow이면 publish를 거부한다.

keyset cursor는 “틀린 대안”이 아니다. typed successor가 실증된 타입에서는 blast radius가 큰 이유로 **미선택**할 수 있지만, successor가 성립하지 않는 타입의 fallback 후보로 남겨야 한다.

---

## 2. Spark connection·lease 재판정

### 2.1 확인된 사실

- Spark 3.5.9/4.2.0의 `sessionInitStatement`는 task read 경로에서 실행되고 schema 경로에는 적용되지 않는다.
- Spark 4.2.0은 driver 측 `JDBCDatabaseMetadata` connection을 추가로 열 수 있다.
- `customSchema`는 원격 schema connection을 제거하지 않는다.
- task partition마다 connection을 열고 task completion에서 닫는다.
- `connectionProvider` 옵션은 Spark가 로드한 provider를 선택한다. `JdbcConnectionProvider`는 `DeveloperApi`이자 `Unstable` API이므로 정확한 Spark patch version pin과 binary compatibility gate가 필요하다. [Spark JDBC options](https://spark.apache.org/docs/4.2.0/sql-data-sources-jdbc.html) [JdbcConnectionProvider API](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/jdbc/JdbcConnectionProvider.html)

### 2.2 “1,000~1,500 동시·lease 밖”은 입증되지 않았다

논리 read 한 번의 connection **생성 사건**과 동시 peak를 분리해야 한다. 단순화하면 한 scan의 생성 수는 다음과 같다.

```
C = S + M + Q + A
S = schema connection
M = Spark 4.2 metadata connection
Q = pushdown 등 추가 driver probe
A = 실제 task attempt connections
```

정상 task가 P개라면 대략 Spark 3.5.9는 `P+1`, Spark 4.2.0은 `P+2`, 추가 aggregate probe가 있으면 `P+3`의 **생성 사건**이 가능하다. 그러나 S/M/Q는 보통 driver planning 중 순차·단명이고 task connection과 모두 동시에 열린다는 증거가 없다. 500 job에 정적 2~3을 곱해 peak session으로 부를 수 없다.

또한 `driver.connect`가 Control의 per-connection hook을 통과하지 않는 사실과 Control budget 전체를 우회한다는 결론은 다르다. A의 job-level weighted lease가 `driver_sessions`를 선예약하고 동일 username/service의 실제 session 수를 관측한다면, budget 우회 여부는 예약식과 실제 overlap을 대조해야 확정된다.

따라서 정직한 판정은 다음이다.

- **session assertion/preamble 우회: 확인**
- **job-wide common snapshot 부재: 확인**
- **Control budget 우회: 미확정**
- **500 burst peak 1,000~1,500: 기각, tracer 실측 필요**

### 2.3 Provider spike는 핵심이지만 단일 완결책은 아니다

custom provider가 schema·metadata·task용 factory 경로를 가로채는 것은 타당하다. 그러나 다음은 별도로 닫아야 한다.

1. Spark 4.2 metadata/일부 probe 경로는 `NonFatal`을 잡아 log warning 후 진행할 수 있다. provider exception만으로 scan 전체 fail-closed가 되지 않을 수 있다.
2. Flashback `AS OF`가 없다면 connection별 read-only transaction은 job-wide snapshot을 만들지 못한다.
3. connection open/init 중 timeout·Spark cancellation race는 bounded하게 만들 수 있을 뿐 즉시 소멸하지 않는다.
4. 정확한 per-connection lease에는 `Control sublease acquire → physical connect → preamble → wrapped Connection.close에서 release → linger/reclaim` 상태기가 필요하다.
5. API가 unstable이므로 `versions.lock`, ServiceLoader/classpath, provider selection, disabled-provider list, Spark minor/patch upgrade gate가 필요하다.

따라서 산출물은 하나의 provider spike로 묶되, 합격식은 최소 다음을 포함해야 한다.

- 모든 connection에 고유 `connection_uuid`, path(schema/metadata/task), open/close timestamp 기록
- speculation과 retry를 포함한 `peak_open_connections ≤ granted_subleases`
- preamble 각 단계의 성공 receipt와 실패 시 connection close
- schema/task에서 assertion 실패 시 scan 실패
- metadata 예외 흡수 경로가 scan을 fail-open하지 않음
- provider 재호출·double close·worker loss 후 sublease 누수 0

### 2.4 preamble의 정확한 순서

검토서의 단순한 `5→4` 정정은 방향은 맞지만 전체 순서는 다음이다.

```
1 → 2 → 3 → 5 (ALTER SESSION SYNC WITH PRIMARY) → 4 (SET TRANSACTION READ ONLY) → data query
```

여기서 1은 `STANDBY_MAX_DATA_DELAY`, 2는 `SYS_CONTEXT` identity assertion, 3은 NLS/time-zone 고정, 5는 apply barrier, 4는 read-only transaction 시작이다.

`SET TRANSACTION`은 transaction의 첫 statement여야 한다. 다만 `SYNC-after-SET`이 반드시 `ORA-01453`을 낸다는 설명은 부정확하다. Oracle은 read-only transaction 안에서도 `ALTER SESSION`을 허용한다. 순서를 바꾸는 진짜 이유는 **snapshot 의미**다. 먼저 SYNC로 apply barrier를 통과하고 그다음 `SET TRANSACTION READ ONLY`로 snapshot을 열어야 한다. 반대로 하면 SYNC 이후에도 이미 고정된 이전 snapshot을 계속 읽을 수 있다. `ORA-01453`은 두 번째 `SET TRANSACTION` 또는 이미 시작된 transaction 뒤의 `SET TRANSACTION`에서 기대하는 양성 증거다. Oracle은 `SET TRANSACTION`이 transaction의 첫 statement여야 한다고 명시한다. [Oracle SET TRANSACTION](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SET-TRANSACTION.html)

익명 PL/SQL block 하나에 전부 넣으려 하지 말고 provider가 명시적인 initialization state machine으로 실행하는 편이 안전하다.

---

## 3. 탐지 3축 재판정

### 3.1 방향은 맞지만 보증이 달라진다

late upsert, hard delete, content drift를 서로 다른 oracle로 분리하는 방향은 타당하다. 하지만 `ORA_ROWSCN sweep + full PK anti-join + sampled hash`는 full PK+covered-column census와 동등하지 않다.

| 실패 클래스 | 3축 분리안의 범위 |
|---|---|
| 살아남은 late insert/update | base table, 안전한 SCN cursor, 완료 sweep, 실제 값 대조가 모두 있을 때 조건부 탐지 |
| hard delete | ROWSCN으로 불가. `target − source` full PK anti-join 필요 |
| source missing row | 반대 방향 `source − target`도 별도 정의 필요 |
| target-only/pipeline value corruption | 무작위 sample에 뽑힐 때만 탐지 |
| insert→delete transient occurrence | source와 target 모두에서 사라지면 미탐지 |
| delete→same-PK reinsert | current final state만 비교하며 사건 이력은 보장하지 않음 |

Oracle은 `ORA_ROWSCN`이 exact commit SCN이 아니고, `ROWDEPENDENCIES`가 없으면 block 수준이며, row update 없이 값이 변할 수도 있다고 명시한다. Flashback Query, view, external table에서도 사용할 수 없다. [Oracle ORA_ROWSCN](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/ORA_ROWSCN-Pseudocolumn.html)

### 3.2 ROWSCN global cursor race

multi-statement 또는 shard sweep에서 다음 영구 누락이 가능하다.

1. shard A를 먼저 읽는다.
2. A의 row가 SCN 105로 commit된다.
3. shard B를 나중 snapshot에서 읽어 SCN 110을 본다.
4. 완료 cursor를 110으로 전진한다.
5. A의 SCN 105 row는 다음 `>110` sweep에서도 제외된다.

`ORA_ROWSCN`은 Flashback Query에서 지원되지 않으므로 단순 `AS OF SCN`으로 shard snapshot을 고정할 수도 없다. 단일 Oracle statement, sweep 전에 고정한 authoritative upper SCN, 또는 동등하게 검증된 two-pass protocol과 원자적 cursor advancement가 필요하다.

### 3.3 비용은 `UNMEASURED`

“두 자릿수 싸다” 또는 “100배 싸다”는 현재 증거가 없다.

- ROWSCN sweep은 일반적인 index access path가 없어 O(N) table pass가 될 수 있다.
- full PK anti-join도 O(N) PK scan·전송·join이다.
- full census는 전 컬럼 전송이 아니라 DB에서 `PK + 32-byte covered digest`를 한 pass로 만들 수 있다.
- 분리안은 ROWSCN table pass + PK index pass + sample pass가 되어 논리 I/O가 더 클 수도 있다.
- 1% random sample이 한 cycle에 100배 싸더라도 동일한 deterministic coverage를 얻는 비교는 아니다.

비용 비교는 narrow/wide/LOB, row dependency on/off, churn 0.1/1/10%, cold/warm cache에서 DB CPU, consistent gets, physical bytes, network, Spark shuffle, elapsed, repair backlog를 같은 coverage horizon으로 측정해야 한다.

### 3.4 정직한 capability 표현

3축 분리안을 선택하면 전체 `upsert_consistency`는 계속 `BEST_EFFORT`여야 한다.

```
late_change_reconciliation = ROWSCN_SURVIVING_ROWS_BY_CYCLE
delete_consistency          = FULL_PK_CENSUS_BOUNDED_LAG
content_detection           = SAMPLED_PROBABILISTIC
occurrence_coverage         = NOT_COVERED
```

PK bucket을 결정적으로 순환해 N cycle 안에 전 모집단을 덮는 경우에만 `ROLLING_FULL_CONTENT_CENSUS_BY_N_CYCLES`를 쓸 수 있다. 무작위 sample 비율이 p라면 persistent single-row corruption의 k cycle 내 발견 확률은 `1-(1-p)^k`이며 유한 hard SLO는 없다.

full PK+covered hash census만 `CURRENT_STATE_DETECT_AND_REPAIR_BY_CYCLE`을 받을 수 있고, 이 경우에도 transient occurrence 비보장을 함께 공시해야 한다.

---

## 4. 현재 G0-0 probe 실행 차단 결함

현재 세 파일은 **gate evidence로 REJECT**한다. 원천 데이터 변경은 없지만 사실 정확성·credential 안전성·원천 부하 안전성이 닫히지 않았다.

### 4.1 즉시 차단(P0)

1. `g0-0-probe.sql:145`의 SHA-256(`abc`) 기대값이 틀렸다. 올바른 값은 `BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD`다. 현재 값은 양성 환경을 false negative로 만든다.
2. README의 `sqlplus "$ORA_USER/$ORA_PW@..."`는 비밀번호를 process argv에 노출한다. “환경변수로만 전달”이라는 규칙과 모순이다. Oracle Wallet/external password store 또는 안전한 interactive prompt를 써야 한다.
3. SQL의 data facts는 문서가 언급한 `LIMIT_ROWS`를 구현하지 않고 전체 테이블을 scan한다. 10,000 job 대상 DB에 기본 실행하면 안 된다.
4. Spark S3 partition probe도 `ROWNUM`/명시적 predicate 없이 전체 테이블을 읽는다. Spark 문서상 `lowerBound`/`upperBound`는 filtering이 아니라 stride 결정에만 사용된다. [Spark JDBC bounds](https://spark.apache.org/docs/4.2.0/sql-data-sources-jdbc.html)
5. `SET TRANSACTION`, `SYNC WITH PRIMARY` 등은 `WHENEVER SQLERROR CONTINUE` 뒤의 SELECT가 apparent success를 출력할 수 있다. 각 statement의 SQLCODE, elapsed, expected error를 한 구조화 record로 캡처해야 한다.

### 4.2 증거 의미 오류(P0/P1)

6. `user_resource_limits.sessions_literal`은 `DEFAULT`/`UNLIMITED`를 거르지 않는다. `p_expect IS NULL`이면 모든 반환값을 `value_interpretable=true`로 만든다.
7. `rows_at_max_wm ≥ 1`은 nonempty table이면 자명하며 tail row 존재만 보인다. idle/equal-tail sequence 없는 상태에서 “영구 누락 실재”를 증명하지 않는다.
8. `future_wm_rows`가 standby `SYSTIMESTAMP`를 기준으로 하므로 cross-clock 문제의 독립 증거가 아니다.
9. `dbms_crypto.hash_blob`은 BLOB이 아니라 `UTL_RAW.CAST_TO_RAW` 결과, 즉 RAW overload를 시험한다.
10. Spark에서 같은 SID가 나와도 connection reuse를 증명하지 않는다. provider-generated UUID 또는 가능한 경우 SID+SERIAL#와 open/close trace가 필요하다.
11. S3 subquery는 watermark를 `wm` alias로 바꾸고도 `partitionColumn=a.wm`을 넘겨 일반적인 이름 불일치 가능성이 있다.
12. `USER_USERS.ACCOUNT_STATUS`는 유용한 evidence지만 standby-local failed-login 상태 전체를 보장하지 않는다.
13. 단일 endpoint·단일 object 성공을 전체 SourceSystem capability로 승격할 수 없다. 모든 TNS `ADDRESS`/service와 대상 object class별 manifest가 필요하다.

### 4.3 누락된 counterexample

G0-0이 fence 설계를 확정하려면 다음이 별도 harness에 있어야 한다.

- supported watermark type별 `typed_successor` round-trip과 overflow
- equal-watermark late commit after CAS
- future-dated outlier가 MAX인 상태
- NULL watermark와 empty bootstrap
- two-watermark Merge의 독립 seal
- multi-connection snapshot skew
- ROWSCN shard cursor 105/110 race
- mid-sweep crash와 cursor non-advance
- transient insert→delete와 same-PK reuse

---

## 5. 권고 실행 패키지

G0-0을 세 실행물로 분리한다.

### G0-0A — production-safe capability inventory

- read-only dictionary/function probes만 포함
- data full scan, `DBMS_SESSION.SLEEP`, DDL, Scheduler create/execute 금지
- Wallet 또는 external password store 사용
- statement별 timeout·외부 watchdog
- 모든 TNS ADDRESS/service에 fresh connection
- script/driver/OJDBC/Spark/versions.lock digest와 필수 probe manifest 기록
- incomplete manifest는 PASS 금지

### G0-0B — `JdbcConnectionProvider` path tracer spike

- disposable Oracle 또는 승인된 저위험 PoC source에서 실행
- schema/metadata/task별 connection UUID와 open/close 기록
- exact preamble `1→2→3→SYNC→SET TRANSACTION READ ONLY`
- provider 실패 전파와 metadata exception swallowing 확인
- retry/speculation/cancel/worker-loss에서 peak session과 sublease leak 측정
- 500 job burst는 먼저 소규모로 계수를 얻은 뒤 단계적으로 확장

### G0-0C — fence·reconciliation counterexample harness

- disposable writable primary + ADG 조합에서만 실행
- typed successor, late commit, future outlier, idle source, ROWSCN cursor race, hard delete, transient occurrence 주입
- full census와 3축 분리안을 동일 coverage horizon으로 비용 비교
- 운영 source의 DDL/Scheduler create·execute는 금지

---

## 6. A v2.0/P v2.0에 넣을 최소 정정

G0 결과 전에는 상태/enum을 확정하지 말고 다음 규칙만 잠정 채택한다.

1. `fence_time_witness=SCN_TO_TIMESTAMP`를 hard lower bound로 부르지 않는다. `T_approx`와 BEST_EFFORT 의미만 허용한다.
2. `typed_successor`는 allowlisted canonical type과 end-to-end round-trip PASS에서만 활성화한다.
3. equal/old-watermark late commit은 successor로 폐쇄됐다고 쓰지 않는다.
4. F-13은 `NO_SOURCE_PROGRESS`, scheduling backoff, freshness state로 별도 폐쇄한다.
5. custom provider는 **필수 spike**이지 확정된 단일 해법이 아니다.
6. Spark session budget은 path tracer에서 얻은 peak 계수와 retry/speculation 상한으로 다시 산정한다.
7. 3축 탐지는 BEST_EFFORT capability로만 제공한다.
8. deterministic current-state 보장이 필요한 source에는 full PK+covered digest census를 유지한다.
9. 두 방식의 비용은 `UNMEASURED`로 두고 benchmark 뒤 확정한다.
10. 현재 G0-0 probe 결과는 수정본의 digest와 manifest가 없으면 gate evidence로 인정하지 않는다.

이 조건 아래에서만 `G0-0 수정본 실행 → provider spike → D1~D4 재확정 → A v2.0/P v2.0 개정` 순서를 승인한다.
