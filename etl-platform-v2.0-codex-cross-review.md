# Codex 교차 리뷰: 무권한 재설계 범위 v2.0

- 검토일: 2026-08-25
- 검토 대상: 무권한 재설계 범위 제안서(S), 목표 아키텍처 v1.2.3.1(A), PoC 기준서 8차(P)
- 검토 원칙: Oracle 19c·Apache Spark·Dagster의 1차 출처와 세 문서의 실제 실행 경로를 대조했다. 공식 문서가 말하지 않는 동작은 `미확인`으로 남겼다.
- 심각도: **P0** 데이터 손상·실행 불가, **P1** 보장 훼손, **P2** 문서·운영 결함

약칭 `S:n`, `A:n`, `P:n`은 각 입력 문서의 실제 줄 번호다.

---

## 1. 최종 판정

### 1.1 한 줄 결론

**DBA 비협조를 전제로 Profile U를 새로 설계하는 방향과 `ZERO_GAP` 삭제는 맞지만, 현재 제안서는 핵심 fence가 최신 행을 영구 누락시키고 Spark의 실제 connection 경로를 통제하지 못하므로 v2.0 규격 동결은 NO-GO다. G0-0 실측과 P0 수정 후의 의미론 개정만 조건부 GO다.**

### 1.2 의사결정 요약

| 대상 | 판정 | 설명 |
|---|---|---|
| 제안서의 사실 기반 신뢰도 | **조건부 / 동결 NO-GO** | 가장 큰 전제인 `AS OF`의 `FLASHBACK` 권한 요건, `STANDBY_MAX_DATA_DELAY`, `USERENV.DATABASE_ROLE`는 확인됐다. 그러나 “23항 전부 확정”은 아니다. ADG read-only transaction, mid-query lag 재평가, ADG의 `ORA_ROWSCN`은 미확인이고, `V$`·`DBA_*`·profile/LOB 증거의 전면 사망 주장은 틀렸다. |
| 제안된 fence 공식 | **NO-GO** | `MAX(watermark)`와 현행 `[low, high)`의 결합이 현재 최대값 행을 제외한 채 cursor를 전진시킨다. 또한 late commit, 서로 다른 시계, 빈/NULL 원천, 두 watermark Merge를 봉인하지 못한다. 완전성 fence가 아니라 query-admission 기반 휴리스틱이다. |
| D1 — `ZERO_GAP` 삭제 | **GO** | Profile U에는 공통 가시 SCN, commit-watermark 상한, C1 반증 수단이 없다. `STANDBY_MAX_DATA_DELAY=0`과 `SYNC WITH PRIMARY`도 늦게 commit된 오래된 watermark와 hard delete를 막지 못한다. |
| D2 — 보증 5축 | **조건부 GO** | 내부 machine-readable 계약에는 유용하다. 다만 축 이름·값이 실제 Spark 단위와 보장 범위를 과장한다. 소비자 UI는 3개 요약으로 줄이고 `source_admission_control`은 정합성 축이 아닌 운영 위험으로 분리해야 한다. |
| D3 — v2.0 + Profile O/U | **부분 GO** | breaking change이므로 v2.0은 맞다. 하지만 A/P를 O와 U 두 벌로 유지하는 것은 NO-GO다. v1.2.3.1은 감사용 archive로 동결하고, 운영 규격은 단일 core + ConnectionRevision capability overlay여야 한다. |
| D4 — 부록 W | **조건부 GO** | 협상용 보존은 가능하다. 선언만으로는 과거와 같은 전제 역류를 막지 못한다. W 전체가 false인 conformance test와 본문→W 참조 금지 lint가 필요하다. |
| A v2.0 / P v2.0 개정 착수 | **조건부 GO** | discovery·G0-0 probe·실행 spike는 즉시 가능하다. 규범 문서 대규모 개정은 G0-0 결과와 §7의 P0 폐쇄 뒤에 시작해야 한다. `S §11:193–194`의 원문 “개정과 병행 가능: G0-0 프로브” 순서는 뒤집어야 한다. |
| Dagster 기반 플랫폼 지속 | **조건부 GO** | 원천 완전성을 보장해서가 아니라 10,000 Job의 공용 Factory, 불변 JobSpec, Hold/Catch-up, ledger, target commit fencing, audit/repair, UI·관측 표준화 때문에 가치가 있다. 오케스트레이터 교체만 남는다면 전환 가치는 낮다. |
| Profile U의 예방적 완전성 | **NO-GO** | DBA 협조·CDC/change log·DB-enforced commit-watermark bound가 없으면 예방형 보장을 만들 수 없다. full PK+covered-column census가 있더라도 cycle 시점 current-state 탐지뿐이며 transient occurrence 완전성은 아니다. 정직한 상한은 `BEST_EFFORT + 명시된 범위의 reconciliation`이다. |

현재 안을 막는 최상위 P0는 다음 여섯 묶음이다.

1. `MAX(watermark)` tail 누락과 무계 late commit 누락
2. Spark schema connection의 preamble 우회
3. “같은 세션” fence와 Job 단위 transaction snapshot의 실행기 부재
4. bounded Audit을 완전한 `DETECT_AND_REPAIR`로 표시한 과장
5. ORA-03172/03173·인증 실패의 burst retry/login 폭풍
6. capability 실측 전에 규격을 개정하는 순서 역전

---

## 2. Oracle 사실 검증표

판정 의미는 `확인`, `부분 확인`, `정정`, `미확인`이다. `정정`은 방향 전체를 기각한다는 뜻이 아니라 제안서 문장 그대로는 규범으로 쓸 수 없다는 뜻이다.

| # | 주장 | 판정 | Oracle 19c 1차 출처 | 설계 영향 |
|---:|---|---|---|---|
| 1 | `SYS_CONTEXT('USERENV', …)`를 일반 세션에서 사용할 수 있다. | **확인** | [SYS_CONTEXT](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SYS_CONTEXT.html)는 `USERENV`를 내장 session namespace로 정의하고 `DATABASE_ROLE` 값을 열거한다. | `V$DATABASE` 없이 role assertion이 가능하다. 실제 계정에서 14속성의 NULL·문자열을 pin해야 한다. |
| 2 | `DATABASE_ROLE`은 `PHYSICAL STANDBY`를 구분한다. | **확인** | [SYS_CONTEXT의 USERENV 표](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SYS_CONTEXT.html) | role fence는 성립한다. chunk 경계 확인은 point-in-time assertion일 뿐 실행 중 role 불변을 증명하지 않는다. |
| 3 | USERENV 이름 tuple이 immutable identity fence를 **전체 대체**한다. | **정정** | 같은 [USERENV 표](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SYS_CONTEXT.html)에는 `DB_NAME`, `DB_UNIQUE_NAME`, service·host는 있지만 PDB `DBID`, `CON_UID`, `GUID`, `RESETLOGS_CHANGE#`는 없다. | 이름을 재사용한 clone은 통과한다. `identity`가 아니라 `role_and_routing_identity`로 낮춰야 한다. `ISDBA=FALSE`도 최소권한 전체를 증명하지 않는다. |
| 4 | `ALTER SESSION SET STANDBY_MAX_DATA_DELAY=N`에 별도 권한이 없다. | **확인** | [ALTER SESSION prerequisites](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/ALTER-SESSION.html)는 별도 표기가 없는 다른 작업에 추가 privilege가 필요 없다고 한다. 해당 parameter에는 별도 gate가 없다. | 무권한 session assertion의 핵심은 타당하다. PDB lockdown 등 실제 환경 결과가 최종 truth다. |
| 5 | lag가 N을 넘으면 query가 fail-closed된다. | **확인하되 코드 정정** | [ALTER SESSION — STANDBY_MAX_DATA_DELAY](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/ALTER-SESSION.html)와 [ORA-03172](https://docs.oracle.com/en/error-help/db/ora-03172/) | 기능 의미는 맞다. machine-facing code는 `ORA-03172`로 정규화해야 한다. 문서 본문의 `ORA-3172`는 축약 표기다. |
| 6 | lag 한도가 장시간 fetch 중에도 계속 재평가된다. | **미확인** | [Data Guard 10.2.1.2](https://docs.oracle.com/en/database/oracle/oracle-database/19/sbydb/managing-oracle-data-guard-physical-standby-databases.html)는 query 실행 조건만 설명하고 parse/open/fetch 재평가 시점을 명시하지 않는다. | 제안서의 “보수적으로 시작 시점만 가정”은 맞다. 오류 부재를 전체 추출 동안 `lag≤D`였다는 증거로 쓰면 안 된다. |
| 7 | `STANDBY_MAX_DATA_DELAY=0`은 primary와 같은 query result를 보장할 수 있다. | **조건부 확인** | [ALTER SESSION](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/ALTER-SESSION.html)과 [Data Guard restrictions](https://docs.oracle.com/en/database/oracle/oracle-database/19/sbydb/managing-oracle-data-guard-physical-standby-databases.html) | SYNC transport, `SYNCHRONIZED`, maximum protection/availability, real-time apply 조건이 필요하다. 그래도 미래 late commit이나 watermark 의미론까지 봉인하지 않는다. |
| 8 | `ALTER SESSION SYNC WITH PRIMARY`에 별도 권한이 없다. | **확인** | [ALTER SESSION](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/ALTER-SESSION.html) 일반 prerequisite와 해당 clause | 실제 ADG 구성에서 ETL 계정 probe가 필요하다. |
| 9 | `SYNC WITH PRIMARY`는 primary의 현재 commit 전체를 적용하는 완전성 barrier다. | **정정** | [Data Guard 10.2.1.3](https://docs.oracle.com/en/database/oracle/oracle-database/19/sbydb/managing-oracle-data-guard-physical-standby-databases.html)는 명령 발행 때 standby가 **이미 받은 redo**의 적용까지만 기다린다고 정의한다. | `RECEIVED_REDO_APPLY_BARRIER`로 이름을 바꿔야 한다. transport 중·미수신 redo, future/uncommitted transaction, commit-watermark 간격은 보장하지 않는다. |
| 10 | SYNC 전제가 깨지면 조용히 통과하지 않고 실패한다. | **확인하되 코드 정정** | [Data Guard restrictions](https://docs.oracle.com/en/database/oracle/oracle-database/19/sbydb/managing-oracle-data-guard-physical-standby-databases.html), [ORA-03173](https://docs.oracle.com/en/error-help/db/ora-03173/) | `ORA-03173`으로 정규화한다. SYNC transport·SYNCHRONIZED·real-time apply 외에 maximum protection/availability도 capability 근거에 포함한다. |
| 11 | `SET TRANSACTION READ ONLY`는 별도 권한 없이 transaction-level consistency를 준다. | **확인** | [SET TRANSACTION](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SET-TRANSACTION.html) | **한 physical connection의 한 transaction**에 한해 맞다. transaction의 첫 statement여야 하며 Spark Job 전체의 공통 snapshot은 아니다. |
| 12 | 위 statement가 ADG `READ ONLY WITH APPLY`에서 명시적으로 지원된다. | **미확인** | [Data Guard physical standby](https://docs.oracle.com/en/database/oracle/oracle-database/19/sbydb/managing-oracle-data-guard-physical-standby-databases.html)는 일반 read-only DB와 같은 제한 및 19c의 SQL/PLSQL 실행을 말하지만 이 statement를 직접 보증하지 않는다. | G0-0 actual-driver probe 전에는 확정 capability로 쓰지 않는다. 실패 시 multi-statement snapshot 등급을 비활성화한다. |
| 13 | read-only transaction에는 운영상 고정 최대 수명이 있다. | **정정** | [Oracle undo 관리](https://docs.oracle.com/en/database/oracle/oracle-database/19/admin/managing-undo.html)와 [data consistency](https://docs.oracle.com/en/database/oracle/oracle-database/19/cncpt/data-concurrency-and-consistency.html)는 long query의 snapshot-too-old 가능성을 설명한다. | 수명은 undo 공간·부하·retention·LOB에 종속된다. ORA-01555와 실제 p99 추출시간을 G0/G1에서 함께 시험해야 한다. |
| 14 | `STANDARD_HASH(expr,'SHA256')`는 무권한 SQL built-in이며 RAW를 반환한다. | **확인** | [STANDARD_HASH](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/STANDARD_HASH.html) | package 배포 없이 scalar hash가 가능하다. Oracle↔Spark canonical byte vector 검증은 별도다. |
| 15 | hash 제외 타입은 LOB·객체 타입뿐이고 `DBMS_CRYPTO` LOB hash 경로는 죽는다. | **정정 / actual grant 조건** | [STANDARD_HASH](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/STANDARD_HASH.html)는 `LONG`과 LOB도 금지하고 [RAWTOHEX](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/RAWTOHEX.html)도 추가 타입 제한이 있다. [DBMS_CRYPTO security model](https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_CRYPTO.html)은 SYS 설치 후 필요한 user/role에 access를 grant하는 모델이다. | type participation matrix에 `LONG`, `LONG RAW`, LOB, UDT와 silent exclusion 금지를 명시한다. `DBMS_CRYPTO`는 default entitlement로 기대할 수 없지만 현재 account의 direct/role grant 또는 실제 호출 성공 여부를 G0에서 확인한다. |
| 16 | `UTL_I18N`, `UTL_RAW`, `COMPOSE/DECOMPOSE`는 모두 무권한 여부가 미확인이다. | **정정 / 부분 미확인** | [UTL_I18N security model](https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/UTL_I18N.html)은 PUBLIC default를 명시한다. [COMPOSE](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/COMPOSE.html)와 [DECOMPOSE](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/DECOMPOSE.html)는 SQL built-in이다. [UTL_RAW](https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/UTL_RAW.html) 19c 문서에는 PUBLIC 보장이 없다. | 네 항목을 분리한다. PUBLIC은 revoke될 수 있으므로 실제 호출도 한다. cross-engine hash의 byte framing은 여전히 별도 규격이다. |
| 17 | `ORA_ROWSCN`은 exact commit SCN이 아니라 보수적으로 과대일 수 있는 표본이다. | **확인** | [ORA_ROWSCN](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/ORA_ROWSCN-Pseudocolumn.html) | bound 또는 fence로 승격하지 않고 sample로만 쓰는 판단은 맞다. `ROWDEPENDENCIES`가 없으면 block-level이다. |
| 18 | `SCN_TO_TIMESTAMP`는 approximate이고 mapping 보존기간이 제한된다. | **확인** | [SCN_TO_TIMESTAMP](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SCN_TO_TIMESTAMP.html) | 일반적으로 3초 수준이고 최소 120시간 mapping을 설명하지만 ADG-specific 의미와 정확한 실패 코드는 actual probe로 남긴다. |
| 19 | ADG에서 `ORA_ROWSCN`·`SCN_TO_TIMESTAMP`가 동일 의미로 작동한다. | **미확인** | 위 SQL reference에는 ADG-specific 가시성·redo apply 의미가 없다. | G0-0에서 실제 base table, 오래된/future/invalid SCN을 구분해 시험한다. ZERO_GAP 근거로는 사용하지 않는다. |
| 20 | `USER_*`는 특별 권한 불요이고 `ALL_*`는 접근 가능한 object 범위다. | **확인** | [Static data dictionary views](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/about-static-data-dictionary-views.html) | schema drift와 자기 권한 감사를 회수할 수 있다. `S:161`의 `ALL_TAB_COLUMNS` 미확인은 §2.1과 모순이다. 반면 `NLS_DATABASE_PARAMETERS`는 USER_/ALL_ 규칙으로 자동 확인되지 않으므로 실제 호출을 남긴다. |
| 21 | `DBMS_APPLICATION_INFO`와 `DBMS_SESSION.SET_IDENTIFIER`로 무권한 tagging이 가능하다. | **확인** | [DBMS_APPLICATION_INFO](https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_APPLICATION_INFO.html), [DBMS_SESSION](https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_SESSION.html) | 중앙 cross-session 대조는 V$에 의존하지만 자기 session은 `SYS_CONTEXT`와 package read API로 대조 가능하다. connection 재사용 시 reset 시험은 custom pool을 쓸 때만 필요하다. |
| 22 | `V$`/`GV$`와 `DBA_*`, profile·LOB evidence는 모두 절대적으로 죽는다. | **정정** | [Dynamic performance views](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/about-dynamic-performance-views.html)는 **기본 설치** 접근을 말하며 direct grant가 가능하다. [DBA view 접근](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/about-static-data-dictionary-views.html)도 direct grant를 인정한다. [USER_RESOURCE_LIMITS](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/USER_RESOURCE_LIMITS.html), [USER_PASSWORD_LIMITS](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/USER_PASSWORD_LIMITS.html), [ALL_LOBS](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/ALL_LOBS.html)로 일부 증거를 일반 계정이 읽을 수 있다. | 실제 grant가 없으면 V$/DBA_* 경로는 죽지만 이를 사전 단정할 수 없다. 각 view의 `QUERY_OK`, `ROW_PRESENT`, `VALUE_INTERPRETABLE`을 따로 기록한다. profile 값은 **할당값**일 뿐 `RESOURCE_LIMIT=TRUE`와 실제 강제를 증명하지 않으며, `ALL_LOBS`는 storage attribute metadata일 뿐 historical version availability·ORA-01555 회피·retention guarantee를 증명하지 않는다. `CONTAINER_DATA`는 root의 common-user cross-container 범위에 한정한다. |
| 23 | 일반 계정에는 자기 부하를 제한하거나 source DDL/Scheduler Job을 만드는 방법이 전혀 없다. | **좁혀서 확인 / actual privilege 조건** | [Resource Manager](https://docs.oracle.com/en/database/oracle/oracle-database/19/admin/managing-resources-with-oracle-database-resource-manager.html)는 consumer-group switch privilege를 요구한다. profile 생성·할당, [CREATE TABLE](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/CREATE-TABLE.html), Scheduler Job 생성도 각각 privilege가 필요하다. | 올바른 문장은 “사전 profile/consumer-group/DDL/Job privilege 없이 ordinary session이 새 DB-enforced account-wide session/CPU/IO hard cap이나 source object/job을 만들 수 없다”이다. 기존 grant의 부재는 G0 전 단정하지 않는다. client budget·query timeout·직렬화는 가능하지만 DB hard admission은 아니다. DG 시험 인스턴스 부재는 Oracle 사실이 아니라 조직 전제다. |

`S:49–52`의 privilege 묶음은 위 번호표와 별도로 다음처럼 직접 판정한다.

| 범위 주장 | 판정 | G0에서 필요한 실제 증거 |
|---|---|---|
| `S:49` — `AS OF`는 `FLASHBACK` + `READ/SELECT`가 필요 | **확인** | 실제 extract가 참조하는 **모든 object**의 권한과 동일 SCN/timestamp에 대한 real-JDBC 다중 physical-connection `AS OF` 성공 |
| `S:50` — `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER`는 `EXECUTE` 필요 | **확인, ADG 동작 미확인** | package actual call, 반환 SCN, 동일 standby에서 그 SCN의 `AS OF` 성공; grant 존재만으로 capability 활성화 금지 |
| `S:51` — `DBMS_CRYPTO` LOB hash 경로는 default entitlement가 아님 | **확인, actual grant 조건** | direct/role grant와 실제 LOB hash 호출을 분리 기록 |
| `S:52` — heartbeat/kill/assertion/canonicalization은 source DDL·Scheduler 권한 필요 | **확인, actual privilege 조건** | 운영 Source에서는 `SESSION_PRIVS` 등 read-only introspection만 수행한다. actual create/execute 양성·음성 대조는 명시적 disposable Oracle PoC에서만 허용한다. ordinary credential의 실패를 default로 두되 사전 grant를 단정하지 않는다. |

### 2.1 최우선 질문 8개에 대한 직접 답

1. **`STANDBY_MAX_DATA_DELAY`는 별도 privilege 없이 설정 가능하다.** 초과 시 fail-closed된다. 다만 공식 오류 계약은 `ORA-03172`이고 mid-query 재평가는 문서상 미확인이다.
2. **`AS OF SCN/TIMESTAMP`에는 대상 object의 `FLASHBACK`과 `READ` 또는 `SELECT`가 필요하다.** SELECT만으로 부족하다는 핵심 전제는 맞다. [Oracle Flashback privilege 절](https://docs.oracle.com/en/database/oracle/oracle-database/19/adfns/flashback.html)
3. **`USERENV.DATABASE_ROLE`은 `PHYSICAL STANDBY`를 구분한다.** 다만 이름 tuple은 immutable clone identity가 아니다.
4. **ADG의 `SET TRANSACTION READ ONLY`는 일반 규칙상 가능성이 높지만 19c 1차 문서에 직접 보증 문장이 없다.** 실제 `READ ONLY WITH APPLY`에서 실행해야 확정된다.
5. **`SYNC WITH PRIMARY`의 전제가 깨지면 즉시 `ORA-03173`으로 실패한다.** 하지만 적용 대상은 호출 시 standby가 받은 redo뿐이다.
6. **`ORA_ROWSCN`/`SCN_TO_TIMESTAMP`의 일반 의미는 확인되지만 ADG-specific 의미는 미확인이다.** fence가 아니라 sample다.
7. **hash 경로는 일부 살아남는다.** `STANDARD_HASH`는 가능하지만 LONG·LOB·UDT 제한과 cross-engine canonical byte 규격이 남는다.
8. **ordinary session이 새 DB hard aggregate cap을 만드는 수단은 확인되지 않았다.** 그러나 자기 profile/password limit와 접근 가능한 LOB 사실까지 모두 잃는 것은 아니다.

---

## 3. fence 공식 공격

제안식은 다음과 같다.

```text
t0   = SELECT SYSTIMESTAMP FROM source_table WHERE ROWNUM = 1
high = min(t0 - D - safety_lag, MAX(watermark))
```

### 3.1 판정

**이 식은 completeness fence가 아니다.** `D`는 query admission 시 apply lag에 관한 predicate이고, `t0`는 standby host clock이며, `MAX(watermark)`는 현재 보이는 row 중 최댓값일 뿐이다. 이 셋 중 어느 것도 아직 commit되지 않은 transaction, 오래된 watermark로 나중에 commit되는 row, hard delete, 모든 Spark connection의 공통 snapshot을 봉인하지 않는다.

| 공격 | 실패 시나리오 | 결과 |
|---|---|---|
| F-01 — 반개구간 tail trap | `S:88–93`이 `high=MAX(wm)`를 만들고 A의 추출이 `[low, high)`를 사용한다. 현재 `MAX=M`인 row는 `<M`에서 제외되는데 cursor는 M으로 CAS된다. 이후 원천이 유휴이거나 같은 timestamp M만 추가되면 계속 제외된다. | **P0 영구 누락.** `(watermark, stable_pk)` tuple cursor, 또는 명시적 inclusive upper + idempotent seal이 필요하다. 임의 `+epsilon`은 datatype precision과 동률 때문에 안전하지 않다. |
| F-02 — 보이지 않는 late commit | high 계산 때 아직 commit되지 않은 transaction이 나중에 `wm < high-overlap`로 commit한다. | `MAX`는 아직 없는 row를 볼 수 없다. DBA/CDC/change log/commit-watermark bound 없이는 예방이 불가능하고, full PK+covered-column census도 완료 cycle의 current-state 차이만 찾는다. |
| F-03 — bounded Audit의 공백 | row의 `UPDATE_DT`는 10일 전이고 transaction은 오늘 commit한다. Audit은 최근 72시간만 조회한다. | row는 적재와 Audit 모두에서 영구 누락된다. `S §6:140–146`의 원문 “빠진 것을 찾아내 메우는 보장이 있습니다”는 bounded Audit만으로는 거짓이다. |
| F-04 — 서로 다른 clock domain | `t0`는 standby OS clock이고 watermark는 primary/app clock이다. standby가 빠른 방향의 skew는 근거 없는 `safety_lag`를 넘어설 수 있다. | `t0-D-safety_lag`는 증명된 bound가 아니다. [SYSTIMESTAMP는 DB host clock](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SYSTIMESTAMP.html)이다. |
| F-05 — 빈 테이블·모두 NULL | 빈 table에서는 `ROWNUM=1` query가 0행, `MAX`는 NULL이다. 모두 NULL인 watermark도 동일하다. | high가 없고 comparison은 UNKNOWN이다. bootstrap, empty, all-NULL, NULL-row 정책이 없다. NULL watermark row는 영구 제외된다. |
| F-06 — Merge의 두 시간축 | 기존 Merge는 `INSERT_DT OR UPDATE_DT`인데 공식은 단일 `MAX(watermark)`만 정의한다. 한 column의 MAX는 다른 column tail을 봉인하지 못한다. | column별 cursor/seal을 정의하거나 canonical event watermark가 필요하다. `GREATEST`는 NULL·conversion·index 문제를 추가한다. |
| F-07 — witness query의 불충분성 | `SELECT SYSTIMESTAMP ... ROWNUM=1`이 어느 access path를 택할지는 optimizer가 결정한다. 빈 table에는 row도 없고, 이 query가 실제 extract predicate·partition을 읽었다는 보장이 없다. | data block witness를 completeness barrier로 부를 1차 근거가 없다. lag guard는 **실제 extract query가 실행되는 각 connection**에 적용해야 한다. |
| F-08 — “같은 세션” 실행기 부재 | Control의 t0/MAX query, Spark schema query, partition data query는 stock Spark에서 서로 다른 connection/action일 수 있다. | 한 session의 ORA-03172 부재가 다른 session을 fence하지 않는다. 제안서와 stock Spark의 분리 action만으로는 same-session 조건이 구현되어 있지 않다. custom adapter 또는 한 query로 묶는 별도 구현과 실증이 필요하다. |
| F-09 — N개의 snapshot | `numPartitions=N`이면 N개 connection과 N개 transaction snapshot이다. retry는 새 connection을 연다. | fixed logical window라도 rowset은 deterministic하지 않다. partition key가 mutable하면 Full에도 cross-partition 누락/중복이 가능하다. |
| F-10 — SYNC 의미 과장 | SYNC는 호출 때 **수신된** redo apply까지만 기다린다. transport 중 redo와 uncommitted transaction은 남는다. | `SYNC_BARRIER`는 source completeness가 아니라 `RECEIVED_REDO_APPLY_BARRIER`다. |
| F-11 — D와 소비자 freshness 혼동 | `D`는 query admission 시 apply lag다. queue, extraction, Iceberg commit, finalization 시간이 더해진다. | `S §6:140–146`의 원문 “신선도: 원천 대비 최대 300초(DB 강제)”는 틀리다. `apply_lag_at_query_admission≤D`와 target publication lateness를 분리한다. |
| F-12 — probe 부하 | 40,000 Run/일마다 `MAX`를 추가하고 Merge는 두 column을 조회한다. plan·cache·동시성에 따라 index/root block 경쟁과 500 burst 부하가 생길 수 있다. | 원천 admission 0인 설계에서 probe 자체도 source budget에 포함하고 plan/cost/burst FI로 상계해야 한다. |
| F-13 — 유휴 운영 결과 | `MAX`가 전진하지 않으면 high도 영구 정지한다. master table의 정상 유휴와 pipeline 장애를 구분할 정보가 없다. | 반복 no-op Run과 published cutoff age 증가는 확실하고, 정책에 따라 freshness 경보·repair backlog가 늘 수 있다. `NO_SOURCE_PROGRESS` 결과, backoff, 소비자 freshness 의미 분리가 필요하다. |

### 3.2 Profile U에서 허용 가능한 정직한 상한

- `STANDBY_MAX_DATA_DELAY`는 각 **실제 data query의 admission guard**로만 사용한다.
- `SYNC WITH PRIMARY`는 `RECEIVED_REDO_APPLY_BARRIER`로만 표시한다.
- `t0-D-safety_lag`는 보장 fence가 아니라 운영자가 선택한 `HEURISTIC_TIME_CUTOFF`로만 기록한다.
- equal-watermark tail은 `(watermark, stable_pk)` keyset 또는 검증된 inclusive seal로 닫는다.
- full PK + covered-column hash census는 **각 완료 cycle 시점의 현재 상태 차이**만 탐지한다. 두 cycle 사이 생겼다가 사라진 transient occurrence/event까지 완전 탐지하려면 CDC/change log가 필요하다.
- census 비용을 감당하지 못하는 대형/high-churn table은 `BEST_EFFORT + ROLLING_HORIZON_RECONCILIATION`으로 표시한다.
- prevention이 필요한 table은 DBA 협조, CDC/source change log, 또는 DB-enforced commit-watermark rule 없이는 Profile U 대상이 아니다.

---

## 4. 제안서가 놓친 결함

### NEW-01 — `MAX` tail row 영구 누락 — **P0**

실패 시나리오: 최초 원천 최대 watermark가 M이고 한 건 이상이 정확히 M이다. `[low,M)`만 읽고 watermark를 M으로 CAS한다. 새 최대값이 생기지 않으면 M row는 어떤 회차에도 들어오지 않는다.

필수 조치: fence 공식을 폐기하고 typed tuple cursor 또는 검증된 inclusive seal을 정의한다. PoC에 동일 timestamp 다건, 장기 유휴, timestamp precision별 양성 반례를 넣는다.

### NEW-02 — 무계 late commit과 bounded Audit의 영구 미탐지 — **P0**

실패 시나리오: 10일 전 watermark를 가진 transaction이 오늘 commit하고 Audit horizon은 72시간이다. 적재 window와 Audit 모두 row를 보지 못한다.

필수 조치: source 전체 PK와 covered-column hash census/cycle이 증명된 경우에도 값은 `CURRENT_STATE_DETECT_AND_REPAIR_BY_CYCLE`로 한정한다. occurrence completeness는 CDC/change log 없이는 금지한다. 그 외 값은 `ROLLING_HORIZON_RECONCILIATION` 또는 `BEST_EFFORT`다. target 인공 결손 FI-55는 repair 동작만 증명하며 source 누락 coverage를 증명하지 않는다. 근거: `S:132–145`, `A:1118–1127`, `P:500–515`.

### NEW-03 — 5단계 preamble이 executable spec이 아님 — **P0**

실패 시나리오: `S:69–75`의 5개 SQL/PLSQL을 Spark `sessionInitStatement`에 단순 세미콜론 목록으로 넣는다. Spark는 [문자열 하나를 `prepareStatement`/`execute` 한 번](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) 호출하므로 목록은 보통 전체 구문 오류다. 여러 session 변경이 일부 남는 경우는 유효한 PL/SQL block 안에서 dynamic SQL을 순차 실행하다 중간 실패할 때다.

필수 조치: pinned Spark·Oracle JDBC에서 실행되는 **정확한 단일 payload**를 규격에 싣고, 각 단계 실패, `SET TRANSACTION` first-statement 조건, SYNC 순서, rollback/close를 실증한다. 성공 로그가 아니라 postcondition을 같은 connection에서 확인한다.

### NEW-04 — Spark schema/metadata connection의 assertion 우회 — **P0**

실패 시나리오: Spark가 data task 전에 별도 driver connection으로 schema query를 실행한다. 실제 `versions.lock`에 pin된 Spark의 source를 대조해야 하며, Spark 4.2.0의 [`resolveTable/getQueryOutputSchema`](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRDD.scala#L93-L116)는 `sessionInitStatement`를 호출하지 않는다. 따라서 500 Run이 role·lag assertion 없이 source를 접촉한다. 현행 weight에 driver session이 포함돼도 token 밖에서 실제로 열리는지는 별도 추적해야 하므로 **budget 우회는 미확인**, assertion 우회는 확인이다.

필수 조치: `customSchema`는 schema read를 제거하지 않고 조회된 schema에 overlay하므로 해결책이 아니다. pinned connection provider/adapter, Spark patch, 또는 schema query까지 같은 assertion·budget 경로에 넣는 구현이 필요하다. G0-0 합격식은 “모든 LOGON 뒤 assertion 완료 전 업무 SQL 0”이며 실제 token/accounting도 별도로 대조한다. 근거: `S:64–76`, `A:993–1003`, `P:45–54`; [Spark 4.2.0 task connection/init/close](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRDD.scala#L289-L347).

### NEW-05 — Job 단위 `TRANSACTION_SNAPSHOT` 과장 — **P0**

실패 시나리오: `numPartitions=1`이지만 t0, MAX, data extract, Audit이 각각 DataFrame/action이다. 각각 새 connection/transaction을 사용해 서로 다른 snapshot을 읽는다.

필수 조치: `TRANSACTION_SNAPSHOT`을 `SINGLE_QUERY_SNAPSHOT`으로 낮춘다. explicit connection adapter가 여러 문장을 같은 transaction에서 수행한 경우에만 `MULTI_STATEMENT_TRANSACTION_SNAPSHOT`을 허용한다.

### NEW-06 — fence의 “같은 세션” 조건이 구현되지 않음 — **P0**

실패 시나리오: Control의 t0 connection 무오류는 executor의 positive evidence도 공통 snapshot도 아니다. executor마다 `sessionInitStatement`가 정상 적용되면 lag가 D를 넘은 data query는 ORA-03172로 실패하고 CAS는 0이어야 한다. preamble 우회 또는 오류 무시가 있으면 그때만 stale read가 가능하다.

필수 조치: actual data query 각각의 preamble evidence를 contract/attempt와 연결한다. Control probe 결과를 executor connection에 상속시키지 않는다.

### NEW-07 — empty/NULL과 두 watermark 의미론 누락 — **P0**

실패 시나리오: empty/all-NULL source 또는 `INSERT_DT`만 전진하고 `UPDATE_DT`는 NULL인 Merge table에서 high가 NULL이거나 잘못된 column tail만 봉인한다.

필수 조치: empty bootstrap, NULL reject/full-reconcile, 두 column의 독립 typed cursor, 동률 stable key 규칙을 publish validator에 넣는다.

### NEW-08 — 서로 다른 시계의 cutoff를 보장으로 표시 — **P0**

실패 시나리오: standby clock이 primary/app보다 빠르고 미래 timestamp outlier가 존재한다. `min`의 time side도 미래로 이동해 cursor를 안전 구간 밖으로 전진시킨다.

필수 조치: 증명된 clock-skew upper bound 없이는 time cutoff를 completeness 보장에 사용하지 않는다. `safety_lag`는 보수 상수가 아니라 근거 없는 heuristic이라는 제안서 자인을 계약 값에 반영한다.

### NEW-09 — ORA-03172/03173의 Spark retry 폭풍 — **P0**

실패 시나리오: 정각 500 Run에서 lag fault가 발생한다. [Spark의 task retry·speculation 설정](https://spark.apache.org/docs/latest/configuration.html#scheduling)이 partition 수만큼 새 connection을 열 수 있다. 현행 `retry_on_asset_or_op_failure=false`이면 이 오류 자체가 Dagster 자동 run retry를 곱하지는 않는다. Dagster 배수는 run-worker failure 또는 v2가 이 오류를 재제출 사유로 잘못 매핑할 때의 조건부 위험이다.

필수 조치: ORA-03172/03173/01017/28000은 **Spark task 내부 즉시 retry 금지**로 분류하고, 첫 관측에 Source circuit/Hold, jitter, probe single-flight를 적용한다. ORA-03172/03173의 control-plane deferred retry는 lag/capability 회복을 확인한 뒤 fresh connection/new Attempt로만 허용한다. source 추출 전용 SparkApplication profile 전체에 `spark.task.maxFailures=1`(총 attempt 1, 자동 retry 0), `spark.speculation=false`를 적용하고 다른 stage retry까지 제거하는 비용을 G1에서 잰다. 근거: `A:343–349`, `S:96–104`, `P:187–197`.

### NEW-10 — blocking SYNC의 cancellation hole — **P0**

실패 시나리오: `SYNC WITH PRIMARY`가 init에서 block되고 JDBC `queryTimeout=0`이다. Spark cancel listener가 data query보다 앞선 init을 즉시 끊지 못해 Run 취소 뒤에도 Oracle session과 Control token을 붙든다.

필수 조치: stock Spark는 init과 data query에 같은 `queryTimeout`을 적용하므로 옵션만으로 preamble 전용 timeout을 만들 수 없다. finite 공통 `queryTimeout` + Oracle network/read timeout을 쓰거나, preamble-only timeout이 필요하면 direct-JDBC preflight/adapter 또는 custom provider를 구현한다. init 중 interrupt도 기본 보호로 가정하지 말고 pod 종료 후 conservative linger reserve를 둔다. 실제 `versions.lock`의 Spark source와 cancel FI로 증명한다. [Spark 4.2.0 실행 경로](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRDD.scala#L325-L367)

### NEW-11 — credential breaker 2회의 분산 race — **P0**

실패 시나리오: 500 Run이 같은 credential로 동시에 처음 로그인한다. Control이 1~2건을 반입하기 전에 profile lock threshold를 훨씬 넘는다.

필수 조치: `USER_PASSWORD_LIMITS`에서 actual assigned `FAILED_LOGIN_ATTEMPTS`를 회수하고 Source별 credential preflight single-flight 뒤에만 lease를 발급한다. 이는 피해 축소일 뿐, preflight 직후 rotation·executor fan-out·외부 client 실패를 막지 못한다. Appendix W의 profile enforcement 증거와 안전한 rollover가 없으면 계정 잠금 예방을 절대 보장으로 표시하지 않는다.

### NEW-12 — G0-0 실패 분기와 순서 부재 — **P0**

실패 시나리오: 문서 개정 중 `SET TRANSACTION READ ONLY`, `STANDBY_MAX_DATA_DELAY`, schema path 중 하나가 실제 driver에서 실패한다. 이미 작성한 A/P의 핵심 상태·등급을 다시 뒤집어야 한다.

필수 조치: G0-0을 normative rewrite보다 먼저 수행하고 §7.2의 분기표를 확정한다.

### NEW-13 — task retry가 source snapshot 결정성을 깨뜨림 — **P0**

실패 시나리오: 한 partition이 ORA-01555/network fault로 retry된다. 새 connection의 read-only transaction은 다른 snapshot을 읽고 앞선 partition과 하나의 시점 이미지를 이루지 않는다.

필수 조치: source task attempt를 정확히 1회(자동 retry 0)로 하거나 기존 Attempt를 fence하고 새 Attempt에서 전체 staging을 폐기·재시작한다. 서로 다른 Attempt의 partial output을 섞지 못하게 한다. 근거: `A:1012–1052`, `A:1065–1067`, `P:187–197`.

### NEW-14 — partial partition 성공의 staging 오염 — **P0**

실패 시나리오: partition 1~9는 staging write를 마치고 partition 10의 preamble이 실패한다. 성공 partition의 임시 산출물이 다음 retry에서 재사용된다.

필수 조치: init/data 실패가 하나라도 있으면 chunk manifest·catalog commit·watermark CAS는 0이어야 한다. stage-attempt 단위 ownership과 orphan 처리 합격식을 추가한다. 근거: `A:1047–1052`, `A:1065–1067`, `P:187–197`.

### NEW-15 — hard delete `BOUNDED_LAG`의 전수 coverage 부재 — **P1**

실패 시나리오: 오래된 PK row가 hard delete되고 source에는 delete timestamp가 없다. 시간 window Audit은 그 삭제를 발견할 row 자체가 없다.

필수 조치: full-PK census 또는 전체 PK shard cycle과 완료 SLO가 증명된 Source만 `FULL_PK_CENSUS_BOUNDED_LAG`를 쓴다. 나머지는 `delete_consistency=NONE`이다. 이 값도 cycle 시점의 current-state delete 탐지이지 transient occurrence 보장은 아니다. 근거: `A:1113–1127`, `P:189–197`.

### NEW-16 — read-only transaction과 ORA-01555 용량 모델 부재 — **P1**

실패 시나리오: 장시간 Full/증분 추출이 undo를 필요로 하는 동안 원천 변경량이 커져 ORA-01555가 난다. 동일 추출 retry가 source 부하를 반복한다.

필수 조치: table size·p95/p99 extract duration·undo-pressure별 실험, query timeout, failure classification, staging 재시작 비용을 G1 용량식에 넣는다. 고정 “최대 수명”을 만들지 않는다. 근거: `A:1043–1052`, `P:193–197`.

### NEW-17 — platform-only budget을 source hard cap으로 오해 — **P1**

실패 시나리오: 이관 중 Airflow, 운영 script, 다른 system이 같은 account/service로 접근한다. Profile U는 `GV$SESSION`도 못 보므로 실제 총 session을 확인하지 못한다.

필수 조치: 필드명을 `PLATFORM_OWNED_PATH_BUDGET`으로 바꾸고 coverage를 공시한다. G1은 “platform-owned path의 choke point”만 증명한다. 이관의 core 경로는 double-read 없는 시간대 분리이며, 별 credential/service는 Appendix W에서 외부 제공 capability로 확인된 경우에만 사용한다. 근거: `A:991–1008`, `P:196–197`.

### NEW-18 — Spark 기본 경로에는 재사용 JDBC pool이 없음 — **P1**

실패 시나리오: 문서는 “JDBC 풀”의 checkout/reset을 가정하지만 stock Spark는 task에서 connection을 열고 닫는다. 반대로 UCP/Hikari/custom provider를 도입하면 새로운 session-state lifecycle이 생긴다.

필수 조치: stock Spark라면 `platform_owned_connection_budget`으로 정확히 명명한다. pinned Spark task 경로는 connection별 `getConnection` 후 completion에서 `close`함을 source로 고정한다. custom pool을 쓰면 checkout마다 preamble, rollback/reset, credential 격리, eviction을 별도 명세·시험한다. 근거: `A:991–1003`; [Spark 4.2.0 task connection lifecycle](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRDD.scala#L289-L347).

### NEW-19 — 오류 **부재**를 1급 증거로 둔 자기모순 — **P1**

실패 시나리오: schema connection이 preamble을 우회하거나 primary에 연결됐는데 ORA-03172가 없다는 이유만으로 합격한다.

필수 조치: 오류는 거부 증거가 될 수 있지만 부재는 단독 증거가 아니다. physical connection별 payload digest, init-completed marker, USERENV 결과, actual data query ID와 completion을 연결한다. 근거: `S:64–76`, `A:1041`, `P:45–54`.

### NEW-20 — `D`를 publication freshness SLO로 노출 — **P1**

실패 시나리오: query admission 때 lag≤300초였지만 queue 20분, extract 30분, commit/finalize 10분이 걸린다. 소비자 UI는 “원천 대비 최대 300초”로 보인다.

필수 조치: `read_admission`과 `target_publication_lateness`를 분리한다. D는 admission predicate이지 table freshness SLO가 아니다. 근거: `S:88–104`, `A:1010–1041`.

### NEW-21 — identity tuple의 가변성과 clone collision — **P1**

실패 시나리오: failover/RAC routing으로 host·instance/service가 바뀌거나 동일 이름 clone이 대체된다. 전자는 false reject, 후자는 false accept다.

필수 조치: mutable routing identity와 immutable DB identity 부재를 분리한다. Profile U에는 `immutable_identity=UNVERIFIED`를 명시하고 소비자·운영 리스크로 남긴다. 근거: `S:24–42`, `A:1040–1041`.

### NEW-22 — profile/LOB 증거를 버려 더 위험한 fallback을 선택 — **P1**

실패 시나리오: 실제 `FAILED_LOGIN_ATTEMPTS=3`인데 조회 불가로 오인해 breaker=2를 고정한다. 동시에 500 login이 계정을 잠근다. 또는 접근 가능한 `ALL_LOBS.RETENTION`을 버려 hash/Flashback 판단을 과도하게 강등한다.

필수 조치: G0-0에 `USER_RESOURCE_LIMITS`, `USER_PASSWORD_LIMITS`, `USER_USERS`, `ALL_LOBS`를 포함한다. view별 query 성공·row 존재·값 해석을 나누고, profile 할당값과 실제 강제, LOB storage attribute와 historical retention 보장을 분리한다. 근거: `S:43–55`, `A:1047–1050`, `P:193–197`.

### NEW-23 — Profile O/U 두 규범의 drift — **P1**

실패 시나리오: 어떤 Source는 Flashback만 있고 V$는 없으며 다른 Source는 반대다. binary profile이 표현하지 못하고 두 A/P 문서의 enum·API·FI가 갈라진다.

필수 조치: 단일 v2 core + capability overlay로 운영한다. O/U는 preset 또는 문서 view이고 v1.2.3.1은 archive다. capability digest는 contract에 pin하고 TTL/revoke 시 재검증한다. 근거: `S:146–159`, `A:1685–1700`, `P:531–540`.

### NEW-24 — Audit 자체의 source capacity 미산정 — **P1**

실패 시나리오: 10,000 Job의 24~72시간 PK/hash scan과 repair가 정상 40,000 Run/일에 겹친다. 원천 admission 0인 상태에서 보호를 위해 만든 Audit이 가장 큰 부하가 된다.

필수 조치: 정상 extract, MAX/schema probe, retry, Audit, repair를 모두 connection/CPU/IO budget과 backlog 모델에 넣는다. full census를 요구할 Source는 coverage manifest(PK, covered columns, LOB/exclusion, cycle 완료 SLO)와 허용 부하를 먼저 통과해야 한다. PK-only census는 값 오염을 찾지 못한다. 근거: `S:132–145`, `P:500–515`, `P:531–540`.

### NEW-25 — Full load의 multi-snapshot image — **P0**

실패 시나리오: 60% Full load를 여러 JDBC partition으로 읽는 중 row의 partition key나 값이 바뀐다. partition별 snapshot이 달라 한 시점의 source image가 아니며 key 이동 시 중복/누락도 가능하다.

필수 조치: `snapshot_scope`를 소비자에게 표시하고 stable partition key를 검증한다. point-in-time Full이 필요한 Job은 common Flashback snapshot 또는 실제 single-query/single-connection 경로 없이는 publish하지 않는다. 근거: `A:1043–1052`, `A:1065–1067`, `P:187–197`.

### NEW-26 — artifact 메타데이터의 1줄 오차 — **P2**

`S:4`는 A 1,739줄·P 650줄이라 쓰지만 검토 입력의 실제 값은 A 1,738줄·P 649줄이다. 요청서 §1의 SHA-256과 줄 수는 실제 파일과 일치한다. 규범 의미에는 영향이 없지만 build가 산출한 식별값만 표기하도록 정리한다.

---

## 5. 결정 4건 판정

### 5.1 D1 — `ZERO_GAP` 삭제: **GO**

Profile U에서 삭제가 맞다.

- `TIMESTAMP_TO_SCN`은 approximate mapping일 뿐 session-visible global SCN을 주지 않고, SCN을 얻어도 `AS OF` 권한 없이는 multi-session snapshot에 적용할 수 없다. [TIMESTAMP_TO_SCN](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/TIMESTAMP_TO_SCN.html)
- `ORA_ROWSCN`은 row/block 수준 최근 변경 SCN의 부정확한 상계이며 정확한 commit SCN도 global visible cutoff도 아니다.
- `STANDBY_MAX_DATA_DELAY=0`은 physical standby의 real-time query mode 등 문서상 전제 아래 **그 query**의 primary-equivalent result를 조건부로 보장하지만 앞으로 commit될 오래된 watermark를 막지 못한다.
- `SYNC WITH PRIMARY`도 수신 redo까지만 적용하며 C1과 open transaction을 반증하지 않는다.
- G0-0에서 일부 table의 Flashback 권한이 발견돼도 회복되는 것은 common snapshot과 retry 결정성이다. commit-watermark bound와 hard-delete coverage 없이 `ZERO_GAP`까지 자동 복원하면 안 된다.

복원 조건은 부록 R에 비선택 capability로 보존할 수 있지만 Profile U enum에는 두지 않는다.

### 5.2 D2 — 5축: **조건부 GO**

내부 계약은 다섯 필드를 유지할 수 있으나 현재 이름과 값은 다음처럼 고쳐야 한다.

| 현재 | 권고 | 이유 |
|---|---|---|
| `source_staleness = SYNC_BARRIER / DB_ENFORCED_MAX_DELAY(N) / UNBOUNDED` | `read_admission = RECEIVED_REDO_APPLIED_AT_ADMISSION / APPLY_LAG_AT_QUERY_ADMISSION_LE(N) / UNBOUNDED` | target freshness나 primary-current completeness가 아니라 query admission의 사실이다. |
| `snapshot_scope = TRANSACTION_SNAPSHOT / PER_STATEMENT` | `COMMON_FLASHBACK_SNAPSHOT / MULTI_STATEMENT_SINGLE_CONNECTION / SINGLE_QUERY / PER_STATEMENT` | Job이 아니라 실제 transaction/query 단위를 표현한다. explicit read-only transaction이 없으면 같은 connection의 여러 statement도 공통 snapshot이 아니다. `PER_CONNECTION`은 connection마다 data statement가 정확히 하나일 때만 `PER_STATEMENT`의 별칭이다. unavailable 값은 capability로 거부한다. |
| `upsert_consistency = DETECT_AND_REPAIR / BEST_EFFORT` | `CURRENT_STATE_DETECT_AND_REPAIR_BY_CYCLE / ROLLING_HORIZON_RECONCILIATION / BEST_EFFORT` | full census도 cycle 시점의 PK + covered-column **현재 상태**만 비교한다. transient occurrence completeness는 CDC/change log 없이는 주장하지 않는다. |
| `delete_consistency = SYNC / BOUNDED_LAG / NONE` | `FULL_PK_CENSUS_BOUNDED_LAG / NONE`을 Profile U 기본으로 사용 | hard delete의 시간 window 탐지는 불가능하다. `SYNC`는 source log/capability가 있을 때만 가능하다. |
| `source_admission_control = SELF_LIMITED_ONLY` | `load_governance_scope = PLATFORM_OWNED_PATH_ONLY` | fixed 공시값은 데이터 정합성 축이 아니다. 실제 통제 범위를 드러내는 Source/ConnectionRevision 운영 badge다. |

소비자 화면은 다음 세 개로 요약하는 편이 낫다.

1. **읽기 시점** — query admission lag 조건 + snapshot 범위
2. **변경 coverage** — upsert/delete coverage + repair/census SLO
3. **운영 제한** — DB hard cap 유무와 platform-only 통제 범위

원시 5필드는 API·감사·정책 엔진에 남기고, 소비자에게 enum 다섯 개를 그대로 노출하지 않는다.

### 5.3 D3 — v2.0 + Profile O/U: **v2.0 GO, 두 규범 fork NO-GO**

enum 삭제와 계약 의미 변경은 breaking change이므로 v2.0이 맞다. 하지만 실제 권한은 O/U 이분법이 아니라 capability 조합이다.

권고 구조:

```text
v2 core state machine / API / ledger / UI
  └─ ConnectionRevision capability overlay
       FLASHBACK_QUERY
       MAX_DELAY_ASSERT
       READ_ONLY_TXN
       RECEIVED_REDO_SYNC
       SERVER_HASH
       SCHEMA_DISCOVERY
       EXTERNAL_SESSION_CAP_EVIDENCE
```

- v1.2.3.1은 감사·rollback reference로 동결한다.
- O/U는 capability preset 또는 documentation view로만 둔다.
- 규범 A/P, DDL, API, test oracle은 한 벌만 유지한다.
- 일부 grant가 생겨도 Profile 전체를 O로 승격하지 않고 해당 capability만 활성화한다.
- 권한·DG 조건이 바뀔 수 있으므로 evidence TTL, release/revoke invalidation, contract capability digest pin이 필요하다.

### 5.4 D4 — 부록 W: **조건부 GO**

부록을 남기는 것은 실무 협상에 유용하다. 다만 다음 자동 조건 없이는 승인하지 않는다.

1. W의 모든 항목이 false인 상태로 core conformance suite가 PASS한다.
2. normative section에서 W anchor·`W-*`를 참조하면 build가 실패한다.
3. W 획득은 문장 수정이 아니라 capability evidence만 바꾼다.
4. W 상실/revoke 시 강등·Hold·기존 contract 처리 시험이 있다.
5. G0/P0/G1의 필수 합격식에는 W 결과가 들어가지 않는다.

Appendix W의 기존 옵션 중 운영 안전을 회복하는 최소 조합은 **1번(table별 Flashback) + 2번(profile 할당·강제 정보)**다. 마감 시각 barrier가 사업 필수면 **3번(DG 구성 사실·SYNC 전제)**까지 필요하다. 어느 조합도 commit-watermark bound, delete/occurrence coverage, real-JDBC 실증 없이 `ZERO_GAP`을 자동 복원하지는 않는다.

---

## 6. 남은 것의 가치

### 6.1 기존 Airflow/HAflow보다 무엇이 나아지는가

원천 보장 등급이 약해져도 다음은 **Phase 0 baseline과 G1에서 검증할 가치 가설**이다.

| 영역 | 신규 플랫폼의 가치 | Oracle 무권한과의 관계 |
|---|---|---|
| Job 생성·변경 | 10,000개 개별 DAG source 생성을 줄이는 공용 Factory + immutable JobSpec/Template release, UI 기반 source/table/column mapping | 원천 보장과 독립적인 개선 가설 |
| 실행 모델 | Occurrence → Contract → Attempt 분리, deterministic submission, 재시도·수동 실행 의미 통일 | 독립적 가치 |
| 운영 중지 | Source/Job/Global Hold, DRAIN/FORCE_STOP, incremental catch-up coalescing | 독립적 가치 |
| target 안전 | Iceberg snapshot lineage, target lease, watermark CAS, commit evidence, adjudication | 대부분 독립적 가치 |
| 관측·알림 | Dagster UI + Prometheus/Grafana/OpenSearch + Kafka alert payload 표준화 | 독립적 가치 |
| schema/credential | ConnectionRevision, schema drift, credential rotation·breaker 표준화 | 일부 source capability 의존 |
| 원천 부하 | platform-owned connection budget과 source-key circuit | **부분 가치**; DB hard cap 또는 외부 client 통제는 아님 |
| 누락·삭제 | Audit/repair workflow와 operator API | **coverage가 증명된 범위만** 가치 |
| 예방적 completeness | 없음 | Profile U로는 개선되지 않음 |

현행 Airflow/HAflow가 schedule·queue·retry를 어느 SLO로 수행하는지는 Phase 0 baseline으로 확정해야 한다. 따라서 **Dagster라는 엔진 교체 자체**는 아직 투자 근거가 아니다. 비교할 가치 가설은 UI/Factory, 불변 사양, Control ledger, Hold/Catch-up, target commit safety, 표준 audit/repair를 한 운영 모델로 묶었을 때 리드타임·MTTR·운영 toil이 실제로 줄어드는지다.

### 6.2 이 조건에서도 Dagster로 갈 이유가 남는가

**조건부로 남는다. 다만 Dagster-first가 아니라 Control-contract-first여야 한다.**

- Dagster는 [configuration 기반 Asset Factory](https://docs.dagster.io/guides/build/assets/creating-asset-factories), 실행 이력, [sensor별 `run_key` 중복 방지](https://docs.dagster.io/guides/automate/sensors#preventing-duplicate-runs), Kubernetes Run, operator UI에 적합하다.
- source completeness, connection hard cap, watermark safety, repair coverage는 Dagster가 제공하지 않는다.
- Dagster [concurrency pool](https://docs.dagster.io/guides/operate/managing-concurrency/concurrency-pools)은 platform 내부 shared-resource 제한에는 유용하지만 DB account hard cap은 아니다. canceled/failed Run의 slot도 기본으로 자동 해제되지 않으므로 Control lease를 대체하지 않는다.
- 핵심 사양과 ledger가 Dagster 내부 상태에 종속되지 않으면 향후 오케스트레이터 교체도 가능하다.
- 기존 HAflow의 custom scheduler/retry를 이름만 바꿔 다시 구현하면 안 된다. Control은 policy·contract·evidence를 소유하고 Dagster는 orchestration·run lifecycle을 소유해야 한다.

따라서 제품 정의는 다음이 정직하다.

> **metadata-driven ETL 실행·증거·탐지/복구 운영 플랫폼**

“Oracle 원천의 모든 변경을 빠짐없이 보장하는 플랫폼”이라고 정의하면 실패한다.

### 6.3 소비자에게 허용 가능한 수준인가

- Bronze/operational landing, 일반 분석, 재생 가능한 내부 dataset: **조건부 허용**. 명시된 rolling horizon 또는 full-census SLO가 소비자 허용치 안이어야 한다.
- 마감 배치: `RECEIVED_REDO_APPLY_BARRIER`와 publication lateness를 별도 표시하고, primary-current 완전성으로 오해하지 않게 하면 조건부 허용이다.
- 재무·규제·안전·생산 의사결정 중 예방적 completeness가 필수인 table: **Profile U 단독 No-Go**. source log/CDC, DB-enforced commit-watermark rule, 또는 검증 가능한 full census가 필요하다.

### 6.4 전환 가치가 사라지는 최소 조건

다음 중 하나라도 충족되면 Dagster 전환을 중단하거나 Airflow 위에서 Control/UI만 개선하는 대안을 다시 비교해야 한다.

1. 실제 driver/executor의 모든 source connection을 role·routing assertion과 platform budget 아래 두지 못한다.
2. Critical table에 필요한 late commit·hard delete current-state coverage와 repair closure SLO를 만들 수 없고, transient occurrence가 필요한 table에도 CDC/change log를 확보하지 못한다.
3. 40,000 Run/일·burst 500에서 retry storm, source wait, Audit/repair backlog를 포함한 SLO가 현행보다 낫지 않다.
4. 기존 Airflow/HAflow가 immutable spec, Hold/Catch-up, target commit evidence, audit/repair를 비슷한 비용과 SLO로 제공할 수 있다.
5. 상위 10개 template가 80% Job을 공용 Factory로 변환하지 못해 10,000개 개별 정의·운영 부담이 그대로 남는다.
6. 이관 중 Airflow와 신규 경로의 double source read를 막지 못한다.
7. Dagster+Control의 운영 인력·장애면·비용이 기존 scheduler/UI 운영 toil 절감보다 크다.

이 판단을 위해 Phase 0에 현행 baseline을 추가해야 한다: Job 등록 리드타임, template 변경 배포시간, 장애 MTTR, 수동 retry/repair 횟수, schedule miss, source incident, 운영자 toil, 인프라 비용, audit coverage와 실제 누락 탐지율.

---

## 7. v2.0 착수 전 필수 정정

### 7.1 P0 — 규범 개정 전에 닫을 것

1. **G0-0을 먼저 실행한다.** 문서 개정과 병행하지 않는다. 실제 ETL credential·TNS descriptor의 모든 ADDRESS/service, 실제 Oracle JDBC, driver/executor fresh connection에서 수행한다.
2. **제안 fence를 completeness 공식에서 삭제한다.** `MAX` tail, equal timestamp, empty/NULL, two-column Merge, late commit 반례를 먼저 통과하는 cursor/seal 규격을 만든다.
3. **정확한 단일 preamble payload를 실행 가능 artifact로 고정한다.** 단순 SQL 나열이 아니라 pinned driver가 실행할 한 statement/유효한 block이어야 한다. 단계별 실패, read-only first-statement, finite timeout, postcondition을 포함한다. stock Spark option에 preamble-only timeout은 없으므로 공통 timeout 또는 adapter 구현을 선택한다.
4. **Spark의 모든 connection 경로를 열거·계수한다.** schema inference 우회를 제거하거나 감싸고 SQL log로 assertion-before-query를 증명한다.
5. **source-read retry 정책을 고정한다.** ORA-03172/03173/auth error는 Spark task 즉시 retry 금지, task attempt 정확히 1회, speculation off, Source circuit/Hold, jitter, credential single-flight를 시험한다. 03172/03173의 control-plane 재시도는 lag/capability 회복 확인 후 fresh connection/new Attempt에서만 허용한다.
6. **`DETECT_AND_REPAIR` 약속의 범위를 정직하게 한정한다.** full PK+covered-column census는 `CURRENT_STATE_DETECT_AND_REPAIR_BY_CYCLE`, bounded audit은 `ROLLING_HORIZON_RECONCILIATION`으로 구분한다. hard delete와 horizon 밖 late commit source-side injection, transient occurrence 비보장 공시가 필요하다.
7. **blocking statement timeout과 lingering session 회계를 닫는다.** cancellation 뒤 token/lease release 조건을 client close만으로 증명했다고 쓰지 않는다.

### 7.2 G0-0 결과 분기표

| 실측 결과 | 필수 설계 결과 |
|---|---|
| extract object-set의 `FLASHBACK` + `READ/SELECT`, common point 취득, real-JDBC 다중 connection 동일 `AS OF`가 모두 성공 | 해당 query object-set에만 `COMMON_FLASHBACK_SNAPSHOT` capability. Profile 전체 승격·ZERO_GAP 자동 복원 금지 |
| `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER` actual call과 반환 SCN의 동일 standby `AS OF`가 성공 | current-SCN acquisition capability를 별도 등록. package grant 존재만으로 활성화하거나 object Flashback privilege와 혼동 금지 |
| `STANDBY_MAX_DATA_DELAY` 설정 또는 ORA-03172 양성 대조 실패 | `read_admission=UNBOUNDED`; bounded admission 필수 Job publish No-Go |
| long fetch 중 lag 변화 probe 결과 | mid-fetch 재평가가 확인된 경우에만 그 좁은 evidence를 기록; 확인되지 않으면 admission-start predicate로 유지 |
| USERENV role/routing assertion 실패 | ConnectionRevision read-path No-Go; best-effort 강등 실행 금지 |
| ADG `SET TRANSACTION READ ONLY` 실패 | multi-statement snapshot 비활성; `SINGLE_QUERY/PER_STATEMENT`만 허용. `PER_CONNECTION`은 connection당 data statement가 정확히 하나일 때만 별칭 |
| SYNC 실패 또는 DG 전제 미확인 | `RECEIVED_REDO_APPLY_BARRIER` 비활성 |
| canonical hash vector 실패 | PK coverage만 유지하고 content-sensitive 계약은 No-Go 또는 exclusion 공개 |
| `UTL_RAW`·`DBMS_CRYPTO` actual call 실패 | 해당 hash path 비활성; `UTL_I18N`/SQL built-in과 하나의 package capability로 합치지 않음 |
| schema discovery connection이 assertion을 우회 | pinned provider/adapter·Spark patch 등으로 schema query도 같은 assertion/budget 경로에 넣기 전 ConnectionRevision No-Go; `customSchema` 단독 합격 금지 |
| `USER_RESOURCE_LIMITS` 또는 `USER_PASSWORD_LIMITS` query 성공 | `ASSIGNED_LIMIT_EVIDENCE`만 기록. 관리자 증거 또는 안전한 concurrent-login 양성 대조로 `RESOURCE_LIMIT` 실제 강제까지 증명하기 전 DB hard cap은 `UNKNOWN` |
| `ALL_LOBS` query 성공 | `QUERY_OK`, `ROW_PRESENT`, `VALUE_INTERPRETABLE`과 `LOB_STORAGE_ATTRIBUTE_ONLY`를 분리. 0행은 non-LOB Source의 정상 결과일 수 있고 historical retention 보장은 아님 |
| V$/DBA_* 실제 query와 expected container row가 일치 | 필요한 capability만 활성화. synonym/underlying `V_$` grant, PDB/root 위치, common-user `CONTAINER_DATA` row scope를 함께 증명하며 Profile O 일괄 전환 금지 |
| ADG `ORA_ROWSCN`/`SCN_TO_TIMESTAMP` probe 실패·미확인 | sample capability 비활성 또는 `UNVERIFIED`; fence·ZERO_GAP 근거로 승격 금지 |
| current consumer-group/switch privilege 또는 server hard-cap enforcement 미증명 | `load_governance_scope=PLATFORM_OWNED_PATH_ONLY`; DB-enforced admission으로 표시 금지 |
| production read-only introspection 또는 disposable Oracle PoC의 DDL/Scheduler actual probe 성공 | core 전제로 역류시키지 않고 선택 capability evidence로만 기록. 운영 Source에서 create/job mutation 금지; 실패가 Profile U 기준선 |

### 7.3 P1 — 의미·계약 정정

1. 오류 code를 `ORA-03172`, `ORA-03173`으로 정규화한다.
2. `SYNC_BARRIER`, `TRANSACTION_SNAPSHOT`, `SELF_LIMITED_ONLY`, `DETECT_AND_REPAIR`를 §5.2의 실제 의미로 바꾼다.
3. identity를 immutable identity와 role/routing identity로 분리한다.
4. `USER_RESOURCE_LIMITS`, `USER_PASSWORD_LIMITS`, `ALL_LOBS`, 실제 direct grant probe를 Oracle 사실 inventory에 복원하되 할당값/실제 강제/storage metadata/historical retention을 서로 다른 evidence qualifier로 기록한다.
5. `STANDARD_HASH` type matrix와 canonical byte framing을 명시한다.
6. 하나의 v2 core + capability overlay로 문서 구조를 바꾸고 v1.2.3.1은 archive로 둔다.
7. Appendix W false conformance와 reference lint를 build gate로 둔다.
8. query-admission lag와 target publication freshness를 소비자 UI에서 분리한다.

### 7.4 G1 — 전환 Go 전에 측정할 것

1. 500 burst에서 정상/ORA-03172/auth fault 각각의 connection peak, retry 수, source wait, circuit-open 시간
2. Full/Incremental별 p95/p99 추출시간과 ORA-01555, single-query 대 parallel snapshot 차이
3. schema/MAX/Audit/repair를 포함한 총 source query/CPU/IO 비용
4. full-census 또는 cyclic PK+covered-column coverage 완료시간과 repair backlog/closure SLO; transient occurrence는 CDC 부재 시 측정 대상 밖임을 공시
5. top-10 Factory의 Job 전환율, Job 생성 리드타임, template release blast radius와 rollback
6. source 추출 전용 SparkApplication에서 task 자동 retry 0으로 인한 전체 stage 성공률·재실행 비용
7. Airflow/HAflow 대비 MTTR·operator toil·인프라/개발비·freshness 개선

위 P0와 G0-0이 닫히기 전에는 A v2.0/P v2.0의 상태·enum·API를 대규모 편집하지 않는 것이 비용이 가장 낮다. 지금 바로 만들 가치가 있는 산출물은 **G0-0 executable probe, exact preamble spike, Spark connection-path tracer, fence 반례 harness**다.

---

## 부록. 검토 artifact 식별값

식별값은 2026-08-25에 PowerShell `Get-FileHash -Algorithm SHA256`과 `Get-Content.Count`로 다시 계산했다.

| 파일 | 줄 수 | SHA-256 |
|---|---:|---|
| `codex-cross-review-prompt-v2.0.md` | 158 | `77F0507B37E4D8B5E30C80675E1C78BACDCF3FE17C9BCDAD946A0195C29B3C50` |
| `etl-platform-v2.0-unprivileged-redesign-scope.md` | 195 | `AEFFE9C257642CAF1F504E7A0F6D736BA77D921B1D9CD42F304B0BFC22F4FFE6` |
| `etl-platform-target-architecture-v1.2.3.1.md` | 1,738 | `FA1C760F5A0B7F4456D0C222A42375DEBBE0F28E219AD70D2DB7B7D96100A7A1` |
| `etl-platform-poc-test-plan-v1.md` | 649 | `D31B79DF6D8881E8DB7335CEBA84E16217AF959183354C49FCE267CD9CAFEE69` |

요청서가 제시한 세 검토 대상의 줄 수와 SHA-256은 모두 실제 파일과 일치했다.
