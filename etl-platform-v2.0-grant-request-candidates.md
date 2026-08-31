# 권한 요청 후보 원자료 (37건)

`etl-platform-v2.0-grant-request-verdict.md` 의 판정 근거다. 7차 교차 리뷰 P1-11(“37개 후보·9개 판정 원자료가 저장소에 없어 재현할 수 없다”)을 닫는다.

> **읽는 법**: `검증` 열이 `적대적 검증`인 9건만 반증 절차를 거쳤고 **전부 기각**됐다. 나머지 **28건은 검증되지 않았다** — 기각도 승인도 아닌 미판정이다. 그 28건 안에 통과 가능한 것이 있을 수 있다.

> **확신도**: `CONFIRMED` 는 1차 출처로 확인, `LIKELY` 는 기전 논증, `UNVERIFIED` 는 미확인이다. 원 조사자가 스스로 매긴 값이며 재판정 대상이다.

---

## 1. 전체 후보 요약

| # | 이름 | 위험 | 확신 | 원천쓰기 | 검증 |
|---|---|---|---|---|---|
| 1 | PROF-5 · CPU_PER_SESSION / COMPOSITE_LIMIT — **요청하지 않는다(반권고)** | HIGH | CONFIRMED | 아니오 | 미검증 |
| 2 | SELECT_CATALOG_ROLE — 요청하지 말 것 (권고: 철회) | HIGH | LIKELY | 아니오 | **적대적 검증 → 기각** |
| 3 | W-S4 — SYNC_COMMIT_GUARD 등록 → 요청 불가. 권한 문제가 아니라 Oracle 에 그런 기전이 없다 | HIGH | CONFIRMED | 예 | 미검증 |
| 4 | [요청하지 않는다] primary UNDO_RETENTION 상향 / RETENTION GUARANTEE 설정 | HIGH | CONFIRMED | 예 | 미검증 |
| 5 | NET-1 · 리스너 접속률 제한 — 요청이 아니라 '언제든 걸어도 좋다'는 동의 표명 | MEDIUM | LIKELY | 아니오 | 미검증 |
| 6 | PROF-4 · LOGICAL_READS_PER_SESSION (폭주 가드 — 스로틀이 아니다) | MEDIUM | CONFIRMED | 예 | 미검증 |
| 7 | W-S10 — 원천에 ETL 제어·커서·감사 테이블을 둔다 → 요청하지 말 것 | MEDIUM | CONFIRMED | 예 | 미검증 |
| 8 | W-S3 — GRANT CREATE JOB TO <etl_user> (우리가 DBMS_SCHEDULER 를 운영) → 요청하지 말 것 | MEDIUM | CONFIRMED | 예 | 미검증 |
| 9 | W-S5 — watermark 컬럼 인덱스 생성 (원천 DDL). 부하를 늘리는 게 아니라 줄이는 유일한 요청이다 | MEDIUM | LIKELY | 예 | 미검증 |
| 10 | EXECUTE ON SYS.DBMS_FLASHBACK — SCN 원점 복구 + SQL 재작성 제거 | LOW | LIKELY | 아니오 | **적대적 검증 → 기각** |
| 11 | FLASHBACK ANY TABLE (시스템 권한) — 규모 때문에 오히려 이쪽이 덜 침습적일 수 있다 | LOW | LIKELY | 아니오 | **적대적 검증 → 기각** |
| 12 | FLASHBACK 객체 권한 (열거된 테이블 한정) — 1순위 요청 | LOW | LIKELY | 아니오 | **적대적 검증 → 기각** |
| 13 | PROF-1 · ETL 전용 프로파일 + SESSIONS_PER_USER (하드 접속 상한) | LOW | CONFIRMED | 예 | 미검증 |
| 14 | PROF-2 · CONNECT_TIME (세션 수명 상한) — **이전 검토 결론의 정정** | LOW | CONFIRMED | 예 | 미검증 |
| 15 | PROF-3 · IDLE_TIME (유휴·좀비 세션 회수) | LOW | CONFIRMED | 예 | 미검증 |
| 16 | RM-1 · ETL 전용 Resource Manager consumer group 배치 (CPU 상한 + 활성세션 큐잉) | LOW | LIKELY | 예 | **적대적 검증 → 기각** |
| 17 | RM-3 · 폭주 문장 자동 취소 — LOG_ONLY 로 먼저 관측, 그 다음 CANCEL_SQL | LOW | LIKELY | 예 | 미검증 |
| 18 | V$ARCHIVE_DEST_STATUS (SYNCHRONIZED · GAP_STATUS — SYNC WITH PRIMARY 가용성 자가 판정 | LOW | LIKELY | 아니오 | **적대적 검증 → 기각** |
| 19 | V$DATABASE (DBID · RESETLOGS_CHANGE# · OPEN_MODE — identity fence 복구) | LOW | CONFIRMED | 아니오 | 미검증 |
| 20 | V$DATAGUARD_STATS (apply lag / transport lag 직접 읽기) | LOW | CONFIRMED | 아니오 | 미검증 |
| 21 | W-S1 — 원천 heartbeat: DBA 가 소유·갱신하고 우리는 SELECT 만 (heartbeat 를 요청하는 유일하게 옳은 형태) | LOW | LIKELY | 예 | 미검증 |
| 22 | W-S2 — ETL 소유 heartbeat 테이블 + 우리 플랫폼이 JDBC 로 직접 갱신 (DBA 가 job 운영을 거부할 때의 변형) | LOW | LIKELY | 예 | 미검증 |
| 23 | W-S6 — 기존 상시 갱신 테이블을 passive fence witness 로 (쓰기 0, 최우선 시도) | LOW | LIKELY | 아니오 | **적대적 검증 → 기각** |
| 24 | W-S8 — 같은 CDB 안의 ETL 전용 PDB 에 heartbeat 를 둔다 (생산 PDB 에 객체 0) | LOW | UNVERIFIED | 예 | **적대적 검증 → 기각** |
| 25 | INFO-1 · primary UNDO_RETENTION 값 통보 — standby 읽기가 primary 에 닿는 **유일하게 문서화된 경로 | NONE | CONFIRMED | 아니오 | 미검증 |
| 26 | INFO-2 · standby PROCESSES/SESSIONS 헤드룸 통보 + 정시 burst 500 의 우리 쪽 상한 선언 | NONE | CONFIRMED | 아니오 | 미검증 |
| 27 | INFO-3 · apply 간섭에 대한 정직한 상태 표기 + apply lag 회신 요청 | NONE | UNVERIFIED | 아니오 | **적대적 검증 → 기각** |
| 28 | RM-2 · 병렬도 상한 PARALLEL_DEGREE_LIMIT_P1 = 1 (Spark×Oracle 곱셈 차단, 서버측) | NONE | LIKELY | 예 | 미검증 |
| 29 | SELF-1 · 세션 프리앰블에서 우리가 먼저 병렬을 끈다 (권한 불필요) | NONE | CONFIRMED | 아니오 | 미검증 |
| 30 | SELF-2 · Job 등록 시 ALL_TABLES 로 DEGREE·BLOCKS 사전 심사 (권한 불필요, 이미 가진 것) | NONE | CONFIRMED | 아니오 | 미검증 |
| 31 | SVC-1 · standby role-based 전용 서비스 (DBA 에게 킬스위치를 준다) | NONE | CONFIRMED | 아니오 | 미검증 |
| 32 | V$RECOVERY_PROGRESS (Last Applied Redo SCN — 조건부 가치) | NONE | CONFIRMED | 아니오 | 미검증 |
| 33 | V$STANDBY_EVENT_HISTOGRAM (apply lag 분포 — SLO 를 술어에서 통계로) | NONE | CONFIRMED | 아니오 | 미검증 |
| 34 | W-S7 — ADG_REDIRECT_DML 현황 확인 + ETL 계정의 DML 권한 부재 확인 (권한 요청이 아니라 이번에 새로 드러난 안전 | NONE | CONFIRMED | 아니오 | 미검증 |
| 35 | W-S9 — DG 구성 사실 통보 (SYNC transport / 보호 모드 / real-time apply). 쓰기 0, 승인 확률 최고, | NONE | CONFIRMED | 아니오 | 미검증 |
| 36 | undo 보존 실측치 3항 통보 (권한 아님 — 값 회신) | NONE | CONFIRMED | 아니오 | 미검증 |
| 37 | 모니터 읽기 예산 약정 (권한 아님 — 권한 요청서에 첨부하는 자기 제한) | NONE | CONFIRMED | 아니오 | 미검증 |

---

## 2. 후보별 상세

### 1. PROF-5 · CPU_PER_SESSION / COMPOSITE_LIMIT — **요청하지 않는다(반권고)**

- **위험도** HIGH · **확신도** CONFIRMED · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- 이 문장을 DBA 에게 제안하지 마라:
--   ALTER PROFILE ETL_RO_PROFILE LIMIT CPU_PER_SESSION <cs>;
--   ALTER PROFILE ETL_RO_PROFILE LIMIT COMPOSITE_LIMIT <su>;
-- 같은 목적을 RM-1 이 '죽이지 않고 양보시키는' 방식으로 달성한다.
  ```
- **연다고 주장하는 것**: 없다. 오히려 잃는다. 이 항목을 목록에 넣는 이유는 **우리가 무엇을 요청하지 않았는지가 요청 목록의 신뢰를 만들기 때문**이다. '자원 제한이니까 다 걸어달라'가 아니라 각각이 무엇을 하는지 알고 골랐다는 증거다.
- **부하 기전**: 기전 자체는 무해하다 — CPU 시간 누적 카운터 비교, 추가 IO 0. 문제는 **의미**다. CPU_PER_SESSION 은 세션을 느리게 하지 않고 죽인다(ORA-02392, "you are being logged off"). 대용량 Full 스캔은 정의상 CPU 를 많이 쓰므로, 정상 동작과 폭주를 이 축으로는 구분할 수 없다. Resource Manager 의 MGMT_P1/UTILIZATION_LIMIT 은 동일한 보호(다른 워크로드에 CPU 를 뺏기지 않게 하기)를 **양보시키는 방식**으로 달성하고, 그쪽은 standby 가 한가하면 아무 비용도 없다. COMPOSITE_LIMIT 은 CPU·CONNECT_TIME·logical reads·PRIVATE_SGA 의 가중합이라 위반 시 어느 축이 터졌는지 사후 판별이 [...]
- **영향 범위**: 걸면 Full Job 대량 실패. DB 위험 0, 우리 위험 HIGH. COMPOSITE_LIMIT 의 경우 원인 분석 불가로 인해 임계 조정 자체가 시행착오가 된다.
- **거절 시 대안**: 해당 없음 — 요청하지 않는 항목이다. CPU 보호가 필요하면 RM-1 이 정답이고, 폭주 종료가 필요하면 RM-3(CANCEL_SQL)이 정답이다.
- **판단 근거**: 19c CREATE PROFILE — CPU_PER_SESSION "CPU time limit for a session, expressed in hundredth of seconds", COMPOSITE_LIMIT "total resource cost for a session, expressed in service units... a weighted sum of CPU_PER_SESSION, CONNECT_TIME, LOGICAL_READS_PER_SESSION, and PRIVATE_SGA" / ORA-02392 "exceeded session limit on CPU usage, you are being logged off" / Admin Guide "The currently active resource plan does not [...]
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 2. SELECT_CATALOG_ROLE — 요청하지 말 것 (권고: 철회)

- **위험도** HIGH · **확신도** LIKELY · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- 요청하지 않는다.
-- GRANT SELECT_CATALOG_ROLE TO etl_reader;   ← 이 문장을 요청서에 쓰지 마라.
--
-- 대체: 위 항목 1~5 의 객체 단위 grant 5줄로 필요한 것이 100% 커버된다.
--   GRANT SELECT ON SYS.V_$DATAGUARD_STATS         TO etl_reader;
--   GRANT SELECT ON SYS.V_$DATABASE                TO etl_reader;
--   GRANT SELECT ON SYS.V_$STANDBY_EVENT_HISTOGRAM TO etl_reader;
--   GRANT SELECT ON SYS.V_$ARCHIVE_DEST_STATUS     TO etl_reader;   -- 선택
--   GRANT SELECT ON SYS.V_$RECOVERY_PROGRESS       TO etl_reader;   -- 후순위 별건
  ```
- **연다고 주장하는 것**: lag·상태 관측 축에서 **객체 단위 grant 5줄이 주지 못하는 것을 하나도 추가하지 않는다.** 이 role 이 더 주는 것은 전부 우리가 필요로 하지 않는 것들이다. 따라서 '되살리는 것' 은 실질적으로 없고, 요청 비용만 크다. 굳이 적자면 부수 효과 하나: DBA_TAB_COLUMNS 등으로 schema drift 감시 범위가 넓어진다. 그러나 ETL 계정은 자기가 접근 가능한 객체에 대해 **ALL_TAB_COLUMNS / USER_TAB_COLUMNS 로 이미 무권한 확보**되어 있고(scope §2.1 기확정), ETL 이 읽지 않는 스키마의 컬럼 정보는 애초에 필요 없다.
- **부하 기전**: **부하 관점에서는 이 role 이 문제가 아니다.** 같은 V$ 뷰를 읽으면 비용은 동일하다. 문제는 전적으로 **정보 노출 범위와 감사 가능성**이며, 이 과제의 판단 기준('DB 에 부하를 주거나 **악영향**을 끼치는가')의 후자에 정면으로 걸린다. **보안 담당 DBA 가 거절할 만한가 — 거절할 만하다. 그리고 거절하는 것이 옳다.** 근거를 기전으로: 1. **V$SQL / V$SQLTEXT / V$SQL_BIND_CAPTURE 도달** — 이 standby 에서 실행된 **모든 애플리케이션의 SQL 텍스트**가 읽힌다. 운영 SQL 은 리터럴을 흔히 포함하므로, ETL 계정이 접근 권한이 없는 스키마의 실제 데이터 값(주민번호·계좌·제품 스펙 등)이 SQL 텍스트를 타고 ETL 로그·메트릭 파이프라인으로 흘러나갈 수 [...]
- **영향 범위**: **요청 자체가 협상을 망칠 위험이 실질적이다.** 이번 요청의 전제는 '우리가 무엇을 얼마나 읽는지 기전으로 설명할 수 있다' 이고, 그 신뢰가 항목 1~3 을 통과시킨다. SELECT_CATALOG_ROLE 을 같은 목록에 넣는 순간 심사자에게 보내는 신호가 '이 팀은 필요 범위를 특정하지 못했다' 로 바뀌고, **1~3번까지 같이 반려될 확률이 올라간다.** 이것이 이 항목의 실제 blast radius다. 승인된 경우의 blast radius: 위 6개 사유가 전부 현실화된다. 특히 (1)은 원천 DB 의 데이터가 ETL 플랫폼의 관측 채널로 새는 경로를 만들고, 이는 생산라인 밀접 DB 의 보안 경계를 우리가 넓힌 것이 된다. **원천 primary 에 부하는 없지만 악영향은 있다.**
- **거절 시 대안**: 해당 없음 — **우리가 먼저 요청에서 뺀다.** 요청서에는 오히려 이렇게 명시적으로 적는다: "SELECT_CATALOG_ROLE 은 요청하지 않습니다. 이 role 은 V$SQL 계열을 통한 타 애플리케이션 SQL 텍스트 노출과 DBA_HIST_* 라이선스 노출을 포함하며, 저희가 필요로 하는 범위를 훨씬 초과합니다. 아래 객체 단위 grant N줄로 충분하며, 이 목록이 저희가 읽는 것의 전부입니다." **이 문장 자체가 협상 자산이다.** 심사자에게 '요청자가 role 과 객체 grant 의 차이를 알고 스스로 좁혔다' 를 보여주며, 항목 1~3 의 통과 확률을 실질적으로 올린다. 만약 DBA 측이 먼저 '개별 grant 5개 관리하기 번거로우니 role 로 주겠다' 고 제안하면 — **거절하지 말되, 전용 role 을 새로 [...]
- **판단 근거**: Oracle 19c Reference: 'queried only by users with SYSDBA … SELECT ANY DICTIONARY … SELECT_CATALOG_ROLE' (scope 문서 §2.2 인용과 일치). SELECT_CATALOG_ROLE 이 SELECT ANY DICTIONARY 보다 좁고(패키지 본문·트리거 소스 미포함) DBA_% 와 V$% 조회 권한을 묶은 role 이라는 점은 **2차 출처(커뮤니티·블로그)에서만 확인했고 Oracle 문서에서 객체 목록을 열거한 페이지를 찾지 못했다.** V$SQL/V$SQLTEXT 및 DBA_HIST_* 포함 여부, Diagnostics Pack 라이선스 저촉 우려도 2차 출처 기반이다 — 그래서 confidence 를 LIKELY 로 둔다. **실측이 가능하고 [...]
- **판정: 적대적 검증 → 기각.** 결론(“SELECT_CATALOG_ROLE 은 요청하지 않는다, 객체 단위 grant 로 간다”)은 옳다. 그러나 **claim 을 지탱하는 기전 4개 중 3개가 틀렸거나 이 환경(physical standby)에서 성립하지 않으며, 특히 이 과제가 명시적으로 의심하라고 한 두 축(권한 범위 정확성 / 부하 낙관)에서 모두 오류가 있다.** 그래서 claim_holds=false 로 둔다. 결론이 아니라 **요청서에 그대로 실릴 근거 문장**이 반증 대상이다. **[1] 단일 최대 결격 사유로 내세운 기전이 틀렸다 (권한 범위 오류)**

### 3. W-S4 — SYNC_COMMIT_GUARD 등록 → 요청 불가. 권한 문제가 아니라 Oracle 에 그런 기전이 없다

- **위험도** HIGH · **확신도** CONFIRMED · **원천 쓰기** 예
- GRANT
  ```sql
  -- 해당 없음. GRANT 로 만들어질 수 있는 대상이 아니다.
-- 요청서에서 이 항목을 삭제하라.
  ```
- **연다고 주장하는 것**: 아무것도 되살리지 못한다. 되살릴 것으로 기대됐던 것은 bound_kind = ENFORCED → ZERO_GAP 인데, **그 경로는 DBA 협조 여부와 무관하게 닫혀 있다.**
- **부하 기전**: 부하 논의 이전에 구현 논의가 끝난다. ENFORCED 의 정의(v1.2.3.1 6.1)는 ‘bound 를 넘는 commit 자체를 거부하는 동기식 fail-closed 장치’다. Oracle 에서 COMMIT 시점에 술어를 평가해 커밋을 거부할 수 있는 유일한 장치는 DEFERRABLE INITIALLY DEFERRED 제약이다(‘defer checking … until a COMMIT statement is submitted. If the constraint check fails, then the database returns an error and the transaction is not committed’). 그런데 CHECK 제약 조건은 ‘Calls to the functions that are not [...]
- **영향 범위**: 가장 근접한 구현 시도가 가장 위험하다. 유일한 근사치는 Resource Manager 나 주기 kill job 으로 오래된 트랜잭션의 **세션을 죽이는 것**인데, 이는 (1) 비동기라 회피 창이 남아 17장 rule 이 이미 TXN_AGE_KILL_JOB 으로 분류·기각한 부류이고, (2) ADMINISTER_RESOURCE_MANAGER 가 필요하며, (3) **생산 애플리케이션 세션을 죽여 트랜잭션을 롤백시킨다** — 이 과제가 정의한 금지선(“DB 가 흔들리면 물리적 생산이 멈춘다”)을 정면으로 밟는다. 절대 요청하지 마라.
- **거절 시 대안**: 대안 없음 — 그리고 대안이 필요 없다. **설계 쪽에서 정리하라**: v2.0 D1(ZERO_GAP 삭제)의 근거를 ‘권한이 없어서 막혔다’에서 **‘기전이 존재하지 않는다’**로 격상하고, 부록 R 의 ZERO_GAP 복원 조건을 ‘DBA 협조 시 복원 가능’이 아니라 **도달 불가**로 표기하라. 같은 이유로 Profile O(v1.2.3.1)도 ZERO_GAP 을 실제로는 발행할 수 없었다는 사실을 보존 문서에 적어야 한다. 누락은 종전대로 Data Reconciliation Audit(12.3)이 탐지·repair 한다(DETECT_AND_REPAIR).
- **판단 근거**: CHECK 제약 제한 목록과 DEFERRABLE 의 COMMIT 시점 검사는 Oracle 19c SQL Language Reference(constraint) 에서 축자 확인했다 — 이 두 줄이 결론 전체를 지탱한다. ‘COMMIT 트리거 없음’은 문서의 트리거 종류 열거에 근거한 부재 논증이라 축자 인용이 아니다(그래서 이 한 갈래만 약간 약하다). ‘트랜잭션 지속시간 강제 파라미터 없음’은 v1.2.3.1 22장이 이미 같은 결론을 적고 있어 교차 확인된다. 이것이 이번 조사에서 가장 값진 발견이다 — 요청 목록에서 한 줄을 지우는 것이 아니라 등급 체계 하나의 근거를 바꾼다.
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 4. [요청하지 않는다] primary UNDO_RETENTION 상향 / RETENTION GUARANTEE 설정

- **위험도** HIGH · **확신도** CONFIRMED · **원천 쓰기** 예
- GRANT
  ```sql
  -- 아래는 요청하지 않을 것을 명시하기 위해 적는다. DBA 에게 제안하지 마라.
-- ALTER SYSTEM SET UNDO_RETENTION = <더 큰 값> SCOPE=BOTH;        -- primary 에서
-- ALTER TABLESPACE <undo_ts> RETENTION GUARANTEE;                 -- primary 에서
-- ALTER TABLE <t> MODIFY LOB (<c>) (RETENTION MAX);               -- primary 에서
  ```
- **연다고 주장하는 것**: 긴 추출(대형 Full 60% 중 초대형 테이블, INITIAL_LOAD)에서 ORA-01555 여유. A §11.4 의 EXTRACT_EXCEEDS_UNDO_BUDGET 거부를 줄이고 retention_guarantee = true 등록을 가능하게 한다. **하지만 이번 과제의 판단 기준('DB 에 부하를 주거나 악영향을 끼치는가')에 정면으로 걸리는 유일한 항목이다.**
- **부하 기전**: 이것만 기전이 다르다. UNDO_RETENTION 을 올리면 **primary 가** 만료되지 않은 undo 를 더 오래 붙들고 있어야 한다. autoextend undo tablespace 면 파일이 자란다(디스크 소진 가능). 고정 크기면 재사용할 수 있는 undo 블록이 줄어든다. RETENTION GUARANTEE 를 켜면 여기서 한 단계 더 나아가 **보존이 신규 트랜잭션의 undo 요구보다 우선**하게 되어, undo 공간이 부족해지는 순간 primary 의 **DML 이 ORA-30036(unable to extend segment in undo tablespace)으로 실패**한다. 즉 우리가 standby 에서 오래 읽고 싶다는 이유로 생산라인의 INSERT/UPDATE 가 죽는 경로가 생긴다. LOB [...]
- **영향 범위**: **primary 직격.** 최악: undo tablespace 포화 → 생산 트랜잭션 DML 실패 → 물리 생산 정지. 이것은 '아마 괜찮다' 로 넘길 수 있는 종류가 아니며, 우리가 그 여파를 관측할 수단(V$UNDOSTAT·DBA_*)조차 없다. 요청 자체를 하지 않는 것이 옳다.
- **거절 시 대안**: 거절이 정상이고 우리가 먼저 요청하지 않는다. 우리 쪽 대응은 전부 설계로 흡수한다: (a) A §11.5 extract-once 를 Critical·대형 Source 에 강제해 Oracle 재독을 없앤다(FLASHBACK 이 승인되면 attempt 간 staging 재사용까지 성립), (b) 위 4번의 경험적 undo 지평 프로브로 얻은 관측치의 0.5 배를 추출 예산 상한으로 두고 EXTRACT_EXCEEDS_UNDO_BUDGET 으로 publish 를 막는다, (c) 예산을 넘는 Job 은 chunk 를 줄이는 게 아니라 **주기를 짧게** 해서 회차당 추출 시간을 줄인다(A §11.4 v1.2.1 정정과 같은 결론 — 같은 visible_scn 재독은 무의미하고 chunk 축소로는 덮인 undo 가 돌아오지 않는다), (d) [...]
- **판단 근거**: 1차 출처: 19c Database Reference, UNDO_RETENTION — "In Oracle Active Data Guard environments, you may want to increase the value of UNDO_RETENTION on the primary instance in order to accommodate undo retention requirements on the standby instances." 이 문장이 standby 쿼리를 위한 undo 연장이 **primary 파라미터 변경**임을 명시한다. 같은 페이지 "The UNDO_RETENTION parameter is honored only if the current undo tablespace has enough space. If an [...]
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 5. NET-1 · 리스너 접속률 제한 — 요청이 아니라 '언제든 걸어도 좋다'는 동의 표명

- **위험도** MEDIUM · **확신도** LIKELY · **원천 쓰기** 아니오
- GRANT
  ```sql
  # standby listener.ora — DBA 판단으로, 우리가 요구하지 않는다.
CONNECTION_RATE_<listener_name> = <R>      # 초당 신규 접속 상한
RATE_LIMIT = ON                             # 해당 엔드포인트에 적용
(ADDRESS=(PROTOCOL=tcp)(HOST=...)(PORT=1521)(QUEUESIZE=<Q>))
  ```
- **연다고 주장하는 것**: 우리 정시 burst 를 **DB 인스턴스 밖에서** 잘라낸다. 우리 제어면 버그로 자기 상한이 뚫려도 초과 접속이 인스턴스에 도달하지 못한다 — INFO-2 의 마비 시나리오에 대한 최후 방어선.
- **부하 기전**: 리스너 프로세스 내 카운터. DB 인스턴스 부하 0. 초과 접속은 "TNS:listener: rate limit reached" 로 즉시 거절되며 **서버 프로세스가 생성되기 전에** 끊기므로, 거절 자체의 비용이 로그온 성공 비용보다 훨씬 싸다. QUEUESIZE 는 TCP accept 백로그를 늘려 burst 를 거절 대신 대기로 흡수한다(정시 500건에는 이쪽이 더 유용하다).
- **영향 범위**: **주의: 리스너는 공용이다.** `CONNECTION_RATE_<listener_name>` 은 그 리스너 전체에 걸리므로 같은 리스너를 쓰는 다른 클라이언트도 함께 제한된다. 우리만 잡으려면 ETL 전용 엔드포인트나 전용 리스너가 필요하고, 그러면 DBA 작업량이 커진다. 그래서 이 항목의 우선순위는 낮고, **요청이 아니라 '우리는 이렇게 잘려도 재시도로 견딘다'는 동의 표명으로 제출하는 편이 낫다.** DBA 가 비상시 쓸 수 있는 카드를 하나 더 인지시키는 것이 목적이다.
- **거절 시 대안**: 우리 쪽 로그온 토큰버킷(초당 K개) + 정시 지터. 실효는 비슷하고 남에게 피해가 없으므로 **사실 이쪽이 1순위여야 한다.** NET-1 은 우리 쪽 장치가 실패했을 때를 위한 이중화일 뿐이다.
- **판단 근거**: Oracle Database Net Services Reference 19c, listener.ora 파라미터 — CONNECTION_RATE_listener_name / RATE_LIMIT / SERVICE_RATE_listener_name / QUEUESIZE 의 존재와 의미는 확인("imposing a user-specified maximum limit on the number of new connections handled by the listener every second", "Client side connection failure is reported with the 'TNS:listener: rate limit reached' error"). **정확한 파라미터 조합 문법(어느 계층에 RATE_LIMIT 을 쓰는지)은 [...]
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 6. PROF-4 · LOGICAL_READS_PER_SESSION (폭주 가드 — 스로틀이 아니다)

- **위험도** MEDIUM · **확신도** CONFIRMED · **원천 쓰기** 예
- GRANT
  ```sql
  -- **값을 실측하기 전에는 걸지 마라.** RM-3 의 LOG_ONLY 관측을 먼저 돌린다.
ALTER PROFILE ETL_RO_PROFILE LIMIT LOGICAL_READS_PER_SESSION <B>;
-- B = 관측된 정상 Full 추출 논리읽기 p99 × 3
-- LOGICAL_READS_PER_CALL 은 요청하지 마라 — 배열 fetch 크기에 따라 무용지물이거나 치명적이 되고 그 사이 안전지대가 없다.
  ```
- **연다고 주장하는 것**: Full 60% 인 우리 워크로드에서 **정상 전수 읽기와 설계 사고를 가르는 유일한 서버측 척도**. 카티션 곱, 인덱스 오선택으로 인한 nested loop 폭주, 종료 조건이 깨진 커서 — 이것들은 우리 쪽 EXPLAIN 검증을 통과할 수 있고, 통과하면 원천을 무제한으로 읽는다. 그 상한을 서버가 쥔다.
- **부하 기전**: Oracle 이 모든 세션에 대해 이미 유지하는 session logical reads 통계 카운터와의 비교. **추가 IO 0, 추가 래치 0.** 초과 시 ORA-02394 로 세션 종료. 여기서 '논리읽기'는 버퍼 캐시 히트를 포함하므로 물리 IO 상한이 아니라 **작업량 상한**이며, 그래서 캐시가 잘 도는 반복 스캔도 잡힌다.
- **영향 범위**: **B 를 틀리면 Full 적재 60% 가 전부 죽는다. 이 목록에서 우리 쪽 가용성 위험이 가장 큰 항목이다.** DB 자체 위험은 0(세션 하나가 죽을 뿐, 원천 데이터·다른 세션·primary 무영향). 그래서 순서를 강제한다: ① RM-3 의 `SWITCH_GROUP=>'LOG_ONLY'` + `SWITCH_IO_LOGICAL` 로 2~4주 관측만, ② 실측 p99 의 3배를 B 로, ③ 그 다음 집행. 값 없이 먼저 걸면 안 된다.
- **거절 시 대안**: Job 등록 시 SELF-2 의 `ALL_TABLES.BLOCKS` 로 예상 논리읽기량을 산정해 상한을 넘는 Job 을 사전 거부 + 추출 행수 상한 + chunk 단위 재시작. 사고를 막지 못하고 사후 탐지만 되며, 통계가 낡았으면 산정 자체가 과소평가된다.
- **판단 근거**: 19c CREATE PROFILE — LOGICAL_READS_PER_SESSION "Specify the permitted number of data blocks read in a session, including blocks read from memory and disk." / ORA-02394 "The current session exceeds IO usage limits; this session is being logged off." — cause 에 LOGICAL_READS_PER_SESSION 명시.
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 7. W-S10 — 원천에 ETL 제어·커서·감사 테이블을 둔다 → 요청하지 말 것

- **위험도** MEDIUM · **확신도** CONFIRMED · **원천 쓰기** 예
- GRANT
  ```sql
  -- 요청 후보였으나 기각한다. 요청서에 넣지 마라.
-- CREATE TABLE <etl>.etl_cursor / etl_run_log / ... on source
  ```
- **연다고 주장하는 것**: 아무것도. 우리는 이미 자체 Control DB 를 갖고 있고 커서·계약·증거는 전부 거기 있다. 원천에 두면 이중 기록이 되고 두 저장소의 정합을 맞추는 문제가 새로 생긴다.
- **부하 기전**: heartbeat 와 달리 **행이 계속 늘어난다.** 40,000 회차/day 를 원천에 기록하면 하루 4만 행 insert + 세그먼트 성장 + 인덱스 유지 + 그만큼의 redo·전송. heartbeat(1행 in-place update, 세그먼트 성장 0)와 비교해 부하 성격 자체가 다르다 — heartbeat 를 ‘무시할 수준’이라고 말할 수 있게 해 준 성질(고정 1블록, 무성장)이 여기엔 없다. 정시 burst 500건이 원천 insert burst 로 그대로 전이되는 것도 나쁘다.
- **영향 범위**: 테이블스페이스 성장, 보존 정책 필요, 원천 백업 대상 증가, 그리고 우리 회차 실패가 원천 쓰기 실패로 나타나는 새 결합. 얻는 게 없이 결합만 는다.
- **거절 시 대안**: 요청하지 않으므로 대안이 필요 없다. 커서·증거는 Control DB 에 그대로 둔다. **한 가지만 기억하라 — 우리 쪽에 둔 마커는 fence witness 를 대체하지 못한다.** 우리가 만든 timestamp 는 redo 스트림 밖에 있어서 apply 가 어디까지 진행됐는지에 대해 아무 정보도 담지 않는다. 무권한으로 접근 가능한 in-stream 신호는 셋뿐이다: (i) redo 로 실려 온 데이터 값(W-S6/W-S1), (ii) SCN→시각 매핑(visible_scn 을 얻을 수 없어 사용 불가), (iii) DB 자체 집행(STANDBY_MAX_DATA_DELAY/ORA-3172). ‘마커를 우리 쪽에 두자’는 선택지는 여기서 닫힌다.
- **판단 근거**: 기각 근거는 부하 기전(무성장 in-place update 대 지속 성장 insert)의 차이와, v2.0 §3 이 이미 Control DB 를 증거 저장소로 확정하고 있다는 사실이다. (ii) 가 막히는 근거는 v2.0 범위 문서 §2.2 — DBMS_FLASHBACK EXECUTE 와 V$DATABASE 가 모두 죽어 visible_scn 의 무권한 출처가 없다. 이 때문에 v1.2.3.1 이 규정한 `SELECT ts FROM etl_heartbeat AS OF SCN :visible_scn` 읽기법도 그대로는 쓸 수 없고, heartbeat 를 받더라도 **SET TRANSACTION READ ONLY 안에서 heartbeat 를 먼저 읽는 방식**으로 바꿔야 한다(같은 스냅샷 안의 heartbeat 값은 그 스냅샷의 [...]
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 8. W-S3 — GRANT CREATE JOB TO <etl_user> (우리가 DBMS_SCHEDULER 를 운영) → 요청하지 말 것

- **위험도** MEDIUM · **확신도** CONFIRMED · **원천 쓰기** 예
- GRANT
  ```sql
  -- 요청 후보였으나 기각한다. 요청서에 넣지 마라.
-- GRANT CREATE JOB TO <etl_user>;
  ```
- **연다고 주장하는 것**: W-S1/W-S2 를 넘어 새로 되살리는 것이 없다. heartbeat 갱신은 W-S2(우리 JDBC) 로 동일하게 달성되고, 그쪽이 스케줄러 오버헤드까지 없앤다.
- **부하 기전**: job slave 기동 + coordinator 관여 + 스케줄러 run-log 기록. 3초 주기라면 하루 28,800 job run 과 그만큼의 SYSAUX insert — heartbeat 본체보다 무겁다. 즉 이 권한은 부하를 **줄이지 않고 늘린다**. (참고로 스케줄러는 ‘a new instance of the job does not start until the current one completes’ 라 겹침은 없지만, 그건 폭주 방지이지 비용 절감이 아니다.)
- **영향 범위**: 부하가 아니라 권한 표면이 문제다. CREATE JOB 은 ETL 계정이 **생산 DB 안에서 임의의 PL/SQL 을 주기적으로 실행할 수 있게** 만든다. 자격증명이 유출되면 공격자는 우리 세션 수명과 무관하게 상주하는 실행 수단을 얻는다. ‘critical 하지 않은 권한’ 기준에 걸리는 항목이며, 이걸 요청 목록에 넣는 순간 목록 전체의 신뢰도가 떨어진다 — 나머지 요청이 전부 ‘읽기 1건’ 수준인데 여기만 성격이 다르다.
- **거절 시 대안**: 요청하지 않으므로 거절도 없다. 같은 목적은 W-S1(DBA 가 job 소유) 또는 W-S2(우리 JDBC 가 갱신)로 달성된다. 둘 다 이 권한보다 낫다.
- **판단 근거**: CREATE JOB 은 표준 시스템 권한이고 그 의미(임의 PL/SQL 의 주기 실행)는 다툼의 여지가 없다. 기각 근거는 Oracle 문서가 아니라 이번 과제가 준 판단 기준(‘DB 에 부하를 주거나 악영향을 끼치는가’)과 최소 범위 원칙이다. 부하보다 권한 표면에서 걸린다.
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 9. W-S5 — watermark 컬럼 인덱스 생성 (원천 DDL). 부하를 늘리는 게 아니라 줄이는 유일한 요청이다

- **위험도** MEDIUM · **확신도** LIKELY · **원천 쓰기** 예
- GRANT
  ```sql
  -- DBA 가 수행. 우리에게 권한이 오는 게 아니다. G0-0A 에서
-- wm_column.leading_valid_visible = 0 으로 나온 (테이블, 컬럼) 에 한해 개별 요청한다.
CREATE INDEX <prod_owner>.ix_<tab>_<wm> ON <prod_owner>.<tab> (<wm_column>) ONLINE;
-- 일괄 요청 금지. 테이블별로 삽입률을 DBA 가 보고 판단하게 하라.
  ```
- **연다고 주장하는 것**: 증분 적재(Append 20% + Merge 20% ≈ 하루 16,000 회차)의 `WHERE wm >= :low AND wm < :high` 가 full table scan 에서 index range scan 이 된다. 동시에 W-S6(passive witness)의 전제 조건이고, g0-0c-fence-facts.sql 의 Q1·Q2·Q4(ACK_FULL_SCAN 게이트에 막혀 있는 전수 질의)를 실행 가능하게 만든다.
- **부하 기전**: **인덱스가 없을 때의 부하가 이 문서 전체에서 가장 크다.** 증분 회차 1건 = 대상 테이블 전체 블록 스캔. 16,000 회차/day × 테이블 크기 → 이것이 플랫폼이 원천에 가하는 부하의 압도적 다수이며, heartbeat 논쟁 전체가 여기에 비하면 반올림 오차다. 인덱스가 있으면 읽는 블록이 결과 행수에 비례한다. **만드는 비용(1회성)**: ONLINE 빌드 = 테이블 1회 전수 스캔 + TEMP 정렬 + 인덱스 세그먼트 전체 크기만큼의 redo. 시작·종료 시점에 짧은 락. 큰 테이블이면 운영 시간대를 피해 잡아야 하는 실제 이벤트다. **영구 비용(이쪽이 진짜 대가다)**: 그 테이블의 모든 INSERT 와 wm 컬럼 UPDATE 가 인덱스를 하나 더 유지한다 — DML 당 수백 바이트 redo 추가. 그리고 [...]
- **영향 범위**: 이 목록에서 원천에 실제로 도달하는 유일한 항목이다. (1) 빌드 실패 시 UNUSABLE 인덱스 잔존 또는 테이블스페이스 소진. (2) 우변 leaf 경합이 실제로 발생하면 생산 INSERT 지연 → 라인 영향. (3) 되돌리기는 쉽다(DROP INDEX) — 이 점이 협상 재료다: 되돌릴 수 있는 요청이다. 삽입률이 낮은 테이블에서는 blast radius 가 사실상 0 이고, 높은 테이블에서만 위험하다 — 그래서 **테이블별 판단이 유일하게 옳은 형태**다.
- **거절 시 대안**: 세 갈래가 있고 전부 대가가 있다. (1) 그 테이블이 이미 날짜로 range partition 되어 있으면 파티션 프루닝으로 대체 — 추가 요청 0. (2) 증분을 포기하고 Full 모드로 전환 — 어차피 전수 스캔이므로 인덱스가 무의미해지고, 대가는 회차 비용 증가와 Merge/Append 등급 상실. (3) 추출 빈도를 낮춰 전수 스캔 횟수 자체를 줄인다 — 대가는 신선도. **거절당하면 (1)→(3)→(2) 순으로 내려가되, 그 테이블의 증분 회차 빈도를 반드시 함께 낮춰라.** 인덱스 없이 빈도를 유지하는 조합이 최악이다.
- **판단 근거**: 판단 근거는 적재 모드 구성비(Full 60 / Append 20 / Merge 20)와 회차 수(40,000/day), 그리고 g0-0a 가 이미 leading_valid_visible 을 측정 항목으로 갖고 있다는 사실이다 — 즉 이 요청의 모집단은 실측으로 확정된다. 단조증가 키의 우변 leaf 집중은 B-tree 의 구조적 성질이지만 실제 경합 발생 여부는 테이블별 삽입률에 달려 있어 일반화할 수 없다(그래서 LIKELY). g0-0c-fence-facts.sql 이 ACK_FULL_SCAN 게이트를 둔 것 자체가 이 위험을 이미 인지한 증거다.
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 10. EXECUTE ON SYS.DBMS_FLASHBACK — SCN 원점 복구 + SQL 재작성 제거

- **위험도** LOW · **확신도** LIKELY · **원천 쓰기** 아니오
- GRANT
  ```sql
  GRANT EXECUTE ON SYS.DBMS_FLASHBACK TO <ETL_USER>;
-- 동반 요청 금지: SELECT ANY TRANSACTION (Flashback Transaction Query 용, 우리는 안 쓴다)
  ```
- **연다고 주장하는 것**: 두 가지이고 두 번째가 재설계 문서의 평가보다 훨씬 크다. (1) GET_SYSTEM_CHANGE_NUMBER — A §11.3 이 visible_scn 의 **기본** 출처로 지정한 함수. V$ 가 전부 죽은 상태에서 SCN 을 얻는 다른 무권한 수단이 없다(V$DATABASE.CURRENT_SCN 은 애초에 A §11.3 이 standby 에서 쓰지 말라고 못박았고 조회 자체가 불가). 이게 없으면 fence 원점이 SCN_TO_TIMESTAMP/TIMESTAMP_TO_SCN 근사(약 3초 정밀도, 방향 보장 없음)로만 남는다. (2) **ENABLE_AT_SYSTEM_CHANGE_NUMBER 를 sessionInitStatement 에 넣으면 생성 SQL 을 하나도 고칠 필요가 없다.** 지금 설계는 Spark JDBC 의 [...]
- **부하 기전**: 권한 부여 시점: SYS 소유 패키지에 EXECUTE 1행. 객체 권한이지만 대상이 dictionary 패키지라 생산 테이블의 커서를 무효화하지 않는다. 실행 시점: GET_SYSTEM_CHANGE_NUMBER 는 메모리 상의 SCN 값 반환 — 블록 읽기 0, redo 0, 락 0. 호출 빈도는 계약당 1회(A §11.3: fence 는 contract 당 1회, chunk 마다 재독하지 않음)이므로 40,000 run/일 = 하루 약 40,000 호출, 정시 burst 500 = 초당 수백 회 수준의 **함수 호출**이며 I/O 를 만들지 않는다. ENABLE_AT_SYSTEM_CHANGE_NUMBER 는 세션 스냅샷 SCN 을 세팅하는 세션 로컬 연산이고, 이후 SELECT 의 실제 부하는 1번 항목의 CR 기전과 동일하다.
- **영향 범위**: 쓰기 위험이 실질적으로 없다. 패키지에 유일한 파괴적 프로시저인 TRANSACTION_BACKOUT 은 (a) DML 을 수행하므로 read-only 로 열린 physical standby 에서 실행 불가이고, (b) Development Guide §20.2.3 이 요구하는 **supplemental logging(minimal + PRIMARY KEY)** 이 primary 에 켜져 있어야 동작하며 그 설정은 우리가 요청하지 않는다. Flashback Transaction Query 는 별도로 SELECT ANY TRANSACTION 을 요구하고(§20.2.5) 우리는 그것을 요청하지 않으므로 이 grant 로 트랜잭션 메타데이터를 읽지 못한다. 남는 것은 GET_SYSTEM_CHANGE_NUMBER / [...]
- **거절 시 대안**: (1) SCN 원점: TIMESTAMP_TO_SCN(SYSTIMESTAMP - INTERVAL 'n' SECOND) 로 대체 가능하다 — 무권한이고 프로브에 이미 들어 있다(g0-0a-capability-inventory.sql: timestamp_to_scn). 단 약 3초 매핑 정밀도의 근사이고 방향 보장이 없으므로 fence 하한 witness 로 승격하면 안 된다(프로브 주석의 경고 그대로). 보수 방향으로 여유를 더해 쓰는 것은 가능. (2) 세션 단위 flashback: 대안 없음 — 1번/2번 권한이 있으면 AS OF SCN 텍스트 주입으로 하고, 생성 SQL 래핑 규약(항상 SELECT ... FROM (<user_sql>) 형태가 아니라 FROM 절 객체마다 AS OF 를 붙여야 함)의 검증 부담을 그대로 진다. [...]
- **판단 근거**: CONFIRMED(1차 출처): 19c PL/SQL Packages and Types Reference §80.2 Security Model — "To use the DBMS_FLASHBACK package, you must have the EXECUTE privilege on it." §80.8.4 GET_SYSTEM_CHANGE_NUMBER — "returns the current SCN as an Oracle number datatype". §80.8.2 ENABLE_AT_SYSTEM_CHANGE_NUMBER — "sets the session snapshot to the specified number. In the Flashback mode, all queries return data consistent as of the [...]
- **판정: 적대적 검증 → 기각.** 기본 입장대로 반증한다. 결론: **claim_holds = false.** 권한 이름은 맞지만, ⑴ 값 (1) 은 더 좁은 권한으로 충분하고, ⑵ 값 (2) 의 핵심 전제가 1차 출처와 어긋나며(요청서 스스로 미확인 표기), ⑶ 부하 산식의 단위가 틀렸고, ⑷ 위험 평가가 "운영상 주의 1건" 으로 축소한 것이 실은 v2.0 증거 모델이 원리적으로 탐지할 수 없는 무증상 오데이터 경로다. ──────────────────────────────

### 11. FLASHBACK ANY TABLE (시스템 권한) — 규모 때문에 오히려 이쪽이 덜 침습적일 수 있다

- **위험도** LOW · **확신도** LIKELY · **원천 쓰기** 아니오
- GRANT
  ```sql
  GRANT FLASHBACK ANY TABLE TO <ETL_USER>;
-- 동반 요청 금지: SELECT ANY TABLE, SELECT ANY DICTIONARY, SELECT_CATALOG_ROLE, DBA
  ```
- **연다고 주장하는 것**: 위 1번과 동일한 것 전부. 추가로: (a) 대상 테이블 목록을 사전에 열거하지 않아도 되므로 신규 Job 추가(10,000개 규모, 계속 증가)마다 DBA 티켓이 필요 없어진다 — 이것이 실무상 가장 큰 차이다. (b) view·synonym 을 통해 읽는 Job 에서 base table 까지 개별 grant 를 추적해야 하는 문제가 사라진다. 객체 단위 grant 는 view 경유 시 어느 객체에 FLASHBACK 이 필요한지가 명확히 문서화되어 있지 않아 운영 중 조용히 깨질 수 있다.
- **부하 기전**: 권한 부여 시점: 시스템 권한 grant 1문 = sys.sysauth$ 1행. **특정 객체에 대한 library cache lock 을 잡지 않는다.** 반대로 객체 단위 경로는 10,000 Job 이 참조하는 수백~수천 개 테이블 각각에 GRANT 를 실행해야 하고, 그 각각이 생산라인 hot table 의 의존 커서를 무효화하며 장기 DML 뒤에서 대기할 수 있다. **부하 기전으로만 보면 grant 이벤트의 총 침습도는 객체 단위 쪽이 크다.** 실행 시점: 1번과 완전히 동일하다. 권한 검사는 parse 시점의 dictionary lookup 이며 런타임 블록 읽기 수는 객체 권한과 1비트도 다르지 않다.
- **영향 범위**: 최악의 경우도 primary 에 영향 0(1번과 같은 기전). 보안 면의 실제 노출 증분은 '시간 축' 뿐이다 — FLASHBACK ANY TABLE 은 SELECT 를 주지 않으므로, 이 계정이 지금 SELECT 권한이 없는 테이블은 AS OF 를 붙여도 ORA-00942 로 여전히 막힌다(Development Guide §20.2.5 가 FLASHBACK 과 READ/SELECT 를 **둘 다** 요구한다). 즉 '이미 읽을 수 있는 테이블의 과거를 읽을 수 있게 된다' 가 전부이고 새로 보이는 객체는 없다. SYS·AUDSYS 스키마는 권한 정의 자체에서 제외된다. 잔여 위험: 이 계정 자격증명이 유출되면 공격자가 '삭제된 과거 데이터'를 조회할 수 있다 — 현재 시점 데이터는 어차피 SELECT 로 보이므로 증분은 삭제·수정 [...]
- **거절 시 대안**: 1번(객체 단위)으로 되돌아간다. 그 경우 반드시 함께 요청할 것: (a) 신규 대상 테이블 추가 시 grant 를 처리하는 **정기 창구·SLA**(없으면 Job 추가가 DBA 티켓 지연에 묶인다), (b) grant 누락 시 우리가 조기에 알 수 있도록 JobSpec publish validator 에서 대상 테이블마다 AS OF 1행 조회로 사전 검증(ORA-01031 이면 publish 거부). 두 대안 모두 있다.
- **판단 근거**: CONFIRMED(1차 출처, 축자): 19c SQL Language Reference, Table 18-1 System Privileges — FLASHBACK ANY TABLE: "Issue a SQL Flashback Query on any table, view, or materialized view in any schema except SYS, AUDSYS. This privilege is not needed to execute the DBMS_FLASHBACK procedures." Development Guide §20.2.5 — "To allow queries on all tables, grant the FLASHBACK ANY TABLE privilege." LIKELY(추론): 'SELECT 없이는 여전히 못 [...]
- **판정: 적대적 검증 → 기각.** ## 결론: 반증됨 (claim_holds = false) 권한 이름·SYS/AUDSYS 제외·"SELECT 를 주지 않는다" 세 가지는 **1차 출처로 확인되어 맞다**. 그러나 이 요청을 정당화하는 **핵심 논거 (a)·(b) 가 둘 다 무너지고**, "primary 에 영향 0" 이 성립하지 않으며, 부하 비교가 잘못된 기준선에서 이루어졌다.

### 12. FLASHBACK 객체 권한 (열거된 테이블 한정) — 1순위 요청

- **위험도** LOW · **확신도** LIKELY · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- primary 에서 1회 실행, redo 로 standby 에 전파됨. 파일럿 대상만 열거.
GRANT FLASHBACK ON <OWNER>.<TABLE_1> TO <ETL_USER>;
GRANT FLASHBACK ON <OWNER>.<TABLE_2> TO <ETL_USER>;
-- ... (SELECT 는 이미 보유. 신규 SELECT 는 요청하지 않는다)
-- 요청하지 않는 것: FLASHBACK ARCHIVE, FLASHBACK ARCHIVE ADMINISTER, SELECT ANY TRANSACTION, 신규 SELECT
  ```
- **연다고 주장하는 것**: AS OF SCN / AS OF TIMESTAMP 복원. 설계에서 되살아나는 것 5가지: (1) A §11.4 읽기 일관성 — Spark JDBC numPartitions>1 의 N개 세션이 동일 literal SCN 을 공유해 한 시점으로 묶인다. v2.0 에서 신설된 snapshot_scope 교환(TRANSACTION_SNAPSHOT 이면 단일 세션, 병렬이면 PER_STATEMENT)이 통째로 사라진다. 40,000 run/일 · 정시 burst 500 에서 numPartitions=1 강제는 처리량 벽이다. (2) A §11.5 extract-once — 같은 contract 의 새 attempt 가 같은 visible_scn 의 staging manifest 를 재사용한다. AS OF 가 없으면 재시도마다 원천을 다시 [...]
- **부하 기전**: 권한 부여 시점: primary 에서 GRANT 1문 = data dictionary 1 트랜잭션(sys.objauth$ 1행) + 수십 바이트 redo. 데이터 블록을 읽지 않고 스캔이 없다. 단, 객체 권한 변경은 해당 객체에 대한 library cache lock 을 잡고 의존 커서를 무효화하므로, 생산라인이 초당 수백 회 실행하는 hot table 이면 직후 hard parse 가 한 번 튄다. 대상 테이블에 장기 DML 이 걸려 있으면 GRANT 자체가 library cache lock 대기로 매달릴 수 있다 → 반드시 유지보수 창에서 테이블 하나씩, DDL_LOCK_TIMEOUT 을 짧게(예: 5초) 걸고 실행하도록 요청한다. 실행 시점(핵심): AS OF SCN 조회는 **일반 consistent read 와 동일한 [...]
- **영향 범위**: 최악: 우리 추출 쿼리가 ORA-01555 로 죽는다. 그게 전부다. primary 에는 어떤 경로로도 영향이 가지 않는다 — physical standby 는 redo 를 단방향으로 받기만 하고, standby 쿼리가 primary 의 undo 재사용을 지연시키거나 primary 자원을 점유하는 채널이 존재하지 않는다. Oracle 이 ADG standby 쿼리의 undo 부족 대책으로 제시하는 유일한 방법이 '**primary 의** UNDO_RETENTION 을 올려라' 라는 사실 자체가 standby 가 스스로 보존을 연장할 수 없다는 근거다(19c Reference, UNDO_RETENTION). standby 측 잔여 위험 1건: CR 블록 복제로 standby 버퍼 캐시 압박 → redo apply 지연(apply [...]
- **거절 시 대안**: 현행 v2.0 설계가 그대로 기준선이다. (1) 일관 읽기: SET TRANSACTION READ ONLY + numPartitions=1(Critical Job) 또는 snapshot_scope=PER_STATEMENT 로 강등 공시. (2) 재시도: staging 재사용 불가 구간은 원천 재독 — 원천 부하가 늘어난다는 점을 DBA 에게 그대로 전달한다(거절의 대가가 원천 부하 증가라는 것은 정직한 협상 재료다). (3) 감사: 안정 구간 비교로 강등(U-6). (4) undo 보존 한계 자가 측정: FLASHBACK 이 거절되면 AS OF 자체를 못 쓰므로 이 측정도 불가능하다. 대안 없음.
- **판단 근거**: CONFIRMED(1차 출처): Oracle 19c Database Development Guide §20.2.5 'Granting Necessary Privileges' 원문 — "To allow access to specific objects during queries, grant FLASHBACK and either READ or SELECT privileges on those objects." 즉 FLASHBACK 은 table/view/MV 에 부여 가능한 **객체 권한**이며 SELECT 만으로는 AS OF 가 불가하다는 재설계 문서 §2.2 의 판단은 맞다. 19c SQL Language Reference FLASHBACK TABLE 페이지도 "either the FLASHBACK object privilege on [...]
- **판정: 적대적 검증 → 기각.** 권한 이름은 맞다. 그러나 (a) 이 권한만으로는 주장한 5가지 중 2개가 아예 안 열리고, (b) "새로운 실패 모드 없음"과 "primary 무영향"은 이 저장소의 자기 문서로 반증되며, (c) 안전장치로 제시한 DDL_LOCK_TIMEOUT 이 GRANT 에 적용된다는 근거가 없다. ■ 맞는 부분 (인정)

### 13. PROF-1 · ETL 전용 프로파일 + SESSIONS_PER_USER (하드 접속 상한)

- **위험도** LOW · **확신도** CONFIRMED · **원천 쓰기** 예
- GRANT
  ```sql
  -- PRIMARY 에서 1회. **DEFAULT 프로파일을 고치지 말 것**(전 계정 영향).
CREATE PROFILE ETL_RO_PROFILE LIMIT
  SESSIONS_PER_USER  <N>;        -- 우리 pool_cap 의 1.2~1.3배. RAC 면 인스턴스별 카운트임에 주의
ALTER USER <ETL_USER> PROFILE ETL_RO_PROFILE;
-- standby 인스턴스에서 RESOURCE_LIMIT 값 확인만 요청(19c 기본 TRUE, 변경 불요일 가능성이 높다)
  ```
- **연다고 주장하는 것**: v2.0 §4.3 이 '원천 집행 0'으로 못박은 세션 한도가 서버 집행으로 바뀐다. U-3("세션 절대 한도를 안 넘었다"는 증명 불가)이 **서버 발신 ORA-02391 이라는 1급 증거**로 승격되고, v2.0 §7 의 개정된 즉시 No-Go 4번(플랫폼 풀 선언 상한 초과)의 판정 근거가 자기 계측 → 서버 오류코드로 올라간다. 부록 W 2번(profile 3값 '통보' 요청)보다 강한 것을 얻는다 — 값을 아는 것이 아니라 값이 집행되는 것이다.
- **부하 기전**: **로그온 시점 1회 카운터 비교뿐이다.** Oracle 이 세션 생성 경로에서 이미 유지하는 사용자별 세션 카운트와의 정수 비교이며, 추가 블록 읽기 0, 실행 중 세션에 대한 재평가 0, 백그라운드 프로세스 신설 0. 초과 시 로그온 자체가 ORA-02391 로 거절되므로 **서버 프로세스가 생성되기 전에** 막힌다 — 즉 이 한도는 부하를 만드는 것이 아니라 부하가 생기기 직전에 잘라내는 장치다. RESOURCE_LIMIT 은 19c Reference 기준 기본값 true 이므로 인스턴스 파라미터 변경이 필요 없을 가능성이 높다.
- **영향 범위**: N 을 낮게 잡으면 정시 burst 에서 ORA-02391 이 나고 **우리 회차가 실패**한다. DB 는 아무 손상도 입지 않는다(로그온 거절은 가장 값싼 실패 모드다). 전용 프로파일이므로 다른 계정 영향 0 — DEFAULT 를 고치면 전 계정에 걸리므로 반드시 새 프로파일이어야 한다. primary 에는 프로파일 행 1개 + ALTER USER 1건(1회, ms). 프로파일은 딕셔너리이므로 이 정의는 standby 에도 그대로 복제되어 우리 접속 지점에서 집행된다.
- **거절 시 대안**: JDBC 풀 hard cap + 제어면 전역 lease. 실효는 있으나 **한도 자체가 자기 선언값**이라 U-3 은 증명 불가로 남고, 풀 누수·중복 기동·재기동 중복 같은 플랫폼 버그로 우회된다. 그리고 우리가 우리를 못 막으면 그 다음 방어선은 인스턴스 전역 PROCESSES 고갈이다(INFO-2).
- **판단 근거**: 19c SQL Language Reference CREATE PROFILE — SESSIONS_PER_USER "Specify the number of concurrent sessions to which you want to limit the user.", "To create a profile, you must have the CREATE PROFILE system privilege." / 19c Reference RESOURCE_LIMIT 기본값 true, "determines whether resource limits are enforced in database profiles", ALTER SYSTEM 으로 변경 가능·PDB 에서 변경 가능 / ORA-02391 "exceeded simultaneous [...]
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 14. PROF-2 · CONNECT_TIME (세션 수명 상한) — **이전 검토 결론의 정정**

- **위험도** LOW · **확신도** CONFIRMED · **원천 쓰기** 예
- GRANT
  ```sql
  ALTER PROFILE ETL_RO_PROFILE LIMIT CONNECT_TIME <M>;   -- 분 단위. INFO-1 에서 받은 primary UNDO_RETENTION 미만으로 잡는다.
  ```
- **연다고 주장하는 것**: **이전 검토의 "자원 제한 위반 뒤에도 COMMIT 은 허용되어 트랜잭션 나이를 제한하지 못한다"는 결론은 틀렸다.** 19c SQL Language Reference CREATE PROFILE 은 자원군을 둘로 나눠 서로 다르게 규정한다: (a) "If a user exceeds the CONNECT_TIME or IDLE_TIME session resource limit, then the database rolls back the current transaction **and ends the session**." (b) "If a user attempts to perform an operation that exceeds the limit for **other** session resources, then the database [...]
- **부하 기전**: 세션 시작 시각과 현재 시각의 비교. 추가 IO 0, 백그라운드 작업 0. 초과 시 ORA-02399 로 세션 종료 → read-consistent 스냅샷이 해제되고 그 스냅샷이 붙잡고 있던 undo 요구가 사라진다. 즉 이 항목의 부하는 음수다.
- **영향 범위**: M 을 짧게 잡으면 긴 Full 추출이 ORA-02399 로 끊긴다 → 회차 실패·재시도. 원천 데이터 무결성 위험 0(우리는 쓰지 않으므로 롤백할 것도 없다). 전용 프로파일이므로 타 계정 영향 0.
- **거절 시 대안**: JDBC `oracle.jdbc.ReadTimeout` / statement queryTimeout + attempt 단위 타임아웃. 한계가 분명하다 — **클라이언트가 끊어도 서버 세션이 남을 수 있고**(U-7 의 본질), 남은 세션이 스냅샷을 계속 물고 있으면 primary undo 압력도 계속된다. PROF-3(IDLE_TIME) 이나 DCD 가 없으면 회수 경로가 없다.
- **판단 근거**: 19c SQL Language Reference CREATE PROFILE 원문 2문장(위 인용) / ORA-02399 "exceeded maximum connect time, you are being logged off" 확인. **미확인: CONNECT_TIME 이 실행 중인 단일 장기 fetch 를 중간에 끊는지, 아니면 다음 콜 경계에서야 평가되는지 문서에 없다.** IDLE_TIME 에 대해서는 "Long-running queries and other operations are not subject to this limit" 가 명시되어 있으므로 CONNECT_TIME 도 콜 경계 평가일 가능성이 높다 → **한 번의 fetch 가 M 분보다 길면 못 끊는다**고 보수적으로 가정하라. 이 빈틈을 메우는 것이 RM-3 이다.
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 15. PROF-3 · IDLE_TIME (유휴·좀비 세션 회수)

- **위험도** LOW · **확신도** CONFIRMED · **원천 쓰기** 예
- GRANT
  ```sql
  ALTER PROFILE ETL_RO_PROFILE LIMIT IDLE_TIME <K>;   -- 분. JDBC 풀의 idleTimeout 보다 **크게** 잡는다.
  ```
- **연다고 주장하는 것**: U-7("잔존 세션 0 이므로 lease RELEASED")의 시간 기반 추정에 서버측 상한을 준다 — lease `RELEASED` 판정의 최대 대기시간을 K 분으로 못박을 수 있고, v1.2.3.1 P0-07 에서 신설한 `source session lingering` 알림의 상한이 근거를 얻는다. 풀 누수·half-open TCP 로 남은 좀비 세션이 standby 의 PROCESSES 슬롯을 영구 점유하는 경로를 차단한다(INFO-2 의 마비 시나리오 예방).
- **부하 기전**: 세션별 연속 무활동 시간과의 비교, PMON 이 주기적으로 sniper 하여 종료(ORA-02396). 추가 IO 0. 회수되는 것은 프로세스 슬롯과 PGA 이므로 순 효과는 부하 감소.
- **영향 범위**: K 가 짧으면 풀의 idle connection 이 계속 죽고 재접속이 폭발한다 — **로그온 자체가 쿼리보다 비싸다**(프로세스 생성 + 딕셔너리 읽기 + 파스). 즉 잘못 잡으면 이 항목이 부하 발생원이 된다. 그래서 K > 풀 idleTimeout 이 반드시 지켜져야 하고, 이 조건만 지키면 실제로 끊기는 것은 우리 풀이 이미 버린 좀비뿐이다.
- **거절 시 대안**: standby `sqlnet.ora` 의 `SQLNET.EXPIRE_TIME`(DCD) 현재값 **통보**만 받아도 절반은 대체된다(죽은 클라이언트 한정, 살아있지만 방치된 세션은 못 잡는다). 그것도 없으면 JDBC 풀 `maxLifetime`/`idleTimeout` 자기 관리 + lease 해제 시간 기반 추정 유지 — v2.0 의 현재 상태 그대로다.
- **판단 근거**: 19c CREATE PROFILE 원문: IDLE_TIME "Specify the permitted periods of continuous inactive time during a session, expressed in minutes. **Long-running queries and other operations are not subject to this limit.**" — **중요한 한계: IDLE_TIME 은 폭주 쿼리 대책이 아니다.** 장기 쿼리는 이걸로 못 끊는다. 오직 유휴 세션 회수용이다. ORA-02396 "exceeded maximum idle time, please connect again" 확인.
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 16. RM-1 · ETL 전용 Resource Manager consumer group 배치 (CPU 상한 + 활성세션 큐잉)

- **위험도** LOW · **확신도** LIKELY · **원천 쓰기** 예
- GRANT
  ```sql
  -- 전부 DBA 가 실행한다. ADMINISTER_RESOURCE_MANAGER 는 우리에게 주지 마라(우리가 우리 상한을 풀 수 있게 되어 제안의 취지가 무너진다).
-- [1] PRIMARY 에서 1회 — 딕셔너리 DDL 이므로 primary 에서만 가능하고 redo 로 standby 에 복제된다.
BEGIN
  DBMS_RESOURCE_MANAGER.CREATE_PENDING_AREA();
  DBMS_RESOURCE_MANAGER.CREATE_CONSUMER_GROUP(
    CONSUMER_GROUP => 'ETL_BULK_READ',
    COMMENT        => 'Spark ETL read-only extract (standby)');
  DBMS_RESOURCE_MANAGER.CREATE_PLAN_DIRECTIVE(
    PLAN                => '<STANDBY 에서 이미 활성인 플랜 이름>',   -- 새 플랜으로 갈아끼우지 말 것
    GROUP_OR_SUBPLAN    => 'ETL_BULK_READ',
    MGMT_P1             => 10,      -- 경합 시 우리가 먼저 양보
    UTILIZATION_LIMIT   => 30,      -- CPU 절대 상한 %
    ACTIVE_SESS_POOL_P1 => 40,      -- 동시 활성 세션 상한(초과분은 거부가 아니라 큐)
    QUEUEING_P1         => 600);    -- 큐 대기 상한 초
  DBMS_RESOURCE_MANAGER.SET_CONSUMER_GROUP_MAPPING(
    DBMS_RESOURCE_MANAGER.ORACLE_USER, '<ETL_USER>', 'ETL_BULK_READ');
  DBMS_RESOURCE_MANAGER.VALIDATE_PENDING_AREA();
  DBMS_RESOURCE_MANAGER.SUBMIT_PENDING_AREA();
END;
/
-- [2] 우리 계정이 받는 유일한 권한 — 이 그룹으로 '들어가는' 권한뿐이다(나가는 권한 없음).
BEGIN
  DBMS_RESOURCE_MANAGER_PRIVS.GRANT_SWITCH_CONSUMER_GROUP(
    GRANTEE_NAME => '<ETL_USER>', CONSUMER_GROUP => 'ETL_BULK_READ', GRANT_OPTION => FALSE);
END;
/
-- [3] STANDBY 인스턴스에서 (인스턴스 파라미터, primary 와 독립)
--     ALTER SYSTEM SET RESOURCE_MANAGER_PLAN='<위와 같은 플랜>' SCOPE=BOTH;
  ```
- **연다고 주장하는 것**: 공시축 `source_admission_control` 을 `SELF_LIMITED_ONLY`(v2.0 §6에서 '원천 집행 0'으로 강등된 값)에서 `DB_ENFORCED` 로 되돌린다. v2.0 §2.2 가 '원천 측 admission control 일체 = 무권한 자기 제한 수단 없음'으로 죽였던 항목이 되살아나는 유일한 경로다. 부수적으로 '우리가 standby CPU 를 포화시켜 MRP 를 굶긴다'는 시나리오가 구조적으로 불가능해진다(우리 그룹이 UTILIZATION_LIMIT 위로 못 올라간다).
- **부하 기전**: MGMT_P1/UTILIZATION_LIMIT 은 CPU 스케줄러 계층에서 동작하며 대상 테이블 블록을 추가로 한 번도 읽지 않는다. Admin Guide 원문: "The currently active resource plan does not enforce allocations until CPU usage is at 100%. If the CPU usage is below 100%, the database is not CPU-bound and hence there is no need to enforce allocations." → standby 가 한가하면 우리 워크로드는 전혀 느려지지 않고, 포화된 순간에만 우리가 먼저 물러난다. ACTIVE_SESS_POOL_P1=40 은 41번째 활성 세션을 거절하지 않고 **큐에 세운다** [...]
- **영향 범위**: 최악: [3]의 `ALTER SYSTEM SET RESOURCE_MANAGER_PLAN` 이 standby 에서 **이미 돌던 플랜을 통째로 교체**하는 것. 우리 디렉티브는 기존 활성 플랜의 하위로 추가되어야 하며 새 플랜으로 대체하면 standby 의 기존 워크로드 우선순위가 전부 바뀐다 — 이 한 줄이 이 목록에서 유일하게 남에게 피해를 줄 수 있는 문장이다. primary 에 가는 영향은 [1]의 딕셔너리 행 추가뿐이고 primary 의 활성 플랜은 건드리지 않는다. 매핑이 primary 딕셔너리에도 존재하게 되지만 ETL 계정은 primary 에 접속하지 않으며(SVC-1 참조), 접속하더라도 스로틀된 그룹에 들어가므로 방향이 안전한 쪽이다. 생산 라인 트랜잭션 경로에는 닿지 않는다.
- **거절 시 대안**: 서버측 CPU 상한을 대체할 무권한 수단은 존재하지 않는다. 우리 쪽 대안은 (a) 제어면 전역 동시 실행 lease 로 self-limit, (b) Full 적재를 정시에서 시간대 분산, (c) JDBC fetchSize 하향으로 초당 블록 소비 속도 제한. 셋 다 자기 선언값이고 플랫폼 버그(풀 누수·중복 기동)로 우회되며, U-3(세션 절대 한도 미증명)은 그대로 남는다.
- **판단 근거**: Oracle 19c Database Administrator's Guide, Managing Resources with Oracle Database Resource Manager — ADMINISTER_RESOURCE_MANAGER 필요, SET_CONSUMER_GROUP_MAPPING/GRANT_SWITCH_CONSUMER_GROUP, MGMT_P1·UTILIZATION_LIMIT·ACTIVE_SESS_POOL_P1·QUEUEING_P1 의미 전부 원문 확인. Oracle 19c Reference RESOURCE_MANAGER_PLAN — "Modifiable: ALTER SYSTEM"(ALTER SESSION 불가 → 우리가 플랜을 끌 수 없다), "If you specify a resource plan that does not [...]
- **판정: 적대적 검증 → 기각.** ## 판정: claim_holds = false. 권한 이름은 대체로 맞지만 **배치 설계가 틀렸고, 그 결과 "primary 는 안 건드린다"는 핵심 안전 주장이 성립하지 않는다.** 부하 평가도 CPU 축만 보고 있어 낙관적이다. ---

### 17. RM-3 · 폭주 문장 자동 취소 — LOG_ONLY 로 먼저 관측, 그 다음 CANCEL_SQL

- **위험도** LOW · **확신도** LIKELY · **원천 쓰기** 예
- GRANT
  ```sql
  -- 1단계: 관측 전용(아무것도 죽이지 않는다). 2~4주.
BEGIN
  DBMS_RESOURCE_MANAGER.CREATE_PENDING_AREA();
  DBMS_RESOURCE_MANAGER.UPDATE_PLAN_DIRECTIVE(
    PLAN=>'<플랜>', GROUP_OR_SUBPLAN=>'ETL_BULK_READ',
    NEW_SWITCH_GROUP      => 'LOG_ONLY',
    NEW_SWITCH_ELAPSED_TIME=> 1800,
    NEW_SWITCH_IO_LOGICAL => <B 후보>,
    NEW_SWITCH_FOR_CALL   => TRUE);
  DBMS_RESOURCE_MANAGER.VALIDATE_PENDING_AREA();
  DBMS_RESOURCE_MANAGER.SUBMIT_PENDING_AREA();
END;
/
-- 2단계: 집행. 같은 문장에서 NEW_SWITCH_GROUP => 'CANCEL_SQL' 로만 바꾼다.
-- KILL_SESSION 은 쓰지 마라 — 재접속 폭풍이 되어 부하가 오히려 늘어난다.
  ```
- **연다고 주장하는 것**: PROF-2 의 빈틈을 메운다 — 프로파일 한도가 콜 경계에서만 평가된다면 단일 장기 fetch 를 못 끊지만, `SWITCH_ELAPSED_TIME` + `SWITCH_FOR_CALL=>TRUE` 는 콜 단위 경과시간을 보고 그 **문장만** 취소한다. 세션이 살아 있으므로 재접속이 없고, 우리 회차는 실패로 기록되되 풀은 온전하다. 1단계 LOG_ONLY 는 PROF-4 의 B 값을 정하는 실측 데이터의 출처이기도 하다.
- **부하 기전**: Resource Manager 가 각 세션에 대해 이미 수집하는 elapsed time / logical IO 통계와의 비교. 추가 IO 0. 1단계 LOG_ONLY 는 위반을 기록만 하고 아무 동작도 하지 않으므로 **부하 기여가 정확히 0 이면서 DBA 에게 근거를 만들어 준다** — 이것이 이 제안 전체의 신뢰를 사는 방법이다. '얼마를 걸어야 하는지 우리도 모르니 먼저 재고 오겠다'가 '아마 이 정도면 괜찮다'보다 통과 가능성이 높다.
- **영향 범위**: 2단계에서 임계를 틀리면 정상 Full 문장이 취소된다(우리 회차 실패, 재시도). DB 위험 0 — CANCEL_SQL 은 그 콜만 취소하며 세션·트랜잭션·다른 세션에 영향이 없다. primary 딕셔너리에 디렉티브 갱신 1회.
- **거절 시 대안**: JDBC statement queryTimeout. 클라이언트 타임아웃은 서버 커서와 진행 중인 스캔을 즉시 정리한다는 보장이 없고(서버는 다음 체크포인트까지 계속 읽을 수 있다), 그 사이 원천 부하는 계속된다. 대안이 열등하다는 점을 명시하라.
- **판단 근거**: 19c Admin Guide — SWITCH_GROUP('CANCEL_SQL' / 'KILL_SESSION' / 'LOG_ONLY'), SWITCH_ELAPSED_TIME, SWITCH_IO_LOGICAL, SWITCH_FOR_CALL 전부 원문 확인. ADG standby 에서의 강제 여부는 RM-1 과 같은 미확인 사항.
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 18. V$ARCHIVE_DEST_STATUS (SYNCHRONIZED · GAP_STATUS — SYNC WITH PRIMARY 가용성 자가 판정)

- **위험도** LOW · **확신도** LIKELY · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- PRIMARY 에서 1회 실행
GRANT SELECT ON SYS.V_$ARCHIVE_DEST_STATUS TO etl_reader;

-- 사용(standby 자신의 목적지 상태): SELECT dest_id, db_unique_name, status, type,
--   database_mode, recovery_mode, protection_mode, synchronization_status,
--   synchronized, gap_status, applied_thread#, applied_seq#, error
--   FROM V$ARCHIVE_DEST_STATUS WHERE status <> 'INACTIVE';
  ```
- **연다고 주장하는 것**: **부록 W 3번(‘standby DG 구성 사실 통보’ — SYNC transport 여부·보호 모드·real-time apply)을 사람의 통보에서 런타임 자가 관측으로 바꾼다.** W3 은 '조직적으로 통과 가능성이 높은 정보 통보' 로 분류돼 있지만, 통보는 (a) 시점이 1회고 (b) 구성이 바뀌면 조용히 틀려진다. 이 뷰는 매 poll 마다 현재 값을 준다. 구체적으로 되살아나는 것: - **SYNCHRONIZATION_STATUS / SYNCHRONIZED / PROTECTION_MODE** → `ALTER SESSION SYNC WITH PRIMARY` 를 **던지기 전에** 성공 가능 여부를 안다. 문서상 이 문장은 transport 가 SYNCHRONIZED 가 아니거나 redo apply 가 비활성이면 즉시 [...]
- **부하 기전**: **읽는 것**: 아카이브 목적지 수만큼의 행. 파라미터상 최대 31, standby 실무에서는 활성 2~5행. 문서가 'runtime and configuration information … The information does not persist across an instance shutdown' 이라고 명시하므로 메모리 상주 구조이며 세그먼트가 아니다. **미확인 항목이 하나 있다 — 정직하게 적는다.** 이 뷰의 조회가 control file 접근이나 DG 프로세스 latch 를 잡는지 **Oracle 공개 문서에서 확인하지 못했다.** DG 가 열화된 상태(목적지 연결 불가, gap 해소 중)에서 이 계열 조회가 느려진다는 현장 보고가 있으나 1차 출처가 아니다. 따라서 **이 항목만 방어적으로 설계한다**: (a) 폴링 [...]
- **영향 범위**: **원천 primary: GRANT DDL 1회.** **standby 최악**: DG 열화 구간에 조회가 지연될 가능성(미확인)이 이 목록에서 유일하게 남는 실질 리스크다. 그래서 timeout·서킷브레이커를 grant 요청서에 **약정으로 명시**한다 — '이 뷰는 60초 주기, 2초 타임아웃, 세션 1개에서만 읽고, 실패 시 자동으로 5분 정지한다'. DBA 가 검증하려면 세션 하나만 보면 된다. **정보 노출**: 아카이브 목적지 이름·DB_UNIQUE_NAME·보호 모드·동기화 상태·에러 문자열. **ERROR 컬럼(VARCHAR2(256))에 DG 오류 메시지가 들어가고 여기에 호스트명·경로가 섞일 수 있다** — 인프라 정보 노출이며 업무 데이터는 아니다. 로그에 남길 때 마스킹 정책을 적용한다. **switchover [...]
- **거절 시 대안**: **거의 동등한 무권한 대체가 존재한다.** `ALTER SESSION SYNC WITH PRIMARY` 자체를 프로브로 쓴다: 모니터 세션에서 주기적으로 1회 실행 → 즉시 ORA-3173 이면 'transport 가 SYNCHRONIZED 가 아니거나 apply 가 비활성', 통과하면 '둘 다 정상'. SYNC_BARRIER 가용성 판정에 필요한 것은 정확히 이 이진 답이므로, **항목 4의 주 용도는 fallback 으로 대체된다.** 잃는 것 두 가지: (1) 두 원인(transport 미동기 vs apply 비활성)을 구분하지 못한다 — 운영자 알림의 정확도가 떨어진다. (2) **GAP_STATUS 를 대체할 수단이 없다** — redo gap 존재 여부는 다른 무권한 경로로 알 수 없다. gap 구간에 lag 값이 [...]
- **판단 근거**: Oracle 19c Reference V$ARCHIVE_DEST_STATUS: 21개 컬럼(DEST_ID·DEST_NAME·STATUS·TYPE·DATABASE_MODE·RECOVERY_MODE·PROTECTION_MODE·DESTINATION·STANDBY_LOGFILE_COUNT·STANDBY_LOGFILE_ACTIVE·ARCHIVED_THREAD#·ARCHIVED_SEQ#·APPLIED_THREAD#·APPLIED_SEQ#·ERROR·SRL·DB_UNIQUE_NAME·SYNCHRONIZATION_STATUS·SYNCHRONIZED·GAP_STATUS·CON_ID) 및 'runtime and configuration information for the archived redo log destinations. The [...]
- **판정: 적대적 검증 → 기각.** ## 결론: 반증된다. 4개 축에서 실패한다 — (A) 뷰 의미론이 standby 에서 성립하지 않음, (B) 더 좁은 객체로 충분함, (C) 부하 논거가 non sequitur 이고 최악 경로를 빠뜨림, (D) 근거로 삼은 전제 자체가 이 조직의 미확정 질문임. ---

### 19. V$DATABASE (DBID · RESETLOGS_CHANGE# · OPEN_MODE — identity fence 복구)

- **위험도** LOW · **확신도** CONFIRMED · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- PRIMARY 에서 1회 실행
GRANT SELECT ON SYS.V_$DATABASE TO etl_reader;

-- 선택(PDB identity tuple 까지 복구하려면 함께 요청)
GRANT SELECT ON SYS.V_$CONTAINERS TO etl_reader;   -- 또는 SYS.V_$PDBS
  ```
- **연다고 주장하는 것**: 이 목록에서 **설계 복구 가치가 가장 큰 항목**이다. DATABASE_ROLE 과 DB_UNIQUE_NAME 은 이미 SYS_CONTEXT('USERENV',…) 로 무권한 확보되어 있으므로 그건 덤이고, 진짜는 **USERENV 에 존재하지 않는 세 값**이다: DBID, RESETLOGS_CHANGE#, OPEN_MODE. - **DBID + RESETLOGS_CHANGE#** → scope 문서 §4.3 이 '이름을 재사용한 clone 은 구분 불가' 라고 인정하며 A 6.1 의 'clone 은 어떤 경로로도 통과하지 못한다' 를 **삭제해야 하는 문장** 으로 못박은 바로 그 구멍을 정확히 메운다. DBID 는 DB 생성 시 계산되어 모든 파일 헤더에 저장되고 RESETLOGS_CHANGE# 는 open resetlogs [...]
- **부하 기전**: **읽는 것**: 1행. 다만 다른 항목들과 성격이 다르다 — 문서 첫 문장이 'V$DATABASE displays information about the database **from the control file**' 이다. 즉 순수 SGA 구조가 아니라 control file 레코드 계열(X$KCC*)이고, control file 접근은 CF enqueue 를 공유 모드로 잠깐 잡는다(이 enqueue 부분은 Oracle 공개 문서에서 확인하지 못했다 — 현장 지식, LIKELY). 그래도 단일 행 읽기이므로 홀딩 시간은 마이크로초 단위다. **횟수가 진짜 문제다.** v1.2.3.1 §11.3 은 이 조회를 **sessionInitStatement 에 넣어 모든 물리 connection 마다** 실행하도록 규범화하고 있다. [...]
- **영향 범위**: **원천 primary: GRANT DDL 1회.** 이후 읽기는 전부 standby. **standby 최악**: 위 §11.3 의 현행 규범을 그대로 두고 grant 만 받는 경우 — 정시 burst 500 connection × sessionInitStatement 마다 control file 접근. CF enqueue 는 공유 모드 단일 행 읽기라 파괴적이진 않지만, checkpoint·archive 활동과 같은 enqueue 를 공유하므로 **이 목록에서 유일하게 '이론상 경합 가능한' 항목**이다. 그래서 위 읽기 배치를 grant 요청과 **한 세트로** 제출한다 — 권한만 받고 호출 패턴을 안 고치면 이 항목이 실제 부하를 만들 수 있는 유일한 곳이다. **정보 노출**: DB 식별자·보호 모드·아카이브 [...]
- **거절 시 대안**: **대안이 없다.** DBID·CON_UID·GUID·RESETLOGS_CHANGE# 는 SYS_CONTEXT('USERENV',…) 14속성에 존재하지 않고, ALL_*/USER_* 뷰에도 없으며, 다른 무권한 경로로 얻을 수 없다(확인 범위 안에서). 거절되면 scope §4.3 의 판정이 그대로 확정된다 — identity fence 는 **이름 tuple 뿐**이고, 이름을 재사용한 clone/재구축본은 원리적으로 구분 불가이며, A 6.1 의 '클론은 어떤 경로로도 통과하지 못한다' 는 문장은 삭제된 채로 남는다. U-9 는 '증명 불가' 목록에 영구 등재된다. OPEN_MODE 쪽만은 부분 대체가 있다: `ALTER SESSION SYNC WITH PRIMARY` 가 ORA-3173 없이 통과하면 real-time [...]
- **판단 근거**: Oracle 19c Reference V$DATABASE: 'V$DATABASE displays information about the database from the control file' 및 CURRENT_SCN 설명 'Current SCN; null if the database is not currently open. For a standby database, it is the checkpoint SCN of the mounted physical standby database during media recovery and is always less than the last applied SCN tracked in V$RECOVERY_PROGRESS.' 를 그대로 확인. DATABASE_ROLE 값 집합(SNAPSHOT [...]
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 20. V$DATAGUARD_STATS (apply lag / transport lag 직접 읽기)

- **위험도** LOW · **확신도** CONFIRMED · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- PRIMARY 에서 1회 실행. standby 는 read-only 이므로 GRANT 를 standby 에서 낼 수 없고,
-- primary 의 dictionary 변경이 redo apply 로 standby 에 전파된다.
-- ETL 계정이 PDB local user 면 primary 에서 ALTER SESSION SET CONTAINER = <PDB> 후 실행.
GRANT SELECT ON SYS.V_$DATAGUARD_STATS TO etl_reader;

-- 확인(standby 에서, ETL 계정으로):
-- SELECT name, value, unit, datum_time, time_computed, con_id
--   FROM V$DATAGUARD_STATS WHERE name IN ('apply lag','transport lag');
  ```
- **연다고 주장하는 것**: A §11.3 'lag 신호 capability 1번'(현재 v2.0 에서 사망 처리)이 그대로 되살아난다. 구체적으로: (1) Guard 6번의 SOURCE_LAG_EXCEEDED 판정이 술어가 아니라 실측값 기반으로 복귀. (2) confidence_reason = NO_LAG_SIGNAL 이 모든 계약에서 상시 켜져 있는 상태가 해소되고, DATUM_TIME / datum_stale_seconds(기본 30) 설계가 원래 의도대로 작동한다 — Oracle 문서가 'TIME_COMPUTED 와 DATUM_TIME 차이가 30초 미만이어야 apply lag 가 정확하다'고 직접 규정하므로 datum_stale_seconds=30 은 이제 근거 없는 상수가 아니라 문서 인용값이 된다. (3) 소비자 노출 문구에서 '플랫폼은 실제 [...]
- **부하 기전**: **읽는 것**: fixed table 파생 뷰에서 NAME 이 4종('apply finish time','apply lag','estimated startup time','transport lag')뿐이므로 최대 4행(CDB 면 컨테이너당). 세그먼트가 아니라 SGA 메모리 구조라 consistent gets / db block gets / physical reads / undo / redo 가 모두 0 이다. 정적 SQL 이므로 첫 실행 후 hard parse 도 없다. **핵심**: 이 쿼리는 lag 를 *계산하지 않는다*. Oracle 문서는 apply lag 가 'periodically received from the primary database' 한 데이터로 **주기적으로 미리 계산**되어 있고 TIME_COMPUTED [...]
- **영향 범위**: **원천 primary 영향: 1회성 GRANT DDL 한 줄뿐이다.** SYS.OBJAUTH$ 에 행 하나, dictionary cache invalidation, redo 수십 바이트. 실행 시점은 DBA 가 고르는 유지보수 창. 이후 ETL 의 모든 읽기는 **standby 에서만** 일어나며 primary 는 영구히 건드리지 않는다. 생산라인과 밀접한 것은 primary 이므로 이 요청의 생산 영향은 사실상 그 한 줄이다. **standby 최악**: 플랫폼 버그로 캐시가 무력화되어 run 마다 읽는 경우 → 정시에 500 세션이 4행 메모리 읽기를 동시 수행. 물리 I/O·사용자 데이터 lock·undo 소모 0. 실제 자원 압박은 '500 세션' 쪽이고 그건 이미 pool_cap(SourceSafetyEnvelope)이 [...]
- **거절 시 대안**: **STANDBY_MAX_DATA_DELAY 사다리(ladder) 프로브** — 무권한으로 lag 값을 *구간*까지 좁힌다. 모니터 세션 1개에서: P ∈ {30,60,120,300,600} 각각에 대해 `ALTER SESSION SET STANDBY_MAX_DATA_DELAY = P` 후 `SELECT /*lagprobe*/ 1 FROM <ETL 소유 소형 테이블> WHERE ROWNUM = 1`(DUAL 아님 — 실제 데이터 블록을 읽어야 한다). ORA-3172 ⟹ lag > P, 성공 ⟹ lag ≤ P. 5회 이분 탐색으로 lag 를 밴드로 확정한다. 이 fallback 은 **덤으로 U-1 의 양성 대조를 상시 제공한다** — 가장 낮은 rung 에서 주기적으로 ORA-3172 가 나오는 것 자체가 'fence 가 무장되어 [...]
- **판단 근거**: Oracle 19c Reference, V$DATAGUARD_STATS: 컬럼 SOURCE_DBID·SOURCE_DB_UNIQUE_NAME·NAME·VALUE·UNIT·TIME_COMPUTED·DATUM_TIME·CON_ID, NAME 4종, 'No rows are returned when queried on a primary database' 확인. Oracle 19c Reference, About Dynamic Performance Views: 'The actual dynamic performance views are identified by the prefix V_$. Public synonyms for these views have the prefix V$.' + 'After installation, only user SYS [...]
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 21. W-S1 — 원천 heartbeat: DBA 가 소유·갱신하고 우리는 SELECT 만 (heartbeat 를 요청하는 유일하게 옳은 형태)

- **위험도** LOW · **확신도** LIKELY · **원천 쓰기** 예
- GRANT
  ```sql
  -- DBA 가 원천 primary 에서 수행(우리는 이 객체를 만들지도 쓰지도 않는다):
CREATE TABLE <dba_schema>.etl_heartbeat (id NUMBER PRIMARY KEY, ts TIMESTAMP WITH TIME ZONE);
INSERT INTO <dba_schema>.etl_heartbeat VALUES (1, SYSTIMESTAMP); COMMIT;
-- 갱신 장치: repeat_interval 30초. 3~5초가 아니다(아래 근거).
-- 그리고 우리가 요청하는 권한은 이 한 줄이 전부다:
GRANT SELECT ON <dba_schema>.etl_heartbeat TO <etl_reader>;
  ```
- **연다고 주장하는 것**: W-S6 과 같은 것을 되살리되, 라인 정지에도 얼지 않는 witness 를 준다. W-S6 의 유일한 약점(생산 정지 = witness 정지)을 메우는 것이 이 요청의 순수 증분 가치다. 주의: **ZERO_GAP 은 이걸로도 돌아오지 않는다** — 7.2 5번은 witness 와 함께 bound_kind = ENFORCED 를 요구하고 그 근거인 SYNC_COMMIT_GUARD 는 W-S4 대로 구축 불가다.
- **부하 기전**: 1회 반복 = 1행 UPDATE + COMMIT. 변경 벡터 4개(데이터 블록 in-place / undo 블록 / undo 세그먼트 헤더 / commit 레코드) → 반복당 redo 대략 300~500 B(추정치, 측정법 아래). 테이블이 1블록이라 물리 읽기 0, 세그먼트 성장 0. DG 전송량 = 같은 redo 바이트. · 30초 주기: 2,880 txn/day → **0.9~1.4 MB/day** · 5초 주기: 17,280 txn/day → 5~9 MB/day · 3초 주기(v1.2.3.1 규정): 28,800 txn/day → 9~15 MB/day 생산라인 DB 의 자체 redo 가 하루 수십~수백 GB 인 것에 비하면 30초 주기는 10^-5 수준이다. **부피는 정직하게 무시할 수준이다.** **정직하게 무시할 수 [...]
- **영향 범위**: production 스키마에 객체 0 · 트리거 0 · DDL 0, 우리 계정의 권한 footprint 는 SELECT 1건. 원천 성능에 도달하는 경로가 사실상 없다 — 1행 테이블이라 job 이 오설정으로 폭주해도 세그먼트는 자라지 않고 상한은 LGWR write rate 다. **진짜 blast radius 는 방향이 반대다: heartbeat 가 멈추면 우리 쪽 10,000 Job 의 cutoff 가 전부 얼어붙는다.** v1.2.3.1 은 heartbeat 미독을 FENCE_UNAVAILABLE 로 fail-closed 시키는데, 그 설계면 원천의 작은 job 하나가 플랫폼 전체의 단일 장애점이 된다. **요청과 함께 설계를 바꿔라 — heartbeat 는 최적화이고, staleness 가 임계를 넘으면 자동으로 `t0 − [...]
- **거절 시 대안**: W-S6(기존 테이블 passive witness) → 그것도 없으면 v2.0 §4.1 기준선 `t0 − D − safety_lag`. **거절의 대가는 정합성이 아니라 신선도다** — 안전은 ORA-3172 가 강제하고 heartbeat 는 거기에 아무것도 더하지 않는다. 협상에서 이 문장을 그대로 쓰라: “이건 지연시간을 사는 요청이지 안전을 사는 요청이 아닙니다. 거절하시면 데이터가 최대 D초 더 오래되고, 그뿐입니다.” **단 하나의 예외**: 유휴 primary 에서 apply lag 가 정지해 ORA-3172 오탐이 나는지가 미확인이다. 그렇다면 heartbeat 는 신선도가 아니라 가용성 장치가 되고 요청의 성격이 바뀐다 → G0-0 에서 한적한 시간대에 STANDBY_MAX_DATA_DELAY 를 작은 값으로 걸어 [...]
- **판단 근거**: 설계 대상은 v1.2.3.1 11.3 · 6.1 fence_time_witness = HEARTBEAT_TABLE · 22장 3·9번. STANDBY_MAX_DATA_DELAY/ORA-3172 의 의미론은 19c Data Guard Concepts 10.2.1.2 에서 축자 확인(“queries … executed only if the apply lag is less than or equal to … Otherwise, an ORA-3172 error is returned”). 스케줄러 class-level 로깅이 job-level 을 덮는다는 서술은 구버전 Scheduler 관리 문서에서 확인했고 19c 관리 문서에서는 재확인되지 않았다 → **가정하지 말고 확인 요청 항목으로 넣어라.** redo 300~500 B 는 변경 벡터 [...]
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 22. W-S2 — ETL 소유 heartbeat 테이블 + 우리 플랫폼이 JDBC 로 직접 갱신 (DBA 가 job 운영을 거부할 때의 변형)

- **위험도** LOW · **확신도** LIKELY · **원천 쓰기** 예
- GRANT
  ```sql
  -- 읽기 계정과 분리된 전용 쓰기 계정을 만든다(읽기 계정은 어디에도 쓰기 권한을 갖지 않는다):
GRANT CREATE SESSION TO <etl_writer>;              -- 원천 primary 접속
GRANT CREATE TABLE  TO <etl_writer>;
ALTER USER <etl_writer> QUOTA 1M ON <etl_tbs>;      -- 1행 테이블. 1M 이면 남는다
-- production 스키마에 대한 INSERT/UPDATE/DELETE 는 요청하지 않는다(명시적으로 적어 두라).
GRANT SELECT ON <etl_writer>.etl_heartbeat TO <etl_reader>;
  ```
- **연다고 주장하는 것**: W-S1 과 같은 witness. 차이는 갱신 주체가 DBA 의 DBMS_SCHEDULER 가 아니라 우리 스케줄러라는 것뿐이다. 부산물: 테이블 소유자는 자기 스키마 객체에 모든 객체 권한을 자동으로 갖는다 — 따라서 나중에 DBMS_FLASHBACK EXECUTE(부록 W #4)만 받으면 `AS OF SCN` 을 이 테이블에 대해서는 FLASHBACK grant 없이 쓸 수 있다.
- **부하 기전**: 원천 primary 에 상주 세션 1개 + 30초마다 UPDATE/COMMIT 1회. redo·undo·전송량은 W-S1 과 동일(0.9~1.4 MB/day @30s). **W-S1 대비 감소**: DBMS_SCHEDULER job slave·coordinator 기동과 run-log insert 가 통째로 사라진다(위 (a) 비용 소멸). **W-S1 대비 증가**: primary 에 상시 세션 1개(세션 상한·profile 계수에 잡힌다 — 6.2 상한식의 항으로 등재해야 한다) + 우리가 primary 에 접속할 수 있어야 한다.
- **영향 범위**: production 스키마 객체 0. 쓰기는 우리 소유 1행 테이블에 국한되고 그 계정은 다른 어떤 객체에도 DML 권한이 없다. 폭주 시나리오의 상한도 W-S1 과 같다. **추가되는 위험은 두 가지다**: (1) primary 에 대한 접속 자체 — 자격증명이 유출되면 공격면이 standby 에서 primary 로 확장된다(그래서 읽기 계정과 반드시 분리하고, 이 계정의 권한을 위 4줄로 못 박는다). (2) 조직적으로 “ETL 이 primary 에 붙는다”가 권한 요청보다 큰 정치적 장벽일 수 있다 — 기술 위험보다 이쪽이 실제 거절 사유가 될 공산이 크다.
- **거절 시 대안**: W-S1(DBA 가 job 을 운영) → W-S6 → `t0 − D`. 셋 다 막히면 신선도만 D초 손해이고 설계는 성립한다.
- **판단 근거**: ‘소유자는 자기 스키마 객체의 모든 객체 권한을 자동으로 가진다’는 Oracle 19c Database Security Guide(Managing Object Privileges)에서 확인. Flashback Query 권한 요건(‘grant FLASHBACK and either READ or SELECT privileges on those objects’)은 19c Development Guide 20.2.5 에서 축자 확인. 다만 이 경로가 실효를 가지려면 visible_scn 출처가 필요한데 무권한으로는 없다(아래 W-S9 참조) — 그래서 AS OF SCN 부활은 부산물일 뿐 이 요청의 근거가 아니다.
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 23. W-S6 — 기존 상시 갱신 테이블을 passive fence witness 로 (쓰기 0, 최우선 시도)

- **위험도** LOW · **확신도** LIKELY · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- witness 로 쓸 테이블이 이미 추출 대상이면 GRANT 자체가 불요.
-- 아니라면 딱 한 줄:
GRANT SELECT ON <prod_owner>.<busy_table> TO <etl_reader>;
-- 자격 조건(둘 다 G0-0A 가 이미 측정한다):
--   (1) watermark_column_facts.timestamp_origin = DB_TRIGGER  (primary 시계여야 한다)
--   (2) wm_column.leading_valid_visible >= 1                  (인덱스 없으면 쓰지 마라)
  ```
- **연다고 주장하는 것**: 11.3 의 T_lb(fence 시각 하한 witness)를 원천에 아무것도 쓰지 않고 되살린다. cutoff 가 `high = t0 − D − safety_lag`(v2.0 §4.1 기준선)에서 `high = min(T_lb, t0) − safety_lag` 로 좁혀져, D(예: 300초)만큼 잃던 신선도를 대부분 회수한다. fence_time_witness 를 HEARTBEAT_TABLE 이 아닌 제3의 값 EXISTING_TABLE 로 등재하면 6.1 스키마 변경이 최소다.
- **부하 기전**: fence 읽기 1회 = `SELECT MAX(wm) FROM prod.tab` → wm 이 leading·VALID·VISIBLE B-tree 인덱스면 index min/max scan: root→branch→leaf 로 논리 읽기 3~4 블록, 물리 I/O 0(상시 캐시). Guard 사전검사 포함 하루 10만 회로 잡아도 40만 buffer gets/day — 생산 DB 가 초당 수백만 gets 를 하는 규모에서 계측 한계 아래다. 쓰기·redo·undo·DG 전송 전부 0. **인덱스가 없으면 이 항목은 full table scan 이 되어 정반대로 이 문서에서 가장 무거운 항목이 된다 — ACK 없이 실행 금지.**
- **영향 범위**: 최악의 경우가 읽기 부하 증가뿐이고, 그 상한은 인덱스 유무로 사전 판정된다. 원천 primary 에 도달하는 경로가 없다(standby 읽기). 생산라인 정지 경로 없음. 유일한 기능적 위험은 라인 정지(교대·주말·휴일)로 MAX(wm)이 얼어붙어 cutoff 가 전진하지 않는 것인데, 이는 누락이 아니라 정체이며 보수 방향이다 — F-13 으로 이미 모델링돼 있고 NO_SOURCE_PROGRESS + 경보로 처리한다.
- **거절 시 대안**: v2.0 §4.1 기준선 `high = t0 − D − safety_lag` 로 복귀. 대가는 신선도 최대 D초이고 정합성 손상은 없다. 거절 확률도 낮다 — 이미 SELECT 권한이 있는 테이블이면 요청 자체가 없다.
- **판단 근거**: 설계 근거는 v1.2.3.1 11.3(T_lb 의 방향 보장 논증)이 그대로 적용된다 — heartbeat 의 방향 보장은 '전용 테이블'이 아니라 '값이 redo 로 실려 온다'는 성질에서 나오므로, primary 트리거가 찍은 timestamp 를 담은 어떤 테이블이든 같은 논증이 성립한다. 인덱스·timestamp_origin 조건은 g0-0a-capability-inventory.sql 의 wm_column.leading_valid_visible 과 6.1 watermark_column_facts 가 이미 측정 항목으로 갖고 있다. LIKELY 인 이유: 각 Source 에 실제로 그런 테이블이 있는지는 실측 전이다.
- **판정: 적대적 검증 → 기각.** REFUTED on four independent grounds. Risk raised LOW → MEDIUM. === (A) The stated qualifying gate is factually wrong — G0-0A does NOT measure condition (1) ===

### 24. W-S8 — 같은 CDB 안의 ETL 전용 PDB 에 heartbeat 를 둔다 (생산 PDB 에 객체 0)

- **위험도** LOW · **확신도** UNVERIFIED · **원천 쓰기** 예
- GRANT
  ```sql
  -- 원천이 멀티테넌트 CDB 이고 그 CDB standby 를 우리가 읽을 수 있을 때만 성립.
-- 생산 PDB 에는 아무것도 만들지 않는다:
--   <etl_pdb> 안에서만  CREATE TABLE etl_heartbeat / 갱신
--   standby 에서 <etl_pdb> 로 가는 별도 커넥션 1개(모니터 세션)
GRANT CREATE SESSION, CREATE TABLE TO <etl_writer>;   -- <etl_pdb> 안에서만
ALTER USER <etl_writer> QUOTA 1M ON <etl_tbs>;
  ```
- **연다고 주장하는 것**: W-S1 과 같은 witness 를, 생산 PDB 를 전혀 건드리지 않고 얻는다. 성립 근거: redo 와 media recovery 는 CDB 단위 단일 스트림이므로, ETL PDB 의 heartbeat 가 primary 시각 T 로 보이면 그 SCN 까지의 CDB redo 가 전부 적용됐고 여기에 생산 PDB 의 redo 도 포함된다. **‘같은 redo 스트림에 실려 온다’가 witness 의 유일한 요건**이라는 점이 이 변형을 가능하게 한다.
- **부하 기전**: redo·undo·전송량은 W-S1/W-S2 와 동일(30초 주기 0.9~1.4 MB/day). 추가되는 것은 standby 쪽 커넥션 1개다 — 일반 사용자는 컨테이너를 넘나들 수 없으므로 fence 읽기용 세션을 PDB 별이 아니라 **CDB 당 1개** 더 잡아야 한다. 이 세션은 6.2 상한식의 항으로 등재해야 한다.
- **영향 범위**: 생산 PDB 기준으로는 W-S6(읽기 전용)과 같은 수준, 즉 사실상 0 이다. 그 대신 ETL PDB 가 standby 구성에 실제로 포함돼 있어야 하고, PDB 신설·복제·plug/unplug 같은 수명주기 이벤트에서 heartbeat 가 조용히 끊길 경로가 하나 늘어난다. W-S1 과 같은 강등 규칙(staleness 초과 시 `t0 − D` 자동 강등)을 반드시 함께 적용하라.
- **거절 시 대안**: W-S1 → W-S6 → `t0 − D`. 그리고 이 변형은 애초에 원천이 CDB 이고 ETL PDB 를 만들 수 있을 때만 후보다 — 조직적으로는 테이블 1개보다 PDB 1개가 더 큰 요청일 수 있으니, 이미 ETL 용 PDB 가 있는 경우에만 꺼내라.
- **판단 근거**: ‘CDB 복구가 단일 redo 스트림이라 PDB 간 적용 순서가 SCN 순으로 보장된다’는 잘 알려진 성질이지만 이번에 1차 출처로 확인하지 못했다 — PDB 별 apply 지연이 갈릴 수 있는지(예: PDB 별 recovery 분리 기능)를 확인하기 전에는 이 항목의 방향 보장을 주장하지 마라. **G0-0 에 확인 항목으로 넣고, 확인 전에는 W-S1 을 우선하라.**
- **판정: 적대적 검증 → 기각.** 기각. DB 부하 때문이 아니라, (a) 근거 논리가 fail-open 이고 (b) 이 변형이 되살리려는 기능이 이미 권한 0으로 존재하며 (c) 요청 범위가 원안 주장보다 크기 때문이다. ■ (1) 권한·기전 검증 — "같은 redo 스트림" 논거는 절반만 맞다

### 25. INFO-1 · primary UNDO_RETENTION 값 통보 — standby 읽기가 primary 에 닿는 **유일하게 문서화된 경로**

- **위험도** NONE · **확신도** CONFIRMED · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- 권한 요청이 아니다. 값 통보 요청 1건 + 그 대가로 우리가 지는 의무 1건.
-- 요청: primary 의 UNDO_RETENTION 설정값과 V$UNDOSTAT.TUNED_UNDORETENTION 최근 최댓값.
-- 대가: 우리는 추출 세션 최대 수명 R 을 그 값 미만으로 자체 고정하고,
--       PROF-2 의 CONNECT_TIME 을 R 로 잡아 **DB 가 우리를 강제로 끊게** 한다.
  ```
- **연다고 주장하는 것**: 질문 "standby 에서 읽는 것이 primary 에 주는 영향은 무엇인가?"의 실제 답. **답은 redo apply 지연이 아니라 undo 보존 압력이다.** 19c Reference UNDO_RETENTION 원문: "In Oracle Active Data Guard environments, you may want to increase the value of UNDO_RETENTION on the primary instance in order to accommodate undo retention requirements on the standby instances. This allows the primary instance to retain undo for a longer period of time to serve [...]
- **부하 기전**: 통보 자체의 부하는 0(파라미터 조회 1회). 그리고 이 항목은 **부하를 주는 요청이 아니라 부하를 주지 않겠다는 약속**이다 — 우리가 R 을 지키면 primary 는 UNDO_RETENTION 을 올릴 필요가 없고, undo tablespace 도 키울 필요가 없다. 정량화: Full 60% × 40,000 Run/일 중 장기 스캔의 수명 분포가 곧 primary 가 견뎌야 하는 undo 보존 창이다. R=600초면 primary 는 최대 10분치 undo 만 붙잡으면 된다.
- **영향 범위**: 값을 모르는 채 R 을 크게 잡으면 → primary undo tablespace 팽창 → 최악의 경우 primary 활성 트랜잭션이 undo 확보 대기 또는 unexpired undo 재사용(그러면 우리 쪽에 ORA-01555). 값을 받으면 이 경로가 닫힌다. 통보 요청 자체의 blast radius 는 0.
- **거절 시 대안**: 값 없이 R 을 보수적으로 고정한다(예: 600초, 근거 없는 플랫폼 상수임을 문서에 그대로 적는다). 그리고 ORA-01555 발생을 **우리가 R 을 어겼다는 신호**로 삼아 R 을 하향한다. 이 대안은 사후적이고 비대칭적으로 나쁘다 — ORA-01555 가 우리에게 보일 때 primary 는 이미 undo 를 오래 물고 있었고, 그 사이 생산 트랜잭션이 무엇을 겪었는지 우리는 볼 수 없다(V$UNDOSTAT 를 못 읽는다).
- **판단 근거**: Oracle Database Reference 19c, UNDO_RETENTION — ADG 문단과 best-effort 문단 모두 원문 인용 확인. 19.7 이후 추가된 자동 튜닝 문장("the system retains undo for at least the time specified in this parameter, and automatically tunes the undo retention period to satisfy the undo requirements of the queries")도 확인 — 이는 primary 가 우리 쿼리 요구에 맞춰 **자동으로 보존을 늘린다**는 뜻이므로 압력이 더 직접적이다.
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 26. INFO-2 · standby PROCESSES/SESSIONS 헤드룸 통보 + 정시 burst 500 의 우리 쪽 상한 선언

- **위험도** NONE · **확신도** CONFIRMED · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- 권한 요청이 아니다. 통보 요청 2값 + 우리 자기 규제 선언.
-- 요청: standby 인스턴스의 PROCESSES, SESSIONS 현재값과 평시 최대 사용량.
-- 선언: 우리는 그 헤드룸의 20% 이하로 제어면 전역 동시 접속 상한을 고정하고,
--       PROF-1 의 SESSIONS_PER_USER 를 그 상한값으로 잡아 달라(서버측 백스톱).
-- 추가 자기 규제(권한 불필요, 즉시 시행):
--   · 정시 정각 대신 ±N초 지터로 500건을 분산
--   · 로그온 획득을 토큰버킷으로 초당 K개 제한
--   · 커넥션을 Job 단위가 아니라 전역 풀 1개에서만 대여
  ```
- **연다고 주장하는 것**: 질문 "정시 burst 500건이 동시에 접속하면 무슨 일이 나는가? 우리 쪽 상한은 어디에 둬야 하는가?"의 답에 **근거 있는 수**를 넣는다. 지금은 pool_cap 이 근거 없는 자기 선언값이다.
- **부하 기전**: dedicated server 라면 **JDBC connection 1개 = standby OS 프로세스 1개 + PGA 할당**이다. 곱셈의 전모: 500 Run × numPartitions(4) = 2,000 세션, 여기에 Oracle DOP 8 이 붙으면 16,000 프로세스(→ RM-2/SELF-1 이 마지막 항을 1 로 만든다). 19c Reference PROCESSES 는 "the maximum number of operating system user processes that can simultaneously connect to Oracle" 이고 "Modifiable: 없음"(인스턴스 재기동 필요), SESSIONS·TRANSACTIONS 기본값이 여기서 파생되며 "should allow for all [...]
- **영향 범위**: 통보 요청 자체 0. 값을 받지 못하면 위 마비 시나리오가 열린 채로 남는다 — 그리고 그 마비의 대상은 standby 이지 primary 가 아니라는 점은 정직하게 적어야 한다(우리 세션은 primary 에 붙지 않는다. 단 SVC-1 이 없으면 role transition 시 그 전제가 깨진다).
- **거절 시 대안**: 값 없이 보수적 상한으로 시작한다(동시 접속 64, 로그온 20/초 수준) + 지터 + 전역 풀. 그리고 PROF-1 의 SESSIONS_PER_USER 를 서버측 백스톱으로 반드시 확보하라 — 그것마저 없으면 **standby 를 한 번 마비시킨 뒤에야 올바른 값을 배우게 된다.** 이 학습 방식은 생산라인 밀접 환경에서 허용되지 않는다.
- **판단 근거**: Oracle Database Reference 19c, PROCESSES — 인용 문장 전부 원문 확인(정의, 파생 파라미터, 백그라운드 프로세스 포함 문장, 수정 불가). **미확인: 프로세스 상한 초과 시의 오류 번호(통상 ORA-00020 으로 알려져 있으나 PROCESSES 문서 페이지에서 확인하지 못했다)** — DBA 에게 제출하는 문서에는 번호를 적지 말고 '인스턴스 전역 접속 거절'로만 기술하라.
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 27. INFO-3 · apply 간섭에 대한 정직한 상태 표기 + apply lag 회신 요청

- **위험도** NONE · **확신도** UNVERIFIED · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- 권한 요청이 아니다. 도입 초기 2주간 주 1회 회신 요청.
-- 요청: 우리 정시 burst 창(매시 정각 +0~+10분) 전후의
--        V$DATAGUARD_STATS 의 apply lag,
--        V$STANDBY_EVENT_HISTOGRAM (NAME='apply lag').
-- 선언: 이 값이 우리 창에서 악화되면 우리가 창을 분산하거나 동시도를 낮춘다.
  ```
- **연다고 주장하는 것**: 우리는 V$ 를 읽을 수 없으므로(v2.0 §2.2), 우리 부하가 MRP 를 굶기는지에 대한 **유일한 증거원**이다. 그리고 논거 하나를 DBA 에게 넘길 수 있다: 우리가 선언하는 `ALTER SESSION SET STANDBY_MAX_DATA_DELAY = D` 는 fence 이면서 동시에 **자동 자해 탐지기**다 — 우리가 standby 를 과부하시켜 apply 가 밀리면 우리 쿼리가 ORA-3172 로 스스로 실패한다. '우리는 apply 를 밀어내면 스스로 죽는 구조로 들어옵니다'는 협상에서 쓸 수 있는 사실이다.
- **부하 기전**: **여기가 이 부류에서 내가 확인하지 못한 지점이다.** read-only 쿼리가 redo apply(MRP)를 직접 차단하는지는 Oracle 19c Data Guard Concepts and Administration ch.10 에서 확인하지 못했다 → 미확인. 확인된 것은 둘뿐이다. (1) 우리 세션은 MRP·복구 슬레이브와 **같은 인스턴스의 CPU·IO·버퍼캐시를 공유**한다 — 자원 경합은 기전상 자명하나 문서화된 차단 규칙은 아니다(그래서 RM-1 의 MGMT_P1 이 경합 시 우리를 뒤로 보내는 것이 정확한 대책이다). (2) DG 문서가 명시한 유일한 primary 성능 영향은 DML redirection 에 대한 것이다: "Avoid running too many DML operations on Active Data [...]
- **영향 범위**: 요청 자체 0.
- **거절 시 대안**: 우리 쪽 간접 관측만 남는다 — ORA-3172 발생률을 apply lag 의 대리 지표로 쓴다. 한계가 크다: ORA-3172 는 D 를 넘었을 때만 뜨는 **술어**이지 값이 아니고(U-1), 발생하지 않았다고 apply 가 건강하다는 뜻도 아니다. 그리고 우리 워크로드가 원인인지 다른 요인인지 구분할 수 없다.
- **판단 근거**: 19c Data Guard Concepts and Administration ch.10 확인 — apply lag 정의("the difference, in elapsed time, between when the last applied change became visible on the standby and when that same change was first visible on the primary"), V$DATAGUARD_STATS / V$STANDBY_EVENT_HISTOGRAM 조회법, ORA-3173 조건("redo transport status at the standby database is not SYNCHRONIZED or ... Redo Apply is not active"), DML redirection 문장 [...]
- **판정: 적대적 검증 → 기각.** ## 판정: claim_holds = false. 다만 **틀린 이유가 이 항목이 스스로 자백한 이유가 아니다.** 이 항목은 "apply 간섭이 문서화되어 있는가"를 미확인으로 남긴 점에서 정직하고, 그 판단은 맞다(아래 ①). 무너지는 곳은 그 정직함이 아니라 **(A) 요청한 데이터가 요청한 질문에 답할 수 없다는 것, (B) "유일한 증거원" 이 거짓이고 더 좁고 더 싼 grant 가 존재한다는 것, (C) "자동 자해 탐지기" 서사가 논리적으로 성립하지 않고 자기 문서와 모순된다는 것** 이다.

### 28. RM-2 · 병렬도 상한 PARALLEL_DEGREE_LIMIT_P1 = 1 (Spark×Oracle 곱셈 차단, 서버측)

- **위험도** NONE · **확신도** LIKELY · **원천 쓰기** 예
- GRANT
  ```sql
  -- PRIMARY 에서, RM-1 의 디렉티브에 두 값만 추가한다.
BEGIN
  DBMS_RESOURCE_MANAGER.CREATE_PENDING_AREA();
  DBMS_RESOURCE_MANAGER.UPDATE_PLAN_DIRECTIVE(
    PLAN                          => '<플랜>',
    GROUP_OR_SUBPLAN              => 'ETL_BULK_READ',
    NEW_PARALLEL_DEGREE_LIMIT_P1  => 1,    -- 우리 그룹의 어떤 문장도 PX 슬레이브를 못 얻는다
    NEW_PARALLEL_SERVER_LIMIT     => 5);   -- 인스턴스 PX 풀 중 우리가 점유 가능한 %
  DBMS_RESOURCE_MANAGER.VALIDATE_PENDING_AREA();
  DBMS_RESOURCE_MANAGER.SUBMIT_PENDING_AREA();
END;
/
  ```
- **연다고 주장하는 것**: 질문에 있는 '곱셈'의 마지막 항을 서버가 1로 못박는다. 우리 쪽 절반(SELF-1)은 프리앰블 누락 한 번으로 뚫리지만 이건 안 뚫린다. bound_kind 관점에서 병렬도만큼은 SELF_DECLARED 가 아니라 ENFORCED 가 된다.
- **부하 기전**: 곱셈의 전모: Spark 가 partition 당 JDBC connection 을 열면 Run 1건 = numPartitions 개 세션. 정시 burst 500 Run × numPartitions 4 = 2000 세션. 여기에 Oracle 이 테이블의 DEGREE 속성이나 힌트로 DOP 8 을 붙이면 세션당 PX 슬레이브 8개가 추가로 뜬다 → 16,000 프로세스. PX 슬레이브 1개 = OS 프로세스 1개 + PGA 할당 + 독립 IO 스트림이고, 19c Reference PROCESSES 항목이 명시하듯 "The value for this parameter should allow for all background processes such as locks, job queue processes, and parallel [...]
- **영향 범위**: 우리 추출이 전부 직렬로 돈다 → 우리 회차 소요시간 증가. 원천 입장에서는 부하가 줄기만 한다. primary 영향 0. 다른 consumer group 영향 0(디렉티브는 그룹 단위).
- **거절 시 대안**: SELF-1(`ALTER SESSION DISABLE PARALLEL QUERY`)로 우리 세션에서 자체 차단한다. 실효는 거의 같고 권한도 필요 없다. 다만 자기 선언이라 sessionInitStatement 누락·풀 재사용 경로에서 뚫릴 수 있고, 테이블 DEGREE 속성을 이기는지도 미확인이다.
- **판단 근거**: Admin Guide 19c 원문 확인 — PARALLEL_DEGREE_LIMIT_P1 "Maximum degree of parallelism for a single operation within consumer group", PARALLEL_SERVER_LIMIT "Maximum percentage of parallel execution server pool". ADG standby 에서의 강제 여부는 RM-1 과 같은 미확인 사항.
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 29. SELF-1 · 세션 프리앰블에서 우리가 먼저 병렬을 끈다 (권한 불필요)

- **위험도** NONE · **확신도** CONFIRMED · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- GRANT 불필요. 우리 sessionInitStatement 에 추가하고, 실패하면 connection 실패로 처리한다.
ALTER SESSION DISABLE PARALLEL QUERY;
ALTER SESSION DISABLE PARALLEL DML;
ALTER SESSION DISABLE PARALLEL DDL;
  ```
- **연다고 주장하는 것**: v2.0 §3 의 세션 프리앰블(현재 5줄)에 6번째 줄이 붙는다. RM-2 가 거절되어도 곱셈의 마지막 항을 우리 손으로 1 로 만든다. **DBA 협상 테이블에 앉기 전에 이미 해 놓고 가야 하는 항목** — '우리는 이미 병렬을 끄고 들어옵니다'가 나머지 요청 전부의 신뢰 담보다.
- **부하 기전**: 세션 속성 1비트 변경. 접속당 파스 1회(왕복 1회), 대상 테이블 블록 읽기 0, 딕셔너리 쓰기 0. 이후 그 세션의 모든 문장이 직렬 실행되므로 PX 슬레이브 프로세스가 생성되지 않는다.
- **영향 범위**: 없다. 우리 쿼리가 직렬로 느려지는 것이 전부다. 원천에는 부하 감소 방향으로만 작용한다.
- **거절 시 대안**: 거절 대상이 아니다(권한 요청이 아님). 다만 DISABLE 이 테이블의 DEGREE 속성을 이기지 못한다는 프로브 결과가 나오면, 생성 SQL 의 SELECT 마다 `/*+ NO_PARALLEL */` 힌트를 강제 삽입하는 것으로 보완한다.
- **판단 근거**: Oracle 19c SQL Language Reference, ALTER SESSION Prerequisites 원문: "To enable and disable the SQL trace facility, you must have ALTER SESSION system privilege. To enable or disable resumable space allocation, you must have the RESUMABLE system privilege. You do not need any privileges to perform the other operations of this statement unless otherwise indicated." — parallel_clause 에 예외 표기 없음 → 권한 불필요 CONFIRMED. [...]
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 30. SELF-2 · Job 등록 시 ALL_TABLES 로 DEGREE·BLOCKS 사전 심사 (권한 불필요, 이미 가진 것)

- **위험도** NONE · **확신도** CONFIRMED · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- GRANT 불필요. 이미 확인된 ALL_* 접근으로 충분하다. Job 등록 시 1회 실행.
SELECT owner, table_name, degree, num_rows, blocks, last_analyzed
  FROM all_tables
 WHERE owner = :owner AND table_name = :table_name;
  ```
- **연다고 주장하는 것**: 세 가지. (1) `DEGREE > 1` 로 정의된 원천 테이블을 **사전에 특정**한다 → 그 테이블 대상 Job 에 SELF-1 프리앰블을 JobSpec validator rule 로 강제할 수 있고, SELF-1 의 미확인 지점(DISABLE 이 DEGREE 를 이기는가)이 걸리는 모집단을 유한하게 만든다. (2) `blocks` 로 **Job 등록 시점에 예상 논리읽기량을 산정**한다 → PROF-4 의 B 후보값 산출 근거이자, RM-1/RM-3 이 전부 거절되었을 때의 유일한 사전 방어선. (3) 이것이 DBA 에게 제출할 **부하 승인 심사표**의 데이터 소스다 — '이 Job 이 매 회차 standby 에서 몇 블록을 읽는가'를 Job 10,000개에 대해 표로 낼 수 있다. 요청서에 이 표를 붙이는 것과 안 붙이는 [...]
- **부하 기전**: 딕셔너리 뷰 1행 조회, Job 등록 시 1회(런타임 경로 아님). 대상 테이블 블록 읽기 0. 40,000 Run/일에는 전혀 관여하지 않는다.
- **영향 범위**: 없다.
- **거절 시 대안**: 거절 대상이 아니다. 다만 `num_rows`/`blocks` 는 옵티마이저 통계 기준이라 `last_analyzed` 가 오래됐으면 실제보다 작게 나온다 → 반드시 보수적 배수를 곱하고, `last_analyzed` 가 N일보다 오래된 테이블은 산정값을 '신뢰 없음'으로 표기하라.
- **판단 근거**: ALL_* 뷰 접근은 v2.0 §2.1 에서 이미 확인된 항목. DEGREE·BLOCKS·NUM_ROWS·LAST_ANALYZED 는 ALL_TABLES 의 표준 컬럼.
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 31. SVC-1 · standby role-based 전용 서비스 (DBA 에게 킬스위치를 준다)

- **위험도** NONE · **확신도** CONFIRMED · **원천 쓰기** 아니오
- GRANT
  ```sql
  # DBA / Grid Infrastructure 측 1회
srvctl add service -db <standby_db_unique_name> -service ETL_RO_SVC -role PHYSICAL_STANDBY
srvctl start service -db <standby_db_unique_name> -service ETL_RO_SVC
# (12c 단축형은 -s / -l PHYSICAL_STANDBY)
# 우리는 이 서비스 이름으로만 접속하고, 프리앰블에서 SYS_CONTEXT('USERENV','SERVICE_NAME') 을 단언한다.
  ```
- **연다고 주장하는 것**: 세 가지가 한 번에 살아난다. **(1) DBA 손에 킬스위치.** `srvctl stop service` 한 줄로 우리 접속만 전부 끊긴다 — 계정 잠금이나 revoke 없이, 다른 워크로드 무영향으로. v2.0 §4.3 이 '잠기면 10k Job 이 전부 정지한다'고 적은 리스크가 DBA 가 감수 가능한 형태로 바뀐다. 이것이 이 목록에서 DBA 가 가장 원할 항목이다. **(2) Resource Manager 매핑 키.** RM-1 의 매핑을 `ORACLE_USER` 대신 `SERVICE_NAME` 으로 걸 수 있어 계정을 공유하는 상황에서도 우리 세션만 정확히 잡는다. **(3) role transition 안전.** role-based 서비스는 이 DB 가 switchover/failover 로 primary 가 되는 순간 [...]
- **부하 기전**: 서비스 등록은 CRS/리스너 메타데이터이며 DB 런타임 부하 0. 접속당 서비스 이름 매칭 1회(리스너 내부). 대상 테이블·딕셔너리 읽기 0. 데이터 파일에 쓰지 않는다.
- **영향 범위**: 서비스 중지 = 우리 전면 정지(의도된 동작). DB·primary 위험 0. 기존 서비스 구성에 항목 하나가 추가될 뿐 기존 서비스의 동작은 바뀌지 않는다.
- **거절 시 대안**: 기존 공용 서비스 이름을 그대로 쓰고 프리앰블의 `DATABASE_ROLE='PHYSICAL STANDBY'` 단언에만 의존한다. **잔여 위험이 남고 대안으로 없앨 수 없다**: role transition 직후의 짧은 창에서 우리 500 세션이 이제 primary 가 된 DB 에 **로그온까지는 성공한다**(단언이 거부하므로 데이터는 읽지 않지만, 로그온 = 프로세스 생성 + 딕셔너리 읽기이며 그 자체가 생산 DB 에 대한 접속 폭주다). 이 항목이 거절되면 우리 쪽에서 role transition 감지 시 전역 서킷브레이커를 즉시 열고 백오프를 매우 크게 잡는 것 외에 방법이 없다.
- **판단 근거**: Oracle 19c RAC Administration and Deployment Guide / SRVCTL reference — `srvctl add service ... -role PHYSICAL_STANDBY`, "Services start and stop automatically after a Data Guard role transition (for example, switchover or failover) based on their roles." 확인. Admin Guide Resource Manager — SET_CONSUMER_GROUP_MAPPING 의 로그인 속성에 SERVICE_NAME 포함 확인.
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 32. V$RECOVERY_PROGRESS (Last Applied Redo SCN — 조건부 가치)

- **위험도** NONE · **확신도** CONFIRMED · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- PRIMARY 에서 1회 실행. **항목 1~3 이 승인된 뒤에 별건으로 요청한다.**
GRANT SELECT ON SYS.V_$RECOVERY_PROGRESS TO etl_reader;

-- 사용: SELECT item, units, sofar, timestamp, comments
--        FROM V$RECOVERY_PROGRESS
--       WHERE type = 'MEDIA RECOVERY'
--         AND item IN ('Last Applied Redo','Standby Apply Lag','Active Apply Rate');
  ```
- **연다고 주장하는 것**: Oracle 문서가 V$DATABASE.CURRENT_SCN 을 설명하면서 **직접 지목한 대체 출처**다 — 'always less than the last applied SCN **tracked in V$RECOVERY_PROGRESS**'. 즉 standby 에서 '적용된 SCN' 을 얻는 문서상 정규 경로는 이 뷰 하나다. 되살리는 것: - ITEM='Last Applied Redo' 의 COMMENTS 가 마지막 적용 redo 의 SCN. A §11.3 이 '보조' 로 적어둔 그 값이 정식 확보된다. - ITEM='Standby Apply Lag' 로 V$DATAGUARD_STATS 와 **독립적인 두 번째 lag 관측치**를 얻는다 — 두 값이 어긋나면 lag 신호를 DEGRADED 로 낮추는 교차검증 규칙을 만들 수 [...]
- **부하 기전**: **읽는 것**: TYPE='MEDIA RECOVERY' 의 ITEM 12종(Active Apply Rate / Average Apply Rate / Maximum Apply Rate / Redo Applied / Log Files / Last Applied Redo / Active Time / Elapsed Time / Apply Time per Log / Checkpoint Time per Log / Standby Apply Lag / Recovery ID) 정도, 즉 십수 행. 복구 진행 상황 메모리 구조이며 세그먼트·control file 이 아니다. 물리 I/O·undo·redo 0. **횟수**: 모니터 poller 가 V$DATAGUARD_STATS 와 **같은 10초 폴에 묶어 함께 읽는다** → 8,640회/일, [...]
- **영향 범위**: **원천 primary: GRANT DDL 1회.** **standby 최악**: 십수 행 메모리 읽기. 폭주해도 물리 I/O 0, lock 0, control file 접근 0. **정보 노출**: 복구 진행률·적용 속도·적용 SCN. 업무 데이터 0. 적용 SCN 은 DB 내부 논리 시계값으로, ETL 계정이 이미 ORA_ROWSCN 으로 (블록 단위지만) 같은 종류의 값을 보고 있으므로 새로운 정보 범주가 아니다. **switchover 시**: primary 에서는 MEDIA RECOVERY 행이 없어 무의미하다.
- **거절 시 대안**: 적용 SCN 에 대한 **대안은 없다.** V$DATABASE.CURRENT_SCN 은 문서가 'always less than the last applied SCN' 이라고 명시했으므로 대체재가 아니라 잘못된 값이다(그리고 ADG open 상태에서의 의미는 미확인이라 더더욱 쓰면 안 된다). ORA_ROWSCN 은 특정 행의 블록 SCN 이지 DB 의 적용 SCN 이 아니다. 두 번째 lag 관측치(교차검증)의 fallback: 항목 1의 STANDBY_MAX_DATA_DELAY 사다리가 V$DATAGUARD_STATS 와 독립적인 관측이므로, 항목 1이 통과하면 사다리를 교차검증용으로 계속 돌리는 것으로 대체된다. 실용상 충분하다. **요청 순위 5순위(최하). 1~3번과 같은 요청서에 넣지 않는다** — flashback 권한 [...]
- **판단 근거**: Oracle 19c Reference V$RECOVERY_PROGRESS: 컬럼 START_TIME(DATE)·TYPE(VARCHAR2(64))·ITEM(VARCHAR2(32))·UNITS(VARCHAR2(32))·SOFAR(NUMBER)·TOTAL(NUMBER)·TIMESTAMP(DATE)·COMMENTS(VARCHAR2(248))·CON_ID(NUMBER) 확인. TYPE='MEDIA RECOVERY' 의 ITEM 값 12종 목록 확인('Last Applied Redo','Standby Apply Lag','Active Apply Rate' 포함). COMMENTS 설명 'Miscellaneous notes; currently displays the SCN for the last applied redo' — 'currently' [...]
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 33. V$STANDBY_EVENT_HISTOGRAM (apply lag 분포 — SLO 를 술어에서 통계로)

- **위험도** NONE · **확신도** CONFIRMED · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- PRIMARY 에서 1회 실행
GRANT SELECT ON SYS.V_$STANDBY_EVENT_HISTOGRAM TO etl_reader;

-- 사용: SELECT time, unit, count, last_time_updated
--        FROM V$STANDBY_EVENT_HISTOGRAM WHERE name = 'apply lag' AND count > 0;
  ```
- **연다고 주장하는 것**: U-1('apply lag 가 X초 이내였다' 증명 불가)을 **술어에서 커버리지 통계로 승격**시킨다. 이게 이 항목의 전부이자 핵심이다. 현재 v2.0 이 소비자에게 말할 수 있는 최선은 '세션이 D 를 선언했고 ORA-3172 가 안 났다' 이며, scope 문서 스스로 '미발생은 fence 가 걸렸다는 증거가 아니다' 라고 적었다. 이 뷰는 standby 가 **apply lag 를 매초 샘플링해 버킷 카운터를 올린 누적 히스토그램**이다. 회차 시작·종료에 스냅샷을 찍어 차분하면(문서가 권장하는 사용법 그대로) 그 구간에 대해 이렇게 말할 수 있다: "이 window 동안 apply lag 는 초 단위 샘플 N 건 중 M 건에서 ≤ D 였다 (M/N = 99.97%)" 즉 freshness 를 **측정된 분포**로 공시할 [...]
- **부하 기전**: **읽는 것**: NAME='apply lag' 의 관측된 버킷 수만큼의 행. Oracle 문서 예시가 COUNT>0 인 버킷 6행이고, 실무에서도 수십 행 규모다. 순수 메모리 카운터(인스턴스 기동 이후 누적, 재기동 시 소실 — 세그먼트 아님). consistent gets / physical reads / undo / redo 전부 0. **부하 논거 중 가장 강한 것이 여기 있다: 샘플링 비용은 이미 지불되고 있다.** 문서상 physical standby 는 **누가 조회하든 말든 매초 apply lag 를 샘플링해 버킷을 증가시킨다.** 즉 이 grant 는 DB 에 새로운 작업을 **하나도** 추가하지 않는다. 이미 유지 중인 카운터를 읽는 것뿐이다. 이 문장을 요청서에 그대로 쓴다. **횟수**: 누적 카운터이므로 [...]
- **영향 범위**: **원천 primary: GRANT DDL 1회.** 그 외 primary 접촉 0. **standby 최악**: 플랫폼이 폭주해 초당 여러 번 읽어도 수십 행 메모리 스캔이다. 물리 I/O 0, 사용자 데이터 lock 0, control file 접근 0(이 뷰는 control file 계열이 아니다), undo 0. 이 목록에서 **부하 관점의 최악 시나리오가 가장 무해한 항목**이다. **정보 노출**: apply lag 버킷 카운트. 업무 데이터·PII·타 애플리케이션 정보 0. 노출되는 것은 '이 standby 의 지연이 어떤 분포였나' 하나이며, 이는 ETL 계정이 이미 STANDBY_MAX_DATA_DELAY 사다리로 측정 가능한 정보의 정밀판이다 — 즉 **새로운 정보 범주를 열지 않는다.** 이 논거도 요청서에 [...]
- **거절 시 대안**: STANDBY_MAX_DATA_DELAY 사다리(항목 1 fallback)를 시계열로 축적해 **직접 히스토그램을 만든다.** 30초마다 5 rung 을 돌리면 하루 2,880 표본으로 밴드 분포를 얻는다. 열화 정도가 크다: (a) Oracle 의 1Hz 샘플링(하루 86,400 표본) 대비 표본이 30배 적고, (b) 밴드 해상도가 rung 개수로 제한되며, (c) **DB 는 공짜로 하던 일을 우리가 쿼리 14,400회/일로 대신 하는 것**이다. 즉 이 항목을 거절하면 standby 의 실제 쿼리 부하는 줄지 않고 늘어난다. 이 대비도 요청서에 산술로 적는다. 대안이 아예 없지는 않으므로 요청 순위는 3순위. 다만 **부하 논거가 가장 깨끗하므로 1·2번과 묶어 한 번에 요청**하는 것이 유리하다 — 심사자가 '샘플링은 이미 [...]
- **판단 근거**: Oracle 19c Reference V$STANDBY_EVENT_HISTOGRAM: 컬럼 NAME(VARCHAR2(64), 현재 유효값은 'APPLY LAG' 뿐)·TIME(NUMBER)·UNIT(VARCHAR2(16))·COUNT(NUMBER)·LAST_TIME_UPDATED(VARCHAR2(20))·CON_ID(NUMBER) 확인. 'The physical standby samples the apply lag every second and increments the corresponding bucket in the histogram' — 조회와 무관한 상시 샘플링임을 문서가 직접 진술. Oracle 19c Data Guard Concepts 10장: '이 뷰는 standby 인스턴스가 마지막으로 기동된 이후의 apply lag [...]
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 34. W-S7 — ADG_REDIRECT_DML 현황 확인 + ETL 계정의 DML 권한 부재 확인 (권한 요청이 아니라 이번에 새로 드러난 안전 항목)

- **위험도** NONE · **확신도** CONFIRMED · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- 권한 요청이 아니다. 확인 요청 2건 + 우리가 자발적으로 거는 제약 1건.
--   확인 1: 원천 standby 의 ADG_REDIRECT_DML 시스템 파라미터 값
--   확인 2: <etl_reader> 가 production 스키마에 대해 갖는 INSERT/UPDATE/DELETE 권한 목록 (기대값: 공집합)
-- 우리 쪽 약속: 읽기 계정은 어떤 객체에도 DML 권한을 요청하지 않는다.
-- 우리 쪽 방어: 모든 추출 세션 프리앰블에 아래를 넣는다.
ALTER SESSION DISABLE ADG_REDIRECT_DML;
  ```
- **연다고 주장하는 것**: 되살리는 것이 아니라 **막는 것**이다. 19c 는 ADG standby 에서의 DML 을 primary 로 투명하게 재지향한다 — ‘DML operations on a standby can be transparently redirected to and run on the primary database … The Active Data Guard session waits until the corresponding changes are shipped to and applied to the Active Data Guard standby.’ 시스템 레벨로 켜져 있으면 **ETL 계정의 실수 DML 한 줄이 조용히 생산 primary 에서 실행된다.** ‘어차피 standby 는 읽기 전용이라 우리가 쓸 수 없다’는 이전 전제가 19c [...]
- **부하 기전**: 재지향된 DML 1건 = primary 왕복 실행 + 변경분이 다시 standby 로 선적·적용될 때까지 세션 블록. Oracle 문서가 직접 경고한다: ‘Avoid running too many DML operations on Active Data Guard standby databases. Because the operations are actually performed on the primary, too many DMLs may impact the performance of the primary.’ **heartbeat 를 이 경로로 구현하려는 유혹이 생길 수 있는데(‘primary 접속 없이 primary 에 쓴다’) 반대로 가라** — 반복당 비용이 W-S2 의 직접 접속보다 훨씬 크고, standby 세션에 primary [...]
- **영향 범위**: 확인·차단 행위 자체의 blast radius 는 0. 확인하지 않았을 때의 blast radius 가 크다 — 40,000 회차/day 를 도는 계정이 생산 primary 에 쓰기를 할 수 있는 상태로 방치된다. 이 항목은 요청 목록이 아니라 **거래 재료**로도 쓰인다: 우리가 W-S1/W-S2 를 요청하면서 동시에 ‘읽기 계정에는 어떤 DML 권한도 없음을 확인해 달라’를 같이 내밀면, 요청 전체가 통제를 늘리는 제안으로 읽힌다.
- **거절 시 대안**: 확인을 못 받아도 **우리 쪽 방어는 단독으로 가능하다** — 세션 프리앰블에 `ALTER SESSION DISABLE ADG_REDIRECT_DML` 을 넣는다(세션 레벨 설정이 시스템 레벨을 덮는다고 문서가 명시한다). 대안이 있고 비용이 0 이므로 이 항목은 거절당해도 잃는 것이 없다.
- **판단 근거**: 19c Data Guard Concepts and Administration ‘DML Operations on Active Data Guard Standby Databases’ 에서 축자 확인 — 재지향 동작, ADG_REDIRECT_DML 의 시스템/세션 레벨 설정과 ‘session level setting overrides the system level setting’, primary 성능 경고, XA 미지원. v2.0 범위 문서와 v1.2.3.1 어디에도 이 기능에 대한 언급이 없다 — ‘무권한이니 쓸 수 없다’는 전제 아래 아무도 확인하지 않은 구멍이다.
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 35. W-S9 — DG 구성 사실 통보 (SYNC transport / 보호 모드 / real-time apply). 쓰기 0, 승인 확률 최고, heartbeat 목적을 부분 대체

- **위험도** NONE · **확신도** CONFIRMED · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- 권한 요청이 아니라 값 통보 요청이다(부록 W #3).
-- 회신 요청 항목 4개:
--   1. redo transport = SYNC 인가 ASYNC 인가
--   2. protection mode = MAX PROTECTION / MAX AVAILABILITY / MAX PERFORMANCE
--   3. real-time apply 활성 여부
--   4. 유휴 primary 구간에서 apply lag 가 계속 갱신되는가 (ORA-3172 오탐 여부)
  ```
- **연다고 주장하는 것**: (1~3) `ALTER SESSION SYNC WITH PRIMARY` 사용 가부를 확정한다. 이건 시각 하한이 아니라 **완결성 배리어**라서, 마감 배치(Merge/Append 의 배치 경계)에 대해서는 heartbeat 보다 강하다 — heartbeat 는 ‘여기까지는 확실히 적용됐다’를 말하고, SYNC WITH PRIMARY 는 ‘발행 시점에 수신된 redo 가 전부 적용됐다’를 말한다. (4) heartbeat 요청의 성격 자체를 결정한다.
- **부하 기전**: 통보 자체는 부하 0. 통보 결과 쓰게 되는 `SYNC WITH PRIMARY` 의 부하: 세션이 apply 완료까지 블록되고 미충족 시 ORA-3173. standby 측 대기이며 primary 에 추가 작업을 만들지 않는다. **다만 배치 경계마다 호출하면 회차가 apply 진행에 직렬로 묶인다** — 10,000 Job 전부에 걸지 말고 배치 경계 Job 에만 걸어라(v2.0 §3 프리앰블 5번이 이미 그렇게 한정하고 있다). 정시 burst 500건이 동시에 이 배리어에 걸리면 대기가 겹치므로 burst 구간에서의 거동을 G1 에서 시험 항목으로 두라.
- **영향 범위**: 없음. 값을 듣는 것이다. 원천에 도달하는 경로가 존재하지 않는다.
- **거절 시 대안**: 3번(real-time apply) 을 모르면 SYNC WITH PRIMARY 는 쓸 수 있어도 그 의미가 약해지므로 쓰지 않고 `t0 − D` 만으로 간다. 4번을 모르면 **우리가 직접 관측하라** — 한적한 시간대(주말·휴일, 이 코드베이스가 이미 휴장일 개념을 갖고 있다)에 STANDBY_MAX_DATA_DELAY 를 작은 값으로 걸고 ORA-3172 발생 여부를 기록한다. 이건 권한이 필요 없는 관측이다.
- **판단 근거**: STANDBY_MAX_DATA_DELAY 3분기 거동과 ORA-3172 는 19c Data Guard Concepts and Administration 10.2.1.2 에서 축자 확인(‘set to 0 … guaranteed to return the exact same result as if the query were issued on the primary’ 포함). SYNC WITH PRIMARY / ORA-3173 은 v2.0 범위 문서가 SQL Language Reference 를 인용한 것을 그대로 따랐다(이번에 재확인하지는 않았다). 유휴 primary 의 apply lag 거동은 어느 문서에서도 확인하지 못했다 — 그래서 4번을 통보 항목으로 넣었다.
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 36. undo 보존 실측치 3항 통보 (권한 아님 — 값 회신)

- **위험도** NONE · **확신도** CONFIRMED · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- GRANT 없음. DBA 에게 값 회신만 요청한다. DBA 가 primary 에서 1회 조회:
--   SHOW PARAMETER UNDO_RETENTION
--   SELECT tablespace_name, retention FROM dba_tablespaces WHERE contents='UNDO';
--   SELECT MAX(tuned_undoretention) FROM v$undostat;                      -- 최근 7일 튜닝값
--   SELECT owner, table_name, column_name, retention_type, retention_value
--     FROM dba_lobs WHERE (owner,table_name) IN (<ETL 대상 목록>);          -- LOB 컬럼 보유 테이블만
-- 회신 항목: UNDO_RETENTION 값 · RETENTION GUARANTEE 여부 · TUNED_UNDORETENTION 최근 최댓값 · 대상 테이블 LOB RETENTION
  ```
- **연다고 주장하는 것**: A §11.4 가 SourceCapability 에 등록하라고 규정한 undo_retention_seconds · retention_guarantee 의 값. 이게 없으면 publish validator 의 rule EXTRACT_EXCEEDS_UNDO_BUDGET(예상 추출 시간 > retention × 0.5 이면 거부/경고)이 우변을 잃어 계산 불가가 되고, LOB 컬럼 보유 Job 의 ZERO_GAP/BEST_EFFORT 분기(A §11.4 v1.2.3 항)도 판정 불가가 된다. 통보만 받으면 AS OF 요청 없이도 이 두 rule 이 live 가 된다. 또한 우리가 '얼마나 긴 추출까지 안전한가' 를 스스로 좁힐 수 있어 **원천에 undo 연장을 요구하지 않아도 되게 만드는 정보**다 — DBA 에게 이 프레이밍으로 [...]
- **부하 기전**: 우리 쪽 DB 접촉 0. DBA 쪽 조회 4건, 전부 dictionary/fixed view 단건 조회로 블록 읽기 수십 단위. 정기 재확인은 분기 1회면 충분하다(UNDO_RETENTION 은 자주 바뀌는 파라미터가 아니다).
- **영향 범위**: 없음. DB 상태를 바꾸지 않고 세션도 만들지 않는다. 유일한 리스크는 값이 오래되어 실제와 어긋나는 것 — 그래서 아래 fallback 의 자가 측정을 **함께** 돌려 상시 대조한다.
- **거절 시 대안**: 있다. **경험적 undo 지평 프로브** — AS OF 권한(1 또는 2번)이 있다면 값 통보 없이 우리가 직접 측정할 수 있다: SELECT COUNT(*) FROM (SELECT 1 FROM <대상> AS OF SCN :s WHERE ROWNUM=1) 을 s = 현재 SCN 에서 뒤로 이분 탐색해 ORA-01555/ORA-08181 이 시작되는 경계를 찾는다. 부하 기전: 1회당 인덱스/테이블 블록 1개 + undo 체인 몇 블록 = 논리 읽기 10 단위 미만, 10회 이분 탐색이면 하루 100 논리 읽기 수준으로 완전히 무시 가능. Source 당 1일 1회 실행해 undo_retention_seconds 를 **관측치로** 등록하고, 통보값이 있으면 둘 중 작은 값을 쓴다(하향 전용). AS OF 권한마저 없으면 대안 없음 [...]
- **판단 근거**: 1차 출처: 19c Database Reference, UNDO_RETENTION — TUNED_UNDORETENTION 이 V$UNDOSTAT 에 있고 우리 계정으로는 조회 불가(V$ 전면 차단)라는 것이 통보를 요청하는 이유다. Development Guide §20.2.1 — "Setting UNDO_RETENTION does not guarantee that unexpired undo data is not discarded. If the system needs more space, Oracle Database can overwrite unexpired undo with more recently generated undo data." 즉 통보받은 UNDO_RETENTION 값은 상한이 아니라 하한 희망치이며, 그래서 자가 [...]
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

### 37. 모니터 읽기 예산 약정 (권한 아님 — 권한 요청서에 첨부하는 자기 제한)

- **위험도** NONE · **확신도** CONFIRMED · **원천 쓰기** 아니오
- GRANT
  ```sql
  -- 권한 요청이 아니다. 위 grant 들과 **한 장에 함께 제출하는 구속 약정**이다.
-- DBA 가 검증하려면 standby 에서 세션 하나만 보면 된다:
--   ETL 계정의 CLIENT_IDENTIFIER = 'etl-dg-monitor/<db_unique_name>' 세션이
--   인스턴스당 정확히 1개, 그 세션 외에는 어떤 V$ 조회도 발생하지 않는다.
--
-- 약정 내용:
--   1. V$ 조회 세션: standby 인스턴스당 1개. Source(스키마) 수와 무관.
--   2. 폴링 주기 상한: V$DATAGUARD_STATS 10s / V$DATABASE 10s /
--      V$RECOVERY_PROGRESS 10s(같은 폴에 병합) / V$STANDBY_EVENT_HISTOGRAM 60s /
--      V$ARCHIVE_DEST_STATUS 60s
--   3. statement timeout 2초 하드 캡. 3회 연속 실패/timeout → 5분 서킷 브레이크.
--   4. sessionInitStatement 에 V$ 조회를 넣지 않는다(SYS_CONTEXT 만).
--   5. Guard / chunk commit / Run hot path 에서 V$ 를 호출하지 않는다.
--   6. 총 V$ 쿼리 상한: 인스턴스당 30,000회/일. 초과 시 자체 알람.
  ```
- **연다고 주장하는 것**: **항목 1~5 를 승인 가능하게 만드는 것이 이 항목이다.** 권한 심사자가 실제로 두려워하는 것은 '뷰를 읽는 비용' 이 아니라 '40,000 run 이 각자 읽기 시작하는 것' 이고, 그 두려움은 정당하다. 약정 없이 권한만 요청하면 심사자는 최악을 가정할 수밖에 없다. 동시에 이 약정은 **v1.2.3.1 §11.2 의 규범을 하나 정정한다**: 현행은 'Control 이 **Source별로** 읽기 전용 세션 1개를 상시 유지한다' 이다. 그러나 apply lag·transport lag·DB identity·DG 상태는 전부 **standby 인스턴스 속성이지 Source(스키마) 속성이 아니다.** Source 가 20개면 현행 규범은 같은 4행을 20배로 읽는다. 캐시 키를 db_unique_name+instance [...]
- **부하 기전**: **산술을 그대로 적는다 — 이게 요청서의 본문이다.** 약정 적용 시, standby 인스턴스 1개당 하루 V$ 쿼리: · 10초 폴 3종(DG_STATS, DATABASE, RECOVERY_PROGRESS, 한 라운드트립에 병합) → 8,640 × 3 = 25,920 · 60초 폴 2종(EVENT_HISTOGRAM, ARCHIVE_DEST_STATUS) → 1,440 × 2 = 2,880 · **합계 28,800회/일 = 평균 0.33 qps**, 세션 1개, 각 쿼리 반환 행수 ≤ 약 50, 물리 I/O 0. 비교 대상: · **약정 없는 naive 설계**: 40,000 run × 1 = 40,000회/일 + **정시 500 세션 동시 조회 스파이크**. · **v1.2.3.1 현행 §11.3(V$DATABASE 를 [...]
- **영향 범위**: **원천 primary: 0.** 이 약정은 순수 플랫폼 측 자기 제한이다. **약정이 깨지는 경우**(코드 회귀로 캐시 우회, 폴링 주기 설정 실수): 최악이 40,000회/일 + 500 동시. 그래도 물리 I/O·undo·사용자 데이터 lock 은 여전히 0 이고, 유일한 실질 압박은 V$DATABASE 의 control file 접근(항목 2 참조)이다. 그래서 약정 4번('sessionInitStatement 에 V$ 를 넣지 않는다')이 이 목록에서 **가장 중요한 한 줄**이다 — 여기만 지키면 회귀의 최악값이 40,000/일 수준에 묶인다. **약정을 코드로 강제한다**: V$ 조회는 단일 클래스(DgMonitorSession)에만 두고, 그 밖에서 `V$`/`V_$` 문자열을 포함한 SQL 을 만들면 빌드가 깨지도록 [...]
- **거절 시 대안**: 해당 없음 — 거절 대상이 아니다(권한이 아니므로). 다만 **권한이 전부 거절되어도 이 약정 구조는 그대로 만든다**: 캐시 키를 인스턴스 단위로 두고, STANDBY_MAX_DATA_DELAY 사다리를 그 단일 poller 에서 돌린다. 그러면 사다리도 40,000 run 에 곱해지지 않고 14,400회/일에 묶인다. **즉 이 항목은 권한 요청의 결과와 무관하게 먼저 구현해야 하는 것이고, 구현해 두면 그 자체가 요청서의 증거물이 된다** — 'DBA 님, 지금 저희는 이미 이 패턴으로 돌고 있고, 권한을 주시면 사다리 14,400회가 fixed table 읽기 8,640회로 바뀝니다.'
- **판단 근거**: 산술과 설계 약정은 이 조사의 산출물이며 Oracle 문서 사실이 아니다(그래서 '확인' 대상이 아니라 '우리가 지키는 값'). 다만 주기 선택의 근거는 문서에서 나온다: apply lag 는 '초 단위로 계산(computed to the nearest second)' 되고 apply lag 히스토그램은 '매초 샘플링' 되며 DATUM_TIME/TIME_COMPUTED 차이가 '30초 미만' 이어야 정확하다 — 따라서 10초 폴은 정보 손실이 없고 1초 이하 폴은 얻는 정보가 없다. 누적 히스토그램은 차분 사용이 문서 권장 패턴이므로 60초로 충분하다. 'Source별 모니터 세션 1개' 는 v1.2.3.1 §11.2 의 현행 규범이며, 이 항목은 그것을 인스턴스 단위로 정정하자는 **제안**이다(규범 변경이므로 승인 필요).
- **판정: 미검증.** 반증 절차를 거치지 않았다 — 기각도 승인도 아니다.

---

## 3. 검증된 9건의 기각 사유 (전문)

각 항목의 반증 논증 전문이다. 요약이 아니라 원문이어야 재판정이 가능하다.

### R1. INFO-3 · apply 간섭에 대한 정직한 상태 표기 + apply lag 회신 요청

## 판정: claim_holds = false. 다만 **틀린 이유가 이 항목이 스스로 자백한 이유가 아니다.**

이 항목은 "apply 간섭이 문서화되어 있는가"를 미확인으로 남긴 점에서 정직하고, 그 판단은 맞다(아래 ①). 무너지는 곳은 그 정직함이 아니라 **(A) 요청한 데이터가 요청한 질문에 답할 수 없다는 것, (B) "유일한 증거원" 이 거짓이고 더 좁고 더 싼 grant 가 존재한다는 것, (C) "자동 자해 탐지기" 서사가 논리적으로 성립하지 않고 자기 문서와 모순된다는 것** 이다.

---

### ① 맞는 것부터 (신용을 줄 부분)
- **뷰 이름·컬럼값이 정확하다.** DG Concepts ch.10 의 실제 예제가 `SELECT * FROM V$STANDBY_EVENT_HISTOGRAM WHERE NAME = 'apply lag' AND COUNT > 0;` 와 `SELECT name, value, datum_time, time_computed FROM V$DATAGUARD_STATS WHERE name like 'apply lag';` 이다. 이름을 지어내지 않았다. 이 계열 항목 중 이름 정확도는 최상위다.
- **"read-only 쿼리가 MRP 를 직접 차단하는가" 를 미확인으로 남긴 것은 옳다.** 19c *Managing Physical and Snapshot Standby Databases* 전문을 훑었으나 read-only 쿼리가 redo apply 를 차단·지연시킨다는 서술도, primary DDL 과 standby 쿼리가 충돌한다는 서술도 **없다**. 문서에 없는 규칙을 있다고 쓰지 않은 판단이 맞다.
- **DML redirection 배제도 옳다** — 우리는 원천에 DML 을 하지 않는다.
- **STANDBY_MAX_DATA_DELAY 가 우리 계정에 적용된다는 전제도 옳다** — 문서가 "for queries issued by **non-administrative users**" 라고 명시하며, ETL 계정은 비관리자다.

### ② (B) "우리는 V$ 를 읽을 수 없으므로 유일한 증거원" — **거짓. 이것 하나로 claim_holds=false 다.**
v2.0 §2.2 가 V$ 를 사망 처리한 근거는 Reference 의 *기본 접근권* 문장("only user SYS or anyone with SYSDBA privilege has access to the dynamic performance tables")이다. 이건 **기본값 서술이지 grant 불가 서술이 아니다.** §2.2 자신이 바로 다음 칸에 "CDB에서는 **grant 후에도** …" 라고 적어 grant 가능성을 전제하고 있다. 즉 §2.2 는 "DBA 협조 0" 이라는 **이번에 무효화된 전제** 위에서만 참이었고, INFO-3 은 그 죽은 전제를 근거로 자기 존재를 정당화한다. 순환이다.
새 제약("무해하면 요청 가능") 하에서 정답은 명백히 **`GRANT SELECT ON SYS.V_$DATAGUARD_STATS` / `SYS.V_$STANDBY_EVENT_HISTOGRAM` 2줄 객체 grant** 다. 이유:
- 부하 기전이 **기전으로** 무해를 증명한다: SGA 상주 X$ 위의 뷰 → datafile IO 0, undo 0, redo 0, lock 0. MRP 와 겨루는 자원(IO 대역·버퍼캐시·undo)을 **하나도** 건드리지 않는다. 이건 "가볍다" 가 아니라 "읽는 자원 종류가 다르다" 다. 이 계열 요청 중 **가장 방어하기 쉬운 요청**인데 이 항목은 그걸 포기하고 사람 메일을 택했다.
- 해상도가 비교 불가다: 우리가 직접 `:59:00`/`:00:30`/`:05`/`:10`/`:11` 에 스냅샷을 떠 **Oracle 이 지정한 차분 방식**으로 창을 분리할 수 있다. 주 1회 메일은 이걸 영원히 못 한다.
- 노출 데이터가 lag 초 값과 버킷 카운트뿐 — 업무 데이터 0. 역할(SELECT_CATALOG_ROLE) 통째가 아닌 객체 2개.
→ **"더 좁은 권한으로 충분하다" 에 정확히 해당하므로 반증 성립.**

부수적이지만 이름 정확성에서 이 항목이 놓친 두 가지 (다른 항목에도 전염되는 오류):
- `GRANT SELECT ON V$DATAGUARD_STATS` 는 **실패한다** (V$ 는 public synonym → ORA-02030). `V_$` 뷰에 grant 해야 한다.
- **모든 grant 는 primary 에서 실행해야 한다.** standby 는 read-only 라 GRANT 가 ORA-16000 으로 거부된다. 따라서 "영향 범위 0" 은 grant 형태 요청에는 성립하지 않는다(metadata 변경 + redo + audit row, 1회성. **양은 측정하지 않았다** — '수백 바이트'라고 적었던 것은 근거 없는 수치다, 9차 §5.2).

### ③ (A) 요청한 데이터가 요청한 창을 해상하지 못한다 — 설계 결함
`V$STANDBY_EVENT_HISTOGRAM` 은 **인스턴스 기동 이후 누적**이고, Oracle 은 "구간 평가는 양 끝 스냅샷을 떠서 비교하라" 고 명시한다. 주 1회 단발 누적 조회로는 매시 10분 창(전체의 16.7%)을 83.3% 의 배경에서 분리할 수 없다. 지정된 사용법을 지키려면 DBA 가 하루 48회 조회해야 하므로, **"정시 창 전후" 와 "주 1회 회신" 은 양립 불가**다 — 요청서 자체가 내부 모순이다. `V$DATAGUARD_STATS` 는 순간값 + DATUM_TIME 기준 최대 30초 staleness 를 문서가 인정하므로, 주 1회 1표본은 정보량 0 이다. 게다가 apply lag 정의가 **transport 포함 end-to-end** 이므로, 정각에 값이 나빠져도 그것이 우리 읽기 때문인지 primary 정각 배치의 redo 폭증 때문인지 **원리적으로 분리 불가**다 — 하필 생산라인 배치와 우리 burst 가 같은 정각에 겹친다.
→ "이 값이 악화되면 창을 분산한다" 는 선언은 **교란된 신호에 제어권을 넘기는 약속**이다. 지키면 무고하게 자해하고, 배경에 묻히면 진짜 신호에 반응하지 않는다.

### ④ (C) "자동 자해 탐지기" 서사 — 성립하지 않는다. 협상에 들고 가면 역효과다.
주장: "우리가 standby 를 과부하시켜 apply 가 밀리면 우리 쿼리가 ORA-3172 로 스스로 죽는다."
- **논리 오류(역이 아님).** ORA-3172 는 *apply lag 초과* 의 함수이지 *우리가 원인* 의 함수가 아니다. lag 은 end-to-end 라 primary redo 폭증만으로도 뜬다. fence 는 **자기 보호 장치로는 유효**하나 **자기 유해성 탐지기로는 무효**다. DBA 는 한 문장으로 이걸 무너뜨릴 수 있고("primary 가 바빠도 그 에러 뜨죠?"), 그러면 이 항목뿐 아니라 세션 프리앰블 전체의 신뢰가 같이 떨어진다.
- **자기 문서와 모순.** v2.0 §8⒝ 는 mid-query 평가 여부를 "문서에 기술 없음 — 설계는 보수적으로 **시작 시점 평가만 가정**" 이라고 확정해 두었다. 문서 원문도 "queries ... **are executed only if** the apply lag is less than or equal to STANDBY_MAX_DATA_DELAY" 로 **실행 개시 조건**이다. 시작 시점 평가만 있다면, **정각에 clean 하게 시작해 20분 동안 돌면서 apply 를 굶기는 Full 추출은 절대 self-fail 하지 않는다** — 즉 우리가 걱정하는 바로 그 시나리오가 fence 가 가장 못 잡는 시나리오다. 자해 탐지기 서사는 §8⒝ 를 부정해야만 성립한다.
- **실패 모드 누락(양의 되먹임).** 정각 500 run 동시 시작 → 상관적 동시 ORA-3172 → 재시도. backoff+jitter+circuit breaker 가 없으면 fence 는 부하를 줄이는 게 아니라 접속·재시작 부하로 **증폭**한다.
- **전제 미확인.** 문서는 `STANDBY_MAX_DATA_DELAY=0` / `SYNC WITH PRIMARY` 에 SYNC transport · 전송상태 SYNCHRONIZED · max protection/availability · real-time apply 를 요구한다. 사이트가 ASYNC/max performance 면 D=0 과 SYNC WITH PRIMARY 는 **사용 불가**다. 그런데 우리는 보호 모드를 확인할 수단이 없다 — 확인하려면 V$DATABASE/V$DATAGUARD_STATS 가 필요하다. **fence 의 유효성 자체가 ②의 grant 없이는 검증 불가**라는 순환이 여기서 한 번 더 나온다. (D>0 만 쓸 거라면 이 전제는 불필요 — 그렇다면 그렇게 명시할 것.)

### ⑤ "primary 영향 경로는 undo 뿐" 단정 반증
DG 문서의 Automatic Block Media Recovery: "If corrupt data blocks are encountered at a standby, then the standby **automatically initiates communication with the primary and requests uncorrupted copies of those blocks**." → 읽기 전용 쿼리가 primary 로 트래픽을 만드는 **문서화된 경로**다. Full 60% 의 대규모 스캔은 조우 확률이 구조적으로 높다. 빈도는 낮으나 "뿐" 은 틀렸고, 절대 단정을 DBA 에게 반박당하는 비용이 크다.

### ⑥ 위험도: NONE → **LOW**
요청 행위 자체의 DB 부하는 실제로 ~0 이므로 MEDIUM 은 과하다. 그러나 NONE 도 아니다: (i) 7일 지연·교란된 신호로 제어 루프를 돌리겠다는 **문서화된 약속**이 남고, (ii) DBA 에게 "우리는 apply lag 를 감시한다" 는 **거짓 안심**을 제공하며, (iii) 짝을 이루는 fence 서사가 재시도 증폭이라는 실 부하 경로를 은폐한다. 생산라인 결합 DB 에서 "감시하고 있다는 오해" 는 그 자체로 사고 확률을 올린다.

### ⑦ 거절 시 대안 — 원문보다 강한 것이 있다
원문의 fallback("ORA-3172 발생률을 대리 지표로")은 한계 서술은 정확하나 **가장 중요한 보강을 빠뜨렸다: 대조군.** 권한 0 으로도 가능한 A/B 를 붙일 수 있다 — **정각 burst 를 짝수 시각에만 투입하고 홀수 시각은 비운 뒤, 시각대별 ORA-3172 발생률을 비교**한다. 이러면 "우리가 원인인가" 라는 질문에 (술어 기반이라 여전히 조악하지만) **인과 방향의 증거**를 만들 수 있고, 이건 주 1회 메일이 절대 줄 수 없는 것이다. 즉 **거절당한 fallback 이 요청 자체보다 질문에 더 잘 답한다** — 요청의 가치 주장이 무너지는 또 하나의 지점이다.
그 외: (1) 회신 형태를 고집한다면 주 1회가 아니라 **1일 · 2시점(:59, :11) paired snapshot + STARTUP_TIME + DATUM_TIME/TIME_COMPUTED** 로 축소(DBA 부담은 줄고 정보량은 늘어난다). (2) OEM/DG Broker 의 lag 차트 export — 사이트에 OEM 이 있는지 **미확인**. (3) standby 용 AWR/Statspack 신설은 **요청하지 말 것** — 스냅샷 인프라가 standby 에 실제 부하를 얹으며, 우리가 시킬 일이 아니다.

### ⑧ 확신도
- V$ 뷰 이름·NAME 값·누적 성질·스냅샷 차분 지침·DATUM_TIME 30초 규칙·STANDBY_MAX_DATA_DELAY 문언·D=0 전제·ABMR primary 통신: **HIGH** (19c *Data Guard Concepts and Administration* ch.10 및 Reference 원문 확인)
- V_$ vs V$ synonym(ORA-02030), standby 에서 GRANT 불가(ORA-16000): **HIGH** (오류 정의 문서 확인)
- read-only 쿼리가 MRP 를 차단하는 문서화된 규칙: **없음 확인 → 원문 판단 유지(미확인)**
- mid-query 재평가 여부: **미확인** — 문서에 없음. 따라서 §8⒝ 대로 "시작 시점 평가만" 가정해야 하고, 그 가정 하에서 자해 탐지기 서사는 무효
- CDB/PDB 에서 ETL 계정이 위 뷰를 실제로 읽을 수 있는지: **미확인** — G0-0a 로 선행 실측 필요. grant 를 요청하기 **전에** 확인하지 않으면 무의미한 grant 를 받아낼 수 있다

---

### R2. W-S8 — 같은 CDB 안의 ETL 전용 PDB 에 heartbeat 를 둔다 (생산 PDB 에 객체 0)

기각. DB 부하 때문이 아니라, (a) 근거 논리가 fail-open 이고 (b) 이 변형이 되살리려는 기능이 이미 권한 0으로 존재하며 (c) 요청 범위가 원안 주장보다 크기 때문이다.

■ (1) 권한·기전 검증 — "같은 redo 스트림" 논거는 절반만 맞다
맞는 부분: 멀티테넌트 CDB 는 online redo 를 CDB(인스턴스/스레드) 단위로 하나만 가지며 SCN 도 CDB 전역이다. MRP 도 CDB 단위 단일 복구다. 따라서 "ETL PDB 의 SCN X 가 보이면 CDB redo 가 X 까지 적용됐다"는 물리적으로 성립한다. 여기까지는 반박하지 않는다.

틀린 부분: 원안은 여기서 "그러므로 생산 PDB 의 데이터도 X 까지 반영됐다"로 건너뛴다. 이 추론은 **"복구 대상에서 제외되거나 offline 인 PDB 가 없다"**는 미검증 전제를 몰래 쓴다. Oracle 은 PDB 를 standby 에서 제외하는 경로를 문서화된 형태로 갖고 있다(생성/plug 시 STANDBYS 절, 19c 이후의 subset standby 계열 기능). 제외된 PDB 의 datafile 은 standby 에서 offline/unnamed 로 남고 MRP 는 나머지 CDB 에 대해 계속 진행한다 — 이 "제외되어도 MRP 는 계속 간다"는 부분과 생산 PDB datafile 이 복구 오류로 offline 된 뒤 MRP 가 계속되는지 여부는 **1차 출처로 확인하지 못했다(미확인, 실측 필요)**. 그러나 가능성이 존재한다는 것만으로 이 설계는 실격이다. 이유:
 · W-S1(생산 PDB 안 heartbeat): 생산 PDB 가 복구되지 않으면 그 안의 heartbeat 도 같이 얼어붙는다 → staleness 초과 → 자동 강등. **fail-safe**.
 · W-S8: 생산 PDB 만 얼어붙고 ETL PDB heartbeat 는 계속 전진한다 → 모니터는 "신선"이라 보고하고, 강등 규칙은 영원히 발화하지 않으며, 파이프라인은 오래된 데이터를 최신으로 라벨링해 하류로 내보낸다. **fail-open**.
witness 의 요건이 "같은 redo 스트림에 실려 온다"뿐이라는 원안의 핵심 주장이 바로 여기서 깨진다. 진짜 요건은 "**증인이 관측 대상과 같은 복구 실패 운명을 공유한다**"이다. W-S8 은 이 결합을 끊는 것이 설계 목적이므로, 구조적으로 witness 자격을 잃는다.

두 번째 반증 경로(이건 이 프로젝트 문서 자체에 이미 적혀 있다): USERENV 에 DBID·CON_UID·GUID·RESETLOGS_CHANGE# 가 없어 **이름을 재사용한 clone 을 구분할 수 없다**(scope 문서 §6.1 / U-9). ETL PDB 는 생산 PDB 보다 훨씬 쉽게 재생성·clone·refreshable clone 대상이 된다. 재생성된 ETL PDB 나 refresh 시점에 멈춘 clone 의 heartbeat 행은 세션 프리앰블의 이름 tuple 단언을 그대로 통과하면서 과거 시각을 신선한 것처럼 보고한다. 원안은 이 경로를 "조용히 끊길 경로가 하나 늘어난다"고 stale 쪽으로만 적었는데, 실제로 무서운 것은 끊기는 게 아니라 **틀린 값으로 살아 있는 것**이다.

세 번째: 원안이 스스로 인정하듯 로컬 사용자는 컨테이너를 넘지 못한다. 그 결과 heartbeat 는 **추출 트랜잭션과 같은 스냅샷 안에서 읽을 수 없다**. W-S1 은 같은 SET TRANSACTION READ ONLY 안에서 heartbeat 와 원천을 함께 읽어 스냅샷에 묶인 하한을 만들 수 있지만, W-S8 은 별도 세션·별도 시각의 관측을 벽시계로 조인해야 한다. 이건 v2.0 이 의도적으로 폐기한 read-then-decide 구조로의 회귀이고, 문서의 증거 등급으로는 **2급(플랫폼 자기 계측)**이다. 1급 증거(서버 발신 오류 코드)가 아니므로 **bound_kind = ENFORCED 의 근거 장치가 될 수 없다** — 그런데 heartbeat 를 되살리는 목적이 바로 그것이었다. 목적 미달성. (하한으로 쓰는 것 자체는 heartbeat 를 추출 시작 '전'에 읽으면 성립하지만, 원안에는 그 순서 규율도 t_hb→t_snapshot 간극을 D 에 더하라는 규정도 없다.)

■ (2) 더 좁은 수단으로 충분하다 — 이게 결정타
MRP 는 CDB 단위다. 따라서 생산 PDB 세션에서 선언하는 `ALTER SESSION SET STANDBY_MAX_DATA_DELAY = D`(권한 불필요, 초과 시 ORA-03172)가 구속하는 apply lag 는 **W-S8 의 heartbeat 가 대리하려던 바로 그 CDB 전역 양**과 같다. 게다가 그쪽은 서버가 강제하는 fail-closed 이고 1급 증거다. transport 가 끊겨도 lag 는 자라므로 primary 사망/전송 단절도 잡힌다. 즉 W-S8 이 STANDBY_MAX_DATA_DELAY 대비 추가로 주는 것은 "apply lag" 대신 "primary 벽시계"라는 표현 형식뿐이며, 그 대가로 PDB 1개 + primary 상주 쓰기 세션 + CDB 수명주기 확약을 요구한다. 최소 범위 원칙 위반. (남는 미확인은 STANDBY_MAX_DATA_DELAY 의 mid-query 평가 여부인데, W-S8 은 교차 컨테이너·교차 세션이라 이 구멍을 메우지도 못한다. 메우려면 같은 스냅샷에 들어갈 수 있는 W-S1 이어야 한다.)

■ (3) 요청 규모 — "생산 PDB 에 객체 0" 은 회계 착시
grant 이름 자체(CREATE SESSION / CREATE TABLE / ALTER USER … QUOTA)는 실재하고 용법도 맞다. 그러나 이 4줄로는 기능이 열리지 않는다. 추가로 DBA 가 해줘야 하는 것: <etl_pdb> 가 standby 구성에 포함될 것(제외 절 미적용), standby 에서 READ ONLY open 및 재기동 후 유지(standby 에서의 save state 동작은 **미확인**), standby 쪽 접속 서비스 생성·role-based 기동, refreshable clone 아님의 확약, 생산 PDB 만 offline 으로 남는 상황의 알림 경로. 전부 grant 가 아니라 CDB 수명주기 확약이다. 원안도 말미에 "PDB 1개가 테이블 1개보다 큰 요청일 수 있다"고 적었는데, 그 인식이 맞고 따라서 위 4줄짜리 GRANT 블록은 요청 규모를 **과소 표기**하고 있다. 생산 PDB 딕셔너리에 객체가 없다는 것은 사실이지만, 인스턴스·redo·세션 풀·운영 확약 수준에서는 W-S1 보다 작지 않다.

■ (4) 위험도
LOW → **MEDIUM**. DB 자원 관점의 위험은 실제로 LOW 가 맞다(생산라인 DB 를 흔들 기전이 없다 — 2,880 commit/day, ~1~2 MB redo/day). 등급을 올리는 근거는 부하가 아니라 두 가지다: ① fail-open witness 로 인한 **무성 데이터 정합성 오류**(오래된 데이터에 신선 라벨) — 생산라인 원천에서 이건 부하 사고보다 비싸다. ② 미통보 profile 하에서 CDB 당 상주 세션 2개(primary+standby) 증가가 SESSIONS_PER_USER / FAILED_LOGIN_ATTEMPTS 와 상호작용하여 계정 잠금(=10k Job 정지) 확률을 올린다. HIGH 로는 올리지 않는다 — 원천 DB 를 불안정하게 만들 경로는 없고, 강등 규칙을 병행하면 피해는 파이프라인 내부에 갇힌다.

■ 결론
"CDB 단일 redo 스트림" 이라는 물리 사실은 맞지만, 그것만으로 witness 가 되지는 않는다. W-S8 은 W-S1 의 안전 성질(같은 스냅샷 co-observation, 복구 실패 운명 공유)을 둘 다 버리고 조직적 요청 규모만 키운다. 채택하지 말고, 0순위는 권한 0인 STANDBY_MAX_DATA_DELAY(+양성 대조), 그래도 벽시계 증인이 필요하면 W-S1, 둘 다 거절되면 `t0 − D` 로 간다. 원안의 fallback 사슬(W-S1 → W-S6 → `t0 − D`)은 방향이 맞다 — 다만 W-S8 은 그 사슬의 상단이 아니라 사슬 밖으로 빼야 한다.

확신도: 권한 이름/문법 HIGH, CDB 단일 redo·전역 SCN HIGH, 스냅샷 교차 불가 HIGH, STANDBYS 제외 절 존재 MEDIUM-HIGH, "생산 PDB datafile offline 상태에서 MRP 계속 진행" LOW(미확인 — G0-0 프로브나 DBA 확인 필요), standby 에서의 PDB open 상태 유지 동작 LOW(미확인).

---

### R3. FLASHBACK 객체 권한 (열거된 테이블 한정) — 1순위 요청

권한 이름은 맞다. 그러나 (a) 이 권한만으로는 주장한 5가지 중 2개가 아예 안 열리고, (b) "새로운 실패 모드 없음"과 "primary 무영향"은 이 저장소의 자기 문서로 반증되며, (c) 안전장치로 제시한 DDL_LOCK_TIMEOUT 이 GRANT 에 적용된다는 근거가 없다.

■ 맞는 부분 (인정)
· 권한 이름/필요 범위: `GRANT FLASHBACK ON <owner>.<table>` 은 실재하며 재설계 문서 49행이 Dev Guide §20.2.5 를 인용한다 — "grant FLASHBACK and either READ or SELECT privileges on those objects". SELECT 만으로는 불가라는 판단도 맞다.
· primary 에서 GRANT → redo 로 standby 전파: 맞다. standby 는 read-only 라 거기서 GRANT 할 수 없다.
· FLASHBACK 이 DML 권한도 FLASHBACK TABLE 문 실행 권한도 주지 않는다: 맞다(FLASHBACK TABLE 은 별도로 SELECT·INSERT·DELETE·ALTER 요구).
· 되살아나는 것 중 (3) §12.3 비교 A 의 시점 비교 복귀(U-6 철회): 이것만이 깨끗한 이득이다. 감사 표본은 최근 시각·소량 읽기라 undo 거리가 짧다.

■ 반증 1 — SCN 출처가 없다. (4)와 (5)는 안 열린다. [결정적]
주장은 "AS OF SCN / AS OF TIMESTAMP 복원", "동일 literal SCN 공유", "fence 의 visible_scn 이 계약당 1회 읽기로 고정", "AS OF visible_scn 재사용" 이라고 SCN 을 전제한다. 그런데 그 SCN 을 어디서 읽는지가 없다.
`etl-platform-target-architecture-v1.2.3.1.md` 1027행이 규범으로 못박고 있다:
  "visible SCN의 출처는 `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER`를 기본으로 하고, 보조로 `V$RECOVERY_PROGRESS` 또는 `V$DATAGUARD_STATS`. **`V$DATABASE.CURRENT_SCN`은 standby에서 checkpoint SCN으로 마지막 적용 SCN보다 작으므로 쓰지 않는다.**"
세 후보가 전부 이번 요청에서 배제됐다 — DBMS_FLASHBACK EXECUTE 는 "요청하지 않는 것"에 명시적으로 들어 있고, V$ 는 여전히 죽어 있다. V$DATABASE.CURRENT_SCN 은 standby 에서 쓰면 안 된다고 문서가 이미 판정했다.
부록 W 1번 자신도 이 권한이 되살리는 것을 **"`AS OF TIMESTAMP` → 다중 세션 공통 시점"** 이라고 적었지 AS OF SCN 이라 적지 않았다. 그리고 부록 W 4번(`GRANT EXECUTE ON SYS.DBMS_FLASHBACK`)에 "1번과 결합해야 실효"라고 붙어 있다. 원저자는 둘이 상보재임을 알고 있었고, 이번 요청은 그 상보 관계를 뒤집어 1번만 요청하면서 SCN 기반 기능이 돌아온다고 주장한다.
무권한 SCN 획득 우회로도 막혀 있다. `g0-0c-counterexamples/scenarios/CE07-rowscn-shard-race/run.py` 가 "SCN 은 권한 없이 얻는다 — marker 행을 commit 한 뒤 그 행의 ORA_ROWSCN 을 읽는다"고 쓰지만 이건 **쓰기 가능한 DB** 전제다. physical standby 에는 INSERT 할 수 없고 primary 에는 쓰기 권한이 없다. 기존 원천 행의 ORA_ROWSCN 을 쓰는 것도 안 된다 — 그건 DB 전역 현재 SCN 이 아니라 특정 행의 마지막 commit 하한이고, MAX(ORA_ROWSCN) 은 전체 스캔이며(원천 부하 폭증), ROWDEPENDENCIES 는 생성 시 속성이라 기존 원천 테이블에 나중에 붙일 수 없다. 게다가 그렇게 얻은 과거 SCN 을 fence 로 쓰면 그 사이 commit 이 조용히 누락된다.
⇒ (4) "fence 의 visible_scn 이 계약당 1회 고정" 은 성립하지 않는다.
⇒ (5) §10.2 undo deadline / `EXTRACT_EXCEEDS_UNDO_BUDGET` 도 성립하지 않는다. undo budget 을 계산하려면 UNDO_RETENTION 값·undo tablespace 크기·V$UNDOSTAT 이 필요한데 전부 없다. 주장 자신이 대안 항목에서 "undo 보존 한계 자가 측정 … 대안 없음"이라고 인정해 놓고 (5)에서는 live 가 된다고 적었다 — 내부 모순이다. 남는 건 ORA-01555 를 맞아 보고 경험적으로 추정하는 것뿐이다.

■ 반증 2 — (1)과 (2)는 "복원"이 아니라 열화 복원이다
(1) AS OF TIMESTAMP 로만 가능한데, timestamp→SCN 매핑은 SMON_SCN_TIME 기반으로 **약 3초 정밀도의 근삿값**이다. 이건 내 추정이 아니라 이 저장소의 `g0-0a-capability-inventory.sql` 145행이 직접 경고한다: "SCN_TO_TIMESTAMP는 약 3초 정밀도의 **근삿값**이며 방향성 있는 하한이 아니다." 같은 파일 140행이 실제로 프로브하는 것도 `AS OF SCN` 이 아니라 `AS OF TIMESTAMP` 다. N개 세션을 한 시점에 묶는다는 서술은 참이지만 그 "한 시점"에 ±3초 슬롭이 있고, 방향 보장이 없어 §11.3 fence 의 보수성 논증을 다시 유도해야 한다. 매핑 보존 기간을 벗어나면 ORA-08180 이 난다.
(2) extract-once 재시도 재사용은 조건부이고, 실패하면 주장과 정반대로 뒤집힌다. 같은 문서 P1-12 가 규범이다:
  "ORA-01555/01466/08181 → `SPARK_FAILED` + **같은 계약 RETRY 금지**(`FENCE_EXPIRED` 동일 처리)"
즉 undo 가 부족해 AS OF 가 깨지면 그 계약은 재시도가 금지되고 새 계약으로 원천을 통째로 다시 읽어야 한다. 부분 추출까지 버린다. "재시도 재독을 없애 부하를 줄인다"는 happy path 한정 주장이고, ORA-01555 발생률이 낮다는 전제 위에 서 있는데 그 발생률은 (반증 1의 이유로) 측정 불가다.

■ 반증 3 — "새로운 실패 모드를 만들지 않는다" 는 거짓
SET TRANSACTION READ ONLY 와 기전이 같다는 것은 맞지만, 실패 모드 집합은 같지 않다. P1-12 가 세 개를 열거한다: ORA-01555, **ORA-01466**, **ORA-08181**.
· ORA-01466 — fence SCN/timestamp 이후 원천에 DDL 이 일어나면 난다. 생산라인 DB 의 파티션 롤링·인덱스 재구성이 그대로 트리거다. 갓 시작한 read-only 트랜잭션은 이 오류를 맞을 수 없다.
· ORA-08181 — 유효하지 않은 SCN. 재시도 재사용으로 오래된 SCN 을 다시 제출하는 설계(=(2)의 핵심 가치)가 정확히 이 오류의 표적이다.
둘 다 AS OF 를 도입해야만 생기는 신규 실패 모드다.

■ 반증 4 — "undo 요구량은 같거나 적다" 는 뒤집혀 있다
undo 보존 요구는 **아직 살아 있어야 하는 가장 오래된 스냅샷**이 결정한다. 스태거된 read-only 세션 N개의 최고령 스냅샷은 가장 먼저 시작한 세션의 시작 시각이며, 세션이 끝나면 해제된다. 반면 `visible_scn` 을 계약 시작에 고정하고 **attempt 를 넘어 재사용**하면(=(2)) 그 스냅샷은 재시도 창 전체(수 시간 가능)만큼 과거로 고정된다. 즉 부하를 줄인다고 내세운 바로 그 기능이 undo 보존 요구를 가장 크게 늘리는 기능이다. 비교는 주장과 반대 방향이다.

■ 반증 5 — "primary 에는 어떤 경로로도 영향이 가지 않는다" 는 두 번 틀렸다
(a) 자기모순: 부하 기전 절에서 GRANT 가 primary 의 library cache lock 을 잡고 의존 커서를 무효화하며 hot table 이면 hard parse 가 튄다고 스스로 적었다. 그건 primary 측 영향이다. "어떤 경로로도 없다"와 양립하지 않는다.
(b) 더 중요한 것 — 주장은 Oracle 이 ADG standby 의 undo 부족 대책으로 "primary 의 UNDO_RETENTION 을 올려라"라고 하는 사실을 **안전의 근거**로 인용했다. 이건 정반대로 읽어야 한다. 그것이 바로 **standby ETL 부하가 production primary 에 도달하는 채널**이다. 이 기능을 실용 수준으로 만들려면(=ORA-01555 를 드물게 만들려면) 누군가 primary 의 UNDO_RETENTION 을 올리고 undo tablespace 를 키워야 한다. undo tablespace 에 RETENTION GUARANTEE 가 걸려 있으면 보존 연장은 **ORA-30036(unable to extend segment in undo tablespace)** 로 production DML 을 실패시킨다 — 정확히 "DB 가 흔들리면 물리적 생산이 멈춘다"는 시나리오다. 아무것도 안 바꾸면 primary 는 안전하지만 그때는 기능이 신뢰할 수 없다. 이 딜레마를 DBA 에게 감추면 안 된다.
(c) 부수적으로, "standby→primary 채널이 존재하지 않는다"는 절대 진술 자체가 과하다. ADG 는 자동 블록 손상 복구에서 standby 가 primary 로부터 블록을 요청한다(11.2+). undo 와 무관하지만 "어떤 경로로도"는 틀린 표현이다. (신뢰도 중)

■ 반증 6 — standby 측 부하 평가가 낙관적이다
"권한 부여가 부하 총량을 늘리지 않는다"는 거짓이다. 같은 행을 읽어도 비용이 다르다.
· 현재 SCN 읽기: standby 의 query SCN 은 사실상 applied SCN 이라 대부분의 블록은 롤백 없이 그대로 읽힌다.
· AS OF (계약 시작에 고정된 과거 시점) 읽기: 그 시점 이후 변경된 **모든** 블록에 대해 undo 체인을 역주행해 CR 사본을 만들어야 한다. 추가 비용은 (추출 지속시간 + 재시도 경과시간) × 원천 DML rate 에 선형이다. Full 모드가 60%, 대형 테이블 장시간 스캔이면 이 항이 지배적이 된다.
· 병렬(numPartitions>1)이 이 주장의 존재 이유인데, 정시 burst 500 run × 파티션 N 이 **동시에** undo 블록 랜덤 읽기를 때린다. 블록 자체는 중복되지 않아도 순간 I/O 와 buffer cache CR 사본 압력은 N배다.
· 추가 기전(신뢰도 중, 확인 필요): read-only standby 는 데이터 블록에 delayed block cleanout 을 기록할 수 없다. 정리되지 않은 ITL 을 가진 블록은 읽을 때마다 undo 로 다시 판정해야 하므로, primary 직관보다 standby 의 undo 읽기 증폭이 크다.
· 귀결: standby undo tablespace 물리 읽기 급증 → standby I/O·buffer cache 포화 → **redo apply 가 같은 자원을 두고 경합해 apply lag 상승**. 주장은 이걸 "권한 유무와 무관한 기존 부하"라고 처리했는데, 위 이유로 AS OF 가 순증시킨다.
· 자기증폭 루프: apply lag 가 오르면 STANDBY_MAX_DATA_DELAY 가 ORA-03172 로 세션을 죽인다 → 재시도 → 재독 → 부하 증가 → lag 추가 상승. 주장은 ORA-03172 를 안전장치로만 제시했지 되먹임 항으로 보지 않았다.

■ 반증 7 — 안전장치가 작동하지 않는다
"DDL_LOCK_TIMEOUT 을 짧게(5초) 걸고 실행" 은 근거가 없다. Oracle 은 DDL_LOCK_TIMEOUT 을 "DDL 문이 **DML lock** 을 기다리는 시간"으로 정의한다. GRANT 는 TM(DML) lock 을 필요로 하지 않고 library cache lock/pin 에서 경합한다. GRANT 가 DDL_LOCK_TIMEOUT 의 적용을 받는다는 것은 **1차 출처 미확인**이며, 받지 않는다면 GRANT 는 무제한으로 'library cache pin' 대기에 매달릴 수 있다 — hot table 이면 그 사이 해당 객체를 파스하려는 production 세션까지 줄줄이 대기한다. 이 안전장치는 제거하거나, 짧은 타임아웃이 실제로 걸리는지 비운영 환경에서 먼저 실증해야 한다.

■ 반증 8 — 범위 누락과 스케일 전이
· 열거 대상이 **테이블만**이다. 원천을 view / synonym 으로 접근하면 뷰와 기반 테이블 양쪽에 FLASHBACK 이 필요하다(미확인 — Dev Guide 로 확인 필요). DB link 경유 AS OF 는 지원되지 않는 것으로 알려져 있으나 오류 번호는 미확인. 파일럿 목록을 만들기 전에 접근 경로가 테이블 직접인지 확인해야 한다.
· 파일럿의 LOW 는 전면 전개로 이전되지 않는다. Job 10,000 개의 가치(처리량 벽 해소)는 전면 전개에서만 실현되는데, 그건 production hot object 수천 개에 대한 커서 무효화 이벤트 수천 건을 뜻한다. 파일럿 리스크로 전면 승인을 받아내는 형태가 되면 안 된다.
· 데이터 노출 확대: FLASHBACK 객체 권한은 AS OF 뿐 아니라 VERSIONS BETWEEN(Flashback Version Query)도 연다(신뢰도 중, Dev Guide 확인 필요). 그러면 현재 SELECT 로는 보이지 않는 **과거 행 버전 — 나중에 정정·마스킹된 값과 삭제된 행** 이 보인다. "신규 SELECT 는 요청하지 않는다 = 신규 가시성 없음" 이라는 프레이밍은 정확하지 않다. 보안 검토에서 이 항목이 나올 것이므로 먼저 공시하는 편이 협상에 유리하다.

■ 종합
되살아난다고 주장한 5개 중 깨끗한 것은 1개((3) 감사 비교), 열화 복원 2개((1) ±3초 슬롭, (2) 조건부·실패 시 역전), 안 열리는 것 2개((4)(5) SCN 출처 부재)다. 부하 평가는 "총량 불변"이 틀렸고, "새 실패 모드 없음"은 이 저장소 문서가 세 개를 열거해 반증하며, "primary 무영향"은 자기모순이자 undo 보존 연장이라는 실제 채널을 놓쳤다. 요청 자체는 여전히 할 만한 가치가 있지만 — 부록 W 1번의 원래 판단은 옳다 — 제출문의 근거와 위험도는 현재 형태로 DBA 에게 내면 안 된다. 특히 "최악은 우리 쿼리가 ORA-01555 로 죽는다, 그게 전부다" 는 재시도 금지(P1-12)와 apply lag 되먹임을 빠뜨린 과소평가다.
확신도: 반증 1·2·3 은 이 저장소 1차 문서 직접 인용이라 HIGH. 반증 5(b), 6, 7 은 기전 논증이라 MEDIUM-HIGH. 반증 5(c), 6 의 cleanout 항, 8 의 VERSIONS BETWEEN·view 항은 Oracle 1차 문서 재확인 필요(MEDIUM, 미확인 표기).

---

### R4. V$ARCHIVE_DEST_STATUS (SYNCHRONIZED · GAP_STATUS — SYNC WITH PRIMARY 가용성 자가 판정)

## 결론: 반증된다. 4개 축에서 실패한다 — (A) 뷰 의미론이 standby 에서 성립하지 않음, (B) 더 좁은 객체로 충분함, (C) 부하 논거가 non sequitur 이고 최악 경로를 빠뜨림, (D) 근거로 삼은 전제 자체가 이 조직의 미확정 질문임.

---

### A. 기능 주장의 오류 — 이 뷰는 standby 에서 "자기 자신의 동기 상태"를 말해주지 않는다 (치명, confidence HIGH)

`V$ARCHIVE_DEST_STATUS` 는 Oracle Reference 정의상 **"archived redo log destinations 의 runtime/configuration 정보"** 다. 즉 **조회하는 인스턴스가 redo 를 *보내는* 목적지(LOG_ARCHIVE_DEST_n)** 의 상태다. `SYNCHRONIZED`·`SYNCHRONIZATION_STATUS`·`GAP_STATUS`·`DATABASE_MODE`·`RECOVERY_MODE`·`APPLIED_SEQ#` 는 전부 **발신자가 본 수신 측(destination) 데이터베이스**의 속성이다.

physical standby 는 이 관계에서 **수신자**다. standby 자신의 `V$ARCHIVE_DEST_STATUS` 에는 통상 `TYPE='LOCAL'` 인 dest_id=1 (standby redo log → 로컬 아카이브) 밖에 없고(cascade 구성이 아니라면), 그 행의 `SYNCHRONIZED`/`GAP_STATUS` 는 **primary 와의 관계가 아니라 로컬 목적지에 대한 값**이다. 요청서가 원하는 정보 — "primary→나 의 transport 가 SYNC 이고 SYNCHRONIZED 인가" — 는 구조적으로 **primary 쪽 V$ARCHIVE_DEST_STATUS 의 standby 목적지 행**에 있다. standby 에서 같은 뷰를 읽는 것은 **다른 질문에 답하는 것**이다.

따라서 요청서의 핵심 문장 "사용(standby 자신의 목적지 상태): … WHERE status <> 'INACTIVE'" 는 성립하지 않는 전제 위에 있다. 이것 하나로 claim_holds=false 다.

**정직하게 미확인으로 남기는 부분**: standby 의 로컬 dest 행이 `DATABASE_MODE='OPEN_READ-ONLY'`, `RECOVERY_MODE='MANAGED REAL TIME APPLY'` 같은 *자기 자신에 대한* 값을 부수적으로 노출하는지는 Oracle 공개 문서에서 **확인하지 못했다**(현장 관측은 있으나 1차 출처 아님). 설령 노출하더라도 그것은 real-time apply 여부의 보조 증거일 뿐 **transport 동기 여부에는 답하지 않는다**. 요청서는 이 미확인을 "부하 특성" 한 곳에만 표시하고 **의미론 쪽 미확인은 표시하지 않았다** — 이 비대칭이 이 항목의 가장 큰 결함이다.

### B. 더 좁은 권한으로 충분하다 (최소 범위 원칙 위반, confidence MEDIUM-HIGH)

standby 관점의 DG 상태를 읽는 **문서화된** 경로는 따로 있고, 전부 이 뷰보다 정보 노출이 좁다:

| 필요 | 정확한 객체 | 노출 |
|---|---|---|
| 보호 모드 / 현재 달성 중인 보호 수준 / role / open mode | `V_$DATABASE` (**1행**) | DB 이름·모드만. 호스트명·경로·DG 에러 문자열 **없음** |
| apply lag / transport lag + `DATUM_TIME`(신뢰 구간) | `V_$DATAGUARD_STATS` (수 행) | 지표 숫자만 |
| redo gap 존재 여부 (standby 관점) | `V_$ARCHIVE_GAP` (0~수 행) | thread#/seq# 만 |

`V$ARCHIVE_DEST_STATUS` 는 **아카이브 목적지 이름·다른 standby 의 DB_UNIQUE_NAME·DG ERROR 문자열(호스트명/경로 포함 가능)** 까지 딸려오는 **가장 넓은** 선택지다. 요청서 스스로 "역할 통째보다 객체 단위" 를 표방하면서, 같은 정보군 중 **가장 넓은 객체**를 골랐다.

특히 요청서의 "**GAP_STATUS 를 대체할 수단이 없다**" 는 **틀렸을 가능성이 높다**. `V$ARCHIVE_GAP` 은 physical standby 에서 아카이브 갭(THREAD#, LOW_SEQUENCE#, HIGH_SEQUENCE#)을 보이는 뷰로 문서화돼 있다. (채워지는 조건이 FAL 동작과 결부되는지는 **미확인** — 그러나 "대체 수단 없음" 이라는 단정은 근거 없이 항목 4의 유일성 논거를 부풀린다.)

**반대로 절대 넣지 말아야 할 것**: `V_$ARCHIVED_LOG`·`V_$LOG_HISTORY` (control file **레코드 스캔**, 행 수 수천~수만, CF enqueue 경합). gap 을 이쪽으로 fallback 하면 부하 논거가 완전히 뒤집힌다. 요청서는 이 함정을 언급하지 않는다.

### C. 부하 평가 — "세그먼트가 아니다 ⇒ 가볍다" 는 논리적 비약이다

1. **저장 형태와 직렬화 자원은 다른 축이다.** fixed view 가 메모리 상주라는 사실은 조회가 **latch/enqueue 를 잡지 않는다는 뜻이 아니다**. 이 뷰의 기반 X$ 구조는 Oracle 이 공개하지 않는다 → 기전은 **알 수 없음**이지 **0 이 아니다**. 요청서는 "세그먼트가 아니다" 를 근거로 즉시 LOW 로 이동했다. 요구된 기준("무엇을 몇 번 읽는가")을 이 항목만 충족하지 못한다.
2. **최악 경로 누락 — standby 지연이 primary 로 역류하는 경로.** 요청서는 "최악 = standby 조회 지연" 으로 닫았다. **MAX AVAILABILITY + SYNC transport** 에서는 이것이 standby 국한이 아니다. standby 측에서 redo 수신 경로(RFS/MRP, control file 갱신)와 자원 경합이 나면 primary 의 NSS/LGWR 가 `NET_TIMEOUT` 까지 커밋 대기 후 목적지를 실패 처리한다 → **생산라인 primary 의 커밋 지연**. 그리고 이 구성이 바로 항목 4 가 알아내려는 값이다. **즉 항목 4 의 안전 논거는 항목 4 가 얻으려는 사실을 미리 가정한다 — 순환이다.**
3. **완화책이 서버 측을 묶지 못한다.** JDBC statement timeout 2 초는 **클라이언트 대기**를 끊고 OCI break 를 보낼 뿐, 서버 세션이 커널 콜 안에서 잡고 있는 enqueue 를 즉시 놓는다는 보장이 없다. "2 초 하드 캡" 은 **DB 부하의 상한이 아니라 애플리케이션 대기의 상한**이다. 이 구분이 grant 요청서에 약정으로 들어가면 심사자를 오도한다.
4. **서킷 브레이커의 방향이 반대다.** "3회 연속 timeout 시 5분 정지" = DG 열화 구간(=DB 를 가만히 둬야 할 구간)에 **5분마다 3번 확정적으로 찌른다**. 열화 지속 시 일 864회의 "느린 조회" 를 보장하는 설계다.
5. **1,440회/일 계산은 RAC 를 무시한다.** standby 가 RAC 면 인스턴스별 세션 고정(N×1,440) 이거나 `GV$` 다. `GV_$ARCHIVE_DEST_STATUS` 는 **별도 객체(별도 grant)** 이고, GV$ 조회는 **인스턴스마다 병렬 슬레이브를 띄우는** 메커니즘이라 한 인스턴스가 무응답이면 조회 전체가 매달린다 — 프롬프트가 지목한 "병렬 폭주" 가 정확히 이 지점이다. 요청서는 standby 의 RAC 여부를 명시하지 않았다.
6. **"DB 부하를 줄이는 방향" 논거는 부풀려졌다.** 실패하는 `ALTER SESSION SYNC WITH PRIMARY` 500회는 세션 레벨 문장이며 redo·undo·세그먼트 I/O 가 **0** 이다. 뒤따르는 500건 추출 대비 무시 가능하다. 게다가 60초 캐시는 **정의상 stale** 이므로 경쟁 구간의 실패 경로를 **그대로 남겨야 한다** → 코드도 부하도 줄지 않는다. 순수한 알림 품질 개선일 뿐이다.

### D. 근거로 삼은 전제가 이 조직의 미확정 질문이다 (절차적 반증)

요청서는 "문서상 transport 가 SYNCHRONIZED 가 아니거나 apply 가 비활성이면 즉시 ORA-3173" 을 사실로 쓴다. 그러나 scope 문서 §2.1 이 인용한 **실제 문서 문장**은 *"blocks until redo apply has applied all redo data **received by the standby** at the time the statement is issued"* 이고, "SYNC transport·SYNCHRONIZED·real-time apply 전제" 는 **문서 인용이 아니라 scope 문서의 괄호 주석**이다. 그리고 `codex-cross-review-prompt-v2.0.md:78` 은 이것을 **미해결 질문으로 명시**한다 — *"그 전제가 깨졌을 때 **조용히 통과하는지** ORA-3173 으로 실패하는지"*.

문서 문언대로라면 이 문장은 **수신된 redo 한정**이므로 ASYNC 에서도 성공한다(약한 보장을 줄 뿐). 그렇다면 항목 4 가 막겠다는 "500건 동시 ORA-3173" 시나리오는 **존재하지 않을 수 있다.** 이 질문은 P v2.0 의 **FI-53 / G0-0** 에서 **비용 0 으로** 확정된다. **미확정 질문을 근거로 생산라인 원천에 grant 를 요청하는 것은 순서가 뒤바뀌었다.**

부수적으로 `g0-0a-capability-inventory.sql` 에는 **`V_$`/`GV_$` 접근 프로브 라인이 하나도 없다**. grant 가 실제로 먹혔는지 검증할 실측 항목조차 없는 상태에서 요청서를 낸다.

### E. switchover — 노출 문제가 아니라 **조용한 의미 반전**이다

요청서는 승격 후 "노출 범위가 넓어진다" 만 다뤘다. 실제 위험은 다르다: 승격 후 같은 쿼리가 **다른 DB 들에 대한 행**을 반환하므로 `SYNCHRONIZED='YES'` 의 의미가 "**내가** 동기화됨" → "**어떤 standby 가 나와** 동기화됨" 으로 **말없이 뒤집힌다**. role fence 는 회차를 막지만 이 모니터는 설계상 **hot path 밖**이므로, 캐시된 `SYNC_BARRIER 가용` 플래그가 **그럴듯하지만 무의미한 값**으로 계속 발행된다. 이건 인프라 정보 노출보다 나쁜 정합성 결함이다.

### F. 아키텍처 정합성 — D4 위반이자 read-then-decide 회귀

scope 문서 D4: "부록 W 를 **전제로 쓰지 않는다**. 참조가 생기는 순간 전제가 되고, 그것이 v1.2.3.1 이 무너진 방식이다." 항목 4 는 W3 을 **런타임 의존**으로 승격시킨다 — D4 가 금지한 바로 그 결합이다. 또한 §3 의 v2.0 제어 모델은 `assert-and-refuse` 이고 `read-then-decide` 는 폐기 대상인데, 항목 4 는 "**뷰를 읽어 → 판단 → 진행**" 을 다시 들여온다(캐시 60초 = 경쟁 창 60초). **v2.0 의 핵심 개선을 되돌리는 항목이다.**

### G. 요청 전술도 역효과다

"이것만은 거절해도 무방하다고 표시해 1~3 번 통과 확률을 올린다" 는 반대로 작동한다. 생산라인 DB 심사자가 읽는 문장은 *"부하 기전을 설명하지 못하지만 어쨌든 달라"* 다. 요청 묶음에 **작성자 스스로 불필요하다고 적은 항목**이 있으면, 1~3 번의 부하 기전 서술도 같은 수준의 엄밀성으로 의심받는다. **항목 4 는 요청서에서 빼는 것이 1~3 번에 유리하다.**

---

### 판정 요약
- 권한 **이름/문법**: 맞음(`SYS.V_$ARCHIVE_DEST_STATUS`, primary 에서 GRANT → redo 로 standby 전파). 단 **CDB 면 1문장이 아니다** — scope 문서 §2.2 스스로 적었듯 `ALTER USER … CONTAINER_DATA` 가 추가로 필요하다(요청서의 "GRANT DDL 1회" 는 부정확). CDB 여부 자체가 미확인.
- **standby 에서 목적한 기능을 열지 못한다** → claim_holds = false (결정적).
- **더 좁은 객체로 충분** → claim_holds = false (독립적으로도 충분한 사유).
- **부하 평가 낙관 + 최악 경로(SYNC 역류) 누락 + 완화책 오표시** → risk LOW → **MEDIUM**.
- MEDIUM 이지 HIGH 가 아닌 이유: 단일 저빈도 세션이고 대상이 standby 이며 MAX PERFORMANCE 구성이면 primary 역류 경로가 없다. 그러나 **어느 보호 모드인지 모르는 것이 이 항목의 존재 이유**이므로 MAX PERFORMANCE 를 가정해 LOW 로 내릴 수 없다.

---

### R5. EXECUTE ON SYS.DBMS_FLASHBACK — SCN 원점 복구 + SQL 재작성 제거

기본 입장대로 반증한다. 결론: **claim_holds = false.** 권한 이름은 맞지만, ⑴ 값 (1) 은 더 좁은 권한으로 충분하고, ⑵ 값 (2) 의 핵심 전제가 1차 출처와 어긋나며(요청서 스스로 미확인 표기), ⑶ 부하 산식의 단위가 틀렸고, ⑷ 위험 평가가 "운영상 주의 1건" 으로 축소한 것이 실은 v2.0 증거 모델이 원리적으로 탐지할 수 없는 무증상 오데이터 경로다.

──────────────────────────────
■ A. 권한 이름 — 이 부분은 살아남는다
`GRANT EXECUTE ON SYS.DBMS_FLASHBACK TO <user>` 는 실재하는 문법이고, 재설계 범위 문서 §2.2 가 인용한 Development Guide 문장("grant the EXECUTE privilege on DBMS_FLASHBACK")과 일치한다. 프로브(g0-0a-capability-inventory.sql:131)에 `dbms_flashback.get_scn` 항이 이미 들어 있어 실측 경로도 있다. SELECT ANY TRANSACTION 을 동반 요청에서 제외한 판단도 옳다 — 같은 문서가 Flashback Transaction Query 의 별도 요건으로 그것을 명시하고, TRANSACTION_BACKOUT 도 같은 권한을 요구하는 것으로 기술된다. **여기까지는 반증되지 않는다.**

■ B. 반증 1 — 값 (1) 은 더 좁은 권한으로 충분하다 (결정타)
Oracle 에는 **패키지 서브프로그램 단위 EXECUTE 가 없다.** 따라서 이 GRANT 는 정의상 "GET_SYSTEM_CHANGE_NUMBER 를 여는 최소 권한" 이 아니다. ENABLE_AT_TIME / ENABLE_AT_SYSTEM_CHANGE_NUMBER / DISABLE / TRANSACTION_BACKOUT 전 표면을 같이 연다.
값 (1) 만 필요하다면 DBA 스키마의 definer's rights 래퍼 함수 1개에 EXECUTE 를 주는 쪽이 **엄격히 더 좁고 기능은 동일**하다(corrected_grant Track A). 보고 규칙이 "역할 통째보다 객체 단위 grant 를 선호하라" 고 했는데, 그 원칙을 한 단계 더 밀면 "패키지 통째보다 함수 1개" 다. 이 대안이 존재하는 이상 현재 형태의 요청은 최소 범위 요건을 통과하지 못한다.
부수 반증: 요청서는 패키지 표면을 "GET_SYSTEM_CHANGE_NUMBER / ENABLE_AT_TIME / ENABLE_AT_SYSTEM_CHANGE_NUMBER / DISABLE + TRANSACTION_BACKOUT" 5개로 단정했다. **19c DBMS_FLASHBACK 의 실제 서브프로그램 전수 목록을 1차 출처로 확인한 흔적이 없다**(TRANSACTION_BACKOUT 오버로드만 해도 복수다). "남는 것은 전부 읽기 전용" 이라는 안전 논증이 검증되지 않은 목록 위에 서 있다. 미확인.

■ C. 반증 2 — 값 (2) 의 전제가 인용한 문장 자체와 충돌한다
요청서의 진짜 주장은 "ENABLE_AT_SYSTEM_CHANGE_NUMBER 를 sessionInitStatement 에 넣으면 생성 SQL 을 하나도 고칠 필요가 없다" 이고, fallback (3) 에서 "그 경로가 객체 FLASHBACK 권한을 우회한다면 이 단독 승인이 1·2번을 대체한다(미확인)" 로 조건을 붙였다.
그 우회 가설은 **재설계 문서가 인용한 바로 그 절과 충돌한다.** §2.2 표가 인용한 Development Guide 문장은 두 개다: ⒜ AS OF 계열에 대해 "grant FLASHBACK and either READ or SELECT privileges on those objects", ⒝ 같은 절에서 "grant the EXECUTE privilege on DBMS_FLASHBACK". 두 문장이 같은 절에 병렬로 놓였다는 것은 ⒝가 ⒜의 **추가 요건**이라는 독법이 자연스럽고, ⒜의 **대체**라는 독법은 그 절 어디에서도 지지되지 않는다. 객체 권한 검사는 "스냅샷을 어떻게 세팅했는가" 가 아니라 "그 객체를 과거 시점으로 읽는가" 에 붙는 것으로 기술돼 있다.
1차 출처에서 우회가 확인되지 않으므로 규칙대로 **미확인**이고, 반증 과제의 기본 입장상 **우회하지 않는다고 가정해야 한다.** 그러면 값 (2) 는 1·2번(객체 FLASHBACK) 승인에 전면 종속되고, 부록 W 의 "4순위 · 1번과 결합해야 실효 · 단독 가치 낮음" 판정은 **정확히 옳다**. 요청서의 "재검토 대상" 주장은 성립하지 않는다.

■ D. 반증 3 — "SCN 을 얻는 다른 무권한 수단이 없다" 는 필요성 논증이 거짓
세 갈래로 깨진다.
⑴ **v2.0 fence 는 SCN 을 아예 요구하지 않는다.** 재설계 §4.1 이 확정한 fence 는 `high = min(t0 − D − safety_lag, MAX(watermark))` 로 **타임스탬프 공간**이다. SCN 원점은 v1.2.3.1 §11.3(= D3 이 Profile O 로 동결한 문서)의 요구사항이다. 요청서는 **폐기된 규범을 근거로 권한을 요구**하고 있다. 이것이 D4 가 "부록 W 를 전제로 쓰지 마라" 고 경고한 바로 그 오염 경로다.
⑵ **부록 W 1번(객체 FLASHBACK)만 승인되면 `AS OF TIMESTAMP` 로 다중 세션 공통 시점이 성립한다 — SCN 도 DBMS_FLASHBACK 도 필요 없다.** W 1번 행이 되살리는 것으로 명시한 것이 정확히 그것이다(§4.2 `snapshot_scope` 교환 해소). 즉 "SCN 을 못 얻으면 병렬 일관성이 죽는다" 는 연결고리가 끊어진다.
⑶ ORA_ROWSCN(+ROWDEPENDENCIES)·TIMESTAMP_TO_SCN 이 무권한으로 살아 있다(요청서도 (1) 을 인정). 정밀도가 나쁜 것은 사실이나, 그 정밀도가 **필요했던 유일한 이유는 ZERO_GAP 등급이었고 D1 이 그것을 삭제했다.** 3초 근사가 등급을 못 올린다는 지적은 옳지만, 올릴 등급이 이미 없다.
⇒ "다른 수단이 없다" 는 문장은 삭제 대상이다.

■ E. 반증 4 — standby 성립 여부가 미확인인데 LIKELY 로 적혀 있다
⑴ 요청서가 근거로 삼은 A §11.3 자신이 "ADG(READ ONLY WITH APPLY)에서 세 값의 정확한 관계는 22장 DBA 확정" 이라고 미결로 남겼고, v1 리뷰(라인 194)도 "ADG 에서의 정확한 의미는 DBA 확인" 이다. codex 교차리뷰(라인 78)는 더 직설적이다: **"확인, ADG 동작 미확인 ... grant 존재만으로 capability 활성화 금지."** 즉 **standby 에서 GET_SYSTEM_CHANGE_NUMBER 가 무엇을 반환하는지 자체가 미확인**이다 — apply SCN 인가, 수신했으나 미적용인 redo 를 포함한 값인가. 후자면 그 SCN 으로 AS OF 를 걸었을 때 무엇이 일어나는지도 미확인이다.
⑵ ADG standby 에서 ENABLE_AT_SYSTEM_CHANGE_NUMBER **자체의 가부**는 어느 문서에서도 확인되지 않았다. 프로브에도 그 항이 **없다**(get_scn 만 있다). 값 (2) 전체가 실측 항목조차 없는 상태다.
⑶ 프리앰블 충돌이 검토되지 않았다. A §11.3 sessionInitStatement 는 이미 identity PL/SQL 블록 + STANDBY_MAX_DATA_DELAY + NLS 3종 + MODULE/ACTION 을 싣고 있고, v2.0 §3 은 여기에 `SET TRANSACTION READ ONLY` 와 `ALTER SESSION SYNC WITH PRIMARY` 를 더한다. flashback 모드 진입 후 ALTER SESSION 이 허용되는지, 트랜잭션 진행 중 ENABLE 이 가능한지(READ ONLY 트랜잭션과의 순서), §4.3 이 요구하는 chunk 경계 재단언이 flashback 모드에서 성립하는지 — **전부 미확인**이다. 관련 오류번호를 기억으로 적을 수 있으나 1차 출처로 확인하지 못했으므로 적지 않는다. 요청서가 "한 줄 추가로 끝난다" 고 단정한 부분이 실제로는 **순서 제약이 있는 다중 문장 조합**이고, 검증되지 않았다.
⇒ 이 상태의 확신도는 LIKELY 가 아니라 UNVERIFIED 다.

■ F. 반증 5 — 위험 평가가 뒤집혀 있다 (가장 중요한 반증)
요청서는 값 (2) 를 "텍스트 주입의 깨지기 쉬움을 없앤다" 는 **견고성 개선**으로 팔고, 커넥션 위생을 "운영상 주의 1건" 으로 각주 처리했다. 방향이 반대다.
A §11.3 마지막 문장이 결정적이다: **"fence revision 은 connection 별로 재검사하지 않는다 — `AS OF SCN` 은 SQL literal 이라 connection 과 무관하다."** 설계가 AS OF 주입을 택한 이유가 바로 이 **커넥션 무관성**이다. 세션 flashback 은 fence 를 SQL 리터럴에서 **커넥션 상태**로 옮긴다 — 설계가 의도적으로 제거한 성질을 되돌리는 것이다.
그 결과 새 실패 모드가 생긴다:
 · fence 가 안 걸린 커넥션(ENABLE 누락·재접속·풀 재사용)은 **현재 SCN 으로 조용히 읽는다.** 오류 없음.
 · flashback 모드로 오염된 커넥션이 반환돼 Guard 의 fence 조회에 재사용되면 **과거 SCN 을 fence 로 읽는다** → window 가 조용히 뒤로 밀린다 → 무증상 누락. 오류 없음.
두 경우 모두 **서버가 오류를 내지 않는다.** 그런데 v2.0 §3 이 확정한 증거 등급은 "**1급 = 서버 발신 오류 코드와 그 부재**" 다. 즉 이 실패 모드는 **v2.0 증거 모델이 원리적으로 탐지할 수 없는 종류**다. AS OF 텍스트 주입의 실패는 SQL 파싱 오류로 시끄럽게 죽지만(fail-closed), 세션 flashback 의 실패는 조용히 틀린 데이터를 낸다(fail-open). **생산라인 원천에서 fail-open 을 fail-closed 와 바꾸는 거래**이며, 요청서는 이 거래를 정반대 방향으로 서술했다.
추가 위험 2건:
 · **role transition 잔존**: read-only 라서 TRANSACTION_BACKOUT 이 불가하다는 논증은 **현재 role 에 대해서만** 참이다. GRANT 는 switchover/failover/snapshot standby 전환 후에도 남는다. SELECT ANY TRANSACTION 미보유가 backout 을 막아준다는 2차 방어는 유효해 보이나(§20.2.5), 이것도 1차 출처 재확인 대상이다.
 · **"primary 영향 0" 은 거짓**: standby 는 read-only 이므로 GRANT 는 **primary 에서만** 실행 가능하다. dictionary 재귀 DML + redo + SYS.DBMS_FLASHBACK 참조 커서 무효화가 발생하고, 생산라인 DB 이므로 변경창이 필요하다. 양은 미미하지만 "0" 은 아니고, 이 문장 하나 때문에 DBA 가 요청서 전체를 불성실하다고 읽을 수 있다.

■ G. 부하 평가 — load_correction 참조
단위 오류(계약당 1회 → 커넥션당 1회, 5~20배), 범주 오류("1번과 동일" → 실제로는 ≥, 세션 전 문장이 CR 재구성 대상), 누락(ORA-01555 를 한 글자도 안 적었다 — A §11.4 가 이미 정면으로 다루는 위험이고 같은 계약 RETRY 금지 규정까지 있다). 함수 호출 자체가 무부하라는 서술만 유효하다.

■ H. G0-0 지시에 대한 판정
요청서의 "G0-0 에서 이것부터 실측하라" 는 **옳고, 순서만 틀렸다.** 실측해야 할 첫 질문은 "GET_SYSTEM_CHANGE_NUMBER 가 도는가" 가 아니라 **"flashback 모드에서 객체 FLASHBACK 권한 없는 테이블이 읽히는가"**(corrected_grant P2)다. 그 답이 '아니오' 면 이 항목의 단독 가치는 0 이고 Track A 래퍼로 끝난다. '예' 면 그때 비로소 Track B 가 논의 대상이 된다. 프로브에 P1~P5 를 추가하지 않은 채 이 요청을 DBA 에게 올리면, 첫 질문에서 답이 막히고 이후 요청 전체의 신뢰가 깎인다.

■ I. 결론
권한 이름은 맞다. 그러나 ⓐ 값 (1) 에 대해 엄격히 더 좁은 대안(definer's rights 래퍼)이 존재하고, ⓑ 값 (2) 의 성립 전제가 1차 출처와 충돌하며 미확인이고, ⓒ standby 성립 여부가 프로브 항목조차 없이 미확인이며, ⓓ 부하 산식의 단위가 틀렸고, ⓔ 위험 서술이 방향까지 뒤집혀 있다(견고성 개선이 아니라 fail-closed → fail-open 교환). 다섯 중 하나만 있어도 claim_holds=false 이고, 다섯 개가 다 있다. 위험도는 LOW → **MEDIUM**. MEDIUM 의 근거는 함수 호출의 DB 부하가 아니라(그건 실제로 무시 가능하다) fence 무증상 누출 · standby 의미 미확인 · role 전환 후 권한 잔존 · 프리앰블 상호작용 미검증의 합이다.

---

### R6. SELECT_CATALOG_ROLE — 요청하지 말 것 (권고: 철회)

결론(“SELECT_CATALOG_ROLE 은 요청하지 않는다, 객체 단위 grant 로 간다”)은 옳다. 그러나 **claim 을 지탱하는 기전 4개 중 3개가 틀렸거나 이 환경(physical standby)에서 성립하지 않으며, 특히 이 과제가 명시적으로 의심하라고 한 두 축(권한 범위 정확성 / 부하 낙관)에서 모두 오류가 있다.** 그래서 claim_holds=false 로 둔다. 결론이 아니라 **요청서에 그대로 실릴 근거 문장**이 반증 대상이다.

**[1] 단일 최대 결격 사유로 내세운 기전이 틀렸다 (권한 범위 오류)**
claim: “V$SQL/V$SQLTEXT/V$SQL_BIND_CAPTURE 도달 → 이 standby 에서 실행된 **모든 애플리케이션의 SQL 텍스트**가 읽힌다 → 타 스키마 리터럴 유출.”
V$ 계열은 **인스턴스 로컬 SGA 구조의 투영**이다(19c Reference, “About Dynamic Performance Views”: 실제 뷰는 `V_$` prefix, `V$` 는 public synonym). physical standby 의 shared pool 에는 **standby 에서 파싱된 커서만** 존재한다. 생산 primary 에서 도는 업무 SQL 은 standby 의 V$SQL 에 **나타나지 않는다.** standby 에서 도는 것은 read-only 리포팅 워크로드와 우리 ETL 자신이 대부분이므로, 이 항목이 주장하는 “원천 업무 데이터가 SQL 리터럴을 타고 샌다”는 경로는 **claim 이 쓴 형태로는 성립하지 않는다.**
진짜 경로는 claim 이 놓쳤고, 더 나쁘다: **DBA_HIST_SQLTEXT / DBA_HIST_ACTIVE_SESS_HISTORY / DBA_HIST_SQLBIND 계열이다.** AWR 은 SYSAUX 세그먼트(WRH$_*)에 저장되고 SYSAUX 는 physical standby 로 **블록 단위 복제**된다. 즉 standby 에서 DBA_HIST_SQLTEXT 를 읽으면 **primary 에서 실행된 SQL 텍스트**가 나온다(ADG 에서 primary DBID 로 AWR 리포트를 뽑는 관행이 이를 전제한다). 노출 범위가 “standby 국소”가 아니라 **“생산 primary 전체”** 로 커진다. 즉 claim 은 결론 방향은 맞지만 근거 기전을 틀리게 써서, DBA 가 “standby V$SQL 에는 우리 업무 SQL 안 보이는데요?” 한마디로 반박하면 항목 전체의 신뢰가 무너진다. **문장을 DBA_HIST_*/SYSAUX 복제 기전으로 교체해야 한다.**

**[2] 부하 평가가 낙관적이다 — “부하 관점에서는 이 role 이 문제가 아니다”는 틀렸다**
claim: “같은 V$ 뷰를 읽으면 비용은 동일하다. 문제는 전적으로 정보 노출 범위다.”
이건 **role 이 여는 쿼리 집합이 달라진다는 점을 빠뜨린 오류**다. 객체 grant 5줄이 여는 것은 전부 **고정 크기 메모리 구조 스캔**(V$DATAGUARD_STATS 는 수 행, V$STANDBY_EVENT_HISTOGRAM 수십 행) — 물리 I/O 0, 논리 읽기 상수. 반면 SELECT_CATALOG_ROLE 은 **WRH$_ACTIVE_SESSION_HISTORY / WRH$_SQLTEXT / WRH$_SQLSTAT 등 GB 급 SYSAUX 세그먼트의 full scan** 과 DBA_TAB_COLUMNS(전 스키마 = OBJ$/COL$ 조인, 수십만 행) 을 여는 권한이다. 여기서 발생하는 **standby 고유 위험**:
 · 대형 SYSAUX 스캔이 standby buffer cache 를 밀어내고 **MRP(redo apply) 와 버퍼/IO 를 경합** → apply lag 증가. 우리가 관측하려는 바로 그 값을 우리가 악화시킨다. 생산라인 밀접 원천에서 lag 증가는 곧 ETL 신선도 fence(ORA-03172) 실패 연쇄.
 · read-only standby 의 장시간 쿼리는 **redo apply 가 undo 를 덮어쓰며 ORA-01555 에 노출**된다 — claim 은 이 축을 “부하 문제 아님”으로 통째로 삭제했다.
 · 40,000 run/일 · 정시 burst 500 인 플랫폼에서 누군가 이 권한을 관측 대시보드에 배선하면 상수 비용이 아니라 **burst 배수로 증폭**된다. role 은 그 배선을 코드리뷰 없이 가능하게 만든다.
따라서 이 항목의 정확한 문장은 “부하는 동일하다”가 아니라 **“객체 grant 는 부하 상한이 상수로 증명되고, role 은 부하 상한이 없다”** 이다. 이게 생산라인 DB 심사자가 실제로 듣고 싶은 문장이다.

**[3] “객체 grant 5줄로 100% 커버”가 미검증 — RAC standby 에서 깨진다**
19c Reference V$DATAGUARD_STATS: apply lag 은 **“relevant only to the applying instance”**. standby 가 RAC 이면 MRP 는 한 인스턴스에서만 돌고, 우리 세션이 다른 인스턴스에 붙으면 **apply lag 이 비거나 무의미**하다. 5줄 목록에는 **GV_$ 가 하나도 없다.** 즉 “100%” 는 단일 인스턴스 standby 라는 미확인 전제 위에서만 참이다. 이 상태로 요청서를 내면 (a) 관측 축이 실제로 안 열리거나 (b) 나중에 GV_$ 추가 요청을 다시 해야 해서 “범위를 특정했다”는 협상 자산이 스스로 무너진다. **standby 구성이 RAC 인지 먼저 확인하고, RAC 면 GV_$DATAGUARD_STATS 를 처음부터 목록에 넣어야 한다.** (부수적으로: 이 지점은 role 이 “우리가 필요한 것을 하나도 추가하지 않는다”는 claim 의 문장도 반증한다 — role 은 GV_$ 를 자동 포함한다. 물론 답은 role 이 아니라 GV_$ 한 줄 추가다.)

**[4] 근거 6번(definer’s rights)은 이 환경에서 무의미하고, 스스로의 역제안과 모순된다**
“role 권한은 definer’s rights PL/SQL 에서 비활성” 자체는 맞다. 그러나 **physical standby 는 read-only 라 etl_reader 가 저장 프로시저를 만들 수 없다** — “서버 측 저장 코드 경로”가 애초에 없다. 이 항목은 요청서에서 빼야 한다(틀린 말을 쓰면 심사자가 나머지도 의심한다). 더구나 claim 은 마지막에 **전용 role(`etl_dg_monitor`) 역제안**을 하는데, 그 role 도 정확히 같은 definer’s rights 제약을 상속한다. 6번을 근거로 쓰면서 role 을 역제안하는 것은 내부 모순이다. (역제안 자체는 유지해도 좋다 — 다만 근거는 “관리 편의는 DBA, 범위는 우리” 하나로 충분하다.)

**[5] 요청서 형식 오류 — GRANT 는 standby 에서 실행 불가**
claim 전체가 “standby 대상 권한”처럼 서술되지만, physical standby 는 read-only 라 GRANT DDL 이 실행되지 않는다(ORA-16000 계열). **모든 grant 는 primary 에서 실행되어 redo 로 전파된다.** 즉 “원천 primary 에 부하는 없지만 악영향은 있다”는 문장은 부정확하다 — 요청 자체가 **primary 측 dictionary DDL**(행 몇 개, redo 수 KB, 상수)이다. 부하는 사실상 0 이지만 **“primary 를 건드리지 않는다”고 쓰면 거짓**이 되고 심사자가 잡아낸다. 요청서에 “primary 에서 1회 실행, dictionary row 단위, redo 수 KB, 세션/커서 영향 없음” 으로 명시해야 오히려 통과율이 올라간다.

**[6] 라이선스 근거는 방향은 맞지만 기전이 부정확**
“권한이 있었다는 사실만으로 라이선스 감사 부담”은 과장이다. Diagnostics/Tuning Pack 사용 판정의 실질 기전은 **DBA_FEATURE_USAGE_STATISTICS 의 실사용 기록**과 **CONTROL_MANAGEMENT_PACK_ACCESS 파라미터** 이지 grant 존재가 아니다. 다만 “권한이 있으면 누군가 조회하고, 조회 순간 위반”은 참이므로 근거로는 살아남는다. 문장을 “grant 자체가 라이선스 위반”이 아니라 **“grant 는 위반 가능 상태를 만들고, 위반 여부는 DBA_FEATURE_USAGE_STATISTICS 로 사후에만 드러난다 — 사전 차단이 유일한 통제”** 로 고쳐야 1차 출처 기준에 맞는다.

**확인/미확인 구분**
· 확인(1차): V_$ 가 실뷰·V$ 가 public synonym, 설치 직후 SYS/SYSDBA 만 접근(19c Reference “About Dynamic Performance Views”). apply lag 은 applying instance 에서만 유효(19c Reference V$DATAGUARD_STATS). DBA_* 는 SYSDBA / SELECT ANY DICTIONARY / SELECT_CATALOG_ROLE 로 조회(scope §, 19c Reference 인용).
· **미확인**: SELECT_CATALOG_ROLE 의 **정확한 포함 객체 집합**. 2차 출처(Oracle Forums, 다수 DBA 블로그)는 V$ 계열 포함이라고 하지만, Oracle 19c 문서에서 “SELECT_CATALOG_ROLE 이 V$ 를 포함한다”는 명시 문장을 찾지 못했다. 포함 집합은 버전·패치별로 변한다(이 자체가 claim 의 근거 4번 “감사 불가능”을 지지한다). 요청서에 role 포함 객체를 열거하지 말 것 — 지어내면 그 자리에서 신뢰를 잃는다.
· 미확인: standby 가 RAC 인지, ADG remote AWR(SYS$UMF, 12.2+)이 구성돼 있는지. 둘 다 요청 전에 확인해야 한다.

**최종 판정**: 철회 권고(=최종 행동)는 유지. 그러나 근거 6개 중 1·2·6 은 기전이 틀렸거나 부정확하고, 6 은 이 환경에서 무효이며, “5줄 100% 커버”는 RAC 에서 거짓이고, 요청 실행 위치(primary) 서술이 빠졌다. 요청서에 실을 문장으로는 **부적합** → claim_holds=false.
**대안(fallback)**: 이 항목은 거절 대상이 아니라 우리가 빼는 항목이므로 fallback 개념이 없다. 다만 DBA 가 “role 로 주겠다”고 역제안할 경우의 대응(전용 role 신설 + 위 객체만 수납)은 유효하며 유지한다.

---

### R7. RM-1 · ETL 전용 Resource Manager consumer group 배치 (CPU 상한 + 활성세션 큐잉)

## 판정: claim_holds = false. 권한 이름은 대체로 맞지만 **배치 설계가 틀렸고, 그 결과 "primary 는 안 건드린다"는 핵심 안전 주장이 성립하지 않는다.** 부하 평가도 CPU 축만 보고 있어 낙관적이다.

---

### F-1 (치명, 설계 자체를 무효화) — 물리 standby 는 플랜 **내용**을 primary 와 다르게 가질 수 없다

기전: consumer group / plan / plan directive / consumer group mapping / switch privilege는 **전부 데이터 딕셔너리 행**이다. 물리 standby 의 딕셔너리는 primary 의 블록 단위 복제본이다. 따라서 `DBA_RSRC_PLAN_DIRECTIVES` 의 내용은 primary 와 standby 가 **정의상 동일**하다. primary 와 standby 가 다르게 가질 수 있는 것은 **`RESOURCE_MANAGER_PLAN` 인스턴스 파라미터(= 어느 플랜을 활성화할 것인가) 하나뿐**이다.

그런데 요청서는 `PLAN => '<STANDBY 에서 이미 활성인 플랜 이름>'` 에 디렉티브를 **추가**하라고 지시하고, 동시에 "새 플랜으로 갈아끼우지 말 것"이라고 못 박는다. standby 의 활성 플랜 이름이 primary 의 활성 플랜 이름과 같은 경우(기본 구성에서 압도적으로 흔하다 — 양쪽 다 `DEFAULT_PLAN` 이거나 사이트 표준 플랜 하나를 쓴다), 이 한 줄은 **생산라인 DB 인 primary 의 살아있는 자원 배분을 그 자리에서 바꾼다.** `SUBMIT_PENDING_AREA` 는 활성 플랜에 대한 변경을 즉시 반영한다.

즉 요청서의 문장 — "primary 에 가는 영향은 [1]의 딕셔너리 행 추가뿐이고 primary 의 활성 플랜은 건드리지 않는다" / "이 한 줄[3]이 이 목록에서 유일하게 남에게 피해를 줄 수 있는 문장이다" — **둘 다 거짓**이다. 위험한 문장은 [3]이 아니라 [1]이고, 요청서가 붙인 안전 가이드("새 플랜 만들지 마라")는 **정확히 거꾸로**다. 올바른 설계는 그 반대다: 기존 활성 플랜의 디렉티브를 복제한 **별도 플랜을 새로 만들고, standby 인스턴스에서만 그 플랜을 활성화**한다.

### F-2 — `MGMT_P1 => 10` 은 "우리만 추가"가 아니라 **타 그룹 재비례**다

`MGMT_Pn` 은 백분율이고 한 레벨의 합은 100 이하여야 한다. 기존 활성 플랜의 level 1 합이 이미 100이면 우리 디렉티브 추가로 110이 되어 `VALIDATE_PENDING_AREA` 가 실패한다 → DBA 는 **다른 그룹의 몫을 깎아야만** 이 요청을 이행할 수 있다. 합이 100 미만이어도 우리가 10을 가져가는 만큼 나머지의 상대 비중이 바뀐다. "하위로 추가되어 남에게 영향 없음"은 성립하지 않는다.
추가로: 기존 플랜이 (백분율이 아닌) **`SHARES` 기반**이면 같은 플랜 안에서 `MGMT_P1`(백분율)과 `SHARES` 를 **혼용할 수 없어** 스크립트가 그대로는 실패한다. 그리고 모든 플랜은 `OTHER_GROUPS` 디렉티브를 반드시 포함해야 하는데 요청서 스크립트에는 없다(기존 플랜에 이미 있다는 전제인데, 별도 플랜을 만드는 순간 필수가 된다).

**결론: 목적(우리 상한)에 실제로 필요한 것은 `UTILIZATION_LIMIT` 하나뿐이다. `MGMT_P1` 은 최소 범위 원칙 위반이며 유일하게 타 그룹을 건드리는 파라미터다. 빼라.**

### F-3 — 1차 출처 인용이 **다른 파라미터에 대한 문장**이다 (부하 평가의 근거가 무너진다)

인용된 Admin Guide 문장("does not enforce allocations until CPU usage is at 100%")은 **자원 배분(`MGMT_Pn` shares)** 에 관한 서술이다. `UTILIZATION_LIMIT` 은 성격이 다르다 — **경합이 없어도 강제되는 절대 상한**이다. 따라서 "standby 가 한가하면 우리 워크로드는 전혀 느려지지 않는다"는 **틀렸다.** 텅 빈 standby 에서도 ETL 은 CPU 30% 에 묶인다.
이게 단순한 자기 손해가 아닌 이유는 F-5 로 이어진다.

### F-4 — 부하 평가가 **CPU 축만** 본다. MRP 기아 시나리오는 닫히지 않는다

"우리가 standby CPU 를 포화시켜 MRP 를 굶긴다는 시나리오가 구조적으로 불가능해진다"는 **과대 주장**이다. Resource Manager 가 통제하는 것은 CPU, 병렬 서버 수(`PARALLEL_SERVER_LIMIT`/`PARALLEL_DEGREE_LIMIT_P1`), undo 풀, idle time, call/IO **임계 기반 스위치(`SWITCH_IO_MEGABYTES`/`SWITCH_IO_REQS`)** 다. **물리 I/O 대역폭 rate limit 은 Exadata IORM 밖에서는 존재하지 않는다.** 버퍼 캐시 점유·PGA 도 통제 대상이 아니다.

Full 60% 워크로드의 지배적 비용은 CPU 가 아니라 **full table scan 의 I/O 와 버퍼 캐시 flush** 다. 500 세션 동시 스캔은 RM 이 걸려 있어도 스토리지 대역폭을 채우고 캐시를 밀어낸다. MRP 는 redo 적용을 위해 블록을 읽어야 하고, 여기서 굶는다. 즉 RM-1 은 MRP 기아의 **CPU 하위 사례만** 막는다. 그리고 요청서는 **병렬 통제 파라미터를 하나도 요청하지 않는다** — 원천 테이블에 `DEGREE > 1` 이 걸려 있거나 Spark 가 힌트를 넣으면 병렬 폭주 경로가 그대로 열려 있다.

### F-5 — 스로틀이 **ORA-01555 위험을 올린다** (자기가 막겠다는 위험을 키우는 방향)

v2.0 §11.4 는 `AS OF SCN` 을 버리고 **`SET TRANSACTION READ ONLY`** 로 갔다. 즉 모든 추출 세션이 열린 읽기 전용 트랜잭션 안에서 돈다. CPU 를 30% 로 묶고 최대 600초를 큐에 세우면 **세션 수명이 길어진다.** ADG standby 에서 긴 읽기 전용 쿼리는 primary 에서 넘어온 undo 로 읽기 일관성을 유지해야 하고, 수명이 길어질수록 **standby 측 ORA-01555 확률이 올라간다.** 요청서는 최악 케이스로 ORA-01555 를 요구받았는데 언급이 없다. 방향까지 반대다(스로틀이 완화가 아니라 악화 요인).

### F-6 — `ACTIVE_SESS_POOL_P1` + `QUEUEING_P1` 은 "거절 폭풍 → 순번 대기"가 아니다. **10분 지연된 거절 폭풍 + 세션 슬롯 점유**다

(a) 큐 대기가 `QUEUEING_P1` 을 넘으면 문장은 **오류로 죽는다**(리소스 매니저 큐 타임아웃; 오류 번호는 ORA-07454 로 알려져 있으나 *1차 출처 확인 필요*). 500 burst 대 pool 40 에서 평균 추출이 48초를 넘으면 꼬리가 600초를 넘어 실패한다. Full 적재 비중 60% 에서 이건 예외가 아니라 기대값이다. **10분을 태운 뒤 오는 실패는 즉시 실패보다 나쁘다** — SLA 창을 태우고 재시도 폭풍을 뒤로 밀어 겹치게 한다.

(b) 더 중요한 것: **큐잉은 세션을 해제하지 않는다.** 큐에 선 세션은 연결·프로세스 슬롯을 그대로 점유한다. 빨리 실패하면 슬롯이 회전하는데, 큐에 세우면 배수 속도가 느려져 **동시 접속 최대치가 올라간다.** standby 의 `PROCESSES`/`SESSIONS` 한도를 압박하고, 그 한도가 차면 **ETL 이 아닌 다른 사용자**(DBA 접속, 모니터링 에이전트)가 ORA-00018/ORA-00020 을 맞는다. 요청서가 "남에게 피해를 줄 수 있는 유일한 문장"이라고 지목한 [3] 말고, **[1]의 이 두 파라미터가 제3자 피해 경로다.** v2.0 U-3(세션 절대 한도 미증명)이 살아 있는 상태라 이 압박의 상한도 우리는 모른다.

(c) **미확인, 반드시 확인**: 이미 트랜잭션이 열린 세션에 대해 active session pool 큐잉이 어떻게 동작하는지. v2.0 의 모든 추출 세션은 `SET TRANSACTION READ ONLY` 로 트랜잭션을 연 상태다. 트랜잭션 중 세션이 큐잉 대상에서 빠진다면 pool 은 **우리 워크로드 패턴에 대해 애초에 바인딩되지 않는다** — 즉 이 요청의 주된 판매 포인트가 통째로 무효다. Oracle 문서로 확정하기 전에는 이 파라미터를 근거로 삼지 마라.

### F-7 — "우리가 우리 상한을 못 푼다"가 **증명되지 않았다** (미확인, 영향 큼)

`GRANT_SWITCH_CONSUMER_GROUP(..., GRANT_OPTION => FALSE)` 는 ETL_BULK_READ **로 들어가는** 권한이다. 그러나 `DEFAULT_CONSUMER_GROUP` 에 대한 switch 권한이 **PUBLIC 에 부여되어 있다면**, ETL 계정은 `DBMS_SESSION.SWITCH_CURRENT_CONSUMER_GROUP('DEFAULT_CONSUMER_GROUP')`(DBMS_SESSION 은 PUBLIC EXECUTE) 로 스스로 스로틀 밖으로 나갈 수 있다. 그러면 "ADMINISTER_RESOURCE_MANAGER 를 안 받으니 우리가 우리 상한을 못 푼다"는 제안의 취지가 그대로 무너진다. Oracle 사전 정의 consumer group 목록의 PUBLIC switch 부여 여부를 **1차 출처로 확인하기 전에는 DBA 에게 "우리는 못 빠져나갑니다"라고 약속하면 안 된다.** (확인 결과 열려 있다면: 그룹 이탈 시 알림이 필요한데 우리는 관측 수단이 없다 → F-8 로 이어진다.)

### F-8 — 열겠다는 공시축 `DB_ENFORCED` 를 **우리는 관측할 수 없다**

G0-0 프로브가 확정한 `SYS_CONTEXT('USERENV', …)` 14개 속성에 **세션의 consumer group 을 알려주는 속성은 없다**(프로브 파일 114–128행 확인). `V$`/`GV$` 는 v2.0 §2.2 에서 죽어 있다. 즉 플랫폼은 자기 세션이 실제로 `ETL_BULK_READ` 에 들어갔는지, 스로틀이 실제로 걸렸는지 **한 번도 관측할 수 없다.** 그 상태로 `source_admission_control = DB_ENFORCED` 를 공시하는 것은 v2.0 §5(U-1의 "미발생은 fence 가 걸렸다는 증거가 아니다", U-3의 "한도 자체가 자기 선언값") 이 세운 규율을 **정확히 그대로 위반**한다. 관측 없는 ENFORCED 는 SELF_LIMITED_ONLY 보다 나쁘다 — 틀린 확신을 준다.
→ 최소 추가 요청이 필요하다: `GRANT SELECT ON SYS.V_$RSRC_SESSION_INFO TO <ETL_USER>;` (CDB 면 `ALTER USER … CONTAINER_DATA` 추가 필요 — v2.0 §2.2 각주). 이게 거절되면 **`DB_ENFORCED` 공시는 포기하고 `SELF_LIMITED_ONLY` 를 유지하되 "서버측 CPU 상한이 걸려 있다(미검증)"를 리스크 대장 주석으로만** 남겨야 한다.

### F-9 — 축 값 `DB_ENFORCED` 는 "되돌리는" 것이 아니라 **신설**이다

v2.0 §6 에서 `source_admission_control` 의 값 집합은 `SELF_LIMITED_ONLY` **고정 1개**다. `DB_ENFORCED` 는 정의된 적이 없다(요청서는 `source_staleness` 축의 `DB_ENFORCED_MAX_DELAY(N)` 에서 이름을 빌려온 것으로 보인다). "강등된 값을 되돌린다"는 서술은 부정확하다 — 새 enum 값 정의 + 소비자 노출 문구 개정이 함께 필요하다. 또한 부록 W(협상용 5건)에 Resource Manager 는 들어 있지 않으므로, 이건 W 확장 안건이지 기존 wish-list 항목의 실행이 아니다.

### F-10 — 스크립트 배치 오류 및 미확인 전제 (부차)

- `[2] GRANT_SWITCH_CONSUMER_GROUP` 은 **딕셔너리 쓰기**다 → read-only standby 에서 실행 불가. 요청서는 [1]에만 "PRIMARY 에서"를 붙이고 [2]는 실행 위치를 명시하지 않았다. **PRIMARY 명시 필요.**
- `SET_CONSUMER_GROUP_MAPPING(ORACLE_USER, …)` 도 딕셔너리 전역이라 primary 에도 적용된다. 요청서는 "primary 에 접속하더라도 스로틀된 그룹에 들어가므로 방향이 안전"이라고 하는데 **틀렸다**: primary 의 활성 플랜에 `ETL_BULK_READ` 디렉티브가 없으면 세션은 `OTHER_GROUPS` 로 떨어진다 — 스로틀 **안 걸린다**. (F-1 의 잘못된 설계에서는 걸리는데, 그건 primary 를 건드렸기 때문이다. 둘 중 하나만 참이다.)
- **CDB/PDB 여부가 전제로 명시되지 않았다.** 원천이 PDB 라면 PDB resource plan 은 제약이 다르고(subplan 불가, consumer group 수 상한 존재), CDB 레벨 플랜의 디렉티브는 consumer group 이 아니라 PDB 를 대상으로 한다. 스크립트가 그대로는 안 돌 수 있다. **미확인 — DBA 사전 조사 항목.**
- `RESOURCE_MANAGER_PLAN` 은 Scheduler 유지보수 윈도우가 `DEFAULT_MAINTENANCE_PLAN` 으로 갈아치울 수 있고 `FORCE:` 접두사로 막는다. read-only standby 에서 Scheduler 윈도우가 도는지는 **미확인**이나, 비용이 0 이므로 `FORCE:` 를 붙여라. 안 붙였다가 야간 배치 창에서 상한이 조용히 사라지는 편이 훨씬 나쁘다.
- `SCOPE=BOTH` 는 spfile 에 남기므로 RAC standby 면 `SID='*'` 명시.

---

### 살아남는 것

`ADMINISTER_RESOURCE_MANAGER` 를 우리가 받지 않는다는 원칙, `GRANT_OPTION => FALSE`, "서버측 CPU 상한을 대체할 무권한 수단이 없다"는 fallback 서술, 딕셔너리 쓰기가 대상 테이블 블록을 추가로 읽지 않는다는 관찰 — 이 넷은 맞다. **`UTILIZATION_LIMIT` 단독 + 별도 플랜 + standby 전용 활성화** 로 좁히면 실제로 LOW 에 가까운, 통과 가능성 높은 요청이 된다. 지금 형태로는 아니다.

### 위험도: LOW → **MEDIUM**

근거: (1) 작성된 대로 실행하면 **생산라인 primary 의 활성 자원 배분이 바뀐다**(F-1, F-2), (2) 큐잉이 제3자 세션 슬롯 고갈 경로를 만든다(F-6b), (3) 지연된 대량 실패 경로(F-6a), (4) 막겠다는 MRP 기아의 지배적 원인(I/O·캐시)을 못 막으면서 막았다고 공시한다(F-4), (5) 집행 사실을 관측할 수 없어 공시가 근거 없는 확신이 된다(F-8). HIGH 는 아니다 — 디렉티브는 CPU 스케줄러 계층이고 추가 블록 읽기가 없으며, 아래 교정안으로 좁히면 실제로 안전하다. 다만 "LOW / LIKELY" 로 DBA 에게 제출하면 안 된다. 교정 후 재제출 기준 확신도: LIKELY(단, F-7·F-6c 는 1차 출처 확인 전까지 MEDIUM 이하).

---

### R8. FLASHBACK ANY TABLE (시스템 권한) — 규모 때문에 오히려 이쪽이 덜 침습적일 수 있다

## 결론: 반증됨 (claim_holds = false)

권한 이름·SYS/AUDSYS 제외·"SELECT 를 주지 않는다" 세 가지는 **1차 출처로 확인되어 맞다**. 그러나 이 요청을 정당화하는 **핵심 논거 (a)·(b) 가 둘 다 무너지고**, "primary 에 영향 0" 이 성립하지 않으며, 부하 비교가 잘못된 기준선에서 이루어졌다.

---

### A. 맞는 부분 (먼저 인정)

| 주장 | 1차 출처 | 판정 |
|---|---|---|
| `FLASHBACK ANY TABLE` 이 AS OF 를 연다 | SQL Ref, SELECT / flashback_query_clause: "you must have the READ or SELECT privilege on the objects in the select list. **In addition**, either you must have FLASHBACK object privilege on the objects in the select list, or you must have FLASHBACK ANY TABLE system privilege." | 정확 |
| SELECT 를 주지 않는다 / 새로 보이는 객체 없음 | 위 문장이 READ|SELECT 를 **별도로** 요구 | 정확 |
| SYS·AUDSYS 제외 | GRANT Table 18-1 원문: "Issue a SQL Flashback Query on any table, view, or materialized view in any schema **except SYS, AUDSYS**." + "Note that ANY system privileges … will not work on SYS objects or other dictionary objects." | 정확. 지어낸 것 아님 |
| FLASHBACK TABLE(DDL) 로 번지지 않음 | FLASHBACK TABLE Prerequisites: FLASHBACK ANY TABLE **에 더해** READ/SELECT + **INSERT, DELETE, ALTER** 필요. ETL 계정에 없고, standby 는 read-only | 정확 |

여기까지는 이 항목이 1번(객체 단위)보다 **보안 노출이 크게 다르지 않다**는 주장을 지지한다. 문제는 그 다음이다.

---

### B. 치명적 반증 1 — 논거 (a) "DBA 티켓이 없어진다" 는 **거짓**이다

이것을 저자 스스로 "실무상 가장 큰 차이" 라고 썼으므로, 이게 무너지면 ANY 를 선택할 근거의 대부분이 사라진다.

신규 Job 이 신규 테이블을 읽으려면 **SELECT 권한이 어차피 필요하다.** 이 계정은 `SELECT ANY TABLE` 이 없고(동반 요청 금지로 본인이 명시), 재설계 범위 문서 §2.2 도 계정을 "열거된 객체에 SELECT만" 으로 전제한다. 따라서:

- 신규 테이블 → **DBA 티켓은 반드시 발생한다** (`GRANT SELECT`)
- FLASHBACK ANY TABLE 이 있어도 이 티켓은 **1건도 줄지 않는다**
- 객체 단위 경로에서 추가되는 것은 이미 발생하는 그 티켓의 문장에 단어 하나: `GRANT SELECT, FLASHBACK ON x TO etl;` — **DDL 문 1개, library cache lock 이벤트 1회, 증분 0**

즉 (a) 가 주장하는 "10,000 Job 규모 때문에 ANY 가 덜 침습적" 은 **존재하지 않는 티켓을 절약한다고 계산한 것**이다. 절약되는 것은 기존 테이블에 대한 **일회성 백필** 뿐이고, 이건 창구 문제이지 권한 모델 문제가 아니다.

### C. 치명적 반증 2 — 논거 (b) "view/synonym 추적 문제" 는 근거가 반대다

1. **1차 출처는 애매하지 않다.** SQL Ref 는 `FLASHBACK object privilege on **the objects in the select list**` 라고 쓴다 — FROM 절에 적힌 객체(=view)이지 base table 이 아니다. "base table 까지 개별 grant 를 추적해야 한다" 는 전제 자체가 문서에 없다. (view 에 FLASHBACK object privilege 를 grant 할 수 있는지 GRANT Table 18-2 행은 **직접 인출 실패 — 미확인**. 그러나 Dev Guide §20.2.5 "grant FLASHBACK and either READ or SELECT privileges on **those objects**" 와 정합.)
2. **추적은 무권한으로 가능하다.** `ALL_DEPENDENCIES` · `ALL_SYNONYMS` · `ALL_VIEWS` 는 이미 "권한 없이도 되는 것" 목록에 있다. view→base table 그래프는 플랫폼이 스스로 열거할 수 있다. "조용히 깨진다" 는 위험은 권한이 아니라 publish validator 로 닫는 문제다 — 그리고 저자 본인이 fallback 에서 그 validator 를 이미 설계했다.
3. **ANY 가 고쳐주지 못하는 하드 제약을 빠뜨렸다.** SQL Ref, Restrictions on Flashback Queries: **"You cannot use the VERSIONS clause in flashback queries to views."** view 경유 Job 의 VERSIONS 기반 변경추적은 FLASHBACK ANY TABLE 로도 살아나지 않는다. (b) 는 "view 문제가 사라진다" 고 썼지만 view 의 진짜 제약은 권한이 아니라 문법이고, 그건 그대로 남는다.

### D. 치명적 반증 3 — "최악의 경우도 primary 에 영향 0" 은 성립하지 않는다

두 갈래로 깨진다.

**(D-1) grant 자체가 primary DDL 이다.** standby 는 read-only 이므로 이 GRANT 는 **생산라인 primary 에서 실행되어 redo 로 전파된다.** "sysauth$ 1행" 이라는 서술은 맞지만, 시스템 권한 grant 가 shared pool 커서 무효화를 유발하는지는 **1차 출처로 확인되지 않았다(미확인)**. 확인 안 된 것을 "영향 0" 으로 적으면 안 된다.

**(D-2) 진짜 문제 — 이 권한만으로는 기능이 안정적으로 열리지 않고, 그 다음 요청이 생산라인을 세운다.**

- Dev Guide §20.2.1 Note 원문: "Additional configuration of the `UNDO_RETENTION` parameter is required **only if you use Oracle Flashback operations or Active Data Guard**." — 우리는 **양쪽 다**다.
- §20.2.1: "Setting UNDO_RETENTION does not guarantee that unexpired undo data is not discarded. If the system needs more space, Oracle Database **can overwrite unexpired undo**."
- ADG standby 의 undo 는 primary redo 의 물리 복제본이다. standby 는 자기 쿼리를 근거로 retention 을 늘릴 수 없다. Full 60% × 40,000 run/일, 정시 500 burst 로 AS OF 스캔을 돌리면 **ORA-01555 는 예외가 아니라 정상 운영 곡선**이다.
- 그때 나올 다음 티켓이 문제다. Administrator's Guide §16.2.2.3 원문: *"If retention guarantee is enabled, then the specified minimum undo retention is guaranteed; the database never overwrites unexpired undo data **even if it means that transactions fail due to lack of space in the undo tablespace**. … **WARNING: Enabling retention guarantee can cause multiple DML operations to fail. Use with caution.**"* → ORA-30036. **primary DML 실패 = 물리적 생산 정지.**

즉 FLASHBACK ANY TABLE 은 그 자체로는 무해하지만, **무해한 상태로는 목적을 달성하지 못하고**, 목적을 달성하려면 primary 의 undo 용량/retention 을 건드려야 한다. 이건 "영향 0" 이 아니라 **foot-in-the-door 요청**이다. 이 사실을 요청서에 적지 않고 LOW 로 제출하는 것이 가장 큰 문제다.

**(D-3) 부수 위험 — DR 목표 훼손.** AS OF 는 CR 블록 재구성을 위해 테이블 블록 **위에** undo 블록을 추가로 읽는다. standby 버퍼캐시/IO 를 redo apply(MRP)와 경합시킨다. apply lag 이 늘면 RPO 가 나빠진다 — primary 를 건드리지 않고도 생산 리스크가 증가하는 경로다. (정량은 워크로드 의존 — 미측정.)

### E. 부하 비교의 기준선 오류 + 런타임 서술 반증

- "객체 단위 쪽 grant 총 침습도가 크다" 는 **이미 발생한/어차피 발생할 `GRANT SELECT` 이벤트를 증분으로 계산**했다(B 참조). 올바른 증분은 "기존 테이블 일회성 백필" 뿐이고, 그건 **점검창에 스케줄 가능**하다. FLASHBACK ANY TABLE 은 스케줄 불가능한 **상시 권한**이다. 일회성·관측가능·창구제어 가능한 비용 vs 영구·무경계 권한을 맞바꾸는 거래다.
- "런타임 블록 읽기 수는 1비트도 다르지 않다" 는 **ANY vs 객체 비교로는 참**이다. 그러나 SQL Ref 가 명시적으로 경고한다: *"When performing a flashback query, Oracle Database **might not use query optimizations** that it would use for other types of queries, which could have a negative impact on performance."* 이 문장이 요청서에 없으면 심사자는 "AS OF ≈ 평범한 SELECT" 로 오독한다.
- ORA-01555 · undo 압박 · redo 증가는 서술에 **전혀 없다.** 프롬프트가 명시적으로 요구한 최악 케이스가 누락됐다.

### F. 최소 범위 원칙 위반 + 미공개 능력 1건

- 보고 규칙이 "역할 통째보다 객체 단위 grant 를 선호하라" 인데, 이 요청은 그 정반대의 시스템 ANY 권한이다. 저자도 "보안 심사에서 거부 가능성이 실질적으로 가장 높다" 고 인정한다 — **거부 가능성이 가장 높은 안을 1순위로 올리는 것 자체가 협상 설계 실패**다. DBA 신뢰 예산은 유한하고, 이 요청은 Appendix W 2·3번(정보 통보, 통과 가능성 최고) 까지 같이 태워버린다.
- **요청서가 빠뜨린 능력**: FLASHBACK TABLE Prerequisites 원문 — *"To flash back a table **to a restore point**, you must have the `SELECT ANY DICTIONARY` or **`FLASHBACK ANY TABLE`** system privilege or the `SELECT_CATALOG_ROLE` role."* Oracle 문서가 이 권한을 **SELECT ANY DICTIONARY / SELECT_CATALOG_ROLE 의 대체물로 취급하는 경로가 최소 한 곳 존재한다.** 요청서는 바로 그 두 개를 "동반 요청 금지" 로 선언해 깨끗함을 주장했다. standby read-only + ALTER/INSERT/DELETE 부재로 실질 악용 경로는 아니지만, **심사자가 이걸 먼저 발견하면 요청서 전체의 신뢰가 죽는다.** 반드시 선제 공시해야 한다.
- **PIPA 축 누락**: "삭제·수정 이력에 한정" 은 사실이나 LOW 로 처리할 근거가 아니다. 파기된 개인정보 행을 **계정이 SELECT 가능한 모든 테이블에 대해** 조회 가능해진다. 이건 보안 사고 시나리오가 아니라 상시 컴플라이언스 표면이다. ETL 이 실제 필요한 N개 테이블이 아니라 전체로 확대된다는 점이 최소범위 위반의 정의다.

### G. standby 성립 여부 — 확정되지 않음 (미확인)

Data Guard Concepts §10 은 flashback query 의 ADG standby 지원 여부를 **긍정도 부정도 하지 않는다**("read-only 로 열린 물리 standby 는 read-only 로 열린 DB 와 동일한 제약을 받는다" 만 서술). 금지 근거도 못 찾았으므로 "된다" 쪽에 무게가 있으나 **1차 출처로 확정 불가.** 추가로 AS OF **TIMESTAMP** 경로에는 standby 고유 실패 모드가 있다:
- SCN↔timestamp 매핑은 `SYS.SMON_SCN_TIME` 이고 standby 에서는 redo apply 로만 채워진다 → gap 시 stale → **ORA-08181**.
- 매핑 보존창(≥120h)과 **undo 보존창은 별개**다. 매핑은 살아 있는데 undo 가 없어 ORA-01555 로 죽는 구간이 존재한다. fallback validator 가 이 둘을 구분하지 못하면 오탐/미탐이 난다.
- SCN_TO_TIMESTAMP 는 문서상 "approximate", AS OF TIMESTAMP 는 **3초 입도**(§20.10) → DDL 직후 시점 지정 시 **ORA-01466**.
→ 권한을 받기 **전에** G0-0 프로브(§2 항목 ⒟)로 실측해야 한다. 실측 없이 요청서를 올리면 "권한은 받았는데 standby 에서 안 되더라" 가 될 수 있고, 그 한 번으로 다음 요청 전부가 막힌다.

### H. fallback 평가

fallback(1번 복귀 + 창구 SLA + publish validator)은 **타당하고, 사실은 이게 1순위여야 한다.** 다만 validator 사양에 결함이 있다: `ORA-01031 이면 publish 거부` 만으로는 부족하다. AS OF 1행 조회는 ORA-01031(권한) / ORA-01555(undo 부족) / ORA-08181(SCN 매핑 없음) / ORA-01466(DDL·3초 입도) / ORA-00942(SELECT 없음) 로 갈린다. **01031·00942 만 publish 거부**, 01555/08181/01466 은 **경고 + 재시도**로 분기해야 오탐 거부로 배포가 막히지 않는다.

### I. 권장 조치

1. 이 항목을 **1순위에서 내리고**, Appendix W 1번(객체 단위)을 1순위로 되돌린다.
2. 객체 단위 요청을 **`GRANT SELECT` 와 같은 문장으로 병합**해 신규 Job 의 grant 이벤트 증분을 0으로 만든다. 기존 테이블 백필은 점검창 배치.
3. **권한 요청 전에 G0-0 프로브를 1회 실행**해 ⒟(ADG AS OF / SCN_TO_TIMESTAMP 가부)를 확정한다. 지금 가장 값싼 확실성이다.
4. FLASHBACK ANY TABLE 을 그래도 올릴 경우 요청서에 반드시 추가: (i) restore point 경로에서 SELECT ANY DICTIONARY 대체물로 문서화되어 있다는 사실, (ii) ORA-01555 가 상시 발생 예상이며 **UNDO_RETENTION / RETENTION GUARANTEE 상향은 요청하지 않겠다는 명시적 약속**(§16.2.2.3 WARNING 인용과 함께), (iii) 삭제 이력 노출 범위와 PIPA 축.
5. 그리고 이 요청은 정보 통보 항목(profile 3값·DG 구성)과 **같은 티켓에 묶지 않는다.** 통과 가능성이 높은 항목을 이 항목의 거부와 함께 태우면 안 된다.

---

### R9. W-S6 — 기존 상시 갱신 테이블을 passive fence witness 로 (쓰기 0, 최우선 시도)

REFUTED on four independent grounds. Risk raised LOW → MEDIUM.

=== (A) The stated qualifying gate is factually wrong — G0-0A does NOT measure condition (1) ===
The item asserts "자격 조건(둘 다 G0-0A 가 이미 측정한다)". Verified false by direct grep:
  grep -c "timestamp_origin" g0-0a-capability-inventory.sql g0-0c-fence-facts.sql g0-0-probe-README.md → 0, 0, 0
G0-0A measures only `wm_column.type_facts` (ALL_TAB_COLUMNS data_type/scale/nullable) and `wm_column.index_facts` / `wm_column.leading_valid_visible`. It never touches ALL_TRIGGERS.
Worse, A v1.2.3.1 §6.1 defines `watermark_column_facts` as **"DBA 등록"** — timestamp_origin is a DBA-asserted registration field, with only `not_null` machine-verified via ALL_TAB_COLUMNS. In Profile U there is no DBA registration, which is the entire premise of v2.0. So condition (1) is not merely unmeasured — in the unprivileged model it is *unobtainable*.
And it is not probe-able even in principle: DB_TRIGGER-ness plus `updated_on_every_change` plus "no application path overrides the trigger value" is static analysis of trigger source (and of every DML path), not a scalar SELECT. A `WHEN (NEW.wm IS NULL)` trigger, or an ON-UPDATE-only trigger, satisfies "a trigger exists" while breaking the primary-clock monotonicity the fence depends on.
→ The one safety predicate that makes T_lb sound is asserted, not established.

=== (B) The baseline is misstated, so the claimed freshness gain is overstated ===
The item says the v2.0 §4.1 baseline is `high = t0 − D − safety_lag`. It is not. §4.1 reads verbatim:
  high = min( t0 − D − safety_lag ,  MAX(watermark_column) )   -- 두 항 모두 하향 전용
MAX(watermark) is already in the baseline as a downward-only cap, on the target table, at zero new grant. What W-S6 actually proposes is not "reviving MAX(wm)" but **deleting the `− D` term** and substituting a third-party table's MAX(wm) as the sole time origin. That is a materially different and much stronger claim than the writeup presents.
Consequence for the guarantee axis: ORA-3172 still fires at D, so the *published* `source_staleness = DB_ENFORCED_MAX_DELAY(D)` does not improve at all. Only observed median latency improves. "D만큼 잃던 신선도를 대부분 회수한다" conflates the published guarantee with the observed p50. Under v2.0 §6 the consumer-facing string is unchanged.
Also note the value proposition inverts: a third-party witness only beats self-witnessing when the *target* is idle — and an idle target's extraction returns nothing, so freshness matters least exactly where the gain is largest.

=== (C) The load mechanism is wrong for the realistic case, by ~3 orders of magnitude ===
"index min/max scan, 논리 읽기 3~4 블록" is correct only for a NON-PARTITIONED index. For a LOCAL partitioned index the INDEX (FULL SCAN (MIN/MAX)) optimization must probe **each partition's** index segment. A busy production-line table with daily partitions over 3 years ≈ 1,000 partitions → ~3,000–4,000 logical reads per witness read, not 4. At the claimed 100k reads/day that is 300–400M buffer gets/day, not 400k. The writeup's entire "계측 한계 아래다" argument rests on the 4-block figure and does not survive partitioning — and large, always-busy, production-line-adjacent tables are precisely the tables that are partitioned.
The gate cannot catch this either. `wm_column.leading_valid_visible` filters `i.status = 'VALID'`, but in Oracle `ALL_INDEXES.STATUS` is **'N/A'** for partitioned indexes (per-partition status lives in ALL_IND_PARTITIONS). So for every partitioned witness table the probe returns 0 — indistinguishable from "no index at all". Verified there is zero partition handling in the probe set (grep -i "partition" over all three files → no hits). The gate therefore either false-negatives every good local index, or someone ACKs past a 0 and runs blind into the case the writeup itself calls "이 문서에서 가장 무거운 항목".
Second, smaller optimism: G0-0A's own inline comment already refutes the writeup's sufficiency claim — "컬럼이 인덱스에 '있다'는 사실만으로는 access path가 보장되지 않는다 … 그래도 실제 plan은 옵티마이저가 정한다(재검증 지적)". The writeup upgrades this to "그 상한은 인덱스 유무로 사전 판정된다", which contradicts the probe file it cites. Implicit datatype conversion, a function on the column, or primary-side stats drift all silently demote MIN/MAX to FTS at run time with no gate firing. On a production-line DB a silent demotion to FTS on a large table, executed by 500 concurrent burst sessions, is a real incident path.
Third, unpriced: the writeup counts buffer gets but not sessions. If the Guard pre-check runs before the extract session exists, it is an *additional* logon per run — 500 extra concurrent logons at the top-of-hour burst against a standby with no admission control (v2.0 §4.3: `source_admission_control = SELF_LIMITED_ONLY`, 원천 집행 0).

=== (D) A new silent-loss path and a new correlated-stall path, both specific to third-party witnesses ===
Silent loss: the t0 cap is asserted to neutralize poisoned rows, but t0 is the **standby wall clock**, which normally runs *ahead* of the primary's apply position. Any witness row whose wm lands between the true apply position and t0 passes the cap and pushes `high` forward past data that is not yet visible → rows in the target permanently skipped, with no error. The dedicated ETL_HEARTBEAT had no poisoning path (DBA-owned, one row, one writer, value = SYSTIMESTAMP). A production table has many writers, manual data fixes, backdated batch reloads, and partition exchange. Partition exchange in particular can move MAX(wm) **backwards or forwards discontinuously** — and because the witness is a table the platform does not extract, it is outside schema-drift detection: nobody on the ETL team is notified when it changes, is archived, or has its trigger disabled.
Correlated stall: the writeup prices the freeze case as "정체이며 보수 방향 … F-13/NO_SOURCE_PROGRESS". That is right for one job. It is wrong at 10,000 jobs: a handful of shared witnesses means one line stop, one trigger disable, or one archive job freezes cutoff for **every job bound to that witness**, including targets that are still actively changing (orders, inventory, master data). Self-witnessing has no such coupling. Correctness-preserving, but a platform-wide availability fault with a large blast radius — that alone is above LOW.

=== (E) What survives ===
Standby validity is fine: a plain SELECT MAX() on ADG works, generates no redo/undo, and reaches no primary. "원천 primary 에 도달하는 경로가 없다" holds for the SELECT itself (caveat: if D were ever set to 0, STANDBY_MAX_DATA_DELAY = 0 forces primary coordination — pin D > 0). The GRANT syntax is correct and is the minimal object-level form. A DB trigger's SYSTIMESTAMP does execute on the primary (triggers do not fire on redo apply), so DB_TRIGGER values are genuinely primary-clock — the writeup is right on that narrow point.

=== Governance point the writeup omits ===
`GRANT SELECT ON <prod_owner>.<busy_table>` exposes **all columns and all rows** of a production-line table to the ETL account for which there is no business read need. On a system where DB trouble stops physical production, that is likely a harder organizational sell than the load question the writeup optimizes for — and it is not the narrowest form available.

=== Narrower / better alternatives the writeup did not consider ===
1. Self-witness (zero grant): keep §4.1's `MAX(target.wm)` as the downward cap and keep `− D`. Already the baseline; costs nothing; no new coupling, no new poisoning path, no new grant.
2. If a borrowed clock is genuinely needed, request `GRANT SELECT ON <owner>.<one_column_view>` where the view is `SELECT wm FROM busy_table` — or better, a DBA-owned single-row MV/view exposing only MAX(wm). Strictly narrower than table SELECT, removes the all-column exposure, and restores single-value semantics closer to the heartbeat.
3. Publish T_lb as `OBSERVED`-tier freshness only, never as a substitute for the `− D` term. Keep `high = min(t0 − D, T_lb) − safety_lag` (both terms), which is monotone-safe and still recovers freshness whenever T_lb is the binding term, without deleting the DB-enforced floor.

=== Verdict ===
Not a LOW-risk, zero-cost item. It deletes the DB-enforced time origin in favor of an unverifiable DBA-asserted property, on a table outside the platform's observability, gated by a probe predicate that misfires on partitioned tables, with a load model that is off by ~1000x in exactly the configuration it targets. Fallback exists and is cheap (option 1 above), which is the correct default. Confidence in this refutation: HIGH for (A), (B), (D); MEDIUM-HIGH for (C) — the partitioned-index STATUS='N/A' behavior and per-partition MIN/MAX probing are stated from Oracle semantics, not re-verified against a live 19c instance here, and should be confirmed on the actual source before being used as the deciding argument. Mark as 미확인 pending that check.

---

## 4. 이 문서의 한계

1. **28건이 미검증이다.** 판정서의 “보류” 결론은 검증된 9건에 근거하며, 28건은 그 결론의 근거가 아니다.

2. **확신도는 원 조사자의 자기평가다.** `CONFIRMED` 라도 1차 출처 URL·판본·확인 일시가 항목마다 붙어 있지는 않다. 리뷰 P1-11 이 요구한 형식에 아직 미치지 못한다.

3. **검증자에게 “확신이 안 서면 기각”을 지시했다.** 0/9 라는 결과는 그 지시의 영향을 받았을 수 있다. 다만 기각 사유가 1차 출처와 저장소 자기 문서를 인용하고 있어 기본 회의가 아니라 실제 결함이다.
