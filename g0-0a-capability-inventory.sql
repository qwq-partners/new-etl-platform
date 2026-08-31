-- =====================================================================
-- G0-0A — 안전한 capability inventory (Profile U)
-- =====================================================================
-- 이 파일은 이전 `g0-0-probe.sql`을 **대체**한다. 이전 파일은 실행 차단
-- 결함 5건(잘못된 SHA-256 기대값 / 미정의 치환변수 LIMIT_ROWS / 대상 테이블 전체
-- scan / README의 비밀번호 argv 노출 / **하드코딩된 "query_ok":true로 인한
-- apparent success**)이 있어 폐기한다.
--
-- 범위: **대상 테이블을 스캔하지 않는다.** 데이터 사실 측정(fence 반례)은
--       G0-0C(`g0-0c-fence-facts.sql`)로 분리했다. 여기서 대상 테이블에
--       닿는 것은 `WHERE ROWNUM = 1` 세 건과 `AS OF` 권한 확인 한 건뿐이다.
--
-- **이기종 원천 대응(§8)**: 원천 Oracle 은 버전·charset·옵션이 제각각이다.
--       그래서 이 프로브는 **버전을 보고 기능을 추정하지 않는다**. 기능을 직접
--       실행해 보고 SQLCODE 로 판정한다("probe the feature, not the version").
--       버전은 맥락 기록용으로만 남긴다. 같은 19c 라도 charset·NLS·옵션이
--       다르면 해시 정본화와 비교 의미가 달라지므로 그것들도 함께 잰다.
--
-- 안전 규칙
--   1. 읽기 전용. DDL·DML·job 생성 0.
--   2. **잘못된 비밀번호를 절대 시도하지 않는다**(계정 잠금 금지).
--   3. `ALTER SESSION`만 사용. `ALTER SYSTEM` 없음.
--   4. **비밀번호를 명령줄 인자로 넘기지 않는다** — README의 `/nolog` 방식 사용.
--   5. 모든 판정은 실제 `SQLCODE`에서 나온다. 성공을 가정한 리터럴 출력 없음.
--
-- 판정 3분리(리뷰 §7.2): query_ok / row_present / value_interpretable
--   "오류 부재"는 그 자체로 capability 증거가 아니다.
-- =====================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED FORMAT WRAPPED
SET LINESIZE 400
SET TRIMSPOOL ON
SET FEEDBACK OFF
SET VERIFY OFF
SET TERMOUT ON
-- 블록 컴파일 실패·접속 실패가 '성공 종료'로 보이지 않게 한다.
-- (probe_summary만으로는 CONNECT/SP2 오류를 막지 못한다 — 재검증 지적)
WHENEVER SQLERROR EXIT FAILURE
WHENEVER OSERROR EXIT FAILURE

-- ── 실행 전 반드시 채울 것 ──────────────────────────────────────────
DEFINE TARGET_OWNER   = 'SCHEMA_NAME'
DEFINE TARGET_TABLE   = 'TABLE_NAME'
DEFINE WM_COLUMN      = 'UPDATE_DT'
DEFINE EXPECT_ROLE    = 'PHYSICAL STANDBY'
DEFINE EXPECT_DBUNAME = 'DBUNIQUENAME'
DEFINE MAX_DELAY_SEC  = '300'

PROMPT ============ G0-0A CAPABILITY INVENTORY ============

DECLARE
  TYPE t_rec IS RECORD (id VARCHAR2(80), val VARCHAR2(4000),
                        ok CHAR(1), rowp CHAR(1), interp CHAR(1),
                        ora_no NUMBER, msg VARCHAR2(500));
  v        VARCHAR2(4000);
  -- 정적 probe 호출 수. 값을 바꿀 때 이 상수도 함께 바꾼다.
  -- emitted != expected 이면 블록이 중간에 끊긴 것이므로 실패로 취급한다.
  c_expected CONSTANT PLS_INTEGER := 87;   -- 실제 호출 수와 일치해야 한다
  -- probe 를 더하거나 뺄 때 이 값을 반드시 함께 고쳐라. 확인 방법(**공백에 무관해야 한다**):
  --   grep -cE "^[[:space:]]*p_(scalar|stmt)[[:space:]]*\(" g0-0a-capability-inventory.sql
  -- 이력: v1 은 57건을 56 으로 선언했고, 2026-08-27 1차 정정은 `p_stmt  (` 처럼 공백이 낀
  --   호출 8건을 세지 못하는 grep 을 써 78 로 잘못 고쳤다. 둘 다 첫 실행에서
  --   manifest_ok=false 를 만들어 **측정 결과 전체 폐기**로 이어질 값이었다.
  g_total  PLS_INTEGER := 0;
  g_fail   PLS_INTEGER := 0;
  g_interp PLS_INTEGER := 0;

  PROCEDURE emit(p t_rec) IS
  BEGIN
    g_total := g_total + 1;
    IF p.ok <> 'Y' THEN g_fail := g_fail + 1; END IF;
    IF p.interp = 'N' THEN g_interp := g_interp + 1; END IF;
    DBMS_OUTPUT.PUT_LINE(
      '{"probe":"'||p.id||'"'||
      ',"query_ok":'||CASE p.ok WHEN 'Y' THEN 'true' ELSE 'false' END||
      ',"row_present":'||CASE p.rowp WHEN 'Y' THEN 'true' WHEN 'N' THEN 'false' ELSE 'null' END||
      ',"value_interpretable":'||CASE p.interp WHEN 'Y' THEN 'true' WHEN 'N' THEN 'false' ELSE 'null' END||
      ',"value":'||CASE WHEN p.val IS NULL THEN 'null'
                        ELSE '"'||REPLACE(REPLACE(SUBSTR(p.val,1,300),'\','\\'),'"','\"')||'"' END||
      ',"ora":'||CASE WHEN p.ora_no IS NULL THEN 'null' ELSE TO_CHAR(p.ora_no) END||
      ',"msg":'||CASE WHEN p.msg IS NULL THEN 'null'
                      ELSE '"'||REPLACE(REPLACE(SUBSTR(p.msg,1,200),'\','\\'),'"','\"')||'"' END||'}');
  END emit;

  PROCEDURE p_scalar(p_id VARCHAR2, p_sql VARCHAR2, p_expect VARCHAR2 DEFAULT NULL) IS
    rr t_rec;
  BEGIN
    rr.id := p_id; rr.ok := 'N'; rr.rowp := '?'; rr.interp := '?';
    BEGIN
      EXECUTE IMMEDIATE p_sql INTO v;
      rr.ok := 'Y'; rr.rowp := 'Y'; rr.val := v;
      rr.interp := CASE WHEN p_expect IS NULL THEN '?'      -- 기대값 없음 = 판단 보류
                        WHEN v = p_expect THEN 'Y' ELSE 'N' END;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN rr.ok := 'Y'; rr.rowp := 'N'; rr.interp := '?';
      WHEN OTHERS THEN rr.ora_no := SQLCODE; rr.msg := SQLERRM;
    END;
    emit(rr);
  END p_scalar;

  PROCEDURE p_stmt(p_id VARCHAR2, p_sql VARCHAR2) IS
    rr t_rec;
  BEGIN
    rr.id := p_id; rr.ok := 'N';
    BEGIN
      EXECUTE IMMEDIATE p_sql;
      rr.ok := 'Y'; rr.val := 'accepted'; rr.interp := '?';
    EXCEPTION WHEN OTHERS THEN rr.ora_no := SQLCODE; rr.msg := SQLERRM;
    END;
    emit(rr);
  END p_stmt;

BEGIN
  -- ── 9차 조치 6: 자리표시자로는 돌지 않는다 (P0-05 · P0-06-8) ───────
  -- 위 DEFINE 블록은 **자리표시자**다. 채우지 않고 돌리면 존재하지 않는
  -- SCHEMA_NAME.TABLE_NAME 을 질의하고 DBUNIQUENAME 과 신원을 대조한다.
  -- 생산라인과 밀접한 원천에서 그것은 그냥 사고다.
  --
  -- **이것은 심층 방어이지 주 방어가 아니다.** 주 방어는 `g0-0a-preflight.sql` —
  -- DUAL 만 읽는 별도 connection 이며 대상 이름이 SQL 에 등장조차 하지 않는다.
  -- 여기 검사는 preflight 를 건너뛴 실행을 막는다.
  IF '&TARGET_OWNER' = 'SCHEMA_NAME' OR '&TARGET_TABLE' = 'TABLE_NAME'
     OR '&EXPECT_DBUNAME' = 'DBUNIQUENAME' THEN
    RAISE_APPLICATION_ERROR(-20051,
      'G0-0A ABORT: DEFINE 블록이 자리표시자 그대로다 (TARGET_OWNER=&TARGET_OWNER, '
      ||'TARGET_TABLE=&TARGET_TABLE, EXPECT_DBUNAME=&EXPECT_DBUNAME). '
      ||'g0-0-runbook.md §4 S5 대로 값을 채운 사본을 만들어라. 대상에 아무 질의도 하지 않았다');
  END IF;

  DBMS_OUTPUT.PUT_LINE('--- 1. IDENTITY / ROLE ---');
  p_scalar('userenv.DATABASE_ROLE',        q'[SELECT SYS_CONTEXT('USERENV','DATABASE_ROLE') FROM DUAL]', '&EXPECT_ROLE');
  p_scalar('userenv.DB_NAME',              q'[SELECT SYS_CONTEXT('USERENV','DB_NAME') FROM DUAL]');
  p_scalar('userenv.DB_UNIQUE_NAME',       q'[SELECT SYS_CONTEXT('USERENV','DB_UNIQUE_NAME',256) FROM DUAL]', '&EXPECT_DBUNAME');
  p_scalar('userenv.DB_DOMAIN',            q'[SELECT SYS_CONTEXT('USERENV','DB_DOMAIN') FROM DUAL]');
  p_scalar('userenv.CDB_NAME',             q'[SELECT SYS_CONTEXT('USERENV','CDB_NAME') FROM DUAL]');
  p_scalar('userenv.CON_NAME',             q'[SELECT SYS_CONTEXT('USERENV','CON_NAME') FROM DUAL]');
  p_scalar('userenv.CON_ID',               q'[SELECT SYS_CONTEXT('USERENV','CON_ID') FROM DUAL]');
  p_scalar('userenv.SERVICE_NAME',         q'[SELECT SYS_CONTEXT('USERENV','SERVICE_NAME') FROM DUAL]');
  p_scalar('userenv.INSTANCE_NAME',        q'[SELECT SYS_CONTEXT('USERENV','INSTANCE_NAME') FROM DUAL]');
  p_scalar('userenv.SERVER_HOST',          q'[SELECT SYS_CONTEXT('USERENV','SERVER_HOST',256) FROM DUAL]');
  p_scalar('userenv.SESSION_USER',         q'[SELECT SYS_CONTEXT('USERENV','SESSION_USER') FROM DUAL]');
  p_scalar('userenv.ISDBA',                q'[SELECT SYS_CONTEXT('USERENV','ISDBA') FROM DUAL]', 'FALSE');
  p_scalar('userenv.IS_DG_ROLLING_UPGRADE',q'[SELECT SYS_CONTEXT('USERENV','IS_DG_ROLLING_UPGRADE') FROM DUAL]');
  p_scalar('userenv.IS_APPLY_SERVER',      q'[SELECT SYS_CONTEXT('USERENV','IS_APPLY_SERVER') FROM DUAL]');
  p_scalar('userenv.SID',                  q'[SELECT SYS_CONTEXT('USERENV','SID') FROM DUAL]');

  DBMS_OUTPUT.PUT_LINE('--- 2. SESSION ASSERTION ---');
  p_stmt  ('alter.STANDBY_MAX_DATA_DELAY.D', 'ALTER SESSION SET STANDBY_MAX_DATA_DELAY = &MAX_DELAY_SEC');
  -- 대상 테이블 접촉은 ROWNUM = 1 한 건뿐이다(스캔 아님).
  p_scalar('after_D.touch_target',           q'[SELECT TO_CHAR(COUNT(*)) FROM (SELECT 1 FROM &TARGET_OWNER..&TARGET_TABLE WHERE ROWNUM = 1)]');
  p_stmt  ('alter.NLS_NUMERIC_CHARACTERS',   q'[ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,']');
  p_stmt  ('alter.TIME_ZONE_UTC',            q'[ALTER SESSION SET TIME_ZONE = '+00:00']');
  p_stmt  ('alter.NLS_TIMESTAMP_FORMAT',     q'[ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS.FF6']');

  DBMS_OUTPUT.PUT_LINE('--- 3. FLASHBACK / SCN (문서 전체의 분기점) ---');
  p_scalar('as_of_timestamp.target',
      q'[SELECT TO_CHAR(COUNT(*)) FROM (SELECT 1 FROM &TARGET_OWNER..&TARGET_TABLE AS OF TIMESTAMP (SYSTIMESTAMP - INTERVAL '1' MINUTE) WHERE ROWNUM = 1)]');
  p_scalar('dbms_flashback.get_scn',   q'[SELECT TO_CHAR(DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER) FROM DUAL]');
  p_scalar('timestamp_to_scn',         q'[SELECT TO_CHAR(TIMESTAMP_TO_SCN(SYSTIMESTAMP - INTERVAL '5' MINUTE)) FROM DUAL]');
  p_scalar('scn_to_timestamp.roundtrip',
      q'[SELECT TO_CHAR(SCN_TO_TIMESTAMP(TIMESTAMP_TO_SCN(SYSTIMESTAMP - INTERVAL '5' MINUTE)),'YYYY-MM-DD HH24:MI:SS') FROM DUAL]');
  -- 주의: SCN_TO_TIMESTAMP는 약 3초 정밀도의 **근삿값**이며 방향성 있는 하한이 아니다.
  --       fence의 하한 witness로 승격하지 마라(재검증 지적).

  DBMS_OUTPUT.PUT_LINE('--- 4. HASH / CANONICAL ---');
  p_scalar('standard_hash.varchar',  q'[SELECT RAWTOHEX(STANDARD_HASH('abc','SHA256')) FROM DUAL]',
           'BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD');  -- SHA-256('abc') 검증됨
  p_scalar('utl_i18n.string_to_raw', q'[SELECT RAWTOHEX(UTL_I18N.STRING_TO_RAW('가','AL32UTF8')) FROM DUAL]', 'EAB080');
  p_scalar('standard_hash.raw_utf8', q'[SELECT RAWTOHEX(STANDARD_HASH(UTL_I18N.STRING_TO_RAW('abc','AL32UTF8'),'SHA256')) FROM DUAL]',
           'BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD');  -- RAW 경로가 같은 값을 내야 한다
  p_scalar('utl_raw.length',         q'[SELECT TO_CHAR(UTL_RAW.LENGTH(HEXTORAW('00FF'))) FROM DUAL]', '2');
  p_scalar('utl_raw.concat',         q'[SELECT RAWTOHEX(UTL_RAW.CONCAT(HEXTORAW('01'),HEXTORAW('02'))) FROM DUAL]', '0102');
  p_scalar('utl_raw.cast_be_int',    q'[SELECT RAWTOHEX(UTL_RAW.CAST_FROM_BINARY_INTEGER(3, UTL_RAW.BIG_ENDIAN)) FROM DUAL]');
  p_scalar('compose_decompose.nfc',  q'[SELECT RAWTOHEX(UTL_I18N.STRING_TO_RAW(COMPOSE(DECOMPOSE(TO_NCHAR('가'),'CANONICAL')),'AL32UTF8')) FROM DUAL]', 'EAB080');
  p_scalar('dbms_crypto.hash_raw',   q'[SELECT RAWTOHEX(DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW('abc'), 4)) FROM DUAL]',
           'BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD');
  p_scalar('nls_session_parameters', q'[SELECT LISTAGG(parameter||'='||value,';') WITHIN GROUP (ORDER BY parameter) FROM nls_session_parameters WHERE parameter IN ('NLS_NUMERIC_CHARACTERS','NLS_SORT','NLS_COMP','NLS_TIMESTAMP_FORMAT')]');
  p_scalar('nls_database_parameters',q'[SELECT LISTAGG(parameter||'='||value,';') WITHIN GROUP (ORDER BY parameter) FROM nls_database_parameters WHERE parameter IN ('NLS_CHARACTERSET','NLS_NCHAR_CHARACTERSET')]');

  DBMS_OUTPUT.PUT_LINE('--- 5. DICTIONARY / SELF-PRIVILEGE ---');
  p_scalar('user_users',             q'[SELECT username||'|'||profile||'|'||account_status FROM user_users WHERE ROWNUM = 1]');
  p_scalar('user_password_limits',   q'[SELECT LISTAGG(resource_name||'='||limit,';') WITHIN GROUP (ORDER BY resource_name) FROM user_password_limits]');
  p_scalar('user_resource_limits',   q'[SELECT LISTAGG(resource_name||'='||limit,';') WITHIN GROUP (ORDER BY resource_name) FROM user_resource_limits]');
  -- LIMIT은 리터럴 'DEFAULT'/'UNLIMITED'일 수 있다. 숫자로 해석되지 않으면 **할당값조차 미확정**이다.
  p_scalar('user_resource_limits.sessions_literal',
      q'[SELECT limit FROM user_resource_limits WHERE resource_name = 'SESSIONS_PER_USER']');
  p_scalar('session_privs.count',    q'[SELECT TO_CHAR(COUNT(*)) FROM session_privs]');
  p_scalar('session_privs.list',     q'[SELECT LISTAGG(privilege,',') WITHIN GROUP (ORDER BY privilege) FROM (SELECT privilege FROM session_privs WHERE ROWNUM <= 40)]');
  p_scalar('session_roles.list',     q'[SELECT LISTAGG(role,',') WITHIN GROUP (ORDER BY role) FROM (SELECT role FROM session_roles WHERE ROWNUM <= 40)]');
  p_scalar('user_tab_privs.on_target',
      q'[SELECT LISTAGG(privilege,',') WITHIN GROUP (ORDER BY privilege) FROM user_tab_privs WHERE table_name='&TARGET_TABLE' AND owner='&TARGET_OWNER']');
  -- watermark 컬럼 타입 사실 — seal 단위 ulp 파생과 NULL 정책의 근거
  p_scalar('wm_column.type_facts',
      q'[SELECT data_type||'|scale='||NVL(TO_CHAR(data_scale),'-')||'|prec='||NVL(TO_CHAR(data_precision),'-')||'|nullable='||nullable
           FROM all_tab_columns WHERE owner='&TARGET_OWNER' AND table_name='&TARGET_TABLE' AND column_name='&WM_COLUMN']');
  -- G0-0C의 비용을 좌우한다: wm 컬럼에 인덱스가 있는가
  -- 주의: 컬럼이 인덱스에 '있다'는 사실만으로는 access path가 보장되지 않는다.
  --       leading column(position=1)·status VALID·visibility VISIBLE을 함께 봐야 하고,
  --       그래도 실제 plan은 옵티마이저가 정한다(재검증 지적).
  p_scalar('wm_column.index_facts',
      q'[SELECT LISTAGG(ic.index_name||'#pos='||ic.column_position||'/'||i.status||'/'||i.visibility,',')
                WITHIN GROUP (ORDER BY ic.index_name)
           FROM all_ind_columns ic JOIN all_indexes i
             ON i.owner = ic.index_owner AND i.index_name = ic.index_name
          WHERE ic.table_owner='&TARGET_OWNER' AND ic.table_name='&TARGET_TABLE' AND ic.column_name='&WM_COLUMN']');
  p_scalar('wm_column.leading_valid_visible',
      q'[SELECT TO_CHAR(COUNT(*)) FROM all_ind_columns ic JOIN all_indexes i
             ON i.owner = ic.index_owner AND i.index_name = ic.index_name
          WHERE ic.table_owner='&TARGET_OWNER' AND ic.table_name='&TARGET_TABLE'
            AND ic.column_name='&WM_COLUMN' AND ic.column_position = 1
            AND i.status = 'VALID' AND i.visibility = 'VISIBLE']');
  p_scalar('all_lobs.count',         q'[SELECT TO_CHAR(COUNT(*)) FROM all_lobs WHERE owner='&TARGET_OWNER' AND table_name='&TARGET_TABLE']');
  p_scalar('all_lobs.retention',     q'[SELECT LISTAGG(column_name||':'||NVL(retention,-1),';') WITHIN GROUP (ORDER BY column_name) FROM all_lobs WHERE owner='&TARGET_OWNER' AND table_name='&TARGET_TABLE']');
  p_scalar('all_constraints.pk',     q'[SELECT constraint_name FROM all_constraints WHERE owner='&TARGET_OWNER' AND table_name='&TARGET_TABLE' AND constraint_type='P' AND ROWNUM=1]');

  DBMS_OUTPUT.PUT_LINE('--- 5b. V$ / DBA_ (실패 기대. 그러나 사전 단정하지 않는다) ---');
  p_scalar('v$database',        q'[SELECT db_unique_name||'|'||database_role FROM v$database]');
  p_scalar('v$dataguard_stats', q'[SELECT LISTAGG(name||'='||value,';') WITHIN GROUP (ORDER BY name) FROM v$dataguard_stats WHERE name IN ('apply lag','transport lag')]');
  p_scalar('v$parameter.max_string', q'[SELECT value FROM v$parameter WHERE name='max_string_size']');
  p_scalar('gv$session.count',  q'[SELECT TO_CHAR(COUNT(*)) FROM gv$session]');
  p_scalar('dba_2pc_pending',   q'[SELECT TO_CHAR(COUNT(*)) FROM dba_2pc_pending]');

  DBMS_OUTPUT.PUT_LINE('--- 5c. TAGGING ---');
  p_stmt  ('dbms_application_info.set_module', q'[BEGIN DBMS_APPLICATION_INFO.SET_MODULE('G0-0A','probe'); END;]');
  p_stmt  ('dbms_session.set_identifier',      q'[BEGIN DBMS_SESSION.SET_IDENTIFIER('g0-0a/probe'); END;]');
  p_scalar('userenv.CLIENT_IDENTIFIER.after',  q'[SELECT SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER') FROM DUAL]', 'g0-0a/probe');

  DBMS_OUTPUT.PUT_LINE('--- 6. TRANSACTION / BARRIER (실제 SQLCODE로만 판정) ---');
  -- 이전 판의 하드코딩된 "query_ok":true를 폐기하고 실제 오류를 잡는다.
  p_stmt('txn.commit_before',            'COMMIT');
  p_stmt('txn.set_read_only',            'SET TRANSACTION READ ONLY');
  p_scalar('txn.select_inside',          q'[SELECT TO_CHAR(COUNT(*)) FROM (SELECT 1 FROM &TARGET_OWNER..&TARGET_TABLE WHERE ROWNUM <= 10)]');
  -- 두 번째 SET TRANSACTION이 ORA-01453이면 첫 트랜잭션이 실제로 열려 있었다는 양성 증거다.
  p_stmt('txn.set_read_only.reissue',    'SET TRANSACTION READ ONLY');
  p_stmt('txn.commit_after',             'COMMIT');
  p_stmt('sync_with_primary',            'ALTER SESSION SYNC WITH PRIMARY');

  DBMS_OUTPUT.PUT_LINE('--- 7. ORA-03172 양성 대조 ---');
  -- 성공해도 fence 미집행의 증거가 아니다. lag가 큰 시간대에 재실행해 ORA-03172를 최소 1회 확보하라.
  p_stmt  ('alter.STANDBY_MAX_DATA_DELAY.zero', 'ALTER SESSION SET STANDBY_MAX_DATA_DELAY = 0');
  p_scalar('max_delay_zero.touch_target',       q'[SELECT TO_CHAR(COUNT(*)) FROM (SELECT 1 FROM &TARGET_OWNER..&TARGET_TABLE WHERE ROWNUM = 1)]');
  p_stmt  ('alter.STANDBY_MAX_DATA_DELAY.restore','ALTER SESSION SET STANDBY_MAX_DATA_DELAY = &MAX_DELAY_SEC');


  DBMS_OUTPUT.PUT_LINE('--- 7b. 측정 대상 식별자 (증거가 무엇을 잰 것인지 스스로 밝힌다) ---');
  -- 이것이 없으면 테이블 A 에서 얻은 ROWDEPENDENCIES 결과를 테이블 B 에 적용하는 것을
  -- 증거가 막지 못한다(7차 교차 리뷰 P0-03).
  p_scalar('target.identity',
      q'[SELECT '&TARGET_OWNER' || '.' || '&TARGET_TABLE' || '#' || '&WM_COLUMN' FROM DUAL]');

  DBMS_OUTPUT.PUT_LINE('--- 8. PORTABILITY / 이기종 원천 (버전 추정 금지, 기능 직접 실행) ---');
  -- 8a. 맥락 기록용 버전·문자집합. PRODUCT_COMPONENT_VERSION·NLS_DATABASE_PARAMETERS 는
  --     PUBLIC 에 열려 있어 권한이 필요 없다. V$VERSION 은 V$ 권한이 필요하므로 쓰지 않는다.
  p_scalar('ver.product_component',      q'[SELECT version FROM PRODUCT_COMPONENT_VERSION WHERE ROWNUM = 1]');
  p_scalar('nls.characterset',           q'[SELECT value FROM nls_database_parameters WHERE parameter = 'NLS_CHARACTERSET']');
  p_scalar('nls.nchar_characterset',     q'[SELECT value FROM nls_database_parameters WHERE parameter = 'NLS_NCHAR_CHARACTERSET']');
  p_scalar('nls.length_semantics',       q'[SELECT value FROM nls_database_parameters WHERE parameter = 'NLS_LENGTH_SEMANTICS']');
  p_scalar('nls.comp',                   q'[SELECT value FROM nls_database_parameters WHERE parameter = 'NLS_COMP']');
  p_scalar('nls.sort',                   q'[SELECT value FROM nls_database_parameters WHERE parameter = 'NLS_SORT']');

  -- 8b. 해시 정본화. STANDARD_HASH 는 12.1+ 다. 없으면 행 단위 해시 대조가 불가능해
  --     Reconciliation 이 건수·PK 대조로 강등된다(ORA_HASH 는 32비트라 대체재가 아니다).
  --     기대값은 SHA-256('abc') 의 표준 시험 벡터다 — charset 이 ASCII 호환인지도 함께 검증된다.
  p_scalar('feat.standard_hash_sha256',  q'[SELECT RAWTOHEX(STANDARD_HASH('abc','SHA256')) FROM DUAL]',
           'BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD');
  p_scalar('feat.ora_hash',              q'[SELECT TO_CHAR(ORA_HASH('abc')) FROM DUAL]');

  -- 8c. 구문 이식성. 11g 에서는 FETCH FIRST 가 없어 ROWNUM 만 쓸 수 있다.
  p_scalar('feat.fetch_first',           q'[SELECT TO_CHAR(1) FROM DUAL FETCH FIRST 1 ROWS ONLY]', '1');
  p_scalar('feat.approx_count_distinct', q'[SELECT TO_CHAR(APPROX_COUNT_DISTINCT(1)) FROM DUAL]');
  p_scalar('feat.listagg_on_overflow',   q'[SELECT LISTAGG('a', ',' ON OVERFLOW TRUNCATE) WITHIN GROUP (ORDER BY 1) FROM DUAL]', 'a');
  p_scalar('feat.extended_varchar',      q'[SELECT TO_CHAR(LENGTH(CAST('x' AS VARCHAR2(32767)))) FROM DUAL]', '1');

  -- 8d. typed_successor 의 전제. CE01 이 겨냥하는 fence granularity 가 이 원천에서 성립하는가.
  p_scalar('feat.timestamp9_precision',  q'[SELECT TO_CHAR(CAST(TIMESTAMP '2026-01-01 00:00:00.123456789' AS TIMESTAMP(9)),'FF9') FROM DUAL]', '123456789');
  p_scalar('feat.interval_ns_successor', q'[SELECT TO_CHAR(TIMESTAMP '2026-01-01 00:00:00.000000000' + INTERVAL '0.000000001' SECOND(1,9),'FF9') FROM DUAL]', '000000001');

  -- 8e. 변경 탐지 축. ROWDEPENDENCIES 가 DISABLED 면 ORA_ROWSCN 은 블록 단위라
  --     행 단위 SCN 으로 쓸 수 없다(CE07 의 전제). 이건 테이블 생성 시 결정되며 사후 변경 불가다.
  p_scalar('feat.ora_rowscn_target',     q'[SELECT TO_CHAR(ORA_ROWSCN) FROM &TARGET_OWNER..&TARGET_TABLE WHERE ROWNUM = 1]');
  p_scalar('feat.rowdependencies_target',q'[SELECT dependencies FROM all_tables WHERE owner = '&TARGET_OWNER' AND table_name = '&TARGET_TABLE']');

  -- 8f. 패키지·뷰 가시성. **실행하지 않고 존재 여부만 본다**(SLEEP 을 실제로 부르지 않는다).
  --     보이지 않는다 = 권한이 없거나 그 버전에 없다. 둘을 구분하려면 ver.* 와 함께 읽어라.
  p_scalar('pkg.dbms_session_sleep',     q'[SELECT TO_CHAR(COUNT(*)) FROM all_procedures WHERE owner='SYS' AND object_name='DBMS_SESSION' AND procedure_name='SLEEP']');
  p_scalar('pkg.dbms_lock',              q'[SELECT TO_CHAR(COUNT(*)) FROM all_objects WHERE owner='SYS' AND object_name='DBMS_LOCK']');
  p_scalar('pkg.dbms_flashback',         q'[SELECT TO_CHAR(COUNT(*)) FROM all_objects WHERE owner='SYS' AND object_name='DBMS_FLASHBACK']');
  -- 아래 둘은 **이미 grant 를 받았는지** 확인하는 용도다. ORA-00942 면 아직 없다는 뜻이고
  --   그 자체가 요청서에 넣을 근거가 된다. 각 1행만 읽으므로 부하가 없다.
  p_scalar('view.v_dataguard_stats',     q'[SELECT TO_CHAR(COUNT(*)) FROM v$dataguard_stats WHERE ROWNUM = 1]');
  p_scalar('view.v_database',            q'[SELECT TO_CHAR(COUNT(*)) FROM v$database WHERE ROWNUM = 1]');

  DBMS_OUTPUT.PUT_LINE('{"probe_summary":{"expected":'||c_expected||
                       ',"emitted":'||g_total||
                       ',"manifest_ok":'||CASE WHEN g_total = c_expected THEN 'true' ELSE 'false' END||
                       ',"total":'||g_total||
                       ',"query_failed":'||g_fail||
                       ',"value_mismatch":'||g_interp||
                       ',"note":"manifest_ok=false 이면 블록이 중간에 끊긴 것이므로 결과 전체를 폐기한다. query_failed>0 은 정상일 수 있다(권한 없음·구버전 미지원 = 그 자체가 측정 결과다). §8 의 실패는 대개 버전 차이이므로 ver.product_component 와 함께 읽어라. value_mismatch>0 은 반드시 조사하라."}}');
END;
/

PROMPT {"probe_run_end":"G0-0A","status":"reached_end"}
PROMPT ============ G0-0A END ============
-- 호출자는 이 sentinel과 exit code 0을 **함께** 확인해야 한다. 둘 중 하나라도 없으면 실패다.
PROMPT 이 출력의 JSON 라인만 추출해 g0_evidence.account_privs 로 등록하라.
PROMPT 데이터 사실(fence 반례)은 이 파일이 아니라 g0-0c-fence-facts.sql 에서 측정한다.
