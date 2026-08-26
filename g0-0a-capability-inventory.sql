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
--       닿는 것은 `WHERE ROWNUM = 1` 한 건과 `AS OF` 권한 확인 한 건뿐이다.
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
  c_expected CONSTANT PLS_INTEGER := 56;
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

  DBMS_OUTPUT.PUT_LINE('{"probe_summary":{"expected":'||c_expected||
                       ',"emitted":'||g_total||
                       ',"manifest_ok":'||CASE WHEN g_total = c_expected THEN 'true' ELSE 'false' END||
                       ',"total":'||g_total||
                       ',"query_failed":'||g_fail||
                       ',"value_mismatch":'||g_interp||
                       ',"note":"manifest_ok=false 이면 블록이 중간에 끊긴 것이므로 결과 전체를 폐기한다. query_failed>0 은 정상일 수 있다(권한 없음 = 측정 결과). value_mismatch>0 은 반드시 조사하라."}}');
END;
/

PROMPT {"probe_run_end":"G0-0A","status":"reached_end"}
PROMPT ============ G0-0A END ============
-- 호출자는 이 sentinel과 exit code 0을 **함께** 확인해야 한다. 둘 중 하나라도 없으면 실패다.
PROMPT 이 출력의 JSON 라인만 추출해 g0_evidence.account_privs 로 등록하라.
PROMPT 데이터 사실(fence 반례)은 이 파일이 아니라 g0-0c-fence-facts.sql 에서 측정한다.
