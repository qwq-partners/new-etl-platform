-- G0-0A 신원 preflight — **대상에 손대기 전에 멈춘다** (9차 조치 6 · P0-05)
--
-- 9차 교차 리뷰 P0-05. `g0-0a-capability-inventory.sql` 은 신원을 **기록만** 하고 계속 돈다.
-- `EXPECT_DBUNAME` 과 다른 DB 에 붙어도 `value_mismatch` 로 적어 두고 그 뒤 대상 테이블을
-- 다섯 번 읽는다. **생산라인과 밀접한 원천에서 기록은 차단이 아니다.**
--
-- 리뷰가 요구한 형태 그대로다 — "DUAL 기반 identity preflight 를 **별도 script/connection**
-- 으로 수행한다". 87 probe 블록을 고치지 않고 그 앞에 세우는 이유는 둘이다.
--
--   ① 이 스크립트는 **`DUAL` 만 읽는다.** 대상 스키마·테이블 이름조차 SQL 에 등장하지 않으므로
--      잘못된 대상을 지정한 회차가 그 대상을 **파싱조차** 하지 않는다.
--   ② 별도 connection 이라 실패가 명확하다. 같은 블록 안에서 분기하면 "어디까지 갔는가" 가
--      산출물 해석에 섞인다.
--
-- **자리표시자도 여기서 막는다**(9차 P0-06-8). 본 SQL 은 `TARGET_OWNER = 'SCHEMA_NAME'`,
-- `EXPECT_DBUNAME = 'DBUNIQUENAME'` 같은 자리표시자를 파일 안에서 DEFINE 한다. 채우지 않고
-- 돌리면 존재하지 않는 대상을 질의하고 엉뚱한 신원과 대조한다. 여기서 먼저 죽는다.
--
-- 사용 (runbook §4 S5 참조)
--   ./g0-sqlplus.sh "$EVID/g0-0a-preflight-run.sql" "$EVID/g0-0a-preflight.log"
--
-- 종료
--   0        신원이 기대와 같다. **그때만** 본 probe 를 돌린다
--   그 외    붙은 DB 가 대상이 아니거나 값을 채우지 않았다. 대상에 아무것도 하지 않았다

SET SERVEROUTPUT ON SIZE UNLIMITED FORMAT WRAPPED
SET FEEDBACK OFF VERIFY OFF PAGESIZE 0 LINESIZE 32767 TRIMSPOOL ON
WHENEVER SQLERROR EXIT FAILURE
WHENEVER OSERROR EXIT FAILURE

-- ── 본 SQL 과 **같은 값**을 채운다 ──────────────────────────────────
DEFINE EXPECT_DBUNAME = 'DBUNIQUENAME'
DEFINE EXPECT_ROLE    = 'PHYSICAL STANDBY'
DEFINE EXPECT_CON     = 'ANY'
DEFINE EXPECT_USER    = 'ANY'

PROMPT ============ G0-0A IDENTITY PREFLIGHT ============

DECLARE
  v_dbun VARCHAR2(128);
  v_role VARCHAR2(64);
  v_con  VARCHAR2(128);
  v_usr  VARCHAR2(128);
  v_svc  VARCHAR2(128);
  v_inst VARCHAR2(128);

  PROCEDURE stop_now(p_why VARCHAR2) IS
  BEGIN
    DBMS_OUTPUT.PUT_LINE('{"probe":"preflight.verdict","query_ok":true,'
      ||'"value":"ABORT","row_present":true,"value_interpretable":false,'
      ||'"ora":null,"msg":"'||REPLACE(SUBSTR(p_why,1,300),'"','\"')||'"}');
    -- **대상에 손대기 전에 죽는다.** WHENEVER SQLERROR 가 이것을 받아 비영 종료로 만들고,
    -- 래퍼가 exit_code 를 manifest 에 박고, 집계기가 그 child 를 FAILED 로 둔다.
    RAISE_APPLICATION_ERROR(-20050, 'G0-0A preflight ABORT: '||SUBSTR(p_why, 1, 1800));
  END stop_now;

  PROCEDURE fact(p_id VARCHAR2, p_val VARCHAR2) IS
  BEGIN
    DBMS_OUTPUT.PUT_LINE('{"probe":"'||p_id||'","query_ok":true,"row_present":true,'
      ||'"value_interpretable":true,"value":"'
      ||REPLACE(NVL(SUBSTR(p_val,1,300),''),'"','\"')||'","ora":null,"msg":null}');
  END fact;
BEGIN
  -- ── 0. 자리표시자를 채웠는가 ─────────────────────────────────────
  -- **접속 정보를 읽기도 전에** 본다. 안 채운 값으로는 어떤 대조도 의미가 없다.
  IF '&EXPECT_DBUNAME' IN ('DBUNIQUENAME', '', 'SCHEMA_NAME') THEN
    stop_now('EXPECT_DBUNAME 이 자리표시자 그대로다(&EXPECT_DBUNAME). '
      ||'g0-0-runbook.md §4 S5 대로 DEFINE 블록을 채운 사본을 만들어 그것을 넘겨라');
  END IF;
  IF '&EXPECT_ROLE' IS NULL OR '&EXPECT_ROLE' = '' THEN
    stop_now('EXPECT_ROLE 이 비어 있다');
  END IF;

  -- ── 1. 신원을 읽는다. **DUAL 뿐이다** ────────────────────────────
  SELECT SYS_CONTEXT('USERENV','DB_UNIQUE_NAME',256),
         SYS_CONTEXT('USERENV','DATABASE_ROLE'),
         SYS_CONTEXT('USERENV','CON_NAME'),
         SYS_CONTEXT('USERENV','SESSION_USER'),
         SYS_CONTEXT('USERENV','SERVICE_NAME'),
         SYS_CONTEXT('USERENV','INSTANCE_NAME')
    INTO v_dbun, v_role, v_con, v_usr, v_svc, v_inst
    FROM DUAL;

  fact('preflight.db_unique_name', v_dbun);
  fact('preflight.database_role',  v_role);
  fact('preflight.con_name',       v_con);
  fact('preflight.session_user',   v_usr);
  fact('preflight.service_name',   v_svc);
  fact('preflight.instance_name',  v_inst);

  -- ── 2. 대조. 하나라도 어긋나면 **여기서 끝난다** ─────────────────
  IF UPPER(NVL(v_dbun,'?')) <> UPPER('&EXPECT_DBUNAME') THEN
    stop_now('DB_UNIQUE_NAME='||NVL(v_dbun,'(null)')||' 인데 기대는 &EXPECT_DBUNAME 다. '
      ||'**대상이 아닌 DB 에 붙었다.** 대상에 아무 질의도 하지 않고 끝낸다');
  END IF;
  IF UPPER(NVL(v_role,'?')) <> UPPER('&EXPECT_ROLE') THEN
    stop_now('DATABASE_ROLE='||NVL(v_role,'(null)')||' 인데 기대는 &EXPECT_ROLE 다. '
      ||'role 이 다르면 이 회차가 재는 것이 무엇인지 말할 수 없다(standby 측정을 '
      ||'primary 에서 했는가)');
  END IF;
  IF '&EXPECT_CON' <> 'ANY' AND UPPER(NVL(v_con,'?')) <> UPPER('&EXPECT_CON') THEN
    stop_now('CON_NAME='||NVL(v_con,'(null)')||' 인데 기대는 &EXPECT_CON 다. '
      ||'같은 CDB 의 다른 PDB 는 같은 DB_UNIQUE_NAME 을 갖는다 — 그것만으로는 부족하다');
  END IF;
  IF '&EXPECT_USER' <> 'ANY' AND UPPER(NVL(v_usr,'?')) <> UPPER('&EXPECT_USER') THEN
    stop_now('SESSION_USER='||NVL(v_usr,'(null)')||' 인데 기대는 &EXPECT_USER 다');
  END IF;

  DBMS_OUTPUT.PUT_LINE('{"probe":"preflight.verdict","query_ok":true,"value":"OK",'
    ||'"row_present":true,"value_interpretable":true,"ora":null,'
    ||'"msg":"신원이 기대와 같다. 본 probe 를 돌려도 된다"}');
END;
/

PROMPT ============ PREFLIGHT PASSED ============
EXIT SUCCESS
