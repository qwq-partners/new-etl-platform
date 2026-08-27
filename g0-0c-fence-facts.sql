-- =====================================================================
-- G0-0C — fence 반례 실측 (대상 테이블 접근이 있으므로 별도 분리)
-- =====================================================================
-- 목적: fence 공격(F-01 tail·F-04 시계·F-05 NULL·F-13 유휴)을
--       "이론"에서 "이 테이블의 사실"로 바꾼다.
--
-- **G0-0A를 먼저 실행하고 `wm_column.leading_valid_visible` 결과를 확인한 뒤 실행하라.**
--   Q1·Q2·Q4는 항상 전수 질의다(표본으로 대체하면 의미가 없다) — 인덱스가
--   leading + VALID + VISIBLE이 아니면 full scan이므로 ACK_FULL_SCAN='Y'로
--   명시 승인하기 전에는 건너뛴다. EXACT_MODE는 Q3(NULL 계수)에만 적용된다.
--
-- 안전 규칙: 읽기 전용. 잘못된 비밀번호 시도 없음. 비밀번호는 argv에 넣지 않는다.
-- **ACK_FULL_SCAN='N'(기본)이면 대상 테이블 질의가 하나도 실행되지 않는다.**
--   Q1·Q2·Q4 는 물론 Q3 도 게이트 뒤에 있다 — SAMPLE 은 표본 추출이지 I/O 절감이 아니다.
-- =====================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 400
SET FEEDBACK OFF
SET VERIFY OFF
WHENEVER SQLERROR CONTINUE

DEFINE TARGET_OWNER = 'SCHEMA_NAME'
DEFINE TARGET_TABLE = 'TABLE_NAME'
DEFINE WM_COLUMN    = 'UPDATE_DT'
-- EXACT_MODE: 'N' = 확률 표본(기본) / 'Y' = 전수(승인 필요)
DEFINE EXACT_MODE   = 'N'
-- SAMPLE_PCT: EXACT_MODE='N'일 때 각 행의 선택 확률(%). 정확한 행수·I/O 상한이 아니다.
DEFINE SAMPLE_PCT   = '1'
-- ACK_FULL_SCAN: Q1/Q2/Q4는 wm 인덱스가 leading+VALID+VISIBLE이 아니면 full scan이다.
--   G0-0A의 wm_column.leading_valid_visible 이 0이면 'Y'로 명시 승인하기 전에는 건너뛴다.
DEFINE ACK_FULL_SCAN = 'N'

PROMPT ============ G0-0C FENCE FACTS (exact_mode=&EXACT_MODE) ============

DECLARE
  v_max      VARCHAR2(200);
  v_at_max   NUMBER;
  v_null     NUMBER;
  v_future   NUMBER;
  v_scanned  NUMBER;
  v_exact    BOOLEAN := ('&EXACT_MODE' = 'Y');
  -- Q1·Q2·Q4는 표본이 의미 없어 항상 전수 질의다. 인덱스가 없으면 full scan이므로
  -- 명시 승인(ACK_FULL_SCAN='Y') 없이는 실행하지 않는다(재검증 지적: EXACT_MODE는 Q3에만 걸렸다).
  v_ack_full BOOLEAN := ('&ACK_FULL_SCAN' = 'Y');
  v_from     VARCHAR2(400);
  PROCEDURE fail(p_id VARCHAR2) IS
  BEGIN
    DBMS_OUTPUT.PUT_LINE('{"probe":"'||p_id||'","query_ok":false,"ora":'||SQLCODE||
                         ',"msg":"'||REPLACE(SUBSTR(SQLERRM,1,180),'"','\"')||'"}');
  END;
BEGIN
  -- 표본 모드에서는 SAMPLE 절을 쓰고, 그 사실을 값에 명시한다.
  v_from := '&TARGET_OWNER..&TARGET_TABLE' ||
            CASE WHEN v_exact THEN '' ELSE ' SAMPLE (&SAMPLE_PCT)' END;

  -- Q1. MAX(wm) — **항상 전수 질의**(MAX는 seal 기준값이라 표본이 의미 없다).
  IF NOT v_ack_full THEN
    DBMS_OUTPUT.PUT_LINE('{"probe":"fence.max_wm","query_ok":null,"skipped":true,'||
      '"reason":"ACK_FULL_SCAN=N — Q1/Q2/Q4는 전수 질의다. G0-0A의 wm_column.leading_valid_visible >= 1 을 확인했거나 운영 승인을 받은 뒤 ACK_FULL_SCAN=Y로 실행하라"}');
    GOTO q3_only;
  END IF;
  BEGIN
    EXECUTE IMMEDIATE 'SELECT TO_CHAR(MAX(&WM_COLUMN),''YYYY-MM-DD HH24:MI:SS.FF6'') FROM &TARGET_OWNER..&TARGET_TABLE'
      INTO v_max;
    DBMS_OUTPUT.PUT_LINE('{"probe":"fence.max_wm","query_ok":true,"value":'||
      CASE WHEN v_max IS NULL THEN 'null' ELSE '"'||v_max||'"' END||
      ',"note":"NULL이면 empty 또는 all-NULL — bootstrap 정책이 필요하다(F-05)"}');
  EXCEPTION WHEN OTHERS THEN fail('fence.max_wm'); v_max := NULL;
  END;

  -- Q2. MAX와 동률인 행 수 — **F-01의 직접 증거**. 인덱스가 있으면 등가 스캔.
  IF v_max IS NOT NULL THEN
    BEGIN
      EXECUTE IMMEDIATE
        'SELECT COUNT(*) FROM &TARGET_OWNER..&TARGET_TABLE
          WHERE &WM_COLUMN = (SELECT MAX(&WM_COLUMN) FROM &TARGET_OWNER..&TARGET_TABLE)'
        INTO v_at_max;
      DBMS_OUTPUT.PUT_LINE('{"probe":"fence.rows_at_max_wm","query_ok":true,"value":'||v_at_max||
        ',"note":"1 이상이면 [low,high) + high=MAX(wm) 조합에서 그 행들이 최소 한 회차 지연되고, MAX가 정지하면 영구 누락이 된다(F-01)"}');
    EXCEPTION WHEN OTHERS THEN fail('fence.rows_at_max_wm');
    END;
  END IF;

  <<q3_only>>
  -- Q3. NULL watermark 행 — 단일 컬럼 B-tree 인덱스는 NULL을 담지 않으므로
  --     이 질의는 인덱스로 가속되지 않는다.
  --     **중요**: 행 단위 SAMPLE(p) 는 I/O 절감이 아니다. Oracle 은 세그먼트의 모든 블록을
  --     읽은 뒤 행을 확률로 버린다(TABLE ACCESS SAMPLE). 따라서 SAMPLE 을 쓰더라도
  --     이 질의는 전수 스캔 계열이며, 같은 게이트를 받아야 한다. 생산라인과 붙은 원천에서
  --     '표본이니까 가볍다' 는 오해가 사고로 이어진다.
  IF NOT v_ack_full THEN
    DBMS_OUTPUT.PUT_LINE('{"probe":"fence.null_wm_rows","query_ok":null,"skipped":true,'||
      '"reason":"ACK_FULL_SCAN=N — SAMPLE 은 표본이지 I/O 절감이 아니다(모든 블록을 읽는다). 전수 스캔 계열이므로 승인 후 실행하라"}');
    GOTO done;
  END IF;
  BEGIN
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM '||v_from||' WHERE &WM_COLUMN IS NULL' INTO v_null;
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM '||v_from INTO v_scanned;
    DBMS_OUTPUT.PUT_LINE('{"probe":"fence.null_wm_rows","query_ok":true,"value":'||v_null||
      ',"scanned":'||v_scanned||',"exact":'||CASE WHEN v_exact THEN 'true' ELSE 'false' END||
      ',"sampling":"SAMPLE(n)은 각 행의 선택 확률이며 정확한 n% 행수·I/O 상한이 아니다"'||
      ',"note":"1 이상이면 그 행들은 적재(wm predicate)와 Audit 양쪽에서 영구 제외된다(F-05 P0). 표본 모드의 0은 부재의 증거가 아니다"}');
  EXCEPTION WHEN OTHERS THEN fail('fence.null_wm_rows');
  END;

  -- Q4. 미래 일자 행 — Q1/Q2와 같은 전수 질의 계열이므로 같은 게이트를 쓴다.
  IF NOT v_ack_full THEN
    DBMS_OUTPUT.PUT_LINE('{"probe":"fence.future_wm_rows","query_ok":null,"skipped":true,'||
      '"reason":"ACK_FULL_SCAN=N — 전수 질의 계열"}');
    GOTO done;
  END IF;
  BEGIN
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM &TARGET_OWNER..&TARGET_TABLE WHERE &WM_COLUMN > SYSTIMESTAMP'
      INTO v_future;
    DBMS_OUTPUT.PUT_LINE('{"probe":"fence.future_wm_rows","query_ok":true,"value":'||v_future||
      ',"note":"1 이상이면 미래 timestamp가 cursor를 안전 구간 밖으로 밀 수 있다(F-04·NEW-08). 배제는 필터로만 하고 배제분을 계수·경보하라"}');
  EXCEPTION WHEN OTHERS THEN fail('fence.future_wm_rows');
  END;

  <<done>>
  -- **완결 선언.** 이 목록이 없던 동안 집계기는 이 summary 한 줄만 있어도 C00 을 MEASURED 로
  -- 올렸다(7차 교차 리뷰 P0-02·P1-09). 몇 개를 낼 예정이었는지 산출물이 스스로 말해야
  -- 집계기가 '블록이 중간에 죽었는가'를 판정할 수 있다.
  -- 개수를 세어 되읽는 것이 아니라 **분기 조건에서 유도**한다 — 카운터를 세어 자기 자신과
  -- 비교하면 항등식이라 언제나 통과한다(CE preflight 에서 이미 저지른 오류다).
  --   · 항상 출력: max_wm / null_wm_rows / future_wm_rows (skipped 여도 라인은 나온다)
  --   · 조건부   : rows_at_max_wm — ACK_FULL_SCAN=Y 이고 MAX(wm) 이 NULL 이 아닐 때만
  DBMS_OUTPUT.PUT_LINE('{"probe":"fence.summary","ack_full_scan":'||
    CASE WHEN v_ack_full THEN 'true' ELSE 'false' END||',"exact_mode":'||
    CASE WHEN v_exact THEN 'true' ELSE 'false' END||
    ',"expected_probes":'||CASE WHEN v_ack_full AND v_max IS NOT NULL THEN '4' ELSE '3' END||
    ',"expected_probe_ids":["fence.max_wm","fence.null_wm_rows","fence.future_wm_rows"'||
    CASE WHEN v_ack_full AND v_max IS NOT NULL THEN ',"fence.rows_at_max_wm"' ELSE '' END||']'||
    ',"reminder":"F-13(유휴 정지)은 1회 실행으로 관측되지 않는다 — 회차를 나눠 MAX(wm)이 전진하는지 며칠에 걸쳐 기록하라"}');
END;
/

PROMPT ============ G0-0C END ============
PROMPT F-13 관측: 이 스크립트를 하루 2~3회, 최소 3일 반복해 fence.max_wm 의 전진 여부를 기록하라.
PROMPT 전진하지 않는 구간이 있으면 그 테이블은 NO_SOURCE_PROGRESS 처리와 backoff가 필요하다.
