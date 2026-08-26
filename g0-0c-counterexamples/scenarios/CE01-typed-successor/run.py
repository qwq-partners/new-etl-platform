#!/usr/bin/env python3
"""CE01 — typed_successor 왕복과 overflow (F-01, NEW-01)

fence 가 [low, successor(M)) 반개구간을 쓰는 한, successor(M) 이 타입마다
**정의되고 · 단조이고 · 무손실로 왕복하는지**가 설계의 전제다. 이 시나리오는 그
전제를 타입별로 깨 본다.

관측 축 셋
  1. server_successor_gt — 서버가 successor(M) > M 을 인정하는가. NUMBER·BINARY_DOUBLE
     처럼 고정 granularity 가 없는 타입에서는 successor(M) == M 이 된다(설계 결함).
  2. store_overflow      — 최대 표현값에서 successor 계산·저장이 ORA 오류를 내는가.
  3. client_roundtrip    — 값이 클라이언트를 거쳐 되돌아와도 같은가.
     **주의**: 3번은 python-oracledb 한정이다. OJDBC·Spark canonical 계층은
     G0-0B1 이 서기 전에는 이 하네스가 덮지 못하며, 여기서 관측된 왕복 손실을
     설계 결함으로 승격하지 않는다(driver_scoped=true 로 표시한다).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _ce import (  # noqa: E402
    Fixture, ora_error_code, q1, run_main, server_now,
    OUTCOME_INCONCLUSIVE, OUTCOME_REPRODUCED,
)

# key, 컬럼정의, M 표현식, successor 표현식, 최대 표현값, 최대값의 successor
TYPES = [
    ("DATE", "c_date DATE",
     "DATE '2026-08-25' + 45296/86400", "+ 1/86400",
     "TO_DATE('9999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')", "+ 1/86400"),
    ("TIMESTAMP(0)", "c_ts0 TIMESTAMP(0)",
     "TIMESTAMP '2026-08-25 12:34:56'", "+ INTERVAL '1' SECOND",
     "TIMESTAMP '9999-12-31 23:59:59'", "+ INTERVAL '1' SECOND"),
    ("TIMESTAMP(3)", "c_ts3 TIMESTAMP(3)",
     "TIMESTAMP '2026-08-25 12:34:56.123'", "+ INTERVAL '0.001' SECOND(1,3)",
     "TIMESTAMP '9999-12-31 23:59:59.999'", "+ INTERVAL '0.001' SECOND(1,3)"),
    ("TIMESTAMP(6)", "c_ts6 TIMESTAMP(6)",
     "TIMESTAMP '2026-08-25 12:34:56.123456'", "+ INTERVAL '0.000001' SECOND(1,6)",
     "TIMESTAMP '9999-12-31 23:59:59.999999'", "+ INTERVAL '0.000001' SECOND(1,6)"),
    ("TIMESTAMP(9)", "c_ts9 TIMESTAMP(9)",
     "TIMESTAMP '2026-08-25 12:34:56.123456789'", "+ INTERVAL '0.000000001' SECOND(1,9)",
     "TIMESTAMP '9999-12-31 23:59:59.999999999'", "+ INTERVAL '0.000000001' SECOND(1,9)"),
    ("TIMESTAMP WITH TIME ZONE", "c_tstz TIMESTAMP(6) WITH TIME ZONE",
     "TIMESTAMP '2026-08-25 12:34:56.123456 +09:00'", "+ INTERVAL '0.000001' SECOND(1,6)",
     "TIMESTAMP '9999-12-31 23:59:59.999999 +00:00'", "+ INTERVAL '0.000001' SECOND(1,6)"),
    ("NUMBER(10,2)", "c_n102 NUMBER(10,2)",
     "12345.67", "+ 0.01",
     "99999999.99", "+ 0.01"),
    ("NUMBER", "c_num NUMBER",
     "1.0E+125", "+ 1",
     "9.9E+125", "* 10"),
    ("BINARY_DOUBLE", "c_bd BINARY_DOUBLE",
     "1.5D", "+ 1.0E-16D",
     "1.79769313486231570E+308D", "* 2"),
]

# 왕복 손실이 드라이버 정밀도 탓인 타입(파이썬 datetime 은 마이크로초까지다).
DRIVER_SCOPED_ROUNDTRIP = {"TIMESTAMP(9)"}


def body(res, ora):
    conn = ora.connect(tag="ce01")
    ora.verify_schema(conn)
    res.obs("server_time_start", server_now(conn))

    cols = ", ".join(t[1] for t in TYPES)
    with Fixture(res, conn, ora) as fx:
        tbl = fx.table("SUCC", f"case_id VARCHAR2(30) PRIMARY KEY, {cols}")

        oracle_defects, driver_losses, rows = [], [], []
        for key, coldef, m_expr, succ_op, max_expr, max_op in TYPES:
            col = coldef.split()[0]
            rec = {"type": key, "column": col}

            # ── 1. M 과 successor(M) 을 서버가 만들어 저장한다 ────────────
            try:
                with conn.cursor() as cur:
                    cur.execute(f"INSERT INTO {tbl}(case_id, {col}) VALUES('M', {m_expr})")
                    cur.execute(
                        f"INSERT INTO {tbl}(case_id, {col}) VALUES('S', {m_expr} {succ_op})")
                conn.commit()
                res.rows_written += 2
            except Exception as e:  # noqa: BLE001
                conn.rollback()
                code = ora_error_code(e)
                rec["insert_error"] = f"ORA-{code}" if code is not None else f"CLIENT_SIDE:{type(e).__name__}"
                if code is not None:
                    oracle_defects.append(f"{key}:insert")
                    res.evidence("ORA_ERROR",
                                 f"{key}: M/successor 저장을 서버가 ORA-{code} 로 거부했다")
                rows.append(rec)
                continue

            # ── 2. 서버가 successor(M) > M 을 인정하는가 (ROW_STATE 증거) ──
            gt = q1(conn,
                    f"SELECT CASE WHEN (SELECT {col} FROM {tbl} WHERE case_id='S') "
                    f"> (SELECT {col} FROM {tbl} WHERE case_id='M') THEN 1 ELSE 0 END FROM DUAL")
            eq = q1(conn,
                    f"SELECT CASE WHEN (SELECT {col} FROM {tbl} WHERE case_id='S') "
                    f"= (SELECT {col} FROM {tbl} WHERE case_id='M') THEN 1 ELSE 0 END FROM DUAL")
            rec["server_successor_gt"] = int(gt or 0)
            rec["server_successor_eq"] = int(eq or 0)
            if not gt:
                oracle_defects.append(f"{key}:successor_not_greater")
                res.evidence("ROW_STATE",
                             f"{key}: 서버 비교에서 successor(M) > M 이 거짓 "
                             f"(gt={gt}, eq={eq}) — 이 타입은 fence granularity 가 없다")

            # ── 3. 클라이언트 왕복 (드라이버 한정) ────────────────────────
            m_val = q1(conn, f"SELECT {col} FROM {tbl} WHERE case_id='M'")
            s_val = q1(conn, f"SELECT {col} FROM {tbl} WHERE case_id='S'")
            try:
                with conn.cursor() as cur:
                    cur.execute(f"INSERT INTO {tbl}(case_id, {col}) VALUES('M2', :v)", v=m_val)
                    cur.execute(f"INSERT INTO {tbl}(case_id, {col}) VALUES('S2', :v)", v=s_val)
                conn.commit()
                res.rows_written += 2
                same = q1(conn,
                          f"SELECT CASE WHEN (SELECT {col} FROM {tbl} WHERE case_id='M2') "
                          f"= (SELECT {col} FROM {tbl} WHERE case_id='M') "
                          f"AND (SELECT {col} FROM {tbl} WHERE case_id='S2') "
                          f"= (SELECT {col} FROM {tbl} WHERE case_id='S') THEN 1 ELSE 0 END FROM DUAL")
                rec["client_roundtrip_lossless"] = int(same or 0)
                if not same:
                    rec["driver_scoped"] = key in DRIVER_SCOPED_ROUNDTRIP
                    driver_losses.append(key)
            except Exception as e:  # noqa: BLE001
                conn.rollback()
                rec["client_roundtrip_lossless"] = 0
                rec["roundtrip_error"] = f"ORA-{ora_error_code(e)}"
                driver_losses.append(key)

            # ── 4. 최대 표현값에서의 overflow ────────────────────────────
            #     **서버가 거부했다는 근거가 있을 때만** 결함으로 센다.
            #     클라이언트 타임아웃(DPY-xxxx)처럼 ORA 코드가 없는 오류는
            #     "Oracle 이 overflow 로 거부했다"의 근거가 아니다.
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"INSERT INTO {tbl}(case_id, {col}) VALUES('MAXS', {max_expr} {max_op})")
                conn.commit()
                res.rows_written += 1
                stored = True
            except Exception as e:  # noqa: BLE001
                conn.rollback()
                stored = False
                code = ora_error_code(e)
                if code is None:
                    # 서버 오류가 아니다 — 판정에 넣지 않는다.
                    rec["max_successor_error"] = f"CLIENT_SIDE: {type(e).__name__}"
                    rec["max_successor_overflow"] = None
                else:
                    rec["max_successor_overflow"] = f"ORA-{code}"
                    oracle_defects.append(f"{key}:overflow")
                    res.evidence("ORA_ERROR",
                                 f"{key}: 최대 표현값의 successor 계산·저장을 서버가 "
                                 f"ORA-{code} 로 거부했다", at=server_now(conn))
            if stored:
                # 조회는 별도 try — 여기서 난 오류를 저장 거부로 오인하지 않는다.
                try:
                    top = q1(conn, f"SELECT TO_CHAR({col}) FROM {tbl} WHERE case_id='MAXS'")
                    rec["max_successor_stored"] = str(top)
                    rec["max_successor_overflow"] = None
                    if top is not None and str(top).strip().lower().lstrip("+").startswith("inf"):
                        rec["max_successor_overflow"] = "SILENT_INFINITY"
                        oracle_defects.append(f"{key}:silent_infinity")
                        res.evidence("ROW_STATE",
                                     f"{key}: 최대값의 successor 가 오류 없이 {top} 로 저장됐다 — "
                                     "무한대가 fence 로 들어가면 이후 모든 회차가 빈 구간이 된다")
                except Exception as e:  # noqa: BLE001
                    rec["max_successor_readback_error"] = f"{type(e).__name__}"

            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {tbl}")
            conn.commit()
            rows.append(rec)

        # ── 판정 ────────────────────────────────────────────────────────
        res.obs("per_type", rows)
        res.obs("oracle_side_defects", sorted(set(oracle_defects)),
                note="서버가 직접 보여 준 결함이다. 드라이버와 무관하다.")
        res.obs("client_roundtrip_losses", sorted(set(driver_losses)),
                note="python-oracledb 한정 관측이다. OJDBC/Spark 는 G0-0B1 이 서야 판정할 수 있다.")
        res.obs("layers_covered", ["oracle_compare", "python_client_roundtrip"],
                expected=["oracle_compare", "ojdbc", "spark_canonical", "control_store"],
                matches=False, note="4계층 중 2계층만 덮었다.")
        res.obs("spark_layer_pending", True,
                note="requires.spark_required=true 인데 Spark canonical 계층 하네스가 없다.")

        # pass_criteria 는 세 축(왕복 불일치 · successor(M) <= M · overflow)을 모두
        # 제외 사유로 든다. 다만 드라이버 정밀도 탓인 왕복 손실은 Oracle 결함이 아니므로
        # 확정 목록과 **보류 목록**을 나눠 낸다 — 보류는 G0-0B1 이후에 판정한다.
        oracle_drop = sorted({d.split(":")[0] for d in oracle_defects})
        driver_only = sorted({k for k in driver_losses if k not in oracle_drop})
        res.obs("seal_allowlist_removals", oracle_drop,
                note="서버 근거로 확정된 제외 대상이다.")
        res.obs("seal_allowlist_removals_pending", driver_only,
                note="왕복 손실이 관측됐으나 python-oracledb 한정일 수 있다. "
                     "OJDBC/Spark(G0-0B1)에서 재확인하기 전에는 확정하지 않는다.")

        if oracle_defects:
            res.outcome = OUTCOME_REPRODUCED
        else:
            res.outcome = OUTCOME_INCONCLUSIVE
            res.obs("why_inconclusive",
                    "Oracle 계층에서는 반례가 없었으나 OJDBC·Spark 계층이 미검증이라 "
                    "타입 allowlist 를 확정할 수 없다.")


if __name__ == "__main__":
    sys.exit(run_main(body))
