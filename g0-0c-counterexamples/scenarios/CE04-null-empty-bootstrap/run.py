#!/usr/bin/env python3
"""CE04 — NULL watermark 영구 제외와 empty bootstrap (F-05)

`wm >= low AND wm < high` 는 wm 이 NULL 이면 참도 거짓도 아닌 **UNKNOWN** 이다.
그래서 그 행은 적재에서도, 같은 축을 쓰는 Audit 에서도 조용히 사라진다.
"조회 결과가 0건"이 아니라 "세 값 논리에서 빠진다"는 것을 서버가 직접 세게 한다:

    COUNT(창 안) + COUNT(창 밖) < COUNT(전체)

이 차이가 곧 UNKNOWN 행 수다. 오류도, 경고도, 0건 로그도 나지 않는다.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _ce import (  # noqa: E402
    Fixture, ex, q1, qall, run_main, server_now,
    OUTCOME_HOLDS, OUTCOME_REPRODUCED,
)

SUCC = "INTERVAL '0.000001' SECOND(1,6)"
N_NULL, N_OK = 4, 8


def cycle(res, conn, src, cur_t, sink, no):
    low = q1(conn, f"SELECT low_wm FROM {cur_t} WHERE job='J'")
    high = q1(conn, f"SELECT MAX(wm) + {SUCC} FROM {src}")
    if high is None:
        res.obs(f"cycle{no}_high", None, note="MAX(wm) IS NULL → high 가 정의되지 않는다.")
        return []
    if low is not None and high <= low:
        res.obs(f"cycle{no}_submitted", 0, note="high==low → FINALIZED_NO_DATA")
        return []
    ids = [r[0] for r in qall(conn, f"SELECT id FROM {src} WHERE wm >= :lo AND wm < :hi ORDER BY id",
                              lo=low, hi=high)]
    if ids:
        with conn.cursor() as c:
            c.executemany(f"INSERT INTO {sink}(cyc, id) VALUES(:1, :2)", [(no, i) for i in ids])
        res.rows_written += len(ids)
    ex(conn, f"UPDATE {cur_t} SET low_wm = :hi WHERE job='J'", hi=high)
    conn.commit()
    res.obs(f"cycle{no}_loaded", ids)
    return ids


def body(res, ora):
    conn = ora.connect(tag="ce04")
    ora.verify_schema(conn)
    res.obs("server_time_start", server_now(conn))

    with Fixture(res, conn) as fx:
        src = fx.table("NW_SRC",
                       "id NUMBER PRIMARY KEY, wm TIMESTAMP(6), created_at TIMESTAMP(6)")
        cur_t = fx.table("NW_CUR", "job VARCHAR2(10) PRIMARY KEY, low_wm TIMESTAMP(6)")
        sink = fx.table("NW_SINK", "cyc NUMBER, id NUMBER")
        empty_t = fx.table("NW_EMPTY", "id NUMBER PRIMARY KEY, wm TIMESTAMP(6)")
        allnull_t = fx.table("NW_ALLNULL", "id NUMBER PRIMARY KEY, wm TIMESTAMP(6)")

        # 정상 8행 + wm NULL 4행. created_at 은 둘 다 채운다(다른 축의 존재를 보이기 위해).
        ex(conn, f"INSERT INTO {src} SELECT LEVEL, "
                 f"TIMESTAMP '2026-08-01 00:00:00' + NUMTODSINTERVAL(LEVEL,'SECOND'), SYSTIMESTAMP "
                 f"FROM DUAL CONNECT BY LEVEL <= {N_OK}")
        ex(conn, f"INSERT INTO {src} SELECT 100+LEVEL, NULL, SYSTIMESTAMP "
                 f"FROM DUAL CONNECT BY LEVEL <= {N_NULL}")
        ex(conn, f"INSERT INTO {cur_t} VALUES('J', TIMESTAMP '2026-08-01 00:00:00')")
        ex(conn, f"INSERT INTO {allnull_t} SELECT LEVEL, NULL FROM DUAL CONNECT BY LEVEL <= 3")
        conn.commit()
        res.rows_written += N_OK + N_NULL + 4

        nulls = q1(conn, f"SELECT COUNT(*) FROM {src} WHERE wm IS NULL")
        res.obs("null_wm_rows", nulls, expected=N_NULL, matches=(nulls == N_NULL))

        # ── 세 값 논리의 직접 증거: 창 안 + 창 밖 < 전체 ────────────────────
        low = q1(conn, f"SELECT low_wm FROM {cur_t} WHERE job='J'")
        high = q1(conn, f"SELECT MAX(wm) + {SUCC} FROM {src}")
        total = q1(conn, f"SELECT COUNT(*) FROM {src}")
        inside = q1(conn, f"SELECT COUNT(*) FROM {src} WHERE wm >= :lo AND wm < :hi", lo=low, hi=high)
        outside = q1(conn, f"SELECT COUNT(*) FROM {src} WHERE NOT (wm >= :lo AND wm < :hi)",
                     lo=low, hi=high)
        unknown = total - inside - outside
        res.obs("three_valued_gap", {"total": total, "inside": inside, "outside": outside,
                                     "unknown": unknown},
                expected={"unknown": N_NULL}, matches=(unknown == N_NULL))
        if unknown > 0:
            res.evidence("ROW_STATE",
                         f"서버 계수: 전체 {total} = 창 안 {inside} + 창 밖 {outside} + "
                         f"UNKNOWN {unknown}. UNKNOWN 행은 술어의 어느 쪽에도 속하지 않는다.",
                         at=server_now(conn))

        # ── 정상 회차를 3회 돌린다 ─────────────────────────────────────────
        for no in (1, 2, 3):
            cycle(res, conn, src, cur_t, sink, no)

        never = [r[0] for r in qall(
            conn, f"SELECT s.id FROM {src} s WHERE NOT EXISTS "
                  f"(SELECT 1 FROM {sink} k WHERE k.id = s.id) ORDER BY s.id")]
        res.obs("never_loaded_ids", never, expected=list(range(101, 101 + N_NULL)),
                matches=(never == list(range(101, 101 + N_NULL))),
                note="3회차까지 한 번도 적재되지 않은 행. 전부 wm IS NULL 이다.")

        # ── Audit 이 그 행을 보는가 ────────────────────────────────────────
        audit_wm = q1(conn, f"SELECT COUNT(*) FROM {src} "
                            f"WHERE wm BETWEEN TIMESTAMP '2026-08-01 00:00:00' AND SYSTIMESTAMP")
        audit_other = q1(conn, f"SELECT COUNT(*) FROM {src} "
                               f"WHERE created_at BETWEEN SYSTIMESTAMP - INTERVAL '1' HOUR "
                               f"AND SYSTIMESTAMP + INTERVAL '1' HOUR")
        res.obs("audit_on_wm_axis_sees", audit_wm, expected=N_OK, matches=(audit_wm == N_OK),
                note="Audit 이 같은 wm 축을 쓰면 NULL 행은 Audit 에서도 사라진다.")
        res.obs("audit_on_independent_axis_sees", audit_other, expected=N_OK + N_NULL,
                matches=(audit_other == N_OK + N_NULL),
                note="wm 과 독립인 축(여기서는 created_at)만이 NULL 행을 본다.")

        # ── bootstrap: 빈 테이블 / 전량 NULL 테이블 ────────────────────────
        for label, t in (("empty", empty_t), ("all_null", allnull_t)):
            mx = q1(conn, f"SELECT MAX(wm) FROM {t}")
            cnt = q1(conn, f"SELECT COUNT(*) FROM {t}")
            hi = q1(conn, f"SELECT MAX(wm) + {SUCC} FROM {t}")
            res.obs(f"bootstrap_{label}",
                    {"rows": cnt, "max_wm": str(mx), "high": str(hi)},
                    note="high 가 NULL 이면 첫 회차의 상한이 정의되지 않는다. "
                         "high=NULL 로 CAS 하면 이후 모든 비교가 UNKNOWN 이 된다.")

        misses_load = set(never) == set(range(101, 101 + N_NULL))
        misses_audit = audit_wm == N_OK
        if nulls and misses_load and misses_audit:
            res.outcome = OUTCOME_REPRODUCED
            res.obs("verdict_note",
                    "NULL watermark 행은 적재·Audit 양쪽에서 빠졌고, 어떤 오류도 나지 않았다. "
                    "wm 은 NOT NULL 이 강제되거나, NULL 전용 회수 경로와 독립 축 Audit 이 필요하다.")
        elif nulls and not misses_load:
            res.outcome = OUTCOME_HOLDS
            res.obs("verdict_note", "NULL 행이 적재됐다 — 이 경로에는 별도 처리가 있다.")


if __name__ == "__main__":
    sys.exit(run_main(body))
