#!/usr/bin/env python3
"""CE05 — 두 watermark Merge 의 독립 seal (F-06)

Merge 원천이 INSERT_DT 와 UPDATE_DT 를 함께 쓰면, 한 축의 MAX 만 seal 해도
**다른 축의 tail 은 봉인되지 않는다**. 두 축은 서로의 상한을 대신하지 못한다.

세 전략을 같은 원천 위에서 독립 cursor 로 나란히 돌린다.
  U_ONLY : UPDATE_DT 축만        → UPDATE_DT 가 NULL 인 신규 행을 못 본다
  I_ONLY : INSERT_DT 축만        → INSERT_DT 가 정지한 갱신 행을 못 본다
  INDEP  : 축마다 독립 cursor    → 둘 다 본다
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _ce import (  # noqa: E402
    Fixture, ex, q1, qall, run_main, server_now,
    OUTCOME_FAIL, OUTCOME_HOLDS,
)

SUCC = "INTERVAL '0.000001' SECOND(1,6)"
AXES = {"I": "insert_dt", "U": "update_dt"}
STRATEGY_AXES = {"U_ONLY": ["U"], "I_ONLY": ["I"], "INDEP": ["I", "U"]}


def cycle(res, conn, src, cur_t, sink, strategy, no):
    axes = STRATEGY_AXES[strategy]
    preds, binds, moves = [], {}, []
    for ax in axes:
        col = AXES[ax]
        lo = q1(conn, f"SELECT low_wm FROM {cur_t} WHERE strategy=:s AND axis=:a",
                s=strategy, a=ax)
        hi = q1(conn, f"SELECT MAX({col}) + {SUCC} FROM {src}")
        if hi is None or (lo is not None and hi <= lo):
            continue
        preds.append(f"({col} >= :lo_{ax} AND {col} < :hi_{ax})")
        binds[f"lo_{ax}"], binds[f"hi_{ax}"] = lo, hi
        moves.append((ax, hi))
    ids = []
    if preds:
        ids = [r[0] for r in qall(
            conn, f"SELECT id FROM {src} WHERE {' OR '.join(preds)} ORDER BY id", **binds)]
        if ids:
            with conn.cursor() as c:
                c.executemany(f"INSERT INTO {sink}(strategy, cyc, id) VALUES(:1, :2, :3)",
                              [(strategy, no, i) for i in ids])
            res.rows_written += len(ids)
        for ax, hi in moves:
            ex(conn, f"UPDATE {cur_t} SET low_wm=:hi WHERE strategy=:s AND axis=:a",
               hi=hi, s=strategy, a=ax)
    conn.commit()
    res.obs(f"{strategy.lower()}_cycle{no}_loaded", ids)
    return ids


def body(res, ora):
    conn = ora.connect(tag="ce05")
    ora.verify_schema(conn)
    res.obs("server_time_start", server_now(conn))

    with Fixture(res, conn, ora) as fx:
        src = fx.table("TW_SRC", "id NUMBER PRIMARY KEY, insert_dt TIMESTAMP(6), "
                                 "update_dt TIMESTAMP(6), grp VARCHAR2(6)")
        cur_t = fx.table("TW_CUR", "strategy VARCHAR2(8), axis VARCHAR2(1), "
                                   "low_wm TIMESTAMP(6), PRIMARY KEY(strategy, axis)")
        sink = fx.table("TW_SINK", "strategy VARCHAR2(8), cyc NUMBER, id NUMBER")

        B = "TIMESTAMP '2026-08-01 00:00:00'"
        # A 군: INSERT_DT 는 전진, UPDATE_DT 는 NULL (한 번도 갱신되지 않은 신규 행)
        ex(conn, f"INSERT INTO {src} SELECT LEVEL, {B} + NUMTODSINTERVAL(LEVEL,'SECOND'), "
                 f"NULL, 'A' FROM DUAL CONNECT BY LEVEL <= 5")
        # B 군: INSERT_DT 는 하루 전에 고정, UPDATE_DT 만 전진 (오래 전 행의 갱신)
        ex(conn, f"INSERT INTO {src} SELECT 10+LEVEL, {B} - INTERVAL '1' DAY, "
                 f"{B} + NUMTODSINTERVAL(LEVEL,'SECOND'), 'B' FROM DUAL CONNECT BY LEVEL <= 5")
        start = q1(conn, f"SELECT {B} - INTERVAL '2' DAY FROM DUAL")
        with conn.cursor() as c:
            c.executemany(f"INSERT INTO {cur_t} VALUES(:1, :2, :3)",
                          [(s, ax, start) for s, axl in STRATEGY_AXES.items() for ax in axl])
        conn.commit()
        res.rows_written += 10 + 4

        null_upd = q1(conn, f"SELECT COUNT(*) FROM {src} WHERE update_dt IS NULL")
        frozen_ins = q1(conn, f"SELECT COUNT(*) FROM {src} WHERE insert_dt < {B}")
        res.obs("rows_with_null_update_dt", null_upd, expected=5, matches=(null_upd == 5))
        res.obs("rows_with_frozen_insert_dt", frozen_ins, expected=5, matches=(frozen_ins == 5))
        if null_upd and frozen_ins:
            res.evidence("ROW_STATE",
                         f"서버 계수: UPDATE_DT 가 NULL 인 행 {null_upd}건, "
                         f"INSERT_DT 가 정지한 행 {frozen_ins}건이 같은 테이블에 공존한다. "
                         "한 축의 MAX 는 다른 축의 상한이 될 수 없다.", at=server_now(conn))

        for s in STRATEGY_AXES:
            cycle(res, conn, src, cur_t, sink, s, 1)

        # 두 축 모두에서 tail 이 더 자란다
        ex(conn, f"INSERT INTO {src} SELECT 20+LEVEL, {B} + NUMTODSINTERVAL(100+LEVEL,'SECOND'), "
                 f"NULL, 'A' FROM DUAL CONNECT BY LEVEL <= 3")
        ex(conn, f"INSERT INTO {src} SELECT 30+LEVEL, {B} - INTERVAL '1' DAY, "
                 f"{B} + NUMTODSINTERVAL(200+LEVEL,'SECOND'), 'B' FROM DUAL CONNECT BY LEVEL <= 3")
        conn.commit()
        res.rows_written += 6

        for s in STRATEGY_AXES:
            cycle(res, conn, src, cur_t, sink, s, 2)

        total = q1(conn, f"SELECT COUNT(*) FROM {src}")
        recall = {}
        for s in STRATEGY_AXES:
            missed = [r[0] for r in qall(
                conn, f"SELECT s.id FROM {src} s WHERE NOT EXISTS "
                      f"(SELECT 1 FROM {sink} k WHERE k.strategy=:s AND k.id=s.id) ORDER BY s.id",
                s=s)]
            recall[s] = {"missed_ids": missed, "recall_pct": round(100 * (total - len(missed)) / total, 1)}
        res.obs("recall_by_strategy", recall, expected={"INDEP": {"missed_ids": []}},
                matches=(recall["INDEP"]["missed_ids"] == []))

        single_missed = recall["U_ONLY"]["missed_ids"] or recall["I_ONLY"]["missed_ids"]
        indep_clean = not recall["INDEP"]["missed_ids"]
        if single_missed and indep_clean:
            res.outcome = OUTCOME_HOLDS
            res.obs("verdict_note",
                    f"단일 축 seal 은 누락했고(U_ONLY {len(recall['U_ONLY']['missed_ids'])}건, "
                    f"I_ONLY {len(recall['I_ONLY']['missed_ids'])}건) 축별 독립 seal 은 "
                    "전량 회수했다. Merge 원천은 축마다 cursor 를 따로 두어야 한다.")
        elif single_missed and not indep_clean:
            res.outcome = OUTCOME_FAIL
            res.obs("verdict_note",
                    f"축별 독립 seal 도 {recall['INDEP']['missed_ids']} 를 놓쳤다 — 완화가 불충분하다.")
        else:
            res.obs("verdict_note", "단일 축 seal 에서 누락이 없었다 — 이 원천 형상에서는 두 축이 갈리지 않는다.")


if __name__ == "__main__":
    sys.exit(run_main(body))
