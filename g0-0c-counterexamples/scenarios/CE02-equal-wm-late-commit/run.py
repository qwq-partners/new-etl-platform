#!/usr/bin/env python3
"""CE02 — CAS 이후의 동률 watermark late commit (F-02, 재검증 §1.3)

R0 가 MAX(wm)=M 을 보고 high=successor(M) 으로 CAS 해 low 를 전진시킨 **뒤에**,
watermark 가 정확히 M 인 트랜잭션이 commit 된다. 그 행은
  · 회차 1 에서는 uncommitted 라 안 보이고,
  · 회차 2 에서는 high==low 라 Spark 제출 자체가 없고(FINALIZED_NO_DATA),
  · 회차 3 이후에는 wm(M) < low 라 창 밖이다.
즉 영구 누락이다. 이 시나리오는 그 셋을 순서대로 서버에서 관측한다.

injection 의 증거는 **독립 세션이 센 행 수의 변화**다. 예외가 안 났다는 사실이 아니다.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _ce import (  # noqa: E402
    Fixture, Unavailable, ex, q1, qall, run_main, server_now,
    OUTCOME_HOLDS, OUTCOME_REPRODUCED,
)

SUCC = "INTERVAL '0.000001' SECOND(1,6)"   # TIMESTAMP(6) 의 typed successor


def cycle(res, conn, src, cur_t, sink, job, no, eligible_only=False):
    """한 회차: MAX(wm) 관측 → [low, successor(MAX)) 적재 → CAS. 적재 id 목록을 준다."""
    low = q1(conn, f"SELECT low_wm FROM {cur_t} WHERE job = :j", j=job)
    filt = "WHERE wm <= SYSTIMESTAMP" if eligible_only else ""
    high = q1(conn, f"SELECT MAX(wm) + {SUCC} FROM {src} {filt}")
    if high is None:
        res.obs(f"cycle{no}_high_undefined", True,
                note="MAX(wm) 이 NULL 이라 high 가 정의되지 않는다(F-05 영역).")
        return []
    if low is not None and high <= low:
        # 반개구간이 비었다 → Spark 제출이 아예 없다.
        res.obs(f"cycle{no}_submitted", 0, expected=0, matches=True,
                note="high==low 라 FINALIZED_NO_DATA. 제출 자체가 없다.")
        return []
    ids = [r[0] for r in qall(
        conn, f"SELECT id FROM {src} WHERE wm >= :lo AND wm < :hi ORDER BY id", lo=low, hi=high)]
    if ids:
        with conn.cursor() as c:
            c.executemany(f"INSERT INTO {sink}(cyc, id) VALUES(:1, :2)", [(no, i) for i in ids])
        res.rows_written += len(ids)
    moved = ex(conn, f"UPDATE {cur_t} SET low_wm = :hi WHERE job = :j "
                     f"AND (low_wm = :lo OR (low_wm IS NULL AND :lo IS NULL))",
               hi=high, j=job, lo=low)
    conn.commit()
    if moved != 1:
        raise Unavailable(f"cycle{no} CAS 실패(rowcount={moved}) — 다른 실행과 경합했다.")
    res.obs(f"cycle{no}_loaded_ids", ids)
    return ids


def body(res, ora):
    reader = ora.connect(tag="reader")
    writer = ora.connect(tag="writer")
    ora.verify_schema(reader)
    res.obs("server_time_start", server_now(reader))

    with Fixture(res, reader) as fx:
        src = fx.table("WM_SRC", "id NUMBER PRIMARY KEY, wm TIMESTAMP(6), tag VARCHAR2(20)")
        cur_t = fx.table("WM_CUR", "job VARCHAR2(20) PRIMARY KEY, low_wm TIMESTAMP(6)")
        sink = fx.table("WM_SINK", "cyc NUMBER, id NUMBER")

        # ── seed: wm 이 1초 간격으로 오르는 5행. MAX(wm) = base+5s = M ──────
        ex(reader, f"INSERT INTO {src} "
                   f"SELECT LEVEL, TIMESTAMP '2026-08-01 00:00:00' + NUMTODSINTERVAL(LEVEL,'SECOND'), "
                   f"'SEED' FROM DUAL CONNECT BY LEVEL <= 5")
        ex(reader, f"INSERT INTO {cur_t} VALUES('J', TIMESTAMP '2026-08-01 00:00:00')")
        reader.commit()
        res.rows_written += 6
        m_val = q1(reader, f"SELECT MAX(wm) FROM {src}")
        res.obs("M", str(m_val))

        # ── writer: wm 이 정확히 M 인 행을 넣고 **commit 하지 않는다** ──────
        ex(writer, f"INSERT INTO {src} VALUES(999, :m, 'LATE')", m=m_val)
        before = q1(reader, f"SELECT COUNT(*) FROM {src} WHERE wm = :m", m=m_val)
        res.obs("rows_at_M_before_commit", before, expected=1, matches=(before == 1),
                note="독립 세션(reader)이 센 값이다. uncommitted 행은 보이지 않는다.")

        # ── 회차 1: reader 가 M 을 보고 CAS 한다 ────────────────────────────
        c1 = cycle(res, reader, src, cur_t, sink, "J", 1)
        low_after = q1(reader, f"SELECT low_wm FROM {cur_t} WHERE job='J'")
        res.obs("low_after_cycle1", str(low_after),
                note="low 가 successor(M) 으로 전진했다. 이 시점에 999 는 아직 commit 전이다.")
        if 999 in c1:
            raise Unavailable("uncommitted 행이 회차1에 보였다 — 격리 수준이 전제와 다르다.")

        # ── late commit ────────────────────────────────────────────────────
        commit_at = server_now(writer)
        writer.commit()
        after = q1(reader, f"SELECT COUNT(*) FROM {src} WHERE wm = :m", m=m_val)
        res.obs("rows_at_M_after_commit", after, expected=2, matches=(after == 2))
        if after == before + 1:
            res.evidence("SERVER_STATE",
                         f"wm=M 인 행 수가 독립 세션 기준 {before} → {after} 로 변했다. "
                         "late commit 이 실제로 서버에 반영됐다는 직접 증거다.", at=commit_at)
        res.evidence("TIMING", f"late commit 서버 시각 {commit_at} — 회차1 CAS 이후다.",
                     at=commit_at)

        # ── 회차 2: high == low 라 제출 자체가 없다 ────────────────────────
        c2 = cycle(res, reader, src, cur_t, sink, "J", 2)
        res.obs("cycle2_loaded_count", len(c2), expected=0, matches=(len(c2) == 0))

        # ── source progress 재개: wm 이 다시 전진한다 ──────────────────────
        ex(writer, f"INSERT INTO {src} VALUES(1000, :m + INTERVAL '60' SECOND, 'NEXT')", m=m_val)
        writer.commit()
        res.rows_written += 1
        c3 = cycle(res, reader, src, cur_t, sink, "J", 3)
        res.obs("cycle3_loaded_ids", c3, expected=[1000], matches=(c3 == [1000]),
                note="source progress 가 재개돼도 999 는 회수되지 않는다. wm(999)=M < low 이기 때문이다.")

        # ── 최종 판정: 999 가 어느 회차에도 들어왔는가 ──────────────────────
        loaded_999 = q1(reader, f"SELECT COUNT(*) FROM {sink} WHERE id = 999")
        exists_999 = q1(reader, f"SELECT COUNT(*) FROM {src} WHERE id = 999")
        res.obs("row999_in_source", exists_999, expected=1, matches=(exists_999 == 1))
        res.obs("row999_ever_loaded", loaded_999, expected=0, matches=(loaded_999 == 0))
        res.obs("recovered_only_by_source_progress", False,
                note="회차3 에서도 회수되지 않았다. 재적재 없이는 영구 누락이다.")

        if exists_999 == 1 and loaded_999 == 0:
            res.outcome = OUTCOME_REPRODUCED
            res.obs("verdict_note",
                    "high=successor(MAX) CAS 는 동률 watermark 의 late commit 을 봉인하지 못한다. "
                    "완화는 seal 지연(commit 가시성 확보 후 CAS) 또는 재적재 경로가 필요하다.")
        else:
            res.outcome = OUTCOME_HOLDS
            res.obs("verdict_note", f"999 가 {loaded_999}회 적재됐다 — 이 환경에서는 봉인이 성립한다.")


if __name__ == "__main__":
    sys.exit(run_main(body))
