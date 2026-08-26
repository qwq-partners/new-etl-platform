#!/usr/bin/env python3
"""CE03 — future-dated outlier 가 MAX 인 상태 (F-04, NEW-08)

원천에 미래 일자 행 하나가 들어오면 raw MAX(wm) 경로의 cursor 가 그 값으로 튄다.
그 뒤에 도착하는 정상 행들은 wm < low 라서 어떤 창에도 들어오지 않는다.

두 경로를 같은 원천 위에서 독립 cursor 로 나란히 돌려 비교한다.
  raw       : high = MAX(wm) + successor
  eligible  : high = MAX(wm WHERE wm <= SYSTIMESTAMP) + successor, outlier 는 quarantine
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _ce import (  # noqa: E402
    Fixture, ex, q1, qall, run_main, server_now,
    OUTCOME_HOLDS, OUTCOME_REPRODUCED,
)

SUCC = "INTERVAL '0.000001' SECOND(1,6)"


def cycle(res, conn, src, cur_t, sink, job, no, eligible):
    low = q1(conn, f"SELECT low_wm FROM {cur_t} WHERE job = :j", j=job)
    filt = "WHERE wm <= SYSTIMESTAMP" if eligible else ""
    high = q1(conn, f"SELECT MAX(wm) + {SUCC} FROM {src} {filt}")
    ids = []
    if high is not None and high > low:
        ids = [r[0] for r in qall(
            conn, f"SELECT id FROM {src} WHERE wm >= :lo AND wm < :hi ORDER BY id",
            lo=low, hi=high)]
        if ids:
            with conn.cursor() as c:
                c.executemany(f"INSERT INTO {sink}(job, cyc, id) VALUES(:1, :2, :3)",
                              [(job, no, i) for i in ids])
            res.rows_written += len(ids)
        ex(conn, f"UPDATE {cur_t} SET low_wm = :hi WHERE job = :j", hi=high, j=job)
    conn.commit()
    res.obs(f"{job.lower()}_cycle{no}", {"low": str(low), "high": str(high), "loaded": ids})
    return ids


def body(res, ora):
    conn = ora.connect(tag="ce03")
    ora.verify_schema(conn)
    res.obs("server_time_start", server_now(conn))

    with Fixture(res, conn) as fx:
        src = fx.table("FO_SRC", "id NUMBER PRIMARY KEY, wm TIMESTAMP(6), tag VARCHAR2(12)")
        cur_t = fx.table("FO_CUR", "job VARCHAR2(10) PRIMARY KEY, low_wm TIMESTAMP(6)")
        sink = fx.table("FO_SINK", "job VARCHAR2(10), cyc NUMBER, id NUMBER")

        # 정상 10행(과거 10분) + 미래 일자 outlier 1행
        ex(conn, f"INSERT INTO {src} SELECT LEVEL, SYSTIMESTAMP - NUMTODSINTERVAL(60-LEVEL,'MINUTE'), "
                 f"'NORMAL' FROM DUAL CONNECT BY LEVEL <= 10")
        ex(conn, f"INSERT INTO {src} VALUES(99, SYSTIMESTAMP + INTERVAL '30' DAY, 'FUTURE')")
        base = q1(conn, f"SELECT MIN(wm) - {SUCC} FROM {src}")
        ex(conn, f"INSERT INTO {cur_t} VALUES('RAW', :b)", b=base)
        ex(conn, f"INSERT INTO {cur_t} VALUES('ELIG', :b)", b=base)
        conn.commit()
        res.rows_written += 13

        # 주입이 서버 기준으로 실제 미래인지 서버가 판정하게 한다.
        fut = q1(conn, f"SELECT COUNT(*) FROM {src} WHERE wm > SYSTIMESTAMP")
        gap_days = q1(conn, f"SELECT ROUND(EXTRACT(DAY FROM (MAX(wm) - SYSTIMESTAMP)),1) "
                            f"FROM {src} WHERE wm > SYSTIMESTAMP")
        res.obs("future_rows", fut, expected=1, matches=(fut == 1))
        if fut:
            res.evidence("ROW_STATE",
                         f"서버가 SYSTIMESTAMP 와 비교해 미래 행 {fut}건, 최대 +{gap_days}일 로 판정했다.",
                         at=server_now(conn))

        # 회차 1 — 두 경로가 각각 무엇을 먹는가
        raw1 = cycle(res, conn, src, cur_t, sink, "RAW", 1, eligible=False)
        elig1 = cycle(res, conn, src, cur_t, sink, "ELIG", 1, eligible=True)
        res.obs("raw_cycle1_took_outlier", 99 in raw1, expected=True, matches=(99 in raw1))
        res.obs("elig_cycle1_took_outlier", 99 in elig1, expected=False, matches=(99 not in elig1))
        res.obs("low_after_cycle1",
                {r[0]: str(r[1]) for r in qall(conn, f"SELECT job, low_wm FROM {cur_t}")},
                note="RAW 의 cursor 는 30일 뒤로 튀었다.")

        # 그 뒤 정상 행이 5건 더 도착한다
        ex(conn, f"INSERT INTO {src} SELECT 100+LEVEL, SYSTIMESTAMP, 'LATER' "
                 f"FROM DUAL CONNECT BY LEVEL <= 5")
        conn.commit()
        res.rows_written += 5

        raw2 = cycle(res, conn, src, cur_t, sink, "RAW", 2, eligible=False)
        elig2 = cycle(res, conn, src, cur_t, sink, "ELIG", 2, eligible=True)

        # 누가 몇 건을 건너뛰었나 — 서버에서 anti-join 으로 센다
        skipped_raw = qall(conn, f"SELECT s.id FROM {src} s WHERE NOT EXISTS "
                                 f"(SELECT 1 FROM {sink} k WHERE k.job='RAW' AND k.id = s.id) "
                                 f"ORDER BY s.id")
        skipped_elig = qall(conn, f"SELECT s.id FROM {src} s WHERE NOT EXISTS "
                                  f"(SELECT 1 FROM {sink} k WHERE k.job='ELIG' AND k.id = s.id) "
                                  f"AND s.wm <= SYSTIMESTAMP ORDER BY s.id")
        sr = [r[0] for r in skipped_raw]
        se = [r[0] for r in skipped_elig]
        res.obs("raw_cycle2_loaded", raw2, expected=[], matches=(raw2 == []))
        res.obs("elig_cycle2_loaded", elig2)
        res.obs("raw_skipped_ids", sr, note="정상 행인데 raw 경로가 영구히 건너뛴 것들이다.")
        res.obs("elig_skipped_eligible_ids", se, expected=[], matches=(se == []))

        # outlier 는 격리된 채 재평가 대상으로 남아 있어야 한다
        quarantined = q1(conn, f"SELECT COUNT(*) FROM {src} s WHERE s.wm > SYSTIMESTAMP "
                               f"AND NOT EXISTS (SELECT 1 FROM {sink} k WHERE k.job='ELIG' AND k.id=s.id)")
        res.obs("elig_outlier_quarantined_pending", quarantined, expected=1,
                matches=(quarantined == 1),
                note="배제는 삭제가 아니다 — 시각이 지나면 eligible 이 되어 다시 잡혀야 한다.")

        normal_skipped = [i for i in sr if i != 99]
        if normal_skipped:
            res.outcome = OUTCOME_REPRODUCED
            res.obs("verdict_note",
                    f"raw MAX 경로가 정상 행 {len(normal_skipped)}건을 건너뛰었다. "
                    "eligible_max + quarantine 이 이를 막았다면 그 사실도 위에 기록돼 있다.")
        elif not se:
            res.outcome = OUTCOME_HOLDS
            res.obs("verdict_note", "raw 경로에서도 누락이 관측되지 않았다 — 이 환경에서는 성립한다.")


if __name__ == "__main__":
    sys.exit(run_main(body))
