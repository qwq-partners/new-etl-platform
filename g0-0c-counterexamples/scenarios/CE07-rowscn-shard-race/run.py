#!/usr/bin/env python3
"""CE07 — ROWSCN shard cursor race (탐지 3축 §3.2)

ORA_ROWSCN 스윕을 shard 로 나누고 **shard 마다 cursor 를 따로 전진**시키면,
두 shard 의 cursor 사이 구간에 commit 된 행은 어느 shard 의 창에도 들어가지 않는다.
shard A 의 창은 SCN 은 맞지만 shard 술어에서 걸러지고, shard B 의 창은 shard 는
맞지만 SCN 하한이 이미 그 행을 지나쳐 있기 때문이다.

SCN 은 권한 없이 얻는다 — V$DATABASE 도 DBMS_FLASHBACK 도 쓰지 않고,
marker 행을 commit 한 뒤 그 행의 ORA_ROWSCN 을 읽는다.
테이블은 ROWDEPENDENCIES 로 만들어 행 단위 SCN 을 확보한다.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _ce import (  # noqa: E402
    Fixture, Unavailable, ex, q1, qall, run_main, server_now,
    OUTCOME_HOLDS, OUTCOME_NOT_OBSERVED, OUTCOME_REPRODUCED,
)

MAX_ATTEMPTS = 5


def commit_row(conn, src, pk, shard, tag):
    ex(conn, f"INSERT INTO {src} VALUES(:p, :s, :t)", p=pk, s=shard, t=tag)
    conn.commit()
    return q1(conn, f"SELECT ORA_ROWSCN FROM {src} WHERE pk = :p", p=pk)


def gap(conn):
    """commit 사이를 서버에서 벌린다. 권한이 없으면 조용히 건너뛴다."""
    try:
        ex(conn, "BEGIN DBMS_SESSION.SLEEP(0.3); END;")
        return "DBMS_SESSION.SLEEP"
    except Exception:  # noqa: BLE001
        try:
            q1(conn, "SELECT COUNT(*) FROM (SELECT 1 FROM DUAL CONNECT BY LEVEL <= 200000)")
            return "cpu_burn"
        except Exception:  # noqa: BLE001
            return "none"


def body(res, ora):
    conn = ora.connect(tag="sweeper")
    writer = ora.connect(tag="writer")
    ora.verify_schema(conn)
    res.obs("server_time_start", server_now(conn))

    with Fixture(res, conn, ora) as fx:
        try:
            src = fx.table("RS_SRC", "pk NUMBER PRIMARY KEY, shard NUMBER, tag VARCHAR2(12)",
                           opts="ROWDEPENDENCIES")
        except Exception as e:  # noqa: BLE001
            raise Unavailable(f"ROWDEPENDENCIES 테이블 생성 실패: {e}") from e
        cur_t = fx.table("RS_CUR", "shard NUMBER PRIMARY KEY, cur_scn NUMBER")

        # 배경 행(스윕이 실제로 무언가를 집도록)
        ex(conn, f"INSERT INTO {src} SELECT LEVEL, MOD(LEVEL,2), 'BG' "
                 f"FROM DUAL CONNECT BY LEVEL <= 20")
        conn.commit()
        res.rows_written += 20
        res.obs("gap_mechanism", gap(conn))

        # ── S0 < SR < S1 이 되도록 marker–대상–marker 를 순서대로 commit ────
        s0 = sr = s1 = None
        pk = 500
        for attempt in range(1, MAX_ATTEMPTS + 1):
            s0 = commit_row(writer, src, pk, 0, "M0"); gap(writer)
            sr = commit_row(writer, src, pk + 1, 1, "TARGET"); gap(writer)
            s1 = commit_row(writer, src, pk + 2, 0, "M1")
            res.rows_written += 3
            if None not in (s0, sr, s1) and int(s0) < int(sr) < int(s1):
                res.obs("scn_ordering_attempts", attempt)
                break
            ex(writer, f"DELETE FROM {src} WHERE pk BETWEEN :a AND :b", a=pk, b=pk + 2)
            writer.commit()
            pk += 10
        else:
            raise Unavailable(
                f"ORA_ROWSCN 이 {MAX_ATTEMPTS}회 시도에서 엄격 증가하지 않았다 "
                f"(S0={s0}, SR={sr}, S1={s1}). 이 환경에서는 행 단위 SCN 해상도가 부족하다.")

        target_pk = pk + 1
        res.obs("scn_markers", {"shard0_marker_S0": int(s0), "target_row_SR": int(sr),
                                "shard0_marker_S1": int(s1), "target_pk": target_pk,
                                "target_shard": 1})
        res.evidence("SERVER_STATE",
                     f"서버가 돌려준 ORA_ROWSCN: S0={int(s0)} < 대상행={int(sr)} < S1={int(s1)}. "
                     f"대상 행(pk={target_pk})은 shard 1 에 있고 그 commit SCN 은 "
                     "두 shard cursor 사이에 정확히 놓인다.", at=server_now(conn))

        # ── shard cursor 가 어긋난 상태를 구성한다 ─────────────────────────
        #    shard 1 은 나중에 스윕돼 cursor 가 S1 까지 가 있고, shard 0 은 S0 에 머문다.
        ex(conn, f"INSERT INTO {cur_t} VALUES(0, :v)", v=int(s0))
        ex(conn, f"INSERT INTO {cur_t} VALUES(1, :v)", v=int(s1))
        conn.commit()
        res.obs("shard_cursors", {"0": int(s0), "1": int(s1)},
                note="shard 별 독립 cursor. **이 어긋난 상태는 구성한 것이다** — 스윕을 여러 번 "
                     "돌려 자연 발생시키지 않았다. 따라서 이 실험이 보이는 것은 '어긋난 cursor 가 "
                     "행을 잃는다'(조건부 불건전성)이지 '독립 cursor 는 반드시 어긋난다'가 아니다. "
                     "후자는 shard 별 스윕 시각이 다를 수 있다는 사실에서 따라 나오며, 그 사실 "
                     "자체는 병렬 스윕 설계에서 자명하다.")

        now_scn = int(commit_row(writer, src, pk + 3, 0, "NOW"))
        res.rows_written += 1

        # ── 스윕: shard 마다 (cursor, now] ────────────────────────────────
        found_by, per_shard = None, {}
        for sh in (0, 1):
            cur_scn = int(q1(conn, f"SELECT cur_scn FROM {cur_t} WHERE shard = :s", s=sh))
            rows = [r[0] for r in qall(
                conn, f"SELECT pk FROM {src} WHERE shard = :s "
                      f"AND ORA_ROWSCN > :lo AND ORA_ROWSCN <= :hi ORDER BY pk",
                s=sh, lo=cur_scn, hi=now_scn)]
            per_shard[sh] = {"window": [cur_scn, now_scn], "found_pks": rows}
            if target_pk in rows:
                found_by = sh
            ex(conn, f"UPDATE {cur_t} SET cur_scn = :v WHERE shard = :s", v=now_scn, s=sh)
        conn.commit()
        res.obs("sharded_sweep", per_shard)
        res.obs("target_found_by_shard", found_by, expected=None, matches=(found_by is None),
                note="어느 shard 도 잡지 못했으면 그 행은 영구히 탐지되지 않는다.")

        # ── 완화: 전역 단조 cursor = MIN(shard cursors) ────────────────────
        global_lo = min(int(s0), int(s1))
        global_rows = [r[0] for r in qall(
            conn, f"SELECT pk FROM {src} WHERE ORA_ROWSCN > :lo AND ORA_ROWSCN <= :hi ORDER BY pk",
            lo=global_lo, hi=now_scn)]
        res.obs("global_cursor_sweep",
                {"window": [global_lo, now_scn], "found_target": target_pk in global_rows},
                expected={"found_target": True}, matches=(target_pk in global_rows),
                note="전역 단조 cursor(=모든 shard 의 최소값) 또는 shard 간 barrier 가 있으면 잡힌다.")

        if found_by is None and target_pk in global_rows:
            res.outcome = OUTCOME_REPRODUCED
            res.obs("verdict_note",
                    "shard 별 독립 cursor 는 shard 경계 구간의 commit 을 잃는다. "
                    "cursor 는 전역 단조여야 하며, shard 병렬화는 창을 나눌 뿐 cursor 를 나눠서는 안 된다.")
        elif found_by is not None:
            res.outcome = OUTCOME_HOLDS
            res.obs("verdict_note", f"shard {found_by} 가 대상 행을 잡았다 — 이 구성에서는 구멍이 없다.")
        else:
            # shard 도 전역 창도 대상 행을 돌려주지 않았다. 그러면 잃어버린 원인이 cursor 어긋남이
            # 아니라 fixture·SCN 해상도 쪽일 수 있으므로 재현을 주장하지 않는다.
            res.outcome = OUTCOME_NOT_OBSERVED
            res.obs("verdict_note",
                    f"전역 단조 cursor 창 ({global_lo}, {now_scn}] 조차 대상 행(pk={target_pk})을 "
                    "돌려주지 않았다 — 주입이 창 안에 놓였다는 전제가 성립하지 않으므로 "
                    "shard cursor 결함으로 판정하지 않는다.")


if __name__ == "__main__":
    sys.exit(run_main(body))
