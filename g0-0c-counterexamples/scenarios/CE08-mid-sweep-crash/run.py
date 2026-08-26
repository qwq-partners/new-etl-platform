#!/usr/bin/env python3
"""CE08 — mid-sweep crash 와 cursor non-advance (NEW-14)

추출이 partition 단위로 진행되는데 cursor 를 partition 마다 전진시키면,
마지막 partition 을 절반만 쓰고 죽었을 때 다음이 동시에 성립한다.
  · cursor 는 "10번까지 끝났다" 고 말한다
  · staging 에는 10번의 **절반만** 있다
  · 다음 회차의 창은 (10, MAX] = 빈 구간이라 아무것도 다시 읽지 않는다
그래서 재실행이 staging 을 재사용하면 나머지 절반은 영구히 사라진다.

crash 는 흉내가 아니라 실제다 — 자식 프로세스를 띄우고 **SIGKILL 로 죽인다**.
정리 훅도, 예외 처리도 돌지 않는다. 부모는 자식이 신호로 죽었음을 확인한 뒤
**서버에 남은 상태만** 근거로 판정한다.
"""
import os
import signal
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _ce import (  # noqa: E402
    Fixture, Unavailable, ex, q1, run_main, server_now,
    OUTCOME_INCONCLUSIVE, OUTCOME_REPRODUCED,
)

NPARTS, ROWS_PER = 10, 20

# 자식은 argv 로 **테이블 이름만** 받는다. 비밀번호는 환경변수로만 상속된다.
CHILD = r"""
import os, sys, signal
import oracledb
src, stg, cur_t, man = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[6]
nparts, rows_per = int(sys.argv[4]), int(sys.argv[5])
c = oracledb.connect(user=os.environ["CE_USER"], password=os.environ["CE_PASSWORD"],
                     dsn=os.environ["CE_DSN"])
cu = c.cursor()
for part in range(1, nparts + 1):
    if part < nparts:
        cu.execute(f"INSERT INTO {stg} SELECT part, pk, payload FROM {src} WHERE part = :p", p=part)
        c.commit()
    else:
        # 마지막 partition 은 절반만 쓰고 죽는다.
        cu.execute(f"INSERT INTO {stg} SELECT part, pk, payload FROM {src} "
                   f"WHERE part = :p AND ROWNUM <= :h", p=part, h=rows_per // 2)
        c.commit()
    # 결함 1: chunk manifest 를 partition 단위로 SEALED 로 봉인한다(전량 완료 전에).
    cu.execute(f"INSERT INTO {man} SELECT :p, 'SEALED', COUNT(*), SYSTIMESTAMP "
               f"FROM {stg} WHERE part = :p", p=part)
    c.commit()
    # 결함 2: partition 마다 cursor 를 전진시킨다(전량 완료 전에).
    cu.execute(f"UPDATE {cur_t} SET done_part = :p WHERE job = 'J'", p=part)
    c.commit()
    if part == nparts:
        sys.stdout.write("PRECRASH\n"); sys.stdout.flush()
        os.kill(os.getpid(), signal.SIGKILL)
"""


def body(res, ora):
    conn = ora.connect(tag="ce08")
    ora.verify_schema(conn)
    res.obs("server_time_start", server_now(conn))
    res.obs("spark_layer_pending", True,
            note="staging 을 DB 테이블로 모델링했다. Parquet staging 의 부분 파일 재사용은 "
                 "G0-0B1 이후 별도로 봐야 한다.")
    res.obs("crash_model", "COMMITTED_THEN_KILLED",
            note="자식은 각 partition 을 commit 한 뒤 죽는다. 즉 여기서 관측되는 잔여물은 "
                 "'commit 되지 않은 조각'이 아니라 **commit 된 부분 진행 상태**다. 이 시나리오가 "
                 "겨냥하는 결함(cursor 를 publish 전에 전진시킨다)은 정확히 그 상태에서 나온다. "
                 "미commit 조각이 남는 경로(대량 INSERT 중간 kill)는 별도 실험이 필요하다.")

    with Fixture(res, conn, ora) as fx:
        src = fx.table("CR_SRC", "pk NUMBER PRIMARY KEY, part NUMBER, payload VARCHAR2(20)")
        stg = fx.table("CR_STG", "part NUMBER, pk NUMBER, payload VARCHAR2(20)")
        cur_t = fx.table("CR_CUR", "job VARCHAR2(4) PRIMARY KEY, done_part NUMBER, "
                                   "published_part NUMBER")
        sink = fx.table("CR_SINK", "pk NUMBER, via VARCHAR2(10)")
        man = fx.table("CR_MAN", "part NUMBER, status VARCHAR2(10), rows_written NUMBER, "
                                 "sealed_at TIMESTAMP(6)")

        ex(conn, f"INSERT INTO {src} SELECT LEVEL, CEIL(LEVEL/{ROWS_PER}), 'v0' "
                 f"FROM DUAL CONNECT BY LEVEL <= {NPARTS * ROWS_PER}")
        ex(conn, f"INSERT INTO {cur_t} VALUES('J', 0, 0)")
        conn.commit()
        res.rows_written += NPARTS * ROWS_PER + 1

        # ── 실제 crash ─────────────────────────────────────────────────────
        proc = subprocess.run(
            [sys.executable, "-c", CHILD, src, stg, cur_t, str(NPARTS), str(ROWS_PER), man],
            capture_output=True, text=True, timeout=300, env={**os.environ})
        killed = (proc.returncode == -signal.SIGKILL)
        res.obs("child_returncode", proc.returncode, expected=-int(signal.SIGKILL), matches=killed)
        res.obs("child_reached_precrash", "PRECRASH" in proc.stdout)
        if killed:  # 자식이 crash 전까지 실제로 쓴 행(staging + manifest + cursor UPDATE)
            res.rows_written += (NPARTS - 1) * ROWS_PER + ROWS_PER // 2 + NPARTS * 2
        if not killed:
            raise Unavailable(
                f"자식이 SIGKILL 로 죽지 않았다(rc={proc.returncode}). "
                f"stderr={proc.stderr.strip()[-300:]}")

        # ── 서버에 남은 상태만으로 판정한다 ────────────────────────────────
        done = q1(conn, f"SELECT done_part FROM {cur_t} WHERE job='J'")
        stg_total = q1(conn, f"SELECT COUNT(*) FROM {stg}")
        stg_last = q1(conn, f"SELECT COUNT(*) FROM {stg} WHERE part = {NPARTS}")
        src_last = q1(conn, f"SELECT COUNT(*) FROM {src} WHERE part = {NPARTS}")
        res.obs("cursor_done_part_after_crash", done, expected=NPARTS, matches=(done == NPARTS),
                note="cursor 는 마지막 partition 까지 끝났다고 말한다.")
        res.obs("staging_rows_after_crash",
                {"total": stg_total, f"part{NPARTS}": stg_last, "source_part_rows": src_last},
                expected={f"part{NPARTS}": src_last}, matches=(stg_last == src_last),
                note="staging 은 마지막 partition 을 절반만 갖고 있다.")
        man_rows = q1(conn, f"SELECT COUNT(*) FROM {man}")
        man_last = q1(conn, f"SELECT rows_written FROM {man} WHERE part = {NPARTS}")
        res.obs("chunk_manifest_after_crash",
                {"sealed_partitions": man_rows, f"part{NPARTS}_rows_written": man_last,
                 "source_part_rows": src_last},
                expected={"sealed_partitions": 0}, matches=(man_rows == 0),
                note="NEW-14 의 요구는 '실패가 하나라도 있으면 chunk manifest·catalog commit·"
                     "watermark CAS 가 모두 0' 이다. 여기서는 manifest 가 partition 단위로 봉인돼 "
                     "절반짜리 partition 까지 SEALED 로 남는다.")
        if man_rows == NPARTS and man_last is not None and 0 < man_last < src_last:
            res.evidence("SERVER_STATE",
                         f"crash 뒤 서버에 남은 chunk manifest: partition {NPARTS} 가 "
                         f"status=SEALED, rows_written={man_last} 로 봉인돼 있는데 원천은 "
                         f"{src_last}행이다. 절반짜리 산출물이 '완료' 로 표시돼 있다.",
                         at=server_now(conn))
        if done == NPARTS and 0 < stg_last < src_last:
            res.evidence("SERVER_STATE",
                         f"자식이 SIGKILL 로 죽은 뒤 **서버에 남아 있는** 상태: cursor done_part={done} "
                         f"(전량 완료 주장) 인데 staging 의 partition {NPARTS} 는 "
                         f"{stg_last}/{src_last} 행뿐이다.", at=server_now(conn))

        # ── 재실행 A: staging 재사용 + cursor 존중 (결함 경로) ─────────────
        ex(conn, f"INSERT INTO {sink} SELECT pk, 'REUSE' FROM {stg}")
        lo = q1(conn, f"SELECT done_part FROM {cur_t} WHERE job='J'")
        refetched = ex(conn, f"INSERT INTO {sink} SELECT pk, 'REUSE' FROM {src} WHERE part > :lo",
                       lo=lo)
        conn.commit()
        res.rows_written += stg_total + refetched
        reuse_cnt = q1(conn, f"SELECT COUNT(DISTINCT pk) FROM {sink} WHERE via='REUSE'")
        lost = q1(conn, f"SELECT COUNT(*) FROM {src} s WHERE NOT EXISTS "
                        f"(SELECT 1 FROM {sink} k WHERE k.via='REUSE' AND k.pk = s.pk)")
        res.obs("rerun_reuse_staging",
                {"refetched_by_window": refetched, "sink_distinct": reuse_cnt, "lost_rows": lost},
                expected={"lost_rows": 0}, matches=(lost == 0),
                note="창이 (done_part, MAX] 라 다시 읽을 것이 없다. 잃은 행은 어느 회차도 회수하지 않는다.")

        # ── 재실행 B: staging 폐기 + published_part 로 되감기 (완화 경로) ──
        ex(conn, f"DELETE FROM {stg}")
        ex(conn, f"DELETE FROM {man}")     # 산출물과 함께 chunk manifest 도 폐기한다
        pub = q1(conn, f"SELECT published_part FROM {cur_t} WHERE job='J'")
        ex(conn, f"UPDATE {cur_t} SET done_part = published_part WHERE job='J'")
        ex(conn, f"INSERT INTO {sink} SELECT pk, 'REWIND' FROM {src} WHERE part > :p", p=pub)
        ex(conn, f"UPDATE {cur_t} SET done_part = {NPARTS}, published_part = {NPARTS} WHERE job='J'")
        conn.commit()
        lost_rw = q1(conn, f"SELECT COUNT(*) FROM {src} s WHERE NOT EXISTS "
                           f"(SELECT 1 FROM {sink} k WHERE k.via='REWIND' AND k.pk = s.pk)")
        res.obs("rerun_discard_and_rewind", {"rewound_to_part": pub, "lost_rows": lost_rw},
                expected={"lost_rows": 0}, matches=(lost_rw == 0),
                note="published_part 까지 되감고 staging 을 버리면 손실이 없다.")

        if lost > 0 and done == NPARTS:
            res.outcome = OUTCOME_REPRODUCED
            res.obs("verdict_note",
                    f"cursor 전진과 staging 재사용이 겹쳐 {lost}행이 영구 누락됐다. "
                    "cursor 는 **publish 성공 이후에만** 전진해야 하고, crash 잔여 staging 은 "
                    "재사용 대상이 아니라 폐기 대상이다."
                    + (f" 되감기 경로에서는 누락이 {lost_rw}건이었다." if lost_rw == 0 else ""))
        elif lost == 0:
            res.outcome = OUTCOME_INCONCLUSIVE
            res.obs("verdict_note",
                    "재사용 경로에서 누락이 관측되지 않았다. 이 하네스의 결함 주입(partition 단위 "
                    "cursor 전진 + 절반 staging)이 의도대로 성립했는지 위 관측을 먼저 확인하라 — "
                    "완화 성립을 주장할 근거는 아니다.")


if __name__ == "__main__":
    sys.exit(run_main(body))
