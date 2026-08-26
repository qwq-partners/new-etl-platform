#!/usr/bin/env python3
"""CE06 — multi-connection snapshot skew (F-09, NEW-05, NEW-25)

Spark JDBC 는 numPartitions>1 이면 **partition 마다 별도 connection** 을 연다.
connection 이 다르면 read consistency 시점도 다르다. 즉 partition 들의 합집합은
"한 시점의 이미지"가 아니다. partition key 가 가변이면 같은 행이 두 partition 에
잡히거나(중복) 어느 partition 에도 안 잡힌다(누락).

이 하네스는 partition 당 connection 을 **직접** 열어 그 구조를 그대로 재현한다.
Spark 를 띄우지는 않으므로 `spark_layer_pending` 를 함께 기록한다 — 다만 여기서
관측되는 것은 Oracle 의 read consistency 사실이며, 그 사실은 Spark 여부와 무관하다.

대조군: 단일 connection + SET TRANSACTION READ ONLY 로 같은 4구간을 읽는다.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _ce import (  # noqa: E402
    Fixture, ex, q1, qall, run_main, server_now,
    OUTCOME_HOLDS, OUTCOME_REPRODUCED,
)

NROWS, NPART = 400, 4


def read_range(conn, src, lo, hi):
    """Spark JDBC 의 partition predicate 와 같은 형태: [lo, hi)"""
    return [r[0] for r in qall(
        conn, f"SELECT pk FROM {src} WHERE part_key >= :lo AND part_key < :hi", lo=lo, hi=hi)]


def tally(res, label, pks):
    seen, dup = set(), []
    for p in pks:
        (dup.append(p) if p in seen else seen.add(p))
    missing = sorted(set(range(1, NROWS + 1)) - seen)
    res.obs(f"{label}_rows_returned", len(pks))
    res.obs(f"{label}_duplicate_pks", sorted(dup)[:20],
            note=f"총 {len(dup)}건. 한 행이 두 partition 에 잡힌 것이다.")
    res.obs(f"{label}_missing_pks", missing[:20],
            note=f"총 {len(missing)}건. 어느 partition 에도 잡히지 않은 것이다.")
    return len(dup), len(missing)


def move(res, writer, verifier, src, from_k, to_k, pks, tag):
    """partition key 를 옮기고, **독립 세션**이 그 변화를 확인하게 한다."""
    n = ex(writer, f"UPDATE {src} SET part_key = :to WHERE pk IN "
                   f"({','.join(str(int(p)) for p in pks)}) AND part_key = :fr",
           to=to_k, fr=from_k)
    at = server_now(writer)
    writer.commit()
    seen = q1(verifier, f"SELECT COUNT(*) FROM {src} WHERE part_key = :to AND pk IN "
                        f"({','.join(str(int(p)) for p in pks)})", to=to_k)
    res.obs(f"move_{tag}", {"updated": n, "verified_by_other_session": seen,
                            "from": from_k, "to": to_k, "at": at})
    if seen == len(pks) and n == len(pks):
        res.evidence("SERVER_STATE",
                     f"{tag}: {n}행의 part_key 가 {from_k}→{to_k} 로 바뀐 것을 "
                     f"독립 세션이 {seen}건으로 확인했다.", at=at)
    return n


def body(res, ora):
    ctl = ora.connect(tag="control")
    writer = ora.connect(tag="writer")
    ora.verify_schema(ctl)
    res.obs("server_time_start", server_now(ctl))
    res.obs("spark_layer_pending", True,
            note="partition 당 connection 구조는 재현하되 Spark JDBC 소스 자체는 띄우지 않는다. "
                 "OJDBC·Spark 계층의 추가 동작은 G0-0B1 이 서야 판정할 수 있다.")

    with Fixture(res, ctl) as fx:
        src = fx.table("MC_SRC", "pk NUMBER PRIMARY KEY, part_key NUMBER, payload VARCHAR2(20)")
        ex(ctl, f"INSERT INTO {src} SELECT LEVEL, MOD(LEVEL,{NPART}), 'v0' "
                f"FROM DUAL CONNECT BY LEVEL <= {NROWS}")
        ctl.commit()
        res.rows_written += NROWS

        # 옮길 대상을 서버에서 고른다(파티션 0 과 3 에서 각각 10건).
        from0 = [r[0] for r in qall(ctl, f"SELECT pk FROM {src} WHERE part_key=0 "
                                         f"AND ROWNUM <= 10 ORDER BY pk")]
        from3 = [r[0] for r in qall(ctl, f"SELECT pk FROM {src} WHERE part_key=3 "
                                         f"AND ROWNUM <= 10 ORDER BY pk")]
        from0b = [r[0] for r in qall(ctl, f"SELECT pk FROM {src} WHERE part_key=0 "
                                          f"AND pk NOT IN ({','.join(map(str, from0))}) "
                                          f"AND ROWNUM <= 10 ORDER BY pk")]
        from3b = [r[0] for r in qall(ctl, f"SELECT pk FROM {src} WHERE part_key=3 "
                                          f"AND pk NOT IN ({','.join(map(str, from3))}) "
                                          f"AND ROWNUM <= 10 ORDER BY pk")]

        # ── A. partition 당 connection (Spark 기본 동작) ────────────────────
        readers = [ora.connect(tag=f"part{i}") for i in range(NPART)]
        pks = read_range(readers[0], src, 0, 1)          # partition 0 을 먼저 읽는다
        res.obs("partition0_rows", len(pks))
        move(res, writer, ctl, src, 0, 3, from0, "p0_to_p3")   # 읽은 뒤 3번으로 이동 → 중복
        move(res, writer, ctl, src, 3, 0, from3, "p3_to_p0")   # 아직 안 읽은 것을 0번으로 → 누락
        for i in range(1, NPART):
            pks += read_range(readers[i], src, i, i + 1)
        dup_multi, miss_multi = tally(res, "multi_connection", pks)

        # ── B. 단일 connection + SET TRANSACTION READ ONLY ─────────────────
        ro = ora.connect(tag="readonly")
        ex(ro, "SET TRANSACTION READ ONLY")
        ro_pks = read_range(ro, src, 0, 1)
        move(res, writer, ctl, src, 0, 3, from0b, "ro_p0_to_p3")
        move(res, writer, ctl, src, 3, 0, from3b, "ro_p3_to_p0")
        for i in range(1, NPART):
            ro_pks += read_range(ro, src, i, i + 1)
        ro.commit()
        dup_ro, miss_ro = tally(res, "read_only_single_connection", ro_pks)

        res.obs("comparison",
                {"multi_connection": {"dup": dup_multi, "missing": miss_multi},
                 "read_only_single": {"dup": dup_ro, "missing": miss_ro}},
                expected={"read_only_single": {"dup": 0, "missing": 0}},
                matches=(dup_ro == 0 and miss_ro == 0))

        if dup_multi or miss_multi:
            res.outcome = OUTCOME_REPRODUCED
            note = (f"partition 별 connection 경로에서 중복 {dup_multi}건 · 누락 {miss_multi}건이 나왔다.")
            if dup_ro == 0 and miss_ro == 0:
                note += " 단일 connection + READ ONLY 경로에서는 둘 다 0 이었다."
            else:
                note += (f" 단, READ ONLY 경로에서도 중복 {dup_ro}·누락 {miss_ro}건이 남았다 — "
                         "완화가 불충분하다.")
            res.obs("verdict_note", note)
        elif dup_ro == 0 and miss_ro == 0:
            res.outcome = OUTCOME_HOLDS
            res.obs("verdict_note",
                    "이 실행에서는 multi-connection 경로에서도 중복·누락이 관측되지 않았다. "
                    "타이밍 의존이므로 반복 실행이 필요하다.")


if __name__ == "__main__":
    sys.exit(run_main(body))
