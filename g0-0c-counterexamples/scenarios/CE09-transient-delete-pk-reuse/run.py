#!/usr/bin/env python3
"""CE09 — transient insert→delete 와 same-PK 재사용 (NEW-15)

current-state 대조(census)는 **두 시점의 사진 두 장**이다. 그 사이에 일어난 일은
사진에 남지 않는다.
  · transient  — 생겼다 사라진 행은 두 사진 어디에도 없다. 원리적으로 탐지 불가다.
  · same-PK 재사용 — PK 는 그대로고 내용만 바뀐 행은 PK anti-join 에 걸리지 않는다.
    표본 해시는 **표본에 뽑혔을 때만** 잡는다.

전자는 설계상 예상된 한계다. 그래서 이 시나리오의 판정은 "탐지했는가"가 아니라
**"그 한계가 문서에 공시돼 있는가"** 로 갈린다.
"""
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _ce import (  # noqa: E402
    Fixture, ex, q1, qall, run_main, server_now,
    OUTCOME_FAIL, OUTCOME_HOLDS, OUTCOME_REPRODUCED,
)

SAMPLE_MOD = 5          # MOD(pk,5)=0 → 20% 결정적 표본
REUSED_PK = 7           # 내용만 바뀌는 PK (표본 밖: MOD(7,5)=2)
SAMPLED_PK = 10         # 표본 안에서 내용이 바뀌는 대조 PK (MOD(10,5)=0)
TRANSIENT_PK = 900      # 두 census 사이에만 존재하는 PK
DELETED_PK = 3          # 양성 대조 — 지우고 되살리지 않는다. anti-join 이 이것만은 잡아야 한다.

DOC_NAME = "etl-platform-target-architecture-v1.2.3.1.md"
# 공시 판정은 **한 축(transient)과 다른 한 축(same-PK 재사용)이 모두** 언급될 때만 성립한다.
# 한국어로만 쓰인 공시를 놓치지 않도록 축마다 동의어를 둔다(대소문자 무시).
TRANSIENT_TERMS = ["transient", "일시적", "생겼다 사라", "사라진 행", "중간에 삭제"]
REUSE_TERMS = ["재삽입", "동일 PK", "same-pk", "same PK", "PK 재사용", "PK 를 재사용", "PK를 재사용"]
CONTEXT_TERMS = ["census", "current-state", "current state", "전수 대조", "PK 대조"]
DISCLOSURE_TERMS = TRANSIENT_TERMS + REUSE_TERMS + CONTEXT_TERMS


def census(conn, src, tbl, label, res):
    ex(conn, f"DELETE FROM {tbl}")
    n = ex(conn, f"INSERT INTO {tbl} SELECT pk, RAWTOHEX(STANDARD_HASH(payload,'SHA256')) FROM {src}")
    conn.commit()
    res.obs(f"census_{label}_rows", n)
    return n


def find_doc(res):
    p = os.environ.get("CE_DOC_PATH")
    cands = [Path(p)] if p else []
    here = Path(__file__).resolve()
    cands += [parent / DOC_NAME for parent in list(here.parents)[:6]]
    for c in cands:
        try:
            if c.is_file():
                return c
        except OSError:
            continue
    return None


def body(res, ora):
    conn = ora.connect(tag="ce09")
    writer = ora.connect(tag="writer")
    ora.verify_schema(conn)
    res.obs("server_time_start", server_now(conn))

    with Fixture(res, conn) as fx:
        src = fx.table("TD_SRC", "pk NUMBER PRIMARY KEY, payload VARCHAR2(40)",
                       opts="ROWDEPENDENCIES")
        c1 = fx.table("TD_C1", "pk NUMBER PRIMARY KEY, h VARCHAR2(64)")
        c2 = fx.table("TD_C2", "pk NUMBER PRIMARY KEY, h VARCHAR2(64)")

        ex(conn, f"INSERT INTO {src} SELECT LEVEL, 'A'||LEVEL FROM DUAL CONNECT BY LEVEL <= 20")
        conn.commit()
        res.rows_written += 20
        census(conn, src, c1, "cycle1", res)
        scn1 = q1(conn, f"SELECT MAX(ORA_ROWSCN) FROM {src}")

        # ── 두 census 사이에 일어나는 일 ───────────────────────────────────
        for pk in (REUSED_PK, SAMPLED_PK):
            ex(writer, f"DELETE FROM {src} WHERE pk = :p", p=pk); writer.commit()
            ex(writer, f"INSERT INTO {src} VALUES(:p, 'REUSED-'||:p)", p=pk); writer.commit()
        # 양성 대조: 지우고 되살리지 않는 hard delete. PK anti-join 이 이것만은 잡아야 한다.
        ex(writer, f"DELETE FROM {src} WHERE pk = {DELETED_PK}"); writer.commit()

        # transient — commit 됐다가 사라진다. **존재했다는 사실을 다른 세션이 서버에서 직접 본다.**
        ex(writer, f"INSERT INTO {src} VALUES({TRANSIENT_PK}, 'GHOST')"); writer.commit()
        alive_at = server_now(conn)
        ghost_alive = q1(conn, f"SELECT COUNT(*) FROM {src} WHERE pk = {TRANSIENT_PK}")
        ghost_scn = q1(conn, f"SELECT ORA_ROWSCN FROM {src} WHERE pk = {TRANSIENT_PK}")
        ex(writer, f"DELETE FROM {src} WHERE pk = {TRANSIENT_PK}"); writer.commit()
        gone_at = server_now(writer)
        res.rows_written += 6
        res.obs("transient_pk_visible_between_cycles", ghost_alive, expected=1,
                matches=(ghost_alive == 1),
                note="census 사이에 그 행이 실제로 존재했다는 서버 측 확인. 이것이 없으면 "
                     "'생겼다 사라졌다'는 클라이언트 주장일 뿐이다.")
        if ghost_alive == 1:
            res.evidence("ROW_STATE",
                         f"pk={TRANSIENT_PK} 가 census1 과 census2 사이에 writer 아닌 세션에서 "
                         f"1건으로 관측됐다(ORA_ROWSCN={ghost_scn}, 서버시각 {alive_at}). "
                         f"그 뒤 삭제돼 {gone_at} 시점에는 0건이다.", at=alive_at)

        # 주입이 실제로 일어났음을 서버가 보여 준다.
        scn_reused = q1(conn, f"SELECT ORA_ROWSCN FROM {src} WHERE pk = :p", p=REUSED_PK)
        payload_now = q1(conn, f"SELECT payload FROM {src} WHERE pk = :p", p=REUSED_PK)
        ghost_now = q1(conn, f"SELECT COUNT(*) FROM {src} WHERE pk = {TRANSIENT_PK}")
        res.obs("reused_pk_payload_now", payload_now, expected=f"REUSED-{REUSED_PK}",
                matches=(payload_now == f"REUSED-{REUSED_PK}"))
        res.obs("transient_pk_present_now", ghost_now, expected=0, matches=(ghost_now == 0))
        if scn1 is not None and scn_reused is not None and int(scn_reused) > int(scn1):
            res.evidence("SERVER_STATE",
                         f"pk={REUSED_PK} 의 ORA_ROWSCN 이 census1 최대 SCN {int(scn1)} 에서 "
                         f"{int(scn_reused)} 로 올라갔고 payload 가 {payload_now!r} 로 바뀌었다. "
                         f"pk={TRANSIENT_PK} 는 commit 됐다가 사라져 지금 {ghost_now}건이다.",
                         at=gone_at)

        census(conn, src, c2, "cycle2", res)

        # ── 탐지기 셋을 같은 두 사진에 돌린다 ──────────────────────────────
        pk_only = [r[0] for r in qall(
            conn, f"SELECT pk FROM (SELECT pk FROM {c1} UNION SELECT pk FROM {c2}) u "
                  f"WHERE NOT (EXISTS (SELECT 1 FROM {c1} a WHERE a.pk=u.pk) "
                  f"AND EXISTS (SELECT 1 FROM {c2} b WHERE b.pk=u.pk)) ORDER BY pk")]
        sampled = [r[0] for r in qall(
            conn, f"SELECT a.pk FROM {c1} a JOIN {c2} b ON a.pk=b.pk "
                  f"WHERE MOD(a.pk,{SAMPLE_MOD})=0 AND a.h <> b.h ORDER BY a.pk")]
        full_hash = [r[0] for r in qall(
            conn, f"SELECT a.pk FROM {c1} a JOIN {c2} b ON a.pk=b.pk "
                  f"WHERE a.h <> b.h ORDER BY a.pk")]

        res.obs("detector_pk_anti_join", pk_only, expected=[DELETED_PK],
                matches=(pk_only == [DELETED_PK]),
                note=f"anti-join 이 잡는 것은 되살아나지 않은 hard delete(pk={DELETED_PK}) 뿐이다. "
                     f"재사용(pk={REUSED_PK}·{SAMPLED_PK})은 PK 가 양쪽 사진에 다 있어 걸리지 않고, "
                     f"transient(pk={TRANSIENT_PK})는 어느 사진에도 없어 걸리지 않는다.")
        res.obs("detector_sample_hash",
                {"sample_rule": f"MOD(pk,{SAMPLE_MOD})=0", "detected": sampled},
                expected={"detected": [SAMPLED_PK]}, matches=(sampled == [SAMPLED_PK]),
                note=f"표본에 든 pk={SAMPLED_PK} 는 잡히고, 표본 밖 pk={REUSED_PK} 는 놓친다.")
        res.obs("detector_full_hash", full_hash,
                expected=sorted([REUSED_PK, SAMPLED_PK]),
                matches=(full_hash == sorted([REUSED_PK, SAMPLED_PK])),
                note="전수 해시만이 재사용을 전부 잡는다. transient 는 전수 해시로도 못 잡는다.")
        res.obs("transient_detected_by_any", TRANSIENT_PK in set(pk_only + sampled + full_hash),
                expected=False, matches=(TRANSIENT_PK not in set(pk_only + sampled + full_hash)),
                note="설계상 예상된 한계다. census 방식은 사이에 사라진 행을 볼 수 없다.")

        # ── 그 한계가 문서에 공시돼 있는가 ─────────────────────────────────
        doc = find_doc(res)
        if doc is None:
            res.obs("disclosure_check", "NOT_PERFORMED",
                    note=f"{DOC_NAME} 을 찾지 못했다. CE_DOC_PATH 로 지정하면 검사한다.")
            res.outcome = OUTCOME_REPRODUCED
            res.obs("verdict_note",
                    "탐지 공백은 재현됐으나 공시 여부를 확인하지 못했다 — HOLDS/FAIL 을 가르려면 "
                    "CE_DOC_PATH 를 주고 다시 돌려라.")
            return
        text = doc.read_text(encoding="utf-8", errors="replace")
        hits = {t: bool(re.search(re.escape(t), text, re.IGNORECASE)) for t in DISCLOSURE_TERMS}
        # 두 축이 모두 언급돼야 공시로 친다 — 한쪽만 있으면 다른 쪽 한계는 여전히 감춰져 있다.
        transient_disclosed = any(hits[t] for t in TRANSIENT_TERMS)
        reuse_disclosed = any(hits[t] for t in REUSE_TERMS)
        disclosed = transient_disclosed and reuse_disclosed
        res.obs("disclosure_check",
                {"doc": doc.name, "terms": hits,
                 "transient_axis": transient_disclosed, "reuse_axis": reuse_disclosed,
                 "disclosed": bool(disclosed)},
                expected={"disclosed": True}, matches=bool(disclosed),
                note="영문 용어만 찾으면 한국어 공시를 놓치므로 축마다 한/영 동의어를 함께 본다.")

        if disclosed:
            res.outcome = OUTCOME_HOLDS
            res.obs("verdict_note",
                    "transient 미탐지와 same-PK 재사용의 한계가 문서에 공시돼 있다. "
                    "탐지 공백은 남지만 보증 축이 그것을 주장하지 않으므로 완화가 성립한다.")
        else:
            res.outcome = OUTCOME_FAIL
            res.obs("verdict_note",
                    "current-state 대조가 transient·same-PK 재사용을 놓치는데 그 한계가 "
                    f"{doc.name} 에 공시돼 있지 않다. 보증 축(delete_consistency·upsert_consistency)에 "
                    "탐지 불가 범위를 명시해야 한다.")


if __name__ == "__main__":
    sys.exit(run_main(body))
