"""capability 축 파생 — 표 기반 pure function.

7차 교차 리뷰 **P0-01·P0-05·P1-03·P1-06** 의 조치다.

**파일명에 밑줄을 쓰는 이유**: 이 저장소의 다른 파일은 전부 하이픈인데 이것만 다르다.
`g0-normalize.py` 가 `import` 해야 하고 파이썬 모듈명에는 하이픈을 쓸 수 없다.

---

## 이전 파생기가 왜 틀렸나

| 증상 | 실물 |
|---|---|
| `SELECT COUNT(*) FROM v$database` 성공이 `AS_OF_SCN` 승격 근거였다 | 접근 가능성 probe 를 값 probe 로 썼다. 같은 파일에 값을 읽는 `v$database` 가 따로 있다 |
| `AS OF TIMESTAMP` 만 성공해도 `READ_ONLY_TXN` | 두 값은 snapshot **scope 가 다르다**. 하나는 connection-local, 하나는 SQL literal |
| `COUNT(*) FROM v$dataguard_stats` 성공이 `DG_STATS`("lag 값을 안다") | 값도 `DATUM_TIME` 도 읽지 않는다 |
| ORA-01453 / ORA-03172 양성 대조 probe 가 있는데 파생기가 안 읽었다 | 수집하고 버렸다 |
| `TIMESTAMP(2)` → `US`, `NUMBER(10,2)` → `SEC` | successor 계산이 틀어진다(CE01 의 표적) |
| 연결 끊김(ORA-03135)도 "기능 없음"으로 강등 | transient 와 unsupported 를 구분하지 않았다 |
| `bound_kind`(watermark commit bound)를 `lag_visibility` 로 대체 | **독립 성질을 합쳤다.** apply lag 0 이어도 늦게 commit 된 오래된 행은 반개구간 밖으로 빠진다 |

## 이 모듈의 규칙

1. **순수 함수.** 입력은 probe 딕셔너리, 출력은 축 딕셔너리. 파일·시각·전역 상태를 읽지 않는다.
2. **읽은 것만 `inputs` 에.** 의도한 목록이 아니다. 읽었는데 판정에 안 쓴 것은
   `considered_but_not_used` 로 분리한다(P1-06).
3. **`NONE` 과 `UNDETERMINED` 를 구분한다.** transient 실패는 기능 부재가 아니다(P1-03).
4. **승격에는 양성 대조를 요구한다.** 오류 부재는 증거가 아니다.
5. **composition 은 별도 단계.** 원자 축을 먼저 내고, 그것들로 합성 축을 만든다.
   합성 축은 입력이 하나라도 `UNDETERMINED` 면 `UNDETERMINED` 다.
"""
from __future__ import annotations

import re

# ─────────────────────────────────────────────────────────────────────
# 1. 실패 taxonomy (P1-03)
# ─────────────────────────────────────────────────────────────────────
# **transient 를 기능 부재로 강등하지 않는 것이 이 표의 유일한 목적이다.**
# 분류는 ORA 코드에서 하고, 코드가 없으면 UNKNOWN 이며 UNKNOWN 은 UNDETERMINED 를 만든다.
UNSUPPORTED = {  # 그 기능·객체·구문이 이 판본에 없다 → NONE 의 근거가 된다
    900,    # invalid SQL statement
    904,    # invalid identifier
    923,    # FROM keyword not found (구문)
    2003,   # invalid USERENV parameter
    6550,   # PL/SQL 컴파일 오류
    30491,  # missing ORDER BY
}
DENIED = {  # 권한이 없다 → 권한 축에서는 사실, 기능 축에서는 UNDETERMINED
    1031,   # insufficient privileges
    1039,   # insufficient privileges on underlying objects
    942,    # table or view does not exist — 권한 부재로도 나타난다(구분 불가)
}
TRANSIENT = {  # 다시 재면 달라질 수 있다 → **절대 NONE 이 아니다**
    1013,   # user requested cancel
    60,     # deadlock
    3113, 3114, 3135,   # 연결 단절
    12170, 12541, 12545, 12571,   # 네트워크·리스너
    1555,   # snapshot too old — undo 부족. 기능 부재가 아니다
    28,     # session killed
}
# ORA-03172 는 특별하다. "실패"가 아니라 **강제가 걸렸다는 양성 증거**다.
STANDBY_DELAY_EXCEEDED = 3172


def _ora(p: dict | None) -> int | None:
    """probe 레코드에서 ORA 코드를 절대값으로 뽑는다. 없으면 None."""
    if not p:
        return None
    v = p.get("ora")
    if isinstance(v, int):
        return abs(v)
    m = re.search(r"ORA-(\d+)", str(p.get("msg") or p.get("error") or ""))
    return int(m.group(1)) if m else None


def classify(p: dict | None) -> str:
    """probe 하나의 결과를 다섯으로 나눈다.

    OK / EMPTY / UNSUPPORTED / DENIED / TRANSIENT / UNKNOWN
    """
    if p is None:
        return "ABSENT"
    if p.get("query_ok") is True:
        if p.get("row_present") is False:
            return "EMPTY"
        if p.get("value") is None and "value" in p:
            return "EMPTY"
        return "OK"
    code = _ora(p)
    if code is None:
        return "UNKNOWN"
    if code in TRANSIENT:
        return "TRANSIENT"
    if code in UNSUPPORTED:
        return "UNSUPPORTED"
    if code in DENIED:
        return "DENIED"
    return "UNKNOWN"


def interpretable(p: dict | None) -> bool:
    """값을 해석할 수 있는가. `query_ok` 만으로는 부족하다."""
    return classify(p) == "OK" and p.get("value_interpretable") is not False


# ─────────────────────────────────────────────────────────────────────
# 2. 축 정의표
# ─────────────────────────────────────────────────────────────────────
# scope 는 g0-0-evidence.schema.json 의 enum 과 같다.
# kind: ATOM(probe 에서 직접) | COMPOSITE(다른 축에서 합성)
AXIS_SPEC: dict[str, dict] = {
    # ── DB / ACCOUNT 단위 ────────────────────────────────────────────
    "snapshot_anchor": {
        "kind": "ATOM", "scope": "ACCOUNT",
        "values": ["SCN", "TIMESTAMP", "NONE", "UNDETERMINED"],
        "floor": "NONE",
        "what": "고정 시점을 지정해 읽을 원점을 얻을 수 있는가. **얻은 원점으로 대상 object 를 "
                "실제 조회할 수 있는지는 snapshot_object_coverage 가 따로 답한다.**",
    },
    "lag_observation": {
        "kind": "ATOM", "scope": "DB",
        "values": ["DG_STATS", "NONE", "UNDETERMINED"], "floor": "NONE",
        "what": "apply lag 값을 **읽을 수 있는가**. 뷰에 접근된다는 것과 값을 해석할 수 있다는 "
                "것은 다르다 — 후자를 요구한다.",
    },
    "lag_admission": {
        "kind": "ATOM", "scope": "CONNECTION",
        "values": ["MAX_DELAY_ENFORCED", "NONE", "UNDETERMINED"], "floor": "NONE",
        "what": "임계 초과 시 서버가 쿼리를 **거절하는가**. `ALTER SESSION` 이 수락되는 것만으로는 "
                "부족하다 — ORA-03172 양성 대조를 요구한다.",
    },
    "hash_function": {
        "kind": "ATOM", "scope": "DB",
        "values": ["SHA256", "NONE", "UNDETERMINED"], "floor": "NONE",
        "what": "SHA-256 함수 한 개가 표준 시험 벡터와 일치하는가. **행 내용 비교 능력이 아니다** — "
                "그쪽은 canonical_row_compare 다.",
    },
    "sql_dialect": {
        "kind": "ATOM", "scope": "DB",
        "values": ["12C_PLUS", "11G", "UNDETERMINED"], "floor": "11G",
        "what": "`FETCH FIRST` 가용 여부로 본 방언 하한.",
    },
    "db_charset": {
        "kind": "ATOM", "scope": "DB",
        "values": None,   # 관측값 그대로
        "floor": "UNDETERMINED",
        "what": "`NLS_CHARACTERSET` 관측값 그 자체. **등급이 아니다.** 두 원천이 비교 가능한지는 "
                "cross_source_comparable 이 따로 답한다(P1-05).",
    },
    # ── TABLE / COLUMN 단위 ──────────────────────────────────────────
    "snapshot_object_coverage": {
        "kind": "ATOM", "scope": "TABLE",
        "values": ["ALL", "PARTIAL", "NONE", "UNDETERMINED"], "floor": "NONE",
        "what": "extract object-set **전체**에 그 원점으로 조회가 되는가. G0-0 은 대상 테이블 "
                "하나만 건드리므로 여기서 `ALL` 이 나올 수 없다 — 최대 `PARTIAL` 이다.",
    },
    "row_change_scn": {
        "kind": "ATOM", "scope": "TABLE",
        "values": ["ROW_LEVEL", "BLOCK_LEVEL", "NONE", "UNDETERMINED"], "floor": "NONE",
        "what": "`ORA_ROWSCN` 의 입도. `ROWDEPENDENCIES` 는 테이블 생성 시 결정되고 사후 변경 불가다.",
    },
    "watermark_commit_bound": {
        "kind": "ATOM", "scope": "COLUMN",
        "values": ["ENFORCED", "OBSERVED", "NONE", "UNDETERMINED"], "floor": "NONE",
        "what": "**복원된 축이다(P0-05).** `commit_time − watermark_value` 의 상한을 무엇으로 "
                "보증하는가. apply lag 와 **독립**이다 — lag 가 0 이어도 오래된 watermark 를 든 "
                "트랜잭션이 늦게 commit 하면 반개구간 fence 밖으로 빠진다. "
                "`ENFORCED` 는 서버 장치(DBA 등록)를 요구하므로 Profile U 에서는 도달 불가이며, "
                "그것이 이 축을 **지울** 이유가 아니라 값이 `OBSERVED` 로 내려갈 이유다.",
    },
    "wm_successor": {
        "kind": "ATOM", "scope": "COLUMN",
        "values": None,   # 구조화 값. detail 참조
        "floor": "UNDEFINED",
        "what": "watermark 컬럼의 successor(M) 를 정의할 수 있는가. **등급 enum 이 아니라 "
                "datatype·exact scale·min step 이다**(P0-01). 이전 판은 TIMESTAMP(2) 를 US 로, "
                "NUMBER(10,2) 를 SEC 으로 보내 successor 계산을 틀어뜨렸다.",
    },
    # ── COMPOSITE ────────────────────────────────────────────────────
    "snapshot_scope": {
        "kind": "COMPOSITE", "scope": "RUNTIME",
        "values": ["JOB", "CONNECTION", "STATEMENT", "NONE", "UNDETERMINED"], "floor": "STATEMENT",
        "what": "한 Job 의 모든 물리 connection 이 **같은** snapshot 을 보는가. "
                "`JOB` 은 공통 anchor ∧ 전체 object coverage ∧ 모든 data connection 적용이 "
                "모두 참일 때만 나온다.",
    },
    "canonical_row_compare": {
        "kind": "COMPOSITE", "scope": "RUNTIME",
        "values": ["VECTORS_PROVEN", "PARTIAL", "NONE", "UNDETERMINED"], "floor": "NONE",
        "what": "행 내용을 엔진 간에 같은 바이트로 비교할 수 있는가. "
                "**`VECTORS_PROVEN` 은 G0-3 V-01~V-16 통과 전에 절대 나오지 않는다** — "
                "G0-0 에서는 구조적으로 도달 불가다.",
    },
    "cross_source_comparable": {
        "kind": "COMPOSITE", "scope": "RUNTIME",
        "values": ["YES", "NO", "UNDETERMINED"], "floor": "NO",
        "what": "두 원천의 행을 비교할 수 있는가. **G0-0 은 원천 하나만 잰다** — "
                "한 원천의 charset 이 AL32UTF8 이라는 사실만으로 주장할 수 없다(P1-05).",
    },
}

TABLE_SCOPES = {"TABLE", "COLUMN"}


# ─────────────────────────────────────────────────────────────────────
# 3. 파생 — 축마다 하나의 함수
# ─────────────────────────────────────────────────────────────────────
class _Ctx:
    """읽은 probe 를 자동으로 기록한다. `inputs` 가 의도가 아니라 사실이 되게 하는 장치."""

    def __init__(self, P: dict[str, dict]):
        self.P = P
        self.read: list[dict] = []
        self.skipped: list[str] = []

    def get(self, pid: str) -> dict | None:
        p = self.P.get(pid)
        obs = None
        if p is not None:
            v = p.get("value")
            obs = v if isinstance(v, (str, bool, int, float)) or v is None else str(v)
        self.read.append({"probe": pid, "used": True,
                          **({"observed": obs} if obs is not None else {})})
        return p

    def note_unused(self, *pids: str) -> None:
        self.skipped.extend(pids)


def _axis(value, ctx: _Ctx, *, scope: str, binding=None, note: str | None = None,
          detail: dict | None = None) -> dict:
    d = {
        "value": str(value),
        "scope": scope if str(value) != "UNDETERMINED" else "UNDETERMINED",
        "binding": binding if str(value) != "UNDETERMINED" else None,
        "measured_at": None,      # 집계기가 child manifest 에서 채운다
        "stale": False,
        "effective_value": str(value),
        "inputs": ctx.read or [{"probe": "<입력 없음>", "used": False}],
    }
    if ctx.skipped:
        d["considered_but_not_used"] = list(dict.fromkeys(ctx.skipped))
    if note:
        d["note"] = note
    if detail:
        d["note"] = (d.get("note", "") + " " if d.get("note") else "") + \
                    "detail=" + repr(detail)
    return d


def d_snapshot_anchor(P) -> dict:
    c = _Ctx(P)
    scn = c.get("dbms_flashback.get_scn")
    ts = c.get("as_of_timestamp.target")
    # **접근 가능성 probe 를 값 probe 로 쓰지 않는다.** view.v_database 는 COUNT(*) 다.
    c.note_unused("view.v_database")

    cs, ct = classify(scn), classify(ts)
    if cs == "OK" and interpretable(scn):
        return _axis("SCN", c, scope="ACCOUNT",
                     note="SCN 원점을 실제로 읽었다. 그 SCN 으로 대상 object 를 조회할 수 있는지는 "
                          "snapshot_object_coverage 가 따로 답한다.")
    if ct == "OK":
        return _axis("TIMESTAMP", c, scope="ACCOUNT",
                     note="AS OF TIMESTAMP 만 성립한다. **READ_ONLY_TXN 이 아니다** — 그것은 "
                          "connection-local transaction snapshot 이고 이것은 SQL literal 이라 "
                          "connection 과 무관하다(이전 판이 둘을 섞었다). Oracle 은 지정 시각을 "
                          "3초 단위로 매핑하며 그 방향은 **이전**이다(미래가 아니다).")
    if "TRANSIENT" in (cs, ct) or "UNKNOWN" in (cs, ct) or "ABSENT" in (cs, ct):
        return _axis("UNDETERMINED", c, scope="ACCOUNT",
                     note=f"판정 불가 — get_scn={cs}, as_of_timestamp={ct}. "
                          f"transient·미실행은 기능 부재의 증거가 아니다.")
    return _axis("NONE", c, scope="ACCOUNT",
                 note=f"두 원점 모두 사용 불가 (get_scn={cs}, as_of_timestamp={ct}).")


def d_snapshot_object_coverage(P, binding) -> dict:
    c = _Ctx(P)
    ts = c.get("as_of_timestamp.target")
    ct = classify(ts)
    if ct == "OK":
        return _axis("PARTIAL", c, scope="TABLE", binding=binding,
                     note="**ALL 이 나올 수 없다.** G0-0 은 대상 테이블 하나만 그 원점으로 읽는다. "
                          "extract object-set 전체에 대한 판정은 object manifest 를 받은 뒤 "
                          "G0-1 이후에서 한다.")
    if ct in ("TRANSIENT", "UNKNOWN", "ABSENT"):
        return _axis("UNDETERMINED", c, scope="TABLE", binding=binding,
                     note=f"as_of_timestamp.target={ct}")
    return _axis("NONE", c, scope="TABLE", binding=binding,
                 note=f"대상 테이블을 그 원점으로 읽지 못했다 ({ct}).")


def d_lag_observation(P) -> dict:
    c = _Ctx(P)
    dg = c.get("v$dataguard_stats")
    # COUNT(*) probe 는 **접근 가능성**만 말한다. 값 판정에 쓰지 않는다.
    c.note_unused("view.v_dataguard_stats")
    k = classify(dg)
    if k == "OK" and interpretable(dg):
        return _axis("DG_STATS", c, scope="DB",
                     note="lag 값을 읽었다. 접근 가능성(COUNT(*))이 아니라 값 해석을 근거로 한다.")
    if k in ("TRANSIENT", "UNKNOWN", "ABSENT"):
        return _axis("UNDETERMINED", c, scope="DB", note=f"v$dataguard_stats={k}")
    if k == "EMPTY":
        return _axis("UNDETERMINED", c, scope="DB",
                     note="뷰는 읽혔으나 행이 없다. PRIMARY 에서는 정상이며 '기능 없음'이 아니다.")
    return _axis("NONE", c, scope="DB", note=f"lag 값을 읽지 못했다 ({k}).")


def d_lag_admission(P) -> dict:
    c = _Ctx(P)
    setd = c.get("alter.STANDBY_MAX_DATA_DELAY.D")
    zero = c.get("alter.STANDBY_MAX_DATA_DELAY.zero")
    touch = c.get("max_delay_zero.touch_target")

    k_set = classify(setd)
    # **양성 대조**: delay=0 으로 두고 대상을 건드렸을 때 ORA-03172 가 나야 강제가 실증된다.
    positive = _ora(touch) == STANDBY_DELAY_EXCEEDED
    if positive:
        return _axis("MAX_DELAY_ENFORCED", c, scope="CONNECTION",
                     note="ORA-03172 양성 대조를 확보했다 — 임계 초과 시 서버가 실제로 거절한다.")
    if k_set == "OK":
        return _axis("UNDETERMINED", c, scope="CONNECTION",
                     note="ALTER SESSION 은 수락됐으나 **ORA-03172 양성 대조가 없다.** "
                          "오류 부재는 강제의 증거가 아니다 — lag 가 큰 시간대에 최소 1회 확보하라. "
                          f"(max_delay_zero.touch_target={classify(touch)}, ORA={_ora(touch)})")
    if k_set in ("TRANSIENT", "UNKNOWN", "ABSENT"):
        return _axis("UNDETERMINED", c, scope="CONNECTION",
                     note=f"alter.STANDBY_MAX_DATA_DELAY.D={k_set}")
    return _axis("NONE", c, scope="CONNECTION",
                 note=f"세션 파라미터를 걸 수 없다 ({k_set}) — 이 판본·권한에서 admission 강제가 없다.")


def d_hash_function(P) -> dict:
    c = _Ctx(P)
    h = c.get("feat.standard_hash_sha256")
    c.note_unused("feat.ora_hash")
    k = classify(h)
    if k == "OK" and h.get("value_interpretable") is True:
        return _axis("SHA256", c, scope="DB",
                     note="표준 시험 벡터와 값이 일치한다. **이것은 함수 하나의 가용성이며 "
                          "행 내용 비교 능력이 아니다**(canonical_row_compare 참조).")
    if k in ("TRANSIENT", "UNKNOWN", "ABSENT"):
        return _axis("UNDETERMINED", c, scope="DB", note=f"feat.standard_hash_sha256={k}")
    return _axis("NONE", c, scope="DB",
                 note=f"SHA-256 을 쓸 수 없다 ({k}). ORA_HASH 는 32비트라 대조용 해시가 못 된다 — "
                      f"Reconciliation 이 건수+PK 로 강등된다.")


def d_sql_dialect(P) -> dict:
    c = _Ctx(P)
    ff = c.get("feat.fetch_first")
    k = classify(ff)
    if k == "OK":
        return _axis("12C_PLUS", c, scope="DB")
    if k == "UNSUPPORTED":
        return _axis("11G", c, scope="DB", note="FETCH FIRST 구문 미지원(ORA-00900/00923 계열).")
    return _axis("UNDETERMINED", c, scope="DB",
                 note=f"feat.fetch_first={k} — 구문 미지원과 구분되지 않는다. "
                      f"연결 오류·권한 오류를 판본 사실로 바꾸지 않는다(P1-03).")


def d_db_charset(P) -> dict:
    c = _Ctx(P)
    cs = c.get("nls.characterset")
    c.note_unused("nls.comp", "nls.sort", "nls.nchar_characterset")
    if classify(cs) == "OK" and cs.get("value"):
        return _axis(str(cs["value"]).upper(), c, scope="DB",
                     note="**관측값 그대로다. 등급이 아니다.** 두 원천의 비교 가능성은 "
                          "cross_source_comparable 이 따로 답한다.")
    return _axis("UNDETERMINED", c, scope="DB", note=f"nls.characterset={classify(cs)}")


def d_row_change_scn(P, binding) -> dict:
    c = _Ctx(P)
    dep = c.get("feat.rowdependencies_target")
    rs = c.get("feat.ora_rowscn_target")
    k_rs, k_dep = classify(rs), classify(dep)
    if k_rs in ("TRANSIENT", "UNKNOWN", "ABSENT") or k_dep in ("TRANSIENT", "UNKNOWN", "ABSENT"):
        return _axis("UNDETERMINED", c, scope="TABLE", binding=binding,
                     note=f"ora_rowscn={k_rs}, rowdependencies={k_dep}")
    if k_rs != "OK":
        return _axis("NONE", c, scope="TABLE", binding=binding,
                     note=f"ORA_ROWSCN 을 읽지 못했다 ({k_rs}).")
    if str(dep.get("value") or "").upper() == "ENABLED":
        return _axis("ROW_LEVEL", c, scope="TABLE", binding=binding)
    return _axis("BLOCK_LEVEL", c, scope="TABLE", binding=binding,
                 note="ROWDEPENDENCIES 가 ENABLED 가 아니다 — 블록 단위 SCN 은 상한만 보장한다. "
                      "테이블 생성 시 결정되므로 사후 변경 불가다.")


def d_watermark_commit_bound(P, binding) -> dict:
    """**복원된 축**(P0-05). apply lag 와 독립이다."""
    c = _Ctx(P)
    # ENFORCED 의 유일한 근거였던 SYNC_COMMIT_GUARD 는 DBA 장치 등록이라 Profile U 에서 도달 불가다.
    # 그러나 그것은 값이 내려갈 이유이지 축을 지울 이유가 아니다.
    t2s = c.get("timestamp_to_scn")
    s2t = c.get("scn_to_timestamp.roundtrip")
    rs = c.get("feat.ora_rowscn_target")
    ks = {classify(x) for x in (t2s, s2t, rs)}
    if ks & {"TRANSIENT", "UNKNOWN", "ABSENT"}:
        return _axis("UNDETERMINED", c, scope="COLUMN", binding=binding,
                     note="commit 시각과 watermark 값의 차를 관측할 수단이 확인되지 않았다. "
                          "**이 축이 UNDETERMINED 인 것은 lag_observation 과 무관하다** — "
                          "apply lag 가 0 이어도 이 위험은 남는다.")
    if classify(rs) == "OK" and classify(s2t) == "OK":
        return _axis("OBSERVED", c, scope="COLUMN", binding=binding,
                     note="ORA_ROWSCN → SCN_TO_TIMESTAMP 로 commit 시각을 **관측**할 수 있다. "
                          "ENFORCED 가 아니다 — 서버가 상한을 강제하는 장치는 없다(Profile U 에서 "
                          "도달 불가). 표본 기반 OBSERVED 값은 preventive guarantee 를 만들지 "
                          "않는다. overlap 은 BEST_EFFORT tuning 이다.")
    return _axis("NONE", c, scope="COLUMN", binding=binding,
                 note="commit 시각을 관측할 수단이 없다. 반개구간 fence 의 tail 누락을 "
                      "탐지할 방법이 없으므로 overlap 재적재에만 의존해야 한다.")


# TIMESTAMP(n) 의 최소 단위. 이전 판은 dict.get(scale, "US") 라 2·5·8 이 전부 US 가 됐다.
_TS_STEP = {0: "1s", 1: "100ms", 2: "10ms", 3: "1ms", 4: "100us", 5: "10us",
            6: "1us", 7: "100ns", 8: "10ns", 9: "1ns"}


def d_wm_successor(P, binding) -> dict:
    """등급 enum 이 아니라 datatype·exact scale·min step 을 낸다(P0-01)."""
    c = _Ctx(P)
    t = c.get("wm_column.type_facts")
    # 구문 지원 확인용 probe 는 successor 판정에 쓰지 않는다 — 읽었지만 안 썼다고 남긴다.
    c.note_unused("feat.interval_ns_successor", "feat.timestamp9_precision")

    k = classify(t)
    if k != "OK":
        return _axis("UNDETERMINED", c, scope="COLUMN", binding=binding,
                     note=f"wm_column.type_facts={k}")
    raw = str(t.get("value") or "")
    dt = raw.split("|")[0].upper()
    m = re.search(r"scale=(-?\d+)", raw)
    scale = int(m.group(1)) if m and m.group(1) not in ("-",) else None

    if dt.startswith("TIMESTAMP"):
        if scale is None:
            return _axis("UNDETERMINED", c, scope="COLUMN", binding=binding,
                         note=f"data_type={dt} 인데 scale 을 읽지 못했다. **기본값 6 을 가정하지 "
                              f"않는다** — 가정하면 successor 가 틀어진다.")
        step = _TS_STEP.get(scale)
        if step is None:
            return _axis("UNDETERMINED", c, scope="COLUMN", binding=binding,
                         note=f"TIMESTAMP({scale}) — 0..9 밖의 scale 이다. 해석하지 않는다.")
        return _axis(f"TIMESTAMP({scale})", c, scope="COLUMN", binding=binding,
                     detail={"datatype": "TIMESTAMP", "scale": scale, "min_step": step,
                             "round_trip_verified": False},
                     note=f"최소 단위 {step}. **round-trip(Oracle→OJDBC→Spark→Control) 은 아직 "
                          f"검증되지 않았다** — 그 전에는 반개구간 seal 의 successor 로 쓰지 마라.")
    if dt == "DATE":
        return _axis("DATE", c, scope="COLUMN", binding=binding,
                     detail={"datatype": "DATE", "scale": 0, "min_step": "1s",
                             "round_trip_verified": False},
                     note="DATE 의 최소 단위는 1초다.")
    if dt == "NUMBER":
        if scale is None:
            return _axis("UNDETERMINED", c, scope="COLUMN", binding=binding,
                         note="정밀도 미지정 NUMBER — successor 가 정의되지 않는다.")
        return _axis(f"NUMBER(scale={scale})", c, scope="COLUMN", binding=binding,
                     detail={"datatype": "NUMBER", "scale": scale,
                             "min_step": f"1e-{scale}" if scale > 0 else f"1e{-scale}",
                             "is_time": False, "round_trip_verified": False},
                     note="**시간 단위가 아니다.** 이전 판은 이것을 SEC 으로 보냈다. NUMBER "
                          "watermark 는 시퀀스·논리 버전일 수 있으므로 시간 축과 섞지 마라.")
    return _axis("UNDEFINED", c, scope="COLUMN", binding=binding,
                 detail={"datatype": dt, "scale": scale},
                 note=f"data_type={dt} scale={scale} — 고정 granularity 가 없어 successor(M)==M "
                      f"이 된다(CE01). 반개구간 seal 대상에서 제외하고 overlap 재적재로만 처리한다.")


# ── COMPOSITE ────────────────────────────────────────────────────────
def _undet(reason: str, deps: list[str]) -> dict:
    return {"value": "UNDETERMINED", "scope": "UNDETERMINED", "binding": None,
            "measured_at": None, "stale": False, "effective_value": "UNDETERMINED",
            "inputs": [{"probe": f"<axis:{d}>", "used": True} for d in deps] or
                      [{"probe": "<입력 없음>", "used": False}],
            "note": reason}


def c_snapshot_scope(axes: dict) -> dict:
    deps = ["snapshot_anchor", "snapshot_object_coverage"]
    a = axes["snapshot_anchor"]["value"]
    cov = axes["snapshot_object_coverage"]["value"]
    if "UNDETERMINED" in (a, cov):
        return _undet("입력 축이 미확정이다. 합성 축은 입력 하나라도 미확정이면 미확정이다.", deps)
    if a == "NONE":
        return {**_undet("", deps), "value": "STATEMENT", "scope": "RUNTIME",
                "effective_value": "STATEMENT",
                "note": "고정 원점이 없다 — 각 SELECT 가 자기 시점을 본다. Full 병렬 읽기는 "
                        "비원자 snapshot 이며 그 사실을 계약에 공시해야 한다."}
    if cov != "ALL":
        return {**_undet("", deps), "value": "CONNECTION", "scope": "RUNTIME",
                "effective_value": "CONNECTION",
                "note": f"원점은 있으나 object coverage 가 {cov} 다. **JOB 으로 올리지 않는다** — "
                        f"JOB 은 공통 anchor ∧ 전체 object coverage ∧ 모든 data connection 적용이 "
                        f"모두 참일 때만 나온다. G0-0 에서는 구조적으로 도달 불가다."}
    return {**_undet("", deps), "value": "JOB", "scope": "RUNTIME", "effective_value": "JOB",
            "note": "세 조건이 모두 참이다."}


def c_canonical_row_compare(axes: dict) -> dict:
    """**G0-0 에서는 VECTORS_PROVEN 이 구조적으로 불가능하다.**"""
    deps = ["hash_function"]
    h = axes["hash_function"]["value"]
    if h == "UNDETERMINED":
        return _undet("hash_function 이 미확정이다.", deps)
    if h == "NONE":
        return {**_undet("", deps), "value": "NONE", "scope": "RUNTIME", "effective_value": "NONE",
                "note": "해시 함수가 없다."}
    return {**_undet("", deps), "value": "PARTIAL", "scope": "RUNTIME",
            "effective_value": "PARTIAL",
            "note": "함수는 있으나 **행 내용 비교는 증명되지 않았다.** NULL framing·NFC·"
                    "NUMBER/TIMESTAMP 직렬화·LOB 경로가 남아 있다. `VECTORS_PROVEN` 은 "
                    "G0-3 V-01~V-16 을 통과한 뒤에만 나오며 G0-0 에서는 도달 불가다."}


def c_cross_source_comparable(axes: dict) -> dict:
    deps = ["db_charset"]
    return {**_undet("", deps), "value": "UNDETERMINED", "scope": "UNDETERMINED",
            "note": "**G0-0 은 원천을 하나만 잰다.** 한 DB 의 charset 이 AL32UTF8 이라는 사실로 "
                    "두 원천의 비교 가능성을 주장할 수 없다(P1-05). NCHAR charset·NLS·"
                    "normalization·매핑 digest 를 합성 입력으로 받아야 하며 그 값들은 "
                    "원천이 둘 이상 측정된 뒤에 나온다."}


# ─────────────────────────────────────────────────────────────────────
# 4. 진입점
# ─────────────────────────────────────────────────────────────────────
def derive_axes(P: dict[str, dict], *, binding: dict | None = None,
                measured_at: str | None = None) -> dict:
    """probe 딕셔너리 → 축 딕셔너리. **순수 함수다.**

    binding: 테이블 단위 축이 묶일 대상. `{"db_identity":…, "owner":…, "object":…}`.
             None 이면 테이블 단위 축은 전부 `UNDETERMINED` 로 내린다 —
             **묶이지 않은 확정값을 만들지 않는다**(P0-03).
    """
    tb = binding if (binding and binding.get("db_identity")) else None
    unbound_note = ("대상 식별자(db_identity·owner·object)가 없다. 테이블 단위 축은 묶이지 않으면 "
                    "다른 테이블에 잘못 적용될 수 있으므로 확정값을 내지 않는다(P0-03).")

    axes: dict[str, dict] = {
        "snapshot_anchor": d_snapshot_anchor(P),
        "lag_observation": d_lag_observation(P),
        "lag_admission": d_lag_admission(P),
        "hash_function": d_hash_function(P),
        "sql_dialect": d_sql_dialect(P),
        "db_charset": d_db_charset(P),
    }
    if tb:
        axes["snapshot_object_coverage"] = d_snapshot_object_coverage(P, tb)
        axes["row_change_scn"] = d_row_change_scn(P, tb)
        axes["watermark_commit_bound"] = d_watermark_commit_bound(P, tb)
        axes["wm_successor"] = d_wm_successor(P, tb)
    else:
        for name in ("snapshot_object_coverage", "row_change_scn",
                     "watermark_commit_bound", "wm_successor"):
            axes[name] = _undet(unbound_note, [])

    axes["snapshot_scope"] = c_snapshot_scope(axes)
    axes["canonical_row_compare"] = c_canonical_row_compare(axes)
    axes["cross_source_comparable"] = c_cross_source_comparable(axes)

    if measured_at:
        for v in axes.values():
            if v["value"] != "UNDETERMINED":
                v["measured_at"] = measured_at
    return axes


AXIS_NAMES = list(AXIS_SPEC.keys())
