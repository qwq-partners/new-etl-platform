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
   `now` 조차 인자로 받는다 — 시각을 읽으면 시험이 흔들린다.
2. **읽은 것만 `inputs` 에.** 의도한 목록이 아니다. 읽었는데 판정에 안 쓴 것은
   `considered_but_not_used` 로 분리한다(P1-06).
3. **`NONE` 과 `UNDETERMINED` 를 구분한다.** transient 실패는 기능 부재가 아니다(P1-03).
4. **승격에는 양성 대조를 요구한다.** 오류 부재는 증거가 아니다.
5. **composition 은 별도 단계.** 원자 축을 먼저 내고, 그것들로 합성 축을 만든다.
   합성 축은 입력이 하나라도 `UNDETERMINED` 면 `UNDETERMINED` 다.

**2026-08-30 — 8차 M3-2·M3-3.** 규칙 둘이 늘었다.

6. **`OK` 가 아닌 것을 한 덩어리로 다루지 않는다**(M3-2). `NONE` 을 지지하는 분류는
   `UNSUPPORTED` 하나뿐이며(`NEGATIVE_EVIDENCE`), 권한 부족·대상 부재·프로브 결함·
   문맥 의존 코드는 전부 `UNDETERMINED` 로 간다. 뜻이 probe 마다 다른 코드는
   **그 probe 가 `PROBE_SPEC[...]['absent_codes']` 로 스스로 선언한다.** 그리고 행·값·문법·
   양성 대조를 `query_ok` 하나에 뭉뚱그리지 않는다(typed predicate).
7. **`value` 와 `effective_value` 는 다르다**(M3-3). `value` 는 관측 사실이고 감사·표시
   전용이며, `effective_value` 는 validator·publish 가 읽는 유일한 값이다.
   floor 는 파생의 마지막 단계(`apply_floors`)이고 **값을 내리기만 한다**.
"""
from __future__ import annotations

import re

# ─────────────────────────────────────────────────────────────────────
# 1. 실패 taxonomy (P1-03 · 8차 M3-2)
# ─────────────────────────────────────────────────────────────────────
# **transient 를 기능 부재로 강등하지 않는 것이 이 표의 첫 목적이고, 권한 부재·대상 오류·
# 프로브 결함을 기능 부재로 강등하지 않는 것이 두 번째 목적이다**(8차 §7 — 현 `ABSENCE_ORA`
# 는 unsupported·permission denied·wrong target·probe bug 를 섞으며 그래서 ORA-01031/00942
# 가 `hash_function=NONE`·`sql_dialect=11G` 가 될 수 있다).
#
# 여기 전역 집합에는 **어느 probe 에서 나와도 뜻이 같은 코드만** 넣는다. 뜻이 probe 마다
# 다른 코드(대표적으로 ORA-00904 `invalid identifier` — 함수가 없다는 뜻일 수도, 컬럼 이름을
# 틀린 프로브 결함일 수도 있다)는 전역에서 AMBIGUOUS 로 두고, 그것이 부재의 증거가 되는
# probe 에서만 `PROBE_SPEC[...]['absent_codes']` 로 **그 probe 가 스스로 선언한다**.
UNSUPPORTED = {  # 그 구문·기능이 이 판본에 없다 → NONE 의 근거가 될 수 있는 유일한 범주
    900,    # invalid SQL statement
    923,    # FROM keyword not found (구문)
    933,    # SQL command not properly ended — 11g 에서 FETCH FIRST 가 내는 코드다
    2003,   # invalid USERENV parameter
    30491,  # missing ORDER BY
}
DENIED = {  # 권한이 없다 → 권한 축에서는 사실, **기능 축에서는 UNDETERMINED**
    1031,   # insufficient privileges
    1039,   # insufficient privileges on underlying objects
}
WRONG_TARGET = {  # 대상이 없다 → 권한 부재와 구분 불가. **기능 부재의 증거가 아니다**
    942,    # table or view does not exist
    4043,   # object does not exist
    1435,   # user does not exist
    1918,   # user does not exist (ALTER 계열)
}
PROBE_BUG = {  # 프로브 SQL 자체가 틀렸다 → 원천의 성질이 아니라 **하네스의 결함**이다
    909,    # invalid number of arguments
    932,    # inconsistent datatypes
    936,    # missing expression
    1722,   # invalid number
    1789, 1790,   # 열 개수·형 불일치
    1858,   # non-numeric character
    6502,   # numeric or value error
}
AMBIGUOUS = {  # 뜻이 probe 마다 다르다 → 전역에서는 판정하지 않는다
    904,    # invalid identifier — 함수 부재일 수도, 컬럼 오타일 수도
    6550,   # PL/SQL 컴파일 오류. PLS-00201 은 **패키지 부재와 EXECUTE 권한 부재를 구분 못 한다**
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

# NONE 을 지지할 수 있는 유일한 범주. 나머지는 전부 UNDETERMINED 로 간다.
# 이 상수가 있는 이유는 각 파생 함수가 `k != "OK"` 로 떨어뜨리는 것을 막기 위해서다 —
# 그 형태가 DENIED 를 NONE 으로 만들던 경로였다.
NEGATIVE_EVIDENCE = frozenset({"UNSUPPORTED"})


# ─────────────────────────────────────────────────────────────────────
# 1b. probe 별 typed predicate (8차 M3-2 · 7차 P0-01)
# ─────────────────────────────────────────────────────────────────────
# 7차 P0-01 이 요구한 것: "probe별 typed predicate 를 둔다 — query 성공, required row,
# interpretable value, value grammar, positive control 을 각각 명시한다."
#
# `query_ok` 하나로 뭉뚱그리면 "질의는 됐는데 행이 없다", "행은 있는데 값이 NULL 이다",
# "값은 있는데 우리가 읽을 수 있는 형태가 아니다" 가 전부 OK 가 된다. 셋은 다른 사실이고
# 축 판정에서 서로 다르게 다뤄야 한다.
#
#   requires_row   행이 있어야 값이 성립하는가
#   requires_value `value` 가 비어 있지 않아야 하는가
#   grammar        값이 이 형태여야 해석한다. 어긋나면 GRAMMAR — **NONE 이 아니다**
#   absent_codes   이 probe 에서만 '기능 부재' 를 뜻하는 ORA 코드
#   positive_ora   승격에 요구되는 양성 대조 코드(참고용 — 판정은 파생 함수가 한다)
PROBE_SPEC: dict[str, dict] = {
    "dbms_flashback.get_scn": {
        "requires_row": True, "requires_value": True, "grammar": r"^\d+$",
        # 6550(PLS-00201)은 패키지 부재와 EXECUTE 권한 부재를 구분하지 못하므로
        # 부재 코드로 선언하지 않는다. 구분 못 하는 것을 부재로 적지 않는다.
        "absent_codes": frozenset(),
    },
    "as_of_timestamp.target": {
        # AS OF TIMESTAMP 는 11g 이후 전 판본에 있다. 이 probe 의 실패는 대개 권한·undo
        # 사정이지 구문 부재가 아니다 — 그래서 probe 고유 부재 코드가 없다.
        "requires_row": True, "absent_codes": frozenset(),
    },
    "v$dataguard_stats": {"requires_row": True, "requires_value": True},
    "alter.STANDBY_MAX_DATA_DELAY.D": {"absent_codes": frozenset({2248})},
    "alter.STANDBY_MAX_DATA_DELAY.zero": {"absent_codes": frozenset({2248})},
    "max_delay_zero.touch_target": {"positive_ora": STANDBY_DELAY_EXCEEDED},
    # STANDARD_HASH 가 없는 판본은 함수 이름을 식별자로 보고 ORA-00904 를 낸다.
    # **이 probe 에서만** 904 가 부재의 증거다.
    "feat.standard_hash_sha256": {"requires_value": True,
                                  "absent_codes": frozenset({904})},
    "feat.fetch_first": {"absent_codes": frozenset({900, 923, 933})},
    "nls.characterset": {"requires_row": True, "requires_value": True,
                         "grammar": r"^[A-Z0-9_]+$"},
    "nls.nchar_characterset": {"requires_value": True, "grammar": r"^[A-Z0-9_]+$"},
    "feat.rowdependencies_target": {"requires_value": True,
                                    "grammar": r"^(ENABLED|DISABLED)$"},
    "feat.ora_rowscn_target": {"requires_row": True},
    "timestamp_to_scn": {},
    "scn_to_timestamp.roundtrip": {},
    "wm_column.type_facts": {
        "requires_value": True,
        # `<DATA_TYPE>[(n[,m])]|scale=<n|->` 형태. 이 형태가 아니면 successor 를 세우지
        # 않는다 — 파싱에 실패한 값을 기본값으로 메우던 것이 P0-01 이었다.
        "grammar": r"^[A-Z][A-Z0-9_ ]*(\(\d+(\s*,\s*-?\d+)?\))?\|",
    },
    "v$parameter.max_string": {"requires_value": True},
}


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
    """probe 하나의 결과를 typed predicate + SQLCODE taxonomy 로 분류한다.

    반환값은 다음 중 하나다. **`OK` 가 아닌 것을 한 덩어리로 다루면 안 된다** —
    그 중 `NONE` 을 지지하는 것은 `UNSUPPORTED` 뿐이다(`NEGATIVE_EVIDENCE`).

      OK                 질의 성공 ∧ 필요한 행·값이 있고 문법도 맞다
      EMPTY              질의는 됐으나 행이 없거나 값이 비었다
      NOT_INTERPRETABLE  값은 있으나 probe 가 스스로 "해석 못 했다" 고 적었다
      GRAMMAR            값이 기대 형태가 아니다 — 원천의 성질이 아니라 파싱 실패다
      UNSUPPORTED        그 구문·기능이 없다(전역 코드 또는 이 probe 의 absent_codes)
      DENIED             권한 부족
      WRONG_TARGET       대상 객체·사용자 부재(권한 부재와 구분 불가)
      PROBE_BUG          프로브 SQL 자체의 결함
      AMBIGUOUS          뜻이 문맥에 따라 다른 코드
      TRANSIENT          다시 재면 달라질 수 있다
      UNKNOWN / ABSENT   분류표에 없는 코드 / probe 자체가 없다

    probe id 는 레코드의 `probe` 필드에서 읽는다 — 호출부가 따로 넘기지 않아도
    typed predicate 가 걸리게 하기 위해서다.
    """
    if p is None:
        return "ABSENT"
    spec = PROBE_SPEC.get(str(p.get("probe") or ""), {})

    if p.get("query_ok") is True:
        if p.get("row_present") is False:
            return "EMPTY"
        v = p.get("value")
        if v is None and ("value" in p or spec.get("requires_value")):
            return "EMPTY"
        if spec.get("requires_value") and str(v).strip() == "":
            return "EMPTY"
        if p.get("value_interpretable") is False:
            return "NOT_INTERPRETABLE"
        g = spec.get("grammar")
        if g and v is not None and not re.match(g, str(v)):
            return "GRAMMAR"
        return "OK"

    code = _ora(p)
    if code is None:
        return "UNKNOWN"
    # transient 가 언제나 먼저다. 연결이 끊긴 회차는 아무것도 말하지 않는다.
    if code in TRANSIENT:
        return "TRANSIENT"
    # **probe 가 선언한 부재 코드가 전역 AMBIGUOUS 를 이긴다.** 뜻을 아는 것은 probe 다.
    if code in (spec.get("absent_codes") or frozenset()):
        return "UNSUPPORTED"
    if code in UNSUPPORTED:
        return "UNSUPPORTED"
    if code in DENIED:
        return "DENIED"
    if code in WRONG_TARGET:
        return "WRONG_TARGET"
    if code in PROBE_BUG:
        return "PROBE_BUG"
    if code in AMBIGUOUS:
        return "AMBIGUOUS"
    return "UNKNOWN"


def is_negative_evidence(kind: str) -> bool:
    """이 분류가 `NONE`(기능 부재) 을 지지하는가.

    **`OK` 가 아니라는 것만으로는 부재가 아니다.** DENIED·WRONG_TARGET·PROBE_BUG·
    AMBIGUOUS·GRAMMAR·TRANSIENT·UNKNOWN·ABSENT 는 전부 "모른다" 이며 UNDETERMINED 로 간다.
    """
    return kind in NEGATIVE_EVIDENCE


def interpretable(p: dict | None) -> bool:
    """값을 해석할 수 있는가. `query_ok` 만으로는 부족하다."""
    return classify(p) == "OK"


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
        "deps": ["snapshot_anchor", "snapshot_object_coverage"],
        "values": ["JOB", "CONNECTION", "STATEMENT", "NONE", "UNDETERMINED"], "floor": "STATEMENT",
        "what": "한 Job 의 모든 물리 connection 이 **같은** snapshot 을 보는가. "
                "`JOB` 은 공통 anchor ∧ 전체 object coverage ∧ 모든 data connection 적용이 "
                "모두 참일 때만 나온다.",
    },
    "canonical_row_compare": {
        "kind": "COMPOSITE", "scope": "RUNTIME",
        "deps": ["hash_function"],
        "values": ["VECTORS_PROVEN", "PARTIAL", "NONE", "UNDETERMINED"], "floor": "NONE",
        "what": "행 내용을 엔진 간에 같은 바이트로 비교할 수 있는가. "
                "**`VECTORS_PROVEN` 은 G0-3 V-01~V-16 통과 전에 절대 나오지 않는다** — "
                "G0-0 에서는 구조적으로 도달 불가다.",
    },
    "cross_source_comparable": {
        "kind": "COMPOSITE", "scope": "RUNTIME",
        "deps": ["db_charset"],
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
    # **NONE 은 두 probe 가 모두 '부재' 를 지지할 때만 나온다**(8차 M3-2). 권한 부족·대상
    # 부재·프로브 결함·모호 코드는 "원점이 없다" 가 아니라 "모른다" 다.
    if not (is_negative_evidence(cs) and is_negative_evidence(ct)):
        return _axis("UNDETERMINED", c, scope="ACCOUNT",
                     note=f"판정 불가 — get_scn={cs}, as_of_timestamp={ct}. "
                          f"transient·권한 부족·대상 부재·프로브 결함·미실행은 "
                          f"기능 부재의 증거가 아니다.")
    return _axis("NONE", c, scope="ACCOUNT",
                 note=f"두 원점 모두 구문·기능 부재로 확인됐다 "
                      f"(get_scn={cs}, as_of_timestamp={ct}).")


def d_snapshot_object_coverage(P, binding) -> dict:
    c = _Ctx(P)
    ts = c.get("as_of_timestamp.target")
    ct = classify(ts)
    if ct == "OK":
        return _axis("PARTIAL", c, scope="TABLE", binding=binding,
                     note="**ALL 이 나올 수 없다.** G0-0 은 대상 테이블 하나만 그 원점으로 읽는다. "
                          "extract object-set 전체에 대한 판정은 object manifest 를 받은 뒤 "
                          "G0-1 이후에서 한다.")
    if not is_negative_evidence(ct):
        return _axis("UNDETERMINED", c, scope="TABLE", binding=binding,
                     note=f"as_of_timestamp.target={ct} — 부재의 증거가 아니다(8차 M3-2).")
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
    if k == "EMPTY":
        return _axis("UNDETERMINED", c, scope="DB",
                     note="뷰는 읽혔으나 행이 없다. PRIMARY 에서는 정상이며 '기능 없음'이 아니다.")
    if not is_negative_evidence(k):
        return _axis("UNDETERMINED", c, scope="DB",
                     note=f"v$dataguard_stats={k} — 부재의 증거가 아니다(8차 M3-2).")
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
    if not is_negative_evidence(k_set):
        return _axis("UNDETERMINED", c, scope="CONNECTION",
                     note=f"alter.STANDBY_MAX_DATA_DELAY.D={k_set} — 부재의 증거가 아니다. "
                          f"권한 부족(ORA-01031)으로 ALTER SESSION 이 막힌 것과 그 파라미터가 "
                          f"이 판본에 없는 것(ORA-02248)은 다른 사실이다(8차 M3-2).")
    return _axis("NONE", c, scope="CONNECTION",
                 note=f"세션 파라미터가 이 판본에 없다 ({k_set}) — admission 강제가 없다.")


def d_hash_function(P) -> dict:
    c = _Ctx(P)
    h = c.get("feat.standard_hash_sha256")
    c.note_unused("feat.ora_hash")
    k = classify(h)
    if k == "OK" and h.get("value_interpretable") is True:
        return _axis("SHA256", c, scope="DB",
                     note="표준 시험 벡터와 값이 일치한다. **이것은 함수 하나의 가용성이며 "
                          "행 내용 비교 능력이 아니다**(canonical_row_compare 참조).")
    # NOT_INTERPRETABLE 은 여기서만 부재의 증거다 — 함수는 돌았는데 **표준 벡터와 값이
    # 다르다**. 그 함수는 우리가 규격에 쓴 SHA-256 이 아니다.
    if k == "NOT_INTERPRETABLE":
        return _axis("NONE", c, scope="DB",
                     note="함수는 실행됐으나 표준 시험 벡터와 값이 다르다 — 규격이 말하는 "
                          "SHA-256 이 아니다.")
    if not is_negative_evidence(k):
        return _axis("UNDETERMINED", c, scope="DB",
                     note=f"feat.standard_hash_sha256={k} — 부재의 증거가 아니다. "
                          f"**ORA-01031(권한 부족)·ORA-00942(대상 부재)를 NONE 으로 내리던 "
                          f"경로가 8차 §7 이 지적한 오염이다.**")
    return _axis("NONE", c, scope="DB",
                 note=f"SHA-256 을 쓸 수 없다 ({k}). ORA_HASH 는 32비트라 대조용 해시가 못 된다 — "
                      f"Reconciliation 이 건수+PK 로 강등된다.")


def d_sql_dialect(P) -> dict:
    c = _Ctx(P)
    ff = c.get("feat.fetch_first")
    k = classify(ff)
    if k == "OK":
        return _axis("12C_PLUS", c, scope="DB")
    if is_negative_evidence(k):
        return _axis("11G", c, scope="DB",
                     note="FETCH FIRST 구문 미지원(ORA-00900/00923/00933 계열).")
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
    if k_rs != "OK":
        if not is_negative_evidence(k_rs):
            return _axis("UNDETERMINED", c, scope="TABLE", binding=binding,
                         note=f"ora_rowscn={k_rs} — 부재의 증거가 아니다(8차 M3-2).")
        return _axis("NONE", c, scope="TABLE", binding=binding,
                     note=f"ORA_ROWSCN 을 읽지 못했다 ({k_rs}).")
    # ORA_ROWSCN 은 읽혔다. 입도는 ROWDEPENDENCIES 가 정하는데, 그 값을 **읽지 못했으면
    # 입도를 모른다**. 권한 부족으로 ALL_TABLES 를 못 읽은 것을 DISABLED 로 읽어
    # BLOCK_LEVEL 을 확정하지 않는다 — 두 값은 뒤의 fence 설계를 다르게 만든다.
    if k_dep != "OK":
        return _axis("UNDETERMINED", c, scope="TABLE", binding=binding,
                     note=f"ORA_ROWSCN 은 읽혔으나 rowdependencies={k_dep} 라 입도를 모른다. "
                          f"ROW_LEVEL 과 BLOCK_LEVEL 은 뒤의 fence 설계를 다르게 만들므로 "
                          f"둘 중 하나를 임의로 고르지 않는다.")
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
    # OK 도 아니고 부재의 증거도 아닌 분류가 하나라도 있으면 판정하지 않는다.
    if any(k != "OK" and not is_negative_evidence(k) for k in ks):
        return _axis("UNDETERMINED", c, scope="COLUMN", binding=binding,
                     note=f"commit 시각과 watermark 값의 차를 관측할 수단이 확인되지 않았다 "
                          f"({sorted(ks)}). **이 축이 UNDETERMINED 인 것은 lag_observation 과 "
                          f"무관하다** — apply lag 가 0 이어도 이 위험은 남는다.")
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
        # **GRAMMAR 도 여기로 온다.** 값이 `<TYPE>[(n)]|scale=…` 형태가 아니면 파싱하지 않는다 —
        # 형태를 못 읽은 값에서 scale 을 추정하면 successor 가 조용히 틀어진다(P0-01).
        return _axis("UNDETERMINED", c, scope="COLUMN", binding=binding,
                     note=f"wm_column.type_facts={k}"
                          + (" — 값이 기대 형태가 아니다. 추정하지 않는다."
                             if k == "GRAMMAR" else ""))
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
# 3b. effective floor (8차 M3-3 · 8차 §6 불변식)
# ─────────────────────────────────────────────────────────────────────
# 8차 §6 의 지적: "스키마 설명은 `effective_value` 를 authoritative 값이라고 하지만 구현은
# 모든 축에 `effective_value = value` 를 넣는다. `stale` 과 axis-level `measured_at` 은
# optional 이고 생성되지 않는다. summary 와 exit 판정도 `effective_value` 가 아니라
# `value` 를 읽는다." — 즉 floor 는 스키마에만 있었고 코드에는 없었다.
#
# 불변식(8차 §6):
#
#   child incomplete/failed/unbound  => dependent effective_value = floor
#   stale = true                     => effective_value = floor
#   profile/runtime/source mismatch  => effective_value = floor
#   value                            => 과거 관측값, audit/display 전용
#   effective_value                  => validator/publish 가 읽는 **유일한** 값
FLOOR_REASONS: dict[str, str] = {
    "CHILD_NOT_MEASURED":
        "이 값을 만든 child 산출물이 MEASURED 가 아니다. 불완전한 산출물에서 뽑은 값을 "
        "실행에 쓰지 않는다.",
    "UNBOUND":
        "대상 식별자(db_identity·owner·object)에 묶이지 않았다. 묶이지 않은 값은 다른 "
        "테이블에 잘못 적용될 수 있다(P0-03).",
    "STALE":
        "measured_at + TTL 이 지났다. 표시용 이전 값을 안전성 판정에 쓰지 않는다(P1-02).",
    "NO_FRESHNESS_BASIS":
        "이 값의 신선도를 판정할 근거가 없다(measured_at 또는 TTL 부재). 신선도를 모르는 "
        "값은 신선하다고 가정하지 않는다.",
    "SOURCE_UNVERIFIED":
        "이 회차의 profile·source·harness 를 대조하지 못했다(manifest 누락 허용 등). "
        "어느 원천을 어느 코드로 잰 값인지 확인되지 않았다.",
    "PROFILE_NOT_AUTHORITATIVE":
        "profile 이 LOCAL_WSL·SANDBOX_CONTAINER 다. 그 레코드는 스스로 '하네스 동작 "
        "확인용이며 설계 주장의 근거가 아니다' 라고 적는다 — 그렇게 적으면서 확정 "
        "capability 를 publish 값으로 내보내면 두 말이 어긋난다.",
    "COMPOSITE_INPUT_FLOORED":
        "합성 입력 축이 floor 로 내려갔다. 입력이 신뢰되지 않으면 합성 결과도 신뢰되지 않는다.",
}

FRESHNESS_BASES = ("OPERATOR_DECLARED_TTL", "NO_TTL_DECLARED", "NO_MEASUREMENT")


def floored_value(name: str, value: str) -> str:
    """`name` 축의 값을 floor 로 내린다.

    **floor 는 값을 올리지 않는다.** 두 가지를 지킨다.

      · `UNDETERMINED` 는 더 내려갈 곳이 없다. `sql_dialect` 의 floor 가 `11G` 라고 해서
        `UNDETERMINED` 를 `11G` 로 만들면 그것은 floor 가 아니라 **승격**이다.
      · 이미 floor 보다 약한 값(`values` 목록에서 더 뒤)은 그대로 둔다.
        `snapshot_scope` 의 floor 는 `STATEMENT` 지만 `NONE` 을 `STATEMENT` 로 올리지 않는다.

    `values` 가 None 인 축(관측값 그대로인 `db_charset`, 구조화 값인 `wm_successor`)은
    순서가 없으므로 선언된 floor 를 그대로 쓴다.
    """
    spec = AXIS_SPEC[name]
    fl = spec["floor"]
    if value == "UNDETERMINED":
        return "UNDETERMINED"
    vals = spec["values"]
    if not vals or value not in vals or fl not in vals:
        return fl
    return value if vals.index(value) >= vals.index(fl) else fl


def _expires_at(measured_at: str | None, ttl_seconds: int | None) -> str | None:
    if not measured_at or not ttl_seconds:
        return None
    from datetime import datetime, timedelta
    try:
        t = datetime.fromisoformat(str(measured_at).replace("Z", "+00:00"))
    except ValueError:
        return None
    return (t + timedelta(seconds=int(ttl_seconds))).isoformat(timespec="seconds")


def apply_floors(axes: dict, *, now: str | None = None, ttl_seconds: int | None = None,
                 reasons_all: tuple[str, ...] | list[str] = (),
                 per_axis: dict[str, list[str]] | None = None) -> dict:
    """모든 축에 `effective_value`·`stale`·`expires_at`·`freshness_basis`·`floor_reasons`
    를 채운다. **순수 함수다** — `now` 는 호출자가 넘긴다.

    반환은 같은 dict(제자리 수정). `reasons_all` 은 전 축에 걸리는 사유,
    `per_axis` 는 축 이름별 추가 사유다.
    """
    from datetime import datetime
    per_axis = per_axis or {}
    now_dt = None
    if now:
        try:
            now_dt = datetime.fromisoformat(str(now).replace("Z", "+00:00"))
        except ValueError:
            now_dt = None

    def base(name: str, a: dict) -> list[str]:
        reasons = [r for r in reasons_all]
        reasons += list(per_axis.get(name, ()))
        value = a["value"]
        measured = a.get("measured_at")
        exp = _expires_at(measured, ttl_seconds)
        a["expires_at"] = exp
        if not measured:
            a["freshness_basis"] = "NO_MEASUREMENT"
        elif not ttl_seconds:
            a["freshness_basis"] = "NO_TTL_DECLARED"
        else:
            a["freshness_basis"] = "OPERATOR_DECLARED_TTL"
        stale = False
        if exp and now_dt is not None:
            try:
                stale = datetime.fromisoformat(exp) < now_dt
            except ValueError:
                stale = False
        a["stale"] = stale
        if stale:
            reasons.append("STALE")
        # 확정값인데 신선도 근거가 없으면 그것도 floor 사유다. UNDETERMINED 는
        # 이미 아무것도 주장하지 않으므로 여기서 더 내릴 것이 없다.
        if value != "UNDETERMINED" and a["freshness_basis"] != "OPERATOR_DECLARED_TTL":
            reasons.append("NO_FRESHNESS_BASIS")
        if AXIS_SPEC[name]["scope"] in TABLE_SCOPES and a.get("binding") is None \
                and value != "UNDETERMINED":
            reasons.append("UNBOUND")
        return list(dict.fromkeys(reasons))

    # 1차 — 모든 축
    for name, a in axes.items():
        reasons = base(name, a)
        a["floor_reasons"] = reasons
        a["effective_value"] = floored_value(name, a["value"]) if reasons else a["value"]

    # 2차 — 합성 축은 입력이 내려가면 같이 내려간다
    for name, a in axes.items():
        deps = AXIS_SPEC[name].get("deps") or []
        if not deps:
            continue
        if any(axes.get(d, {}).get("floor_reasons") for d in deps):
            a["floor_reasons"] = list(dict.fromkeys(
                list(a["floor_reasons"]) + ["COMPOSITE_INPUT_FLOORED"]))
            a["effective_value"] = floored_value(name, a["value"])
    return axes


# ─────────────────────────────────────────────────────────────────────
# 4. 진입점
# ─────────────────────────────────────────────────────────────────────
def derive_axes(P: dict[str, dict], *, binding: dict | None = None,
                measured_at: str | None = None,
                now: str | None = None, ttl_seconds: int | None = None,
                floor_reasons: tuple[str, ...] | list[str] = ()) -> dict:
    """probe 딕셔너리 → 축 딕셔너리. **순수 함수다.**

    binding: 테이블 단위 축이 묶일 대상. `{"db_identity":…, "owner":…, "object":…}`.
             None 이면 테이블 단위 축은 전부 `UNDETERMINED` 로 내린다 —
             **묶이지 않은 확정값을 만들지 않는다**(P0-03).
    now / ttl_seconds / floor_reasons:
             `apply_floors` 로 넘어간다(8차 M3-3). `now` 를 인자로 받는 이유는 이 모듈이
             시각을 읽지 않기 때문이다 — 순수성을 유지해야 시험이 시각에 흔들리지 않는다.
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

    # **floor 는 파생의 마지막 단계다**(8차 M3-3). 여기까지의 `value` 는 관측 사실이고,
    # 아래에서 붙는 `effective_value` 가 validator·publish 가 읽는 유일한 값이다.
    per_axis: dict[str, list[str]] = {}
    if not tb:
        for name in ("snapshot_object_coverage", "row_change_scn",
                     "watermark_commit_bound", "wm_successor"):
            per_axis[name] = ["UNBOUND"]
    return apply_floors(axes, now=now, ttl_seconds=ttl_seconds,
                        reasons_all=tuple(floor_reasons), per_axis=per_axis)


AXIS_NAMES = list(AXIS_SPEC.keys())
