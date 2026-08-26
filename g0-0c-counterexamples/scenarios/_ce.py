#!/usr/bin/env python3
"""G0-0C counterexample 시나리오 공용 라이브러리.

각 시나리오(run.py)가 공유하는 것만 담는다. 이 파일은 실험을 하지 않는다.

원칙
  1. **비밀번호는 argv에 절대 오지 않는다.** CE_USER / CE_PASSWORD / CE_DSN 환경변수만
     읽고, 예외 메시지에 값이 섞이면 지운다(scrub).
  2. **injection_observed 는 서버 측 관측으로만 true 다.** "예외가 안 났다"·클라이언트
     로그는 근거가 아니다. 근거는 ORA 오류 코드·서버가 돌려준 행 상태·서버 시각뿐이다.
  3. **접속 실패·모듈 부재·스키마 불일치는 INCONCLUSIVE 다.** 조용한 성공이 없다.
  4. **모든 객체는 object_prefix로 시작하고, 끝나면 반드시 지운다.** 남으면 그대로 보고한다.

환경변수
  CE_USER          접속 계정. suite 의 allowed_schema 와 같아야 한다.
  CE_PASSWORD      비밀번호(환경변수 또는 wallet). argv 금지.
  CE_DSN           easy connect 또는 tnsnames alias. 폐기용 primary.
  CE_STANDBY_DSN   (선택) ADG standby. 없으면 해당 관측은 건너뛴다.
"""
from __future__ import annotations

import json
import os
import re
import sys
import traceback
from datetime import datetime, timezone

DRIVER = "python-oracledb(thin)"
DRIVER_CAVEAT = (
    "이 하네스는 python-oracledb thin 이다. OJDBC 가 아니다 — "
    "OJDBC·Spark 계층의 형변환/스냅샷 동작은 이 결과로 대체할 수 없다."
)

OUTCOME_REPRODUCED = "COUNTEREXAMPLE_REPRODUCED"
OUTCOME_HOLDS = "MITIGATION_HOLDS"
OUTCOME_FAIL = "MITIGATION_FAIL"
OUTCOME_NOT_OBSERVED = "INJECTION_NOT_OBSERVED"
OUTCOME_INCONCLUSIVE = "INCONCLUSIVE"


class Unavailable(Exception):
    """환경이 갖춰지지 않아 판정할 수 없다 → INCONCLUSIVE."""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def scrub(x) -> str:
    """예외 메시지에서 비밀번호·사용자명을 지운다."""
    s = str(x)
    for var in ("CE_PASSWORD", "CE_USER"):
        v = os.environ.get(var)
        if v and len(v) >= 3:
            s = s.replace(v, f"<{var}>")
    return s[:400]


def ora_error_code(exc) -> int | None:
    """oracledb 예외에서 ORA 번호를 뽑는다. 없으면 None."""
    err = getattr(exc, "args", [None])
    if err and hasattr(err[0], "code"):
        try:
            return int(err[0].code)
        except Exception:
            pass
    m = re.search(r"ORA-(\d{4,5})", str(exc))
    return int(m.group(1)) if m else None


def parse_suite(argv=None) -> dict:
    argv = list(sys.argv if argv is None else argv)
    if "--suite" in argv:
        try:
            return json.loads(argv[argv.index("--suite") + 1])
        except Exception:
            return {}
    return {}


# ── 결과 누적기 ──────────────────────────────────────────────────────
class CeResult:
    def __init__(self, cfg: dict):
        self.cfg = cfg or {}
        self.prefix = str(self.cfg.get("object_prefix") or "CE_")
        self.schema = str(self.cfg.get("schema") or "")
        self.outcome = OUTCOME_INCONCLUSIVE
        self.injection_observed = False
        self.injection_evidence: list[dict] = []
        self.objects_created: list[str] = []
        self.rows_written = 0
        self.sessions_peak = 0
        self.observations: list[dict] = []
        self.cleanup = {"attempted": False, "succeeded": False, "leftover_objects": []}

    def obs(self, name, value, expected=None, matches=None, note=None) -> None:
        rec = {"name": name, "value": value}
        if expected is not None:
            rec["expected"] = expected
        if matches is not None:
            rec["matches_expected"] = matches
        if note:
            rec["note"] = note
        self.observations.append(rec)

    def evidence(self, kind, value, at=None) -> None:
        """injection 이 실제로 일어났다는 **서버 측** 근거를 남긴다.

        kind: ORA_ERROR | SERVER_STATE | TIMING | ROW_STATE | LOG_WITH_SERVER_ID
        """
        assert kind in ("ORA_ERROR", "SERVER_STATE", "TIMING", "ROW_STATE", "LOG_WITH_SERVER_ID")
        rec = {"kind": kind, "value": str(value)[:600]}
        if at:
            rec["at"] = at
        self.injection_evidence.append(rec)
        self.injection_observed = True

    def payload(self) -> dict:
        if not self.observations:
            self.obs("no_observation", True, note="시나리오가 아무것도 관측하지 못했다.")
        # 증거 없이 재현/완화를 주장할 수 없다. runner도 같은 강등을 하지만 여기서 먼저 막는다.
        outcome = self.outcome
        if outcome in (OUTCOME_REPRODUCED, OUTCOME_HOLDS, OUTCOME_FAIL) and not self.injection_observed:
            self.obs("self_downgrade", "injection_evidence 가 비어 있어 강등",
                     note="주입의 서버 측 근거 없이는 판정을 주장하지 않는다.")
            outcome = OUTCOME_NOT_OBSERVED
        return {
            "outcome": outcome,
            "injection_observed": self.injection_observed,
            "injection_evidence": self.injection_evidence,
            "fixture": {"objects_created": self.objects_created,
                        "rows_written": self.rows_written,
                        "sessions_peak": self.sessions_peak},
            "observations": self.observations,
            "cleanup": self.cleanup,
        }

    def emit(self) -> None:
        print("SCENARIO_RESULT " + json.dumps(self.payload(), ensure_ascii=False))


# ── Oracle 접속 ──────────────────────────────────────────────────────
class Ora:
    def __init__(self, res: CeResult):
        self.res = res
        self._mod = None
        self._conns: list = []

    @property
    def mod(self):
        if self._mod is None:
            try:
                import oracledb  # type: ignore
            except ImportError as e:
                raise Unavailable("python-oracledb 미설치 (pip install oracledb)") from e
            self._mod = oracledb
        return self._mod

    def connect(self, dsn_env: str = "CE_DSN", tag: str = "main"):
        user = os.environ.get("CE_USER")
        pw = os.environ.get("CE_PASSWORD")
        dsn = os.environ.get(dsn_env)
        missing = [n for n, v in (("CE_USER", user), ("CE_PASSWORD", pw), (dsn_env, dsn)) if not v]
        if missing:
            raise Unavailable(
                f"환경변수 미설정 {missing}. 비밀번호는 argv가 아니라 환경변수/wallet 으로만 넘긴다.")
        budget = int((self.res.cfg.get("budgets") or {}).get("max_sessions", 12) or 12)
        if len(self._conns) >= budget:
            raise Unavailable(f"max_sessions({budget}) 초과 — 시나리오가 예산을 넘겼다.")
        try:
            c = self.mod.connect(user=user, password=pw, dsn=dsn)
        except Exception as e:  # noqa: BLE001
            # 잘못된 비밀번호를 재시도하지 않는다(계정 잠금 방지). 한 번 실패하면 끝이다.
            raise Unavailable(f"접속 실패({tag}): ORA-{ora_error_code(e)} {scrub(e)}") from e
        # budgets.statement_timeout_s → call_timeout(ms). 한 문장이 무한히 매달리지 않게 한다.
        try:
            t = int((self.res.cfg.get("budgets") or {}).get("statement_timeout_s", 0) or 0)
            if t > 0:
                c.call_timeout = t * 1000
        except Exception:  # noqa: BLE001
            pass
        self._conns.append(c)
        self.res.sessions_peak = max(self.res.sessions_peak, len(self._conns))
        return c

    def verify_schema(self, conn) -> None:
        """접속 계정이 suite 의 allowed_schema 와 같은지 확인한다.

        다르면 남의 스키마에 객체를 만들 위험이 있으므로 실행하지 않는다."""
        cur = q1(conn, "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL")
        want = self.res.schema
        if want and str(cur).upper() != str(want).upper():
            raise Unavailable(
                f"CURRENT_SCHEMA={cur!r} 가 suite.allowed_schema={want!r} 와 다르다. "
                "대상 스키마가 아니면 한 줄도 쓰지 않는다.")
        self.res.obs("current_schema", cur, expected=want, matches=True)
        # 어느 DB에 붙었는지 서버가 스스로 밝힌 값으로 남긴다(primary/standby 혼동 방지).
        try:
            role, dbun, inst = qrow(conn,
                                    "SELECT SYS_CONTEXT('USERENV','DATABASE_ROLE'),"
                                    "       SYS_CONTEXT('USERENV','DB_UNIQUE_NAME'),"
                                    "       SYS_CONTEXT('USERENV','INSTANCE_NAME') FROM DUAL")
            self.res.obs("database_role", role, expected="PRIMARY",
                         matches=(str(role).upper() == "PRIMARY"),
                         note=f"db_unique_name={dbun} instance={inst}. "
                              "쓰기 fixture는 primary 에서만 만든다.")
            if "STANDBY" in str(role).upper():
                raise Unavailable(
                    f"DATABASE_ROLE={role} — standby 는 읽기 전용이라 fixture 를 만들 수 없다.")
        except Unavailable:
            raise
        except Exception as e:  # noqa: BLE001
            self.res.obs("database_role", None, note=f"조회 실패: {scrub(e)}")

    def close_all(self) -> None:
        for c in self._conns:
            try:
                c.close()
            except Exception:
                pass
        self._conns = []


# ── SQL 헬퍼 ────────────────────────────────────────────────────────
def q1(conn, sql, **binds):
    with conn.cursor() as cur:
        cur.execute(sql, binds)
        row = cur.fetchone()
        return row[0] if row else None


def qrow(conn, sql, **binds):
    with conn.cursor() as cur:
        cur.execute(sql, binds)
        return cur.fetchone()


def qall(conn, sql, **binds):
    with conn.cursor() as cur:
        cur.execute(sql, binds)
        return cur.fetchall()


def ex(conn, sql, **binds) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, binds)
        return cur.rowcount


def exmany(conn, sql, rows) -> int:
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
        return cur.rowcount


def server_now(conn) -> str:
    """서버 시각. 클라이언트 시계를 증거로 쓰지 않기 위해 항상 이것을 쓴다."""
    return str(q1(conn, "SELECT TO_CHAR(SYSTIMESTAMP,'YYYY-MM-DD\"T\"HH24:MI:SS.FF6 TZH:TZM') FROM DUAL"))


# ── fixture 소유·정리 ───────────────────────────────────────────────
class Fixture:
    """시나리오가 만든 객체를 추적하고 반드시 지운다.

    with Fixture(ora, res, conn) as fx:
        fx.table("WM_SRC", "id NUMBER PRIMARY KEY, wm TIMESTAMP(6)")
    나가면서 DROP → USER_OBJECTS 로 prefix 잔여를 재확인한다."""

    def __init__(self, res: CeResult, conn):
        self.res = res
        self.conn = conn
        self.tables: list[str] = []

    def name(self, suffix: str) -> str:
        n = (self.res.prefix + suffix).upper()
        if not n.startswith(self.res.prefix.upper()):
            raise Unavailable(f"객체 이름 {n} 이 object_prefix 를 벗어났다.")
        if len(n) > 30:
            raise Unavailable(f"객체 이름 {n} 이 30자를 넘는다.")
        return n

    def table(self, suffix: str, body: str, opts: str = "") -> str:
        n = self.name(suffix)
        self.drop(n)
        with self.conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {n} ({body}){(' ' + opts) if opts else ''}")
        self.tables.append(n)
        self.res.objects_created.append(n)
        return n

    def drop(self, n: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "BEGIN EXECUTE IMMEDIATE 'DROP TABLE ' || :n || ' PURGE'; "
                "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;", n=n)

    def leftovers(self) -> list[str]:
        rows = qall(self.conn,
                    "SELECT object_name FROM user_objects "
                    "WHERE object_name LIKE :p ESCAPE '\\' ORDER BY object_name",
                    p=self.res.prefix.replace("_", "\\_") + "%")
        return [r[0] for r in rows]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.res.cleanup["attempted"] = True
        ok = True
        for n in reversed(self.tables):
            try:
                self.drop(n)
            except Exception as e:  # noqa: BLE001
                ok = False
                self.res.obs("cleanup_error", f"{n}: {scrub(e)}")
        try:
            left = self.leftovers()
        except Exception as e:  # noqa: BLE001
            ok = False
            left = [f"<verify_failed: {scrub(e)}>"]
        self.res.cleanup["succeeded"] = ok and not left
        self.res.cleanup["leftover_objects"] = left
        return False  # 예외를 삼키지 않는다


# ── 진입점 래퍼 ─────────────────────────────────────────────────────
def run_main(body) -> int:
    """body(res, ora) 를 실행하고 무슨 일이 있어도 SCENARIO_RESULT 한 줄을 낸다."""
    res = CeResult(parse_suite())
    res.obs("driver", DRIVER, note=DRIVER_CAVEAT)
    ora = Ora(res)
    try:
        body(res, ora)
    except Unavailable as e:
        res.outcome = OUTCOME_INCONCLUSIVE
        res.obs("unavailable", scrub(e), note="환경이 갖춰지지 않아 판정하지 않는다. PASS 아님.")
    except Exception as e:  # noqa: BLE001
        res.outcome = OUTCOME_INCONCLUSIVE
        res.obs("exception", f"{type(e).__name__}: {scrub(e)}")
        res.obs("traceback", traceback.format_exc()[-1200:])
    finally:
        ora.close_all()
        res.emit()
    return 0
