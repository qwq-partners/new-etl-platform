#!/usr/bin/env python3
"""G0-0C counterexample suite runner.

역할은 셋이다.
  1. **환경 가드** — 폐기용 쓰기 가능 환경이 아니면 한 줄도 실행하지 않는다.
  2. **오케스트레이션** — scenarios/CE0*/scenario.yaml 을 읽어 실행하고 증거를 모은다.
  3. **판정** — 시나리오 누락·injection 미관측·cleanup 잔여는 **PASS가 아니다**.

이 파일은 fixture를 직접 만들지 않는다. 각 시나리오가 자기 fixture·주입·정리를
소유하고, runner는 가드·예산·증거 스키마·판정만 강제한다.

사용:
    python3 runner.py --suite suite.yaml --out evidence.json          # 실행
    python3 runner.py --suite suite.yaml --dry-run                    # 접속 없이 계획만
    python3 runner.py --suite suite.yaml --only CE01,CE04             # 부분 실행(PASS 불가)

**환경 신원은 서버에서 직접 읽는다.** runner 가 CE_DSN 으로 한 번 접속해
DB_UNIQUE_NAME·CURRENT_SCHEMA·DATABASE_ROLE 을 읽고 suite 의 expected 값과 대조한다.
운영자가 손으로 적어 넣는 --observed-env 는 **보조 확인용**이며, 주면 서버가 돌려준
값과도 일치해야 한다. 이 preflight 는 두 가지를 동시에 막는다.
  · CE_DSN 이 expected 와 다른 DB(운영 원천 포함)를 가리키는 경우
  · 자격증명이 틀렸을 때 시나리오 9개가 각각 로그온을 시도해 계정이 잠기는 경우
    (preflight 1회 실패 → 즉시 SUITE_ABORT. 재시도하지 않는다.)

종료 코드: 0 = suite PASS / 2 = 가드 실패(SUITE_ABORT) / 3 = PASS 아님 / 4 = 내부 오류
"""
from __future__ import annotations
import argparse, hashlib, json, os, re, subprocess, sys, time, uuid
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = "1.0.0"
SUITE_ID = "G0-0C-COUNTEREXAMPLES"
OUTCOMES = {"COUNTEREXAMPLE_REPRODUCED", "MITIGATION_HOLDS", "MITIGATION_FAIL",
            "INJECTION_NOT_OBSERVED", "INCONCLUSIVE"}
PASSING_OUTCOMES = {"COUNTEREXAMPLE_REPRODUCED", "MITIGATION_HOLDS", "MITIGATION_FAIL"}
EVIDENCE_KINDS = {"ORA_ERROR", "SERVER_STATE", "TIMING", "ROW_STATE", "LOG_WITH_SERVER_ID"}


def _valid_ev(ev) -> int:
    """kind·value 가 모두 규격에 맞는 증거만 센다. 배열 길이를 유효 건수로 쓰면
    전부 무효인데도 증거가 있었던 것처럼 기록된다."""
    if not isinstance(ev, list):
        return 0
    return sum(1 for e in ev
               if isinstance(e, dict) and e.get("kind") in EVIDENCE_KINDS
               and isinstance(e.get("value"), str) and e["value"].strip())

def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def die(code: int, msg: str) -> None:
    print(f"\n[SUITE_ABORT] {msg}", file=sys.stderr)
    sys.exit(code)

# ── YAML: 의존성을 늘리지 않기 위해 필요한 부분만 읽는 최소 파서 ──────────
def load_yaml(path: Path) -> dict:
    try:
        import yaml  # type: ignore
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except ImportError:
        pass
    # PyYAML이 없으면 이 suite가 쓰는 부분집합만 파싱한다(중첩 2단, 리스트, 스칼라).
    root: dict = {}
    stack = [(0, root)]
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].rstrip() if not raw.strip().startswith("#") else ""
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip())
        while stack and indent < stack[-1][0]:
            stack.pop()
        cur = stack[-1][1]
        body = line.strip()
        if body.startswith("- "):
            cur.setdefault("__list__", []).append(_scalar(body[2:]))
            continue
        if ":" not in body:
            continue
        k, _, v = body.partition(":")
        k, v = k.strip(), v.strip()
        if v == "":
            child: dict = {}
            cur[k] = child
            stack.append((indent + 2, child))
        elif v.startswith("["):
            cur[k] = [_scalar(x) for x in v.strip("[]").split(",") if x.strip()]
        else:
            cur[k] = _scalar(v)
    return _flatten_lists(root)

def _scalar(v: str):
    v = v.strip().strip('"').strip("'")
    if v in ("true", "false"): return v == "true"
    if re.fullmatch(r"-?\d+", v): return int(v)
    return v

def _flatten_lists(o):
    if isinstance(o, dict):
        if set(o.keys()) == {"__list__"}:
            return o["__list__"]
        return {k: _flatten_lists(v) for k, v in o.items()}
    return o

# ── 0. preflight — 신원을 **서버에서** 읽는다 ────────────────────────
def preflight(suite: dict) -> dict:
    """CE_DSN 으로 1회 접속해 DB 신원을 읽어 온다.

    운영자 자기신고를 없애는 것이 목적이다. 실패하면 즉시 중단하며 **재시도하지 않는다**
    (자격증명 오류로 시나리오마다 로그온을 시도해 계정이 잠기는 것을 막는다)."""
    try:
        import oracledb  # type: ignore
    except ImportError:
        die(2, "python-oracledb 가 없어 환경 신원을 서버에서 확인할 수 없다. "
               "`pip install oracledb` 후 다시 실행하라(계획만 보려면 --dry-run).")
    user, pw, dsn = (os.environ.get(k) for k in ("CE_USER", "CE_PASSWORD", "CE_DSN"))
    missing = [k for k, v in (("CE_USER", user), ("CE_PASSWORD", pw), ("CE_DSN", dsn)) if not v]
    if missing:
        die(2, f"환경변수 {missing} 가 없다. 비밀번호는 argv 가 아니라 환경변수/wallet 으로 넘긴다.")
    try:
        conn = oracledb.connect(user=user, password=pw, dsn=dsn)
    except Exception as e:  # noqa: BLE001
        msg = str(e)
        if pw and len(pw) >= 3:
            msg = msg.replace(pw, "<CE_PASSWORD>")
        die(2, f"preflight 접속 실패 — 재시도하지 않는다(계정 잠금 방지): {msg[:300]}")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT SYS_CONTEXT('USERENV','DB_UNIQUE_NAME'),"
                        "       SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),"
                        "       SYS_CONTEXT('USERENV','DATABASE_ROLE'),"
                        "       SYS_CONTEXT('USERENV','INSTANCE_NAME'),"
                        "       SESSIONTIMEZONE, DBTIMEZONE FROM DUAL")
            dbun, schema, role, inst, stz, dtz = cur.fetchone()
    finally:
        try:
            conn.close()
        except Exception:
            pass
    if str(role or "").upper() != "PRIMARY":
        die(2, f"DATABASE_ROLE={role!r} — 이 suite 는 쓰기 fixture 를 만들므로 primary 에서만 돈다.")
    print(f"[preflight] db_unique_name={dbun} schema={schema} role={role} instance={inst}")
    print(f"[preflight] session_tz={stz} db_tz={dtz}")
    out = {"primary_db_unique_name": str(dbun or ""), "schema": str(schema or ""),
           "database_role": str(role or ""), "instance_name": str(inst or ""),
           "session_timezone": str(stz or ""), "db_timezone": str(dtz or "")}
    # standby 는 CE_STANDBY_DSN 이 있을 때만 서버에서 읽는다. 없으면 **미검증**으로 표시한다.
    sdsn = os.environ.get("CE_STANDBY_DSN")
    if sdsn:
        try:
            sc = oracledb.connect(user=user, password=pw, dsn=sdsn)
            try:
                with sc.cursor() as cur:
                    cur.execute("SELECT SYS_CONTEXT('USERENV','DB_UNIQUE_NAME'),"
                                "       SYS_CONTEXT('USERENV','DATABASE_ROLE') FROM DUAL")
                    sdbun, srole = cur.fetchone()
            finally:
                sc.close()
            out["standby_db_unique_name"] = str(sdbun or "")
            out["standby_verified"] = True
            print(f"[preflight] standby db_unique_name={sdbun} role={srole}")
        except Exception as e:  # noqa: BLE001
            die(2, f"CE_STANDBY_DSN 접속 실패 — 재시도하지 않는다: {str(e)[:200]}")
    else:
        exp = str((suite.get("environment_guard") or {}).get("expected_standby_db_unique_name") or "")
        out["standby_db_unique_name"] = exp
        out["standby_verified"] = False
        print("[preflight] CE_STANDBY_DSN 미설정 — standby 신원은 **미검증**으로 기록한다")
    return out


# ── 1. 환경 가드 ─────────────────────────────────────────────────────
def enforce_guard(suite: dict, observed: dict) -> list[str]:
    """observed = {"primary_db_unique_name":..., "standby_db_unique_name":..., "schema":...}
    실패하면 즉시 종료한다. 통과 항목 목록을 돌려준다."""
    g = suite.get("environment_guard") or {}
    checks: list[str] = []

    if g.get("class") != "DISPOSABLE_WRITABLE_PRIMARY_ADG":
        die(2, "environment_guard.class 가 DISPOSABLE_WRITABLE_PRIMARY_ADG 가 아니다.")
    for key in ("expected_primary_db_unique_name", "expected_standby_db_unique_name", "allowed_schema"):
        if not str(g.get(key) or "").strip():
            die(2, f"environment_guard.{key} 가 비어 있다. 운영 사고 방지를 위해 빈 값으로는 실행하지 않는다.")

    pairs = [("primary_db_unique_name", "expected_primary_db_unique_name"),
             ("standby_db_unique_name", "expected_standby_db_unique_name"),
             ("schema", "allowed_schema")]
    for obs_key, exp_key in pairs:
        obs, exp = str(observed.get(obs_key, "")), str(g[exp_key])
        if obs != exp:
            die(2, f"{obs_key} 불일치: 관측 {obs!r} != 기대 {exp!r}. 대상 환경이 아니다.")
        if obs_key == "standby_db_unique_name" and not observed.get("standby_verified"):
            # **정직하게 적는다.** CE_STANDBY_DSN 이 없으면 preflight 가 suite 의 expected 를
            # 그대로 복사해 두므로, 이 비교는 자기 자신과의 비교(항등식)라 언제나 통과한다.
            # "가드가 전부 켜졌다" 고 읽히면 안 된다 — 이 축은 **검사되지 않았다**.
            checks.append(f"{obs_key}=NOT_CHECKED (CE_STANDBY_DSN 미설정 — "
                          f"expected 값 {exp!r} 를 복사해 자기 자신과 비교했다. 항등식이며 검증이 아니다)")
        else:
            checks.append(f"{obs_key}=={exp}")

    if g.get("production_forbidden", True):
        pats = g.get("forbidden_name_patterns") or []
        # 서버가 돌려준 값(db_unique_name·schema·instance_name)을 전부 훑는다.
        blob = " ".join(str(v) for k, v in observed.items() if k != "standby_verified").upper()
        for p in pats:
            if str(p).upper() in blob:
                die(2, f"운영 식별자 패턴 {p!r} 이 관측값에 있다. production_forbidden=true 이므로 중단한다.")
        checks.append("production_forbidden_patterns_clear")

    prefix = str(g.get("object_prefix") or "")
    if not prefix:
        die(2, "environment_guard.object_prefix 가 비어 있다.")
    checks.append(f"object_prefix={prefix}")
    return checks

# ── 2. artifact 해시 ─────────────────────────────────────────────────
# 해시가 고정하는 것은 **코드**(runner·스키마·시나리오)다. suite 파일 자신은 제외한다 —
# 계산한 해시를 suite.yaml 에 적는 순간 해시가 또 바뀌어 영원히 불일치가 되기 때문이다.
# 실행 산출물(evidence)과 바이트코드도 제외한다.
_HASH_SKIP_DIRS = {"__pycache__", ".git", ".pytest_cache"}

def artifact_hash(root: Path, exclude: set[str]) -> str:
    h = hashlib.sha256()
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue
        rel = p.relative_to(root)
        if _HASH_SKIP_DIRS & set(rel.parts):
            continue
        if rel.as_posix() in exclude or p.suffix in (".pyc", ".pyo"):
            continue
        if p.name.startswith("evidence") and p.suffix == ".json":
            continue
        h.update(rel.as_posix().encode())
        h.update(p.read_bytes())
    return h.hexdigest()

# ── 3. 시나리오 실행 ─────────────────────────────────────────────────
def run_scenario(sdir: Path, suite: dict, env: dict, dry: bool,
                 deadline: float | None = None) -> dict:
    try:
        meta = load_yaml(sdir / "scenario.yaml")
    except Exception as e:  # noqa: BLE001
        meta = {}
        print(f"[warn] {sdir.name}/scenario.yaml 을 읽지 못했다: {type(e).__name__}: {e}")
    sid = meta.get("id") or sdir.name[:4]
    rec = {"id": sid, "title": meta.get("title", ""),
           "traceability": (suite.get("traceability") or {}).get(sid, []),
           "started_at": now(), "finished_at": None,
           "outcome": "INCONCLUSIVE", "injection_observed": False,
           "injection_evidence": [],
           "fixture": {"objects_created": [], "rows_written": 0},
           "observations": [], "cleanup": {"attempted": False, "succeeded": False,
                                           "leftover_objects": []},
           "error": None}

    entry = sdir / str(meta.get("entrypoint") or "run.py")
    if dry:
        rec["observations"] = [{"name": "dry_run", "value": True,
                                "note": f"실행하지 않음. entrypoint={entry.name}"}]
        rec["finished_at"] = now()
        return rec
    if not entry.exists():
        rec["error"] = f"entrypoint 없음: {entry}"
        rec["observations"] = [{"name": "entrypoint_missing", "value": str(entry)}]
        rec["finished_at"] = now()
        return rec

    g = suite["environment_guard"]
    prefix = str(g["object_prefix"])
    # 시나리오가 자기 접속 DB 를 expected 와 대조할 수 있도록 신원 기대값을 함께 넘긴다.
    cmd = [sys.executable, str(entry), "--suite", json.dumps({
        "schema": env["schema"], "object_prefix": prefix,
        "expected_primary_db_unique_name": g.get("expected_primary_db_unique_name", ""),
        "forbidden_name_patterns": g.get("forbidden_name_patterns", []),
        "budgets": suite.get("budgets", {}), "versions": suite.get("versions", {}),
    }, ensure_ascii=False)]

    budgets = suite.get("budgets") or {}
    per = int(budgets.get("scenario_timeout_s", 600) or 600)
    if deadline is not None:
        per = min(per, max(1, int(deadline - time.monotonic())))
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=per,
                             cwd=str(sdir),
                             env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        payload = None
        for line in reversed(out.stdout.splitlines()):
            if line.startswith("SCENARIO_RESULT "):
                try:
                    payload = json.loads(line[len("SCENARIO_RESULT "):])
                except json.JSONDecodeError as e:
                    rec["error"] = f"SCENARIO_RESULT 가 올바른 JSON 이 아니다: {e}"
                break
        if payload is None and not rec["error"]:
            rec["error"] = "SCENARIO_RESULT 라인이 없다(시나리오가 결과를 보고하지 않았다)."
            rec["observations"] = [{"name": "stderr_tail", "value": out.stderr[-800:]}]
        elif isinstance(payload, dict):
            # **타입을 확인하고 받는다.** 시나리오 stdout 은 신뢰 대상이 아니다 —
            # 잘못된 타입 하나가 runner 를 죽여 증거 파일 자체가 안 남는 일을 막는다.
            if isinstance(payload.get("outcome"), str):
                rec["outcome"] = payload["outcome"]
            rec["injection_observed"] = payload.get("injection_observed")
            if isinstance(payload.get("injection_evidence"), list):
                rec["injection_evidence"] = payload["injection_evidence"]
            if isinstance(payload.get("fixture"), dict):
                f = payload["fixture"]
                rec["fixture"] = {
                    "objects_created": f.get("objects_created") if isinstance(
                        f.get("objects_created"), list) else [],
                    "rows_written": f.get("rows_written") if isinstance(
                        f.get("rows_written"), int) else 0,
                    **({"sessions_peak": f["sessions_peak"]}
                       if isinstance(f.get("sessions_peak"), int) else {}),
                }
            if isinstance(payload.get("observations"), list):
                rec["observations"] = [o for o in payload["observations"] if isinstance(o, dict)]
            if isinstance(payload.get("cleanup"), dict):
                c = payload["cleanup"]
                rec["cleanup"] = {
                    "attempted": bool(c.get("attempted")),
                    "succeeded": bool(c.get("succeeded")),
                    "leftover_objects": c.get("leftover_objects") if isinstance(
                        c.get("leftover_objects"), list) else ["<malformed>"],
                }
        elif payload is not None:
            rec["error"] = f"SCENARIO_RESULT 가 객체가 아니다: {type(payload).__name__}"
        # **종료 코드도 함께 요구한다**(7차 리뷰). 통과 모양의 SCENARIO_RESULT 를 찍은 뒤
        # exit 1 로 죽어도 지금까지는 suite PASS 후보가 됐다.
        if out.returncode != 0:
            rec["observations"].append({
                "name": "nonzero_exit",
                "value": out.returncode,
                "note": "시나리오가 결과를 보고했더라도 프로세스가 정상 종료하지 않았다. "
                        "payload 를 신뢰할 수 없다."})
            rec["error"] = (rec["error"] or "") + f" [exit={out.returncode}]"
    except subprocess.TimeoutExpired:
        rec["error"] = f"scenario_timeout_s({per}s) 초과"
    except Exception as e:  # noqa: BLE001
        rec["error"] = f"{type(e).__name__}: {e}"

    if not isinstance(rec["observations"], list) or not rec["observations"]:
        # 스키마가 observations 최소 1건을 요구한다. 타임아웃·예외 경로에서도 비지 않게 한다.
        rec["observations"] = [{"name": "no_observation_reported",
                                "value": rec["error"] or "시나리오가 관측을 하나도 보고하지 않았다"}]

    if rec["outcome"] not in OUTCOMES:
        rec["observations"].append({"name": "invalid_outcome", "value": str(rec["outcome"])})
        rec["outcome"] = "INCONCLUSIVE"

    # R4 — 시나리오가 신고한 객체 이름이 object_prefix 를 벗어나면 실패로 본다.
    # prefix 강제는 _ce.Fixture 안에만 있어 시나리오가 우회할 수 있으므로 여기서 다시 본다.
    stray = [o for o in rec["fixture"]["objects_created"]
             if not str(o).upper().startswith(prefix.upper())]
    if stray:
        rec["observations"].append({"name": "object_prefix_violation", "value": stray})
        rec["cleanup"]["leftover_objects"] = list(rec["cleanup"]["leftover_objects"]) + stray

    # R3 — injection 증거 없이 재현/완화 판정을 주장할 수 없다.
    #      truthy 검사가 아니라 **동일성** 검사다("no" 같은 문자열도 truthy 이기 때문).
    ev = rec["injection_evidence"]
    ev_ok = (isinstance(ev, list) and len(ev) >= 1
             and all(isinstance(e, dict) and e.get("kind") in EVIDENCE_KINDS
                     and isinstance(e.get("value"), str) and e["value"].strip() for e in ev))
    if rec["injection_observed"] is not True or not ev_ok:
        if rec["outcome"] in PASSING_OUTCOMES:
            rec["observations"].append({
                "name": "downgraded",
                "value": f"injection_observed={rec['injection_observed']!r}, "
                         f"제출 {len(ev) if isinstance(ev, list) else 0}건 중 "
                         f"유효 {_valid_ev(ev)}건 → INJECTION_NOT_OBSERVED 로 강등"})
            rec["outcome"] = "INJECTION_NOT_OBSERVED"
        rec["injection_observed"] = False
    rec["finished_at"] = now()
    return rec

# ── 4. 판정 ─────────────────────────────────────────────────────────
def verdict(suite: dict, scen: list[dict], partial: bool, skipped: list[str] | None = None,
            schema_errors: list[str] | None = None) -> dict:
    required = list(suite.get("required_scenarios") or [])
    ran = {s["id"] for s in scen}
    missing = [r for r in required if r not in ran]
    not_obs = [s["id"] for s in scen if s["outcome"] == "INJECTION_NOT_OBSERVED"]
    incon = [s["id"] for s in scen if s["outcome"] == "INCONCLUSIVE"]
    leftover = [s["id"] for s in scen if s.get("cleanup", {}).get("leftover_objects")]
    # cleanup.succeeded/attempted 도 판정에 넣는다. DROP 이 실패했는데 잔여 조회까지
    # 실패하면 leftover_objects 가 비어 보일 수 있어, 그 경로가 PASS 로 새는 것을 막는다.
    dirty = [s["id"] for s in scen
             if not s.get("cleanup", {}).get("attempted")
             or not s.get("cleanup", {}).get("succeeded")]
    errored = [s["id"] for s in scen if s.get("error")]

    reasons = []
    if partial:  reasons.append("부분 실행(--only)이므로 suite PASS를 주장할 수 없다")
    if not required:
        reasons.append("suite.required_scenarios 가 비어 있다 — 요구 목록 없이 PASS 는 없다")
    if not scen:
        reasons.append("실행된 시나리오가 0건이다")
    elif required and len(scen) < len(required):
        reasons.append(f"실행 {len(scen)}건 < required {len(required)}건")
    if skipped:  reasons.append(f"scenario.yaml 이 없어 건너뛴 디렉터리: {skipped}")
    if missing:  reasons.append(f"required 미실행: {missing}")
    if not_obs:  reasons.append(f"injection 미관측: {not_obs}")
    if incon:    reasons.append(f"판정 불가: {incon}")
    if leftover: reasons.append(f"cleanup 잔여 객체: {leftover}")
    if dirty:    reasons.append(f"cleanup 미시도/실패: {dirty}")
    if errored:  reasons.append(f"시나리오 오류: {errored}")
    if schema_errors: reasons.append(f"evidence 스키마 위반 {len(schema_errors)}건")

    # **실행 완결(execution_complete)과 완화 성립(mitigation_holds)은 다르다**(7차 리뷰 P2).
    # COUNTEREXAMPLE_REPRODUCED·MITIGATION_FAIL 도 pass=true 에 들어가는데, 그것은
    # "하네스가 끝까지 돌았다" 는 뜻이지 "설계가 통과했다" 가 아니다.
    holds = [s["id"] for s in scen if s["outcome"] == "MITIGATION_HOLDS"]
    reproduced = [s["id"] for s in scen if s["outcome"] == "COUNTEREXAMPLE_REPRODUCED"]
    mfail = [s["id"] for s in scen if s["outcome"] == "MITIGATION_FAIL"]
    return {"pass": not reasons, "rule": "all_required_executed_and_injection_observed",
            "execution_complete": not reasons,
            "mitigation_holds": holds,
            "counterexample_reproduced": reproduced,
            "mitigation_failed": mfail,
            "design_verdict_note": ("pass=true 는 **하네스가 끝까지 돌았다**는 뜻이다. "
                                    "설계 판정이 아니다 — reproduced/mitigation_failed 가 있으면 "
                                    "그것이 설계 결함이다."),
            "required_missing": missing, "not_observed": not_obs, "inconclusive": incon,
            "reason": "; ".join(reasons) if reasons else "모든 required 시나리오가 실행되고 injection이 관측됨"}

def validate_evidence(ev: dict, root: Path) -> list[str]:
    """evidence.schema.json 으로 자기 출력을 검증한다.

    jsonschema 가 없으면 그 사실을 **결함으로 보고**한다 — 조용히 건너뛰지 않는다."""
    sp = root / "evidence.schema.json"
    if not sp.is_file():
        return [f"evidence.schema.json 이 없다({sp})"]
    try:
        import jsonschema  # type: ignore
    except ImportError:
        return ["jsonschema 미설치 — evidence 를 검증하지 못했다 (pip install jsonschema)"]
    try:
        schema = json.loads(sp.read_text(encoding="utf-8"))
        probe = json.loads(json.dumps(ev, default=str))   # 직렬화 가능성까지 함께 본다
        errs = sorted(jsonschema.Draft202012Validator(schema).iter_errors(probe),
                      key=lambda e: list(e.path))
        return [f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message[:180]}" for e in errs]
    except Exception as e:  # noqa: BLE001
        return [f"검증 중 오류: {type(e).__name__}: {e}"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", default="suite.yaml")
    ap.add_argument("--out", default="evidence.json")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", default="", help="쉼표 구분 CE id. 지정하면 suite는 PASS가 될 수 없다.")
    ap.add_argument("--observed-env", default="",
                    help='선택. JSON 으로 신고한 신원이 preflight 가 서버에서 읽은 값과 '
                         '일치하는지 **교차 확인**한다. 생략해도 preflight 가 서버에서 직접 읽는다.')
    a = ap.parse_args()

    root = Path(a.suite).resolve().parent
    if not (root / "scenarios").is_dir():
        die(2, f"{root}/scenarios 가 없다. --suite 는 **패키지 안의** suite 파일을 가리켜야 한다 "
               f"(scenarios/ 와 같은 디렉터리). 지금 값: {a.suite}")
    suite = load_yaml(Path(a.suite))
    if suite.get("schema_version") != SCHEMA_VERSION or suite.get("suite_id") != SUITE_ID:
        die(2, "suite.yaml 의 schema_version/suite_id 가 runner와 맞지 않는다.")

    if a.dry_run:
        g = suite["environment_guard"]
        observed = {"primary_db_unique_name": g.get("expected_primary_db_unique_name", ""),
                    "standby_db_unique_name": g.get("expected_standby_db_unique_name", ""),
                    "schema": g.get("allowed_schema", ""), "standby_verified": False}
        print("[dry-run] 접속하지 않는다. 신원은 suite 의 expected 값을 그대로 쓴다.")
    else:
        observed = preflight(suite)
        # --observed-env 를 줬다면 **서버가 돌려준 값과도** 일치해야 한다.
        if a.observed_env:
            claimed = json.loads(a.observed_env)
            compared, unknown = [], []
            for k, v in claimed.items():
                if k not in observed:
                    unknown.append(k)          # 서버에서 읽지 않은 키 — 조용히 넘기지 않는다
                    continue
                if str(observed[k]) != str(v):
                    die(2, f"--observed-env 의 {k}={v!r} 가 서버가 돌려준 {observed[k]!r} 와 다르다. "
                           "운영자 신고와 실제 접속 대상이 어긋났다.")
                compared.append(k)
            if unknown:
                die(2, f"--observed-env 에 서버가 읽지 않는 키가 있다: {unknown}. "
                       "대조되지 않은 신고값을 '일치' 로 취급하지 않는다.")
            if not compared:
                die(2, "--observed-env 를 줬는데 대조된 키가 하나도 없다.")
            observed["_cross_checked_keys"] = compared
            print(f"[preflight] --observed-env 대조 {compared} 전부 서버 관측과 일치")

    # CE09 는 공시 검사로 HOLDS/FAIL 을 가른다. 대상 문서가 없으면 그 시나리오는
    # 자기 판정 기준을 한 번도 평가하지 못하고, suite 는 9개를 다 돌린 뒤에야 PASS 불가가 된다.
    # 시작 전에 알려 주는 편이 낫다.
    if not a.dry_run:
        doc = os.environ.get("CE_DOC_PATH")
        if not doc:
            die(2, "CE_DOC_PATH 가 없다. CE09 의 공시 검사 대상 문서 경로를 지정하라 "
                   "(이 패키지 tarball 에는 그 문서가 없다).")
        if not Path(doc).is_file():
            die(2, f"CE_DOC_PATH={doc!r} 가 파일이 아니다.")
        print(f"[preflight] CE_DOC_PATH={doc}")

    checks = enforce_guard(suite, observed)
    print(f"[guard] 통과: {', '.join(checks)}")

    # **코드 digest 와 suite 설정 digest 를 따로 기록한다**(7차 리뷰 P1-10).
    # suite.yaml 을 해시에서 빼면 required_scenarios·budgets·versions·pass_rule 이
    # 증거에 묶이지 않아, 다른 설정으로 돌린 결과를 같은 것으로 오인할 수 있다.
    h = artifact_hash(root, exclude={Path(a.suite).resolve().name, Path(a.out).name})
    suite_digest = hashlib.sha256(Path(a.suite).read_bytes()).hexdigest()
    declared = str(suite.get("artifact_sha256") or "")
    if declared and declared != h:
        die(2, f"artifact_sha256 불일치. 선언 {declared[:16]}… != 실제 {h[:16]}…")
    if not declared:
        print(f"[artifact] sha256={h}  ← suite.yaml 의 artifact_sha256 에 기록하라")

    only = {s.strip() for s in a.only.split(",") if s.strip()}
    sdirs = sorted(p for p in (root / "scenarios").iterdir()
                   if p.is_dir() and (p / "scenario.yaml").is_file())
    # 바이트코드·숨김 디렉터리는 시나리오 후보가 아니다. 그것 때문에 정상 실행이
    # 실패하면 안 되므로 skipped 에서 제외한다. 진짜로 scenario.yaml 이 없는
    # 시나리오 디렉터리만 남겨 판정에 반영한다.
    _NOT_SCENARIO = {"__pycache__", ".pytest_cache", ".ipynb_checkpoints"}
    skipped = sorted(p.name for p in (root / "scenarios").iterdir()
                     if p.is_dir() and not (p / "scenario.yaml").is_file()
                     and p.name not in _NOT_SCENARIO and not p.name.startswith("."))
    if skipped:
        print(f"[skip] scenario.yaml 이 없어 시나리오로 세지 않은 디렉터리: {skipped}")
    if only:
        sdirs = [p for p in sdirs if p.name[:4] in only]

    suite_budget = int((suite.get("budgets") or {}).get("suite_timeout_s", 3600) or 3600)
    deadline = None if a.dry_run else time.monotonic() + suite_budget
    scen = []
    for sd in sdirs:
        if deadline is not None and time.monotonic() >= deadline:
            print(f"[abort] suite_timeout_s({suite_budget}s) 소진 — 남은 시나리오를 실행하지 않는다")
            break
        print(f"[run] {sd.name}")
        scen.append(run_scenario(sd, suite, observed, a.dry_run, deadline))

    envrec = {"class": suite["environment_guard"]["class"],
              "primary_db_unique_name": observed["primary_db_unique_name"],
              "standby_db_unique_name": observed.get("standby_db_unique_name", ""),
              "schema": observed["schema"],
              "object_prefix": suite["environment_guard"]["object_prefix"],
              "guard_passed": True, "guard_checks": checks,
              "standby_verified": bool(observed.get("standby_verified"))}
    for k in ("database_role", "instance_name", "session_timezone", "db_timezone"):
        if observed.get(k):
            envrec[k] = observed[k]

    ev = {"schema_version": SCHEMA_VERSION, "suite_id": SUITE_ID,
          "run_id": str(uuid.uuid4()), "started_at": scen[0]["started_at"] if scen else now(),
          "finished_at": now(), "artifact_sha256": h,
          "suite_config_sha256": suite_digest,
          "environment": envrec,
          "versions": suite.get("versions", {}),
          "scenarios": scen,
          "suite_verdict": {"pass": False, "rule": "all_required_executed_and_injection_observed",
                            "required_missing": [], "not_observed": [], "inconclusive": [],
                            "reason": "판정 전"}}

    # **runner 는 자기 출력을 스키마로 검증한다.** 스키마 설명이 그렇게 약속하고 있다.
    schema_errors = validate_evidence(ev, root)
    ev["suite_verdict"] = verdict(suite, scen, partial=bool(only) or a.dry_run,
                                  skipped=skipped, schema_errors=schema_errors)
    for e in schema_errors[:5]:
        print(f"[schema] {e}")

    Path(a.out).write_text(json.dumps(ev, ensure_ascii=False, indent=1,
                                      default=str), encoding="utf-8")
    v = ev["suite_verdict"]
    print(f"\n[verdict] pass={v['pass']}  {v['reason']}")
    print(f"[evidence] {a.out}")
    return 0 if v["pass"] else 3

if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001
        print(f"[internal] {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(4)
