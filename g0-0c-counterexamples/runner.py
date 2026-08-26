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
    python3 runner.py --suite suite.yaml --dry-run                    # 가드·계획만
    python3 runner.py --suite suite.yaml --only CE01,CE04             # 부분 실행(PASS 불가)

종료 코드: 0 = suite PASS / 2 = 가드 실패(SUITE_ABORT) / 3 = PASS 아님 / 4 = 내부 오류
"""
from __future__ import annotations
import argparse, hashlib, json, os, re, subprocess, sys, uuid
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = "1.0.0"
SUITE_ID = "G0-0C-COUNTEREXAMPLES"
OUTCOMES = {"COUNTEREXAMPLE_REPRODUCED", "MITIGATION_HOLDS", "MITIGATION_FAIL",
            "INJECTION_NOT_OBSERVED", "INCONCLUSIVE"}
PASSING_OUTCOMES = {"COUNTEREXAMPLE_REPRODUCED", "MITIGATION_HOLDS", "MITIGATION_FAIL"}

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
        checks.append(f"{obs_key}=={exp}")

    if g.get("production_forbidden", True):
        pats = g.get("forbidden_name_patterns") or []
        blob = " ".join(str(v) for v in observed.values()).upper()
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
def run_scenario(sdir: Path, suite: dict, env: dict, dry: bool) -> dict:
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

    cmd = [sys.executable, str(entry), "--suite", json.dumps({
        "schema": env["schema"], "object_prefix": suite["environment_guard"]["object_prefix"],
        "budgets": suite.get("budgets", {}), "versions": suite.get("versions", {}),
    }, ensure_ascii=False)]
    try:
        timeout = int((suite.get("budgets") or {}).get("suite_timeout_s", 3600))
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                             cwd=str(sdir),
                             env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        payload = None
        for line in reversed(out.stdout.splitlines()):
            if line.startswith("SCENARIO_RESULT "):
                payload = json.loads(line[len("SCENARIO_RESULT "):])
                break
        if payload is None:
            rec["error"] = "SCENARIO_RESULT 라인이 없다(시나리오가 결과를 보고하지 않았다)."
            rec["observations"] = [{"name": "stderr_tail", "value": out.stderr[-800:]}]
        else:
            for k in ("outcome", "injection_observed", "injection_evidence",
                      "fixture", "observations", "cleanup"):
                if k in payload:
                    rec[k] = payload[k]
    except subprocess.TimeoutExpired:
        rec["error"] = "suite_timeout_s 초과"
    except Exception as e:  # noqa: BLE001
        rec["error"] = f"{type(e).__name__}: {e}"

    if rec["outcome"] not in OUTCOMES:
        rec["observations"].append({"name": "invalid_outcome", "value": rec["outcome"]})
        rec["outcome"] = "INCONCLUSIVE"
    # injection 증거 없이 재현/완화 판정을 주장할 수 없다.
    if rec["outcome"] in PASSING_OUTCOMES and not rec["injection_observed"]:
        rec["observations"].append({"name": "downgraded",
                                    "value": "injection_observed=false 이므로 INJECTION_NOT_OBSERVED로 강등"})
        rec["outcome"] = "INJECTION_NOT_OBSERVED"
    rec["finished_at"] = now()
    return rec

# ── 4. 판정 ─────────────────────────────────────────────────────────
def verdict(suite: dict, scen: list[dict], partial: bool) -> dict:
    required = list(suite.get("required_scenarios") or [])
    ran = {s["id"] for s in scen}
    missing = [r for r in required if r not in ran]
    not_obs = [s["id"] for s in scen if s["outcome"] == "INJECTION_NOT_OBSERVED"]
    incon = [s["id"] for s in scen if s["outcome"] == "INCONCLUSIVE"]
    leftover = [s["id"] for s in scen if s.get("cleanup", {}).get("leftover_objects")]

    reasons = []
    if partial:  reasons.append("부분 실행(--only)이므로 suite PASS를 주장할 수 없다")
    if missing:  reasons.append(f"required 미실행: {missing}")
    if not_obs:  reasons.append(f"injection 미관측: {not_obs}")
    if incon:    reasons.append(f"판정 불가: {incon}")
    if leftover: reasons.append(f"cleanup 잔여 객체: {leftover}")

    return {"pass": not reasons, "rule": "all_required_executed_and_injection_observed",
            "required_missing": missing, "not_observed": not_obs, "inconclusive": incon,
            "reason": "; ".join(reasons) if reasons else "모든 required 시나리오가 실행되고 injection이 관측됨"}

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", default="suite.yaml")
    ap.add_argument("--out", default="evidence.json")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", default="", help="쉼표 구분 CE id. 지정하면 suite는 PASS가 될 수 없다.")
    ap.add_argument("--observed-env", default="",
                    help='JSON: {"primary_db_unique_name":..,"standby_db_unique_name":..,"schema":..}. '
                         '생략하면 --dry-run 에서만 허용된다.')
    a = ap.parse_args()

    root = Path(a.suite).resolve().parent
    suite = load_yaml(Path(a.suite))
    if suite.get("schema_version") != SCHEMA_VERSION or suite.get("suite_id") != SUITE_ID:
        die(2, "suite.yaml 의 schema_version/suite_id 가 runner와 맞지 않는다.")

    if a.observed_env:
        observed = json.loads(a.observed_env)
    elif a.dry_run:
        g = suite["environment_guard"]
        observed = {"primary_db_unique_name": g.get("expected_primary_db_unique_name", ""),
                    "standby_db_unique_name": g.get("expected_standby_db_unique_name", ""),
                    "schema": g.get("allowed_schema", "")}
    else:
        die(2, "--observed-env 가 필요하다. 실제 접속에서 읽은 DB_UNIQUE_NAME·schema를 넘겨라.")

    checks = enforce_guard(suite, observed)
    print(f"[guard] 통과: {', '.join(checks)}")

    h = artifact_hash(root, exclude={Path(a.suite).resolve().name, Path(a.out).name})
    declared = str(suite.get("artifact_sha256") or "")
    if declared and declared != h:
        die(2, f"artifact_sha256 불일치. 선언 {declared[:16]}… != 실제 {h[:16]}…")
    if not declared:
        print(f"[artifact] sha256={h}  ← suite.yaml 의 artifact_sha256 에 기록하라")

    only = {s.strip() for s in a.only.split(",") if s.strip()}
    sdirs = sorted(p for p in (root / "scenarios").iterdir()
                   if p.is_dir() and (p / "scenario.yaml").is_file())
    skipped = sorted(p.name for p in (root / "scenarios").iterdir()
                     if p.is_dir() and not (p / "scenario.yaml").is_file())
    if skipped:
        print(f"[skip] scenario.yaml 이 없어 시나리오로 세지 않은 디렉터리: {skipped}")
    if only:
        sdirs = [p for p in sdirs if p.name[:4] in only]

    scen = []
    for sd in sdirs:
        print(f"[run] {sd.name}")
        scen.append(run_scenario(sd, suite, observed, a.dry_run))

    ev = {"schema_version": SCHEMA_VERSION, "suite_id": SUITE_ID,
          "run_id": str(uuid.uuid4()), "started_at": scen[0]["started_at"] if scen else now(),
          "finished_at": now(), "artifact_sha256": h,
          "environment": {"class": suite["environment_guard"]["class"],
                          "primary_db_unique_name": observed["primary_db_unique_name"],
                          "standby_db_unique_name": observed["standby_db_unique_name"],
                          "schema": observed["schema"],
                          "object_prefix": suite["environment_guard"]["object_prefix"],
                          "guard_passed": True, "guard_checks": checks},
          "versions": suite.get("versions", {}),
          "scenarios": scen,
          "suite_verdict": verdict(suite, scen, partial=bool(only) or a.dry_run)}

    Path(a.out).write_text(json.dumps(ev, ensure_ascii=False, indent=1), encoding="utf-8")
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
