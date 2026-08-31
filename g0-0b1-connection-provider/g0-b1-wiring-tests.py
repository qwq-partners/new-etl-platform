#!/usr/bin/env python3
"""B1 종단 배선 시험 — **실물 `run.sh` 를 돌린다** (9차 조치 1).

9차 교차 리뷰 P0-03 이 무엇을 잡았나. `run.sh` 는 회차를 `failclosed_task` 라 부르고
`-Dg0b1.run` 에 그 값을 넣는데, python 에는 `--mode failclosed` 만 넘겼다. python 은
phase 파일·마커·terminal token 을 전부 **mode 이름**으로 썼고 provider 는 **run 이름**으로
읽었다. 그래서

  · `failclosed_task` 는 `declaredPhase()` 가 `UNDECLARED` → **주입이 아예 안 걸린다**
  · 두 회차 다 terminal token 이름이 달라 analyzer 가 못 찾는다 → **`PROVEN` 도달 불가**

**그런데 회귀 시험 72건이 전부 통과했다.** `InjectionMatrix` 는 `shouldFail()` 을 순수
함수로 시험하고, `g0-b1-analyzer-tests.py` 는 producer 가 만들지 **않는** 토큰을 합성해
판정기에 넣는다. 둘 다 통과하는데 **그 사이 배선은 아무도 안 본다.**

이 파일이 그 사이를 시험한다. 원리는 하나다 — **판정기에 합성 입력을 주지 않는다.**
실물 `run.sh` 를 돌리고, 그것이 만든 산출물을 실물 `analyze-trace.py` 에 넣는다.

## Oracle 도 Spark 도 없이 어떻게 실물을 도는가

둘을 대역으로 세운다. **대역이 흉내 내는 것은 판정 대상이 아니라 판정 대상의 상대편이다.**

  spark-submit 대역   `--conf spark.driver.extraJavaOptions` 의 `-D` 를 뽑아 환경으로 넘기고
                      실제 `run-g0-0b1.py` 를 실행한다
  pyspark 대역        JVM 쪽을 연기한다 — **`Trace.java`·`Preamble.java` 와 같은 규칙으로**
                      phase 파일 이름을 만들고, 같은 규칙으로 주입을 정하고,
                      같은 모양의 추적 라인을 쓴다

대역이 Java 와 다른 규칙을 쓰면 이 시험은 무의미하다. 그래서 **§1 이 Java 소스에서 규칙을
직접 읽어 대역과 대조**한다. 그 대조가 깨지면 시험 자체가 실패한다.

## 무엇을 시험하지 않는가

실제 Spark 배포판·ServiceLoader·Oracle connection·executor 분산은 여기 없다. 이 시험이
말하는 것은 **"launcher·producer·판정기가 같은 이름을 쓰는가"** 하나뿐이다. 그것이 P0-03 의
전부였다.

    python3 g0-b1-wiring-tests.py
"""
from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
FAIL: list[str] = []
PASS = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL.append(f"{name} — {detail}")
        print(f"  FAIL  {name}  {detail}")


# ─────────────────────────────────────────────────────────────────────
# 대역이 흉내 내는 Java 규칙 — **소스에서 읽어 대조한다**
# ─────────────────────────────────────────────────────────────────────
def java_run_sanitize(run: str) -> str:
    """`Trace.run()` 의 정규화. 같은 정규식을 쓴다."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run)


def java_phase_filename(run: str) -> str:
    """`Trace.phaseFile()` 이 만드는 basename."""
    return f"g0-0b1-phase-{java_run_sanitize(run)}.txt"


def java_trace_filename(run: str, jvm: str) -> str:
    return f"g0-0b1-trace-{java_run_sanitize(run)}-{jvm}.jsonl"


def t_java_rule_matches_source() -> None:
    """§1 — 대역이 쓰는 규칙이 `Trace.java` 의 규칙과 같은가.

    이 시험이 실패하면 아래 모든 시험이 무의미하다. Java 를 고치고 대역을 안 고치면
    대역이 통과시켜도 실물은 어긋난다 — P0-03 과 정확히 같은 종류의 사고다.
    """
    print("\n[1] 대역의 규칙이 Trace.java 의 규칙과 같은가")
    src = (HERE / "src/main/java/etl/g0b1/Trace.java").read_text(encoding="utf-8")

    check("Trace.run() 의 정규식이 [^A-Za-z0-9_.-] 다",
          'replaceAll("[^A-Za-z0-9_.-]", "_")' in src, "정규식이 바뀌었다면 대역도 고쳐라")
    check("phase 파일이 g0-0b1-phase-<run>.txt 다",
          '"g0-0b1-phase-" + run() + ".txt"' in src,
          "패턴이 바뀌었다면 java_phase_filename() 도 고쳐라")
    check("추적 파일이 g0-0b1-trace-<run>-<jvm>.jsonl 다",
          '"g0-0b1-trace-" + run() + "-" + jvm + ".jsonl"' in src,
          "패턴이 바뀌었다면 java_trace_filename() 도 고쳐라")

    # python 쪽 phase 파일 이름이 같은 규칙인가 — **정적 대조**
    py = (HERE / "run-g0-0b1.py").read_text(encoding="utf-8")
    check("python 의 phase 파일이 a.run 으로 만들어진다",
          'f"g0-0b1-phase-{a.run}.txt"' in py,
          "a.mode 로 되어 있으면 그것이 P0-03 이다")
    check("python 도 같은 정규화를 한다",
          're.sub(r"[^A-Za-z0-9_.-]", "_", a.run)' in py,
          "정규화가 없으면 특수문자가 든 run 이름에서 다시 갈린다")


# ─────────────────────────────────────────────────────────────────────
# 대역 만들기
# ─────────────────────────────────────────────────────────────────────
SPARK_SUBMIT = r'''#!/usr/bin/env bash
# spark-submit 대역 (g0-b1-wiring-tests.py). **실제 Spark 가 아니다.**
# 하는 일은 둘뿐이다 — extraJavaOptions 의 -D 를 환경으로 옮기고, python 을 실행한다.
set -uo pipefail
props='{}'
args=()
while [ $# -gt 0 ]; do
  case "$1" in
    --master|--jars|--driver-class-path) shift 2;;
    --conf)
      case "$2" in
        spark.driver.extraJavaOptions=*)
          opts="${2#spark.driver.extraJavaOptions=}"
          props=$(OPTS="$opts" python3 -c '
import json,os,shlex
d={}
for tok in shlex.split(os.environ["OPTS"]):
    if tok.startswith("-D") and "=" in tok:
        k,v=tok[2:].split("=",1); d[k]=v
print(json.dumps(d))')
          ;;
      esac
      shift 2;;
    *) args+=("$1"); shift;;
  esac
done
export G0B1_JVM_PROPS="$props"
export PYTHONPATH="$G0B1_STUB_DIR${PYTHONPATH:+:$PYTHONPATH}"
exec python3 "${args[@]}"
'''

PYSPARK_STUB = r'''"""pyspark 대역 — **JVM 쪽을 연기한다**(g0-b1-wiring-tests.py).

이 대역이 흉내 내는 것은 판정 대상이 아니라 그 상대편이다. `Trace.java` 와 같은 규칙으로
phase 파일 이름을 만들고, `Preamble.shouldFail` 과 같은 규칙으로 주입을 정하고,
`TracingConnectionProvider` 와 같은 모양의 추적 라인을 쓴다.

**규칙이 Java 와 다르면 시험이 거짓말을 한다.** 그래서 시험 §1 이 Java 소스에서 규칙을
읽어 이 파일과 대조한다.
"""
import atexit, json, os, pathlib, re

_PROPS = json.loads(os.environ.get("G0B1_JVM_PROPS") or "{}")
_JVM = "stubjvm"
_LINES = [0]


def _prop(k, d=None):
    v = _PROPS.get(k)
    return d if v is None or v == "" else v


def _sanitize(r):
    return re.sub(r"[^A-Za-z0-9_.-]", "_", r)


def _run():
    return _sanitize(_prop("g0b1.run", "unspecified"))


def _trace_dir():
    return pathlib.Path(_prop("g0b1.trace.dir", "/tmp"))


def _phase_file():
    # Trace.phaseFile() 과 같은 규칙.
    return _trace_dir() / ("g0-0b1-phase-" + _run() + ".txt")


def _declared_phase():
    f = _phase_file()
    if not f.is_file():
        return "UNDECLARED"
    v = f.read_text(encoding="utf-8").strip()
    return v or "UNDECLARED"


def _should_fail(declared):
    # Preamble.shouldFail() 과 같은 규칙.
    fail = (_prop("g0b1.fail", "none") or "none").strip()
    if not fail or fail.lower() == "none":
        return False
    if fail.lower() == "all":
        return True
    want = (_prop("g0b1.fail.phase", "") or "").strip()
    if not want:
        return False
    return want.lower() == declared.lower()


def _trace(obj):
    f = _trace_dir() / ("g0-0b1-trace-" + _run() + "-" + _JVM + ".jsonl")
    f.parent.mkdir(parents=True, exist_ok=True)
    with f.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
    _LINES[0] += 1


@atexit.register
def _end():
    if _PROPS:
        _trace({"event": "trace_end", "run": _run(), "jvm": _JVM,
                "lines_written": _LINES[0]})


class _PreambleFailure(Exception):
    pass


def _open_connection():
    """provider 가 물리 connection 하나를 여는 것을 연기한다."""
    declared = _declared_phase()
    inject = _should_fail(declared)
    err = ("[g0-0b1] 의도적 프리앰블 실패 (declared_phase=%s)" % declared) if inject else None
    _trace({"event": "connection", "run": _run(), "jvm": _JVM,
            "declared_phase": declared, "fail_phase": _prop("g0b1.fail.phase", ""),
            "path_guess": "STUB", "injection_applied": bool(inject and err),
            "preamble_error": err, "open_error": None})
    if inject:
        raise _PreambleFailure(err)


class _Conf:
    def get(self, k, default=None):
        return default


class _Ctx:
    class _JvmSystem:
        @staticmethod
        def getProperty(name):
            return _PROPS.get(name)

    class _Jvm:
        System = None

    def __init__(self):
        self._jvm = _Ctx._Jvm()
        self._jvm.System = _Ctx._JvmSystem()

    def getConf(self):
        return _Conf()


class _Field:
    def __init__(self, n):
        self.name = n


class _Schema:
    fields = [_Field("C1"), _Field("C2")]


class _DF:
    def __init__(self):
        _open_connection()          # schema 해석용 connection

    @property
    def schema(self):
        return _Schema()

    def count(self):
        _open_connection()          # task connection
        return 0

    def rdd(self):
        return self


class _Reader:
    def __init__(self):
        self._o = {}

    def format(self, *_a):
        return self

    def options(self, **kw):
        self._o.update(kw)
        return self

    def option(self, k, v):
        self._o[k] = v
        return self

    def load(self):
        return _DF()


class _Read:
    def format(self, *_a):
        return _Reader()


class SparkSession:
    version = "stub-4.2.0"

    class _Builder:
        def appName(self, *_a):
            return self

        def getOrCreate(self):
            return SparkSession()

    builder = _Builder()

    def __init__(self):
        self.sparkContext = _Ctx()
        self.read = _Read()

    def stop(self):
        pass
'''


def make_stub(root: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    """spark-submit 대역과 pyspark 대역을 만든다."""
    spark_home = root / "fakespark"
    (spark_home / "bin").mkdir(parents=True, exist_ok=True)
    ss = spark_home / "bin" / "spark-submit"
    ss.write_text(SPARK_SUBMIT, encoding="utf-8")
    ss.chmod(0o755)

    stub = root / "stub"
    (stub / "pyspark").mkdir(parents=True, exist_ok=True)
    (stub / "pyspark" / "__init__.py").write_text("", encoding="utf-8")
    (stub / "pyspark" / "sql.py").write_text(PYSPARK_STUB, encoding="utf-8")
    return spark_home, stub


def approved_envelope(root: pathlib.Path, source_id: str = "ETLSTB") -> pathlib.Path:
    """이 시험용 **승인된 봉투**(9차 조치 6).

    B1 은 봉투 없이 원천에 붙지 않는다. 저장소에 번들된 `default` 는 미승인이라
    `run.sh` 가 spark-submit 직전에 죽는다 — 그것이 정상 동작이다. 여기서는 배선을
    시험하려는 것이므로 승인된 봉투를 만들어 준다. 봉투가 **없을 때** 죽는 것은
    `t_envelope_gate()` 가 따로 본다.
    """
    p = root / "approved-envelope.json"
    p.write_text(json.dumps({"envelopes": {source_id: {
        "approved_by": "g0-b1-wiring-tests", "max_partitions": 1,
        "max_concurrent_sessions": 2, "max_active_runs": 1,
        "statement_timeout_seconds": 30, "target_touch_allowed": True}}},
        ensure_ascii=False), encoding="utf-8")
    return p


def run_real_runsh(root: pathlib.Path, *, break_wiring: bool = False,
                   envelope: pathlib.Path | None = None):
    """**실물 `run.sh` 를 돌린다.**

    `break_wiring=True` 면 `--run` 전달을 지운 판으로 돌린다 — 음성 대조다.
    그 판에서도 통과한다면 이 시험은 아무것도 시험하지 않는 것이다.
    """
    work = root / ("broken" if break_wiring else "wired")
    shutil.copytree(HERE, work, ignore=shutil.ignore_patterns(
        "build", "*.jar", "__pycache__", ".git"))
    if break_wiring:
        sh = work / "run.sh"
        s = sh.read_text(encoding="utf-8")
        s = s.replace('--run "$run" ', '')      # P0-03 의 상태로 되돌린다
        sh.write_text(s, encoding="utf-8")
    (work / "g0-0b1-tracer.jar").write_text("stub", encoding="utf-8")
    (work / "run.sh").chmod(0o755)

    spark_home, stub = make_stub(root)
    env = dict(os.environ)
    env.update({
        "SPARK_HOME": str(spark_home),
        "G0B1_STUB_DIR": str(stub),
        "ORA_PW": "unused-stub",
        "OJDBC_JAR": str(root / "ojdbc-stub.jar"),
        # ── 9차 조치 6 ────────────────────────────────────────────────
        # `work` 는 저장소 밖으로 복사된 사본이라 B1 의 `dirname(dirname(__file__))`
        # 로는 `g0_source_envelope` 를 찾지 못한다. 실제 저장소 루트를 PYTHONPATH 로 준다.
        "PYTHONPATH": str(HERE.parent) + os.pathsep + os.environ.get("PYTHONPATH", ""),
        "G0_SOURCE_ID": "ETLSTB",
        "G0_RUN_ID": "wiring-test",
        "G0_LEASE_DIR": str(root / "lease"),
        "G0_SOURCE_ENVELOPE": str(envelope) if envelope else
                              str(approved_envelope(root)),
    })
    (root / "ojdbc-stub.jar").write_text("stub", encoding="utf-8")
    r = subprocess.run(
        ["bash", "run.sh", "jdbc:oracle:thin:@//127.0.0.1:1/NO_SUCH", "NOBODY",
         "X.Y", "ETLSTB", "PHYSICAL_STANDBY", "none"],
        cwd=str(work), env=env, capture_output=True, text=True, timeout=300)
    ev = work / "g0-0b1-evidence.json"
    evidence = json.loads(ev.read_text(encoding="utf-8")) if ev.is_file() else {}
    return r, evidence, work


# ─────────────────────────────────────────────────────────────────────
def t_end_to_end() -> None:
    print("\n[2] 실물 run.sh → 실물 analyze-trace.py 종단")
    root = pathlib.Path(tempfile.mkdtemp(prefix="g0b1wire-"))
    try:
        r, ev, work = run_real_runsh(root)
        if not ev:
            check("증거 파일이 생겼다", False, f"stdout={r.stdout[-400:]} stderr={r.stderr[-400:]}")
            return
        check("증거 파일이 생겼다", True)

        terms = {t.get("run") for t in ev.get("terminal_tokens", [])}
        check("terminal token 의 run 이 launcher 라벨과 같다",
              {"coverage", "failclosed_schema", "failclosed_task"} <= terms,
              f"관측된 run: {sorted(terms)}")

        per = (ev.get("verdict") or {}).get("per_run") or {}
        if not per:
            for f in ev.get("findings", []):
                if isinstance(f.get("observed"), dict) and "per_run" in f["observed"]:
                    per = f["observed"]["per_run"]
        for rn in ("failclosed_schema", "failclosed_task"):
            d = per.get(rn) or {}
            check(f"{rn}: 주입이 실제로 걸렸다",
                  (d.get("injection_applied") or 0) > 0,
                  f"injection_applied={d.get('injection_applied')} — "
                  f"phase 파일 이름이 갈리면 여기가 0 이 된다(P0-03)")
            check(f"{rn}: terminal token 을 찾았다",
                  d.get("terminal_token_present") is True, str(d)[:160])

        # phase 파일이 실제로 provider(대역)가 읽는 이름으로 쓰였는가
        tdir = None
        for ln in r.stdout.splitlines():
            if ln.startswith("[run] trace dir: "):
                tdir = pathlib.Path(ln.split(": ", 1)[1].strip())
        if tdir and tdir.is_dir():
            names = {p.name for p in tdir.glob("g0-0b1-phase-*.txt")}
            # **`metadata_probe` 는 여기 없는 것이 정상이다.** `metadata_only` 시나리오는
            # 이 하네스가 유발하지 못하는 경로라 step 이 하나도 돌지 않고, phase 를 선언할
            # 일도 없다. 그 회차를 남기는 이유는 0 건을 "없다" 로 읽지 않기 위해서다.
            want = {java_phase_filename(x) for x in
                    ("coverage", "failclosed_schema", "failclosed_task")}
            check("phase 파일 이름이 Java 규칙과 같다", want <= names,
                  f"없는 것: {sorted(want - names)} / 있는 것: {sorted(names)}")
            check("metadata_probe 는 phase 를 선언하지 않는다(step 이 없다)",
                  java_phase_filename("metadata_probe") not in names,
                  "유발하지 못하는 경로가 phase 를 선언하면 그것이 더 이상한 일이다")
        else:
            check("추적 디렉터리를 찾았다", False, "run.sh 출력에서 trace dir 을 못 읽었다")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t_negative_control() -> None:
    """**음성 대조.** 배선을 P0-03 상태로 되돌리면 이 시험이 실패해야 한다.

    이것이 없으면 위 시험이 '무조건 통과하는 시험'과 구분되지 않는다. 9차가 지적한 것이
    정확히 그것이다 — 통과하는데 아무것도 보증하지 않는 시험.
    """
    print("\n[3] 음성 대조 — --run 전달을 지우면 시험이 잡아내는가")
    root = pathlib.Path(tempfile.mkdtemp(prefix="g0b1wire-neg-"))
    try:
        r, ev, work = run_real_runsh(root, break_wiring=True)
        terms = {t.get("run") for t in ev.get("terminal_tokens", [])}
        check("배선을 깨면 launcher 라벨의 terminal token 이 없다",
              not ({"failclosed_schema", "failclosed_task"} <= terms),
              f"관측된 run: {sorted(terms)} — 깨진 판에서도 맞으면 이 시험은 무의미하다")

        per = (ev.get("verdict") or {}).get("per_run") or {}
        for f in ev.get("findings", []):
            if isinstance(f.get("observed"), dict) and "per_run" in f["observed"]:
                per = f["observed"]["per_run"]
        d = per.get("failclosed_task") or {}
        check("배선을 깨면 failclosed_task 주입이 0 이다",
              (d.get("injection_applied") or 0) == 0,
              f"injection_applied={d.get('injection_applied')}")
        check("배선을 깨면 PROVEN 이 나오지 않는다",
              (ev.get("verdict") or {}).get("coverage") != "PROVEN",
              str((ev.get("verdict") or {}).get("coverage")))
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t_envelope_gate() -> None:
    """**봉투 게이트가 실물 경로에서 실제로 막는가**(9차 조치 6 · P0-04).

    앞의 시험들은 승인된 봉투를 만들어 준다. 그러면 "봉투가 있으면 돈다" 만 알고
    "없으면 안 돈다" 는 모른다 — 9차가 지적한 시험 경계 문제와 같은 모양이다.
    여기서는 **미승인 봉투**로 실물 `run.sh` 를 돌려 원천에 붙기 전에 죽는지 본다.
    """
    print("\n[4] 봉투 게이트 — 미승인 봉투에서 실물 run.sh 가 붙지 않는가")
    root = pathlib.Path(tempfile.mkdtemp(prefix="g0b1wire-env-"))
    try:
        # 저장소에 번들된 봉투 그대로다. `default` 는 UNAPPROVED 이고
        # target_touch_allowed=false 다.
        bundled = HERE.parent / "g0-source-envelope.json"
        r, ev, work = run_real_runsh(root, envelope=bundled)
        out = r.stdout + r.stderr
        check("미승인 봉투에서 run.sh 가 실패한다", r.returncode != 0,
              f"rc={r.returncode}")
        check("사유가 봉투 위반이다", "봉투" in out, out[-400:])
        check("미승인이라는 사실을 말한다",
              "UNAPPROVED" in out or "미승인" in out or "전용 봉투가 없어" in out,
              out[-400:])
        # 대상 질의 승인이 없으면 **spark-submit 이 실제 읽기를 하기 전에** 죽어야 한다.
        check("대상 질의 미승인을 사유로 든다", "target_touch_allowed" in out, out[-400:])
        check("terminal token 이 하나도 없다 — 원천에 붙지 않았다",
              not ev.get("terminal_tokens"), str(ev.get("terminal_tokens"))[:200])
    finally:
        shutil.rmtree(root, ignore_errors=True)


def main() -> int:
    print("=" * 70)
    print("B1 종단 배선 시험 — 9차 조치 1 (실물 run.sh 를 돈다)")
    print("=" * 70)
    for t in (t_java_rule_matches_source, t_end_to_end, t_negative_control,
              t_envelope_gate):
        t()
    print("\n" + "=" * 70)
    print(f"통과 {PASS}건 · 실패 {len(FAIL)}건")
    for f in FAIL:
        print(f"  - {f}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
