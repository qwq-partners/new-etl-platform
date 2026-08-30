#!/usr/bin/env python3
"""M0(실행 안전성) 회귀 시험 — 8차 교차 리뷰 §11 M0.

**이 시험이 막는 것은 "안전 장치가 있다"가 아니라 "안전 장치가 실제로 거부한다"이다.**
7차 리뷰 P0-02 가 잡은 결함이 정확히 그것이었다 — 통과 모양의 결과를 찍고 exit 1 로
죽어도 집계기가 PASS 후보로 셌다. 그래서 여기서는 **종료 코드를 본다.**

돌리는 법: python3 g0-m0-safety-tests.py
Oracle·Spark 없이 돈다 — 인자 검증과 종료 코드 전파만 시험하기 때문이다.
"""
from __future__ import annotations
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
PASS = FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}" + (f"\n        {detail}" if detail else ""))


def run(cmd: list[str], env: dict | None = None, cwd: Path | None = None):
    e = {**os.environ, **(env or {})}
    return subprocess.run(cmd, capture_output=True, text=True, env=e,
                          cwd=str(cwd or HERE), timeout=60)


# ─────────────────────────────────────────────────────────────────────
print("\n[1] M0-2 — B0 는 main() 의 반환값을 종료 코드로 내보낸다")

B0 = HERE / "g0-0b0-spark-smoke.py"
BASE = ["--url", "jdbc:oracle:thin:@//h:1521/s", "--user", "U",
        "--table", "S.T", "--wm", "WM", "--expect-db-unique-name", "DBX"]

r = run([sys.executable, str(B0), *BASE, "--probe-rows", "0"], env={"ORA_PW": "x"})
check("probe_rows=0 → exit 2 (0 이 아니다)", r.returncode == 2,
      f"returncode={r.returncode}\n{r.stdout[-400:]}")
check("거부 사유를 산출물에 남긴다", "probe_rows" in (r.stdout + r.stderr))

r = run([sys.executable, str(B0), *BASE, "--probe-rows", "100001"], env={"ORA_PW": "x"})
check("probe_rows 상한 초과 → exit 2", r.returncode == 2, f"returncode={r.returncode}")

print("\n[2] M0-3 — partitions·sessions 하드 상한이 실제로 거부한다")

r = run([sys.executable, str(B0), *BASE, "--partitions", "0"], env={"ORA_PW": "x"})
check("partitions=0 → exit 2", r.returncode == 2, f"returncode={r.returncode}")

r = run([sys.executable, str(B0), *BASE, "--partitions", "9"], env={"ORA_PW": "x"})
check("partitions=9 (상한 8 초과) → exit 2", r.returncode == 2, f"returncode={r.returncode}")
check("파티션=세션이라는 이유를 적는다", "세션" in (r.stdout + r.stderr))

r = run([sys.executable, str(B0), *BASE, "--partitions", "8"], env={"ORA_PW": "x"})
check("partitions=8 (경계값)은 인자 검증을 통과한다 — 이후 pyspark 부재로 실패해도 2 가 아니다",
      r.returncode != 2, f"returncode={r.returncode}")

src = B0.read_text(encoding="utf-8")
check("MAX_PARTITIONS 가 코드에 있다", "MAX_PARTITIONS = 8" in src)
check("MAX_CONCURRENT_SESSIONS 가 코드에 있다", "MAX_CONCURRENT_SESSIONS" in src)
check("`sys.exit(main())` 이다 — 반환값을 버리지 않는다", "sys.exit(main())" in src)

print("\n[3] M0-4 — 신원 preflight 는 필수 인자이고 대상 접촉 전에 온다")

r = run([sys.executable, str(B0),
         "--url", "jdbc:oracle:thin:@//h:1521/s", "--user", "U",
         "--table", "S.T", "--wm", "WM"], env={"ORA_PW": "x"})
check("--expect-db-unique-name 없이는 실행되지 않는다", r.returncode != 0,
      f"returncode={r.returncode}")
check("argparse 가 그 인자를 required 로 잡는다",
      "expect-db-unique-name" in (r.stderr + r.stdout))

# **본문** 위치로 본다. EXPECTED_STEPS 목록에도 같은 문자열이 있으므로 첫 index 를
# 쓰면 목록 안에서 비교하게 되고, 그 비교는 아무것도 검사하지 않는다.
i_pre = src.index("# ── S-1. 신원 preflight")
i_s0 = src.index("# ── S0. 기준선")
check("preflight 코드가 S0 기준선 읽기보다 앞에 있다", i_pre < i_s0,
      f"preflight@{i_pre} S0@{i_s0}")
# **여기가 핵심이다.** q10 문자열이 어디서 정의되는지는 상관없다 — 문자열은 원천을
# 건드리지 않는다. 원천을 실제로 읽는 것은 `.load()` 다. preflight 앞에 `.load()` 가
# 하나라도 있으면 "대상 접촉 전 검사"가 성립하지 않는다.
head = src[:i_pre]
loads_before = head.count(".load()")
check("preflight 앞에 .load() 가 하나도 없다 — 대상 접촉 전이라는 뜻",
      loads_before == 0, f"preflight 앞 .load() {loads_before}건")
seg = src[i_pre:i_s0]
check("preflight 구간은 DUAL 만 읽는다 — 대상 테이블을 쓰지 않는다",
      "FROM DUAL" in seg and "{a.table}" not in seg,
      f"구간 길이 {len(seg)}")
check("EXPECTED_STEPS 에 preflight 가 들어 있다 — 빠지면 미완주로 잡힌다",
      '"S-1.identity_preflight"' in src.split("def emit")[0])

print("\n[4] M0-3 — B1 도 같은 상한을 건다")

B1 = HERE / "g0-0b1-connection-provider" / "run-g0-0b1.py"
b1src = B1.read_text(encoding="utf-8")
check("B1 에 MAX_PARTITIONS 가 있다", "MAX_PARTITIONS = 8" in b1src)
check("B1 이 상한을 실제로 검사한다", "1 <= a.num_partitions <= MAX_PARTITIONS" in b1src)
check("B1 도 `sys.exit(main())` 이다", "sys.exit(main())" in b1src)

r = run([sys.executable, str(B1), "--url", "u", "--user", "U", "--table", "S.T",
         "--num-partitions", "9"], env={"ORA_PW": "x"})
check("B1 num-partitions=9 → exit 2", r.returncode == 2, f"returncode={r.returncode}")

print("\n[5] M0-5 — CE 는 패키지 밖 allowlist 없이 실행되지 않는다")

CE = HERE / "g0-0c-counterexamples" / "runner.py"
cesrc = CE.read_text(encoding="utf-8")
check("load_external_allowlist 가 있다", "def load_external_allowlist" in cesrc)
check("CE_ENV_ALLOWLIST 를 요구한다", "CE_ENV_ALLOWLIST" in cesrc)
check("패키지 안 경로를 거부하는 검사가 있다", "relative_to(root.resolve())" in cesrc)
check("enforce_guard 가 allowlist 를 인자로 받는다",
      "def enforce_guard(suite: dict, observed: dict, allowlist: list[str])" in cesrc)
check("외부 allowlist 검사가 suite 비교보다 먼저다",
      cesrc.index("external_allowlist_ok") < cesrc.index('g.get("class") !='))
check("증거에 allowlist digest 를 남긴다", '"env_allowlist_sha256"' in cesrc)

# 실제 거부 동작 — 파일 없이 / 패키지 안 경로로
suite = HERE / "g0-0c-counterexamples" / "suite.yaml"
if suite.is_file():
    r = run([sys.executable, str(CE), "--suite", str(suite), "--out", os.devnull],
            env={"CE_ENV_ALLOWLIST": ""})
    check("CE_ENV_ALLOWLIST 없이 → exit 2", r.returncode == 2, f"returncode={r.returncode}")

    inside = HERE / "g0-0c-counterexamples" / "_m0_test_allowlist.txt"
    try:
        inside.write_text("DBX\n", encoding="utf-8")
        # 배포되는 suite.yaml 의 expected_* 는 비어 있다(운영자가 채운다). 그래서
        # dry-run 도 가드에서 멈추는 것이 정상이다. 여기서 보는 것은
        # **allowlist 때문에 멈추지 않는다**는 것 하나다.
        r = run([sys.executable, str(CE), "--suite", str(suite), "--out", os.devnull,
                 "--dry-run"], env={"CE_ENV_ALLOWLIST": ""})
        check("--dry-run 은 allowlist 부재를 이유로 멈추지 않는다",
              "CE_ENV_ALLOWLIST" not in (r.stdout + r.stderr),
              f"returncode={r.returncode}\n{(r.stdout + r.stderr)[-300:]}")

        r = run([sys.executable, str(CE), "--suite", str(suite), "--out", os.devnull],
                env={"CE_ENV_ALLOWLIST": str(inside)})
        check("패키지 **안**의 allowlist → exit 2 (승인 근거가 되지 못한다)",
              r.returncode == 2, f"returncode={r.returncode}")
        check("거부 사유가 '패키지 안'임을 말한다", "패키지 안" in (r.stdout + r.stderr))
    finally:
        inside.unlink(missing_ok=True)

    with tempfile.TemporaryDirectory() as td:
        empty = Path(td) / "allow.txt"
        empty.write_text("# 주석만 있고 항목이 없다\n\n", encoding="utf-8")
        r = run([sys.executable, str(CE), "--suite", str(suite), "--out", os.devnull],
                env={"CE_ENV_ALLOWLIST": str(empty)})
        check("빈 allowlist → exit 2", r.returncode == 2, f"returncode={r.returncode}")
else:
    print("  SKIP  suite.yaml 이 없어 CE 실행 시험을 건너뛴다")

print("\n[6] M0-1 — 래퍼와 헬퍼가 종료 코드를 삼키지 않는다")

for name in ("g0-run-child.sh", "g0-sqlplus.sh",
             "g0-0b1-connection-provider/run.sh"):
    f = HERE / name
    if not f.is_file():
        check(f"{name} 존재", False)
        continue
    t = f.read_text(encoding="utf-8")
    check(f"{name}: pipefail", "pipefail" in t)

wrap = (HERE / "g0-run-child.sh").read_text(encoding="utf-8")
check("래퍼가 child 종료 코드를 manifest 에 적는다", "exit_code" in wrap and "$RC" in wrap)
check("래퍼가 child 종료 코드로 자신도 끝난다", "exit $RC" in wrap)
runsh = (HERE / "g0-0b1-connection-provider" / "run.sh").read_text(encoding="utf-8")
check("run.sh 가 tee 뒤에서 PIPESTATUS 로 producer exit 를 잡는다",
      "PIPESTATUS[0]" in runsh)

# 래퍼가 실제로 비0 을 보존하는지 — 껍데기 명령으로 확인
with tempfile.TemporaryDirectory() as td:
    art = Path(td) / "a.json"
    art.write_text("{}", encoding="utf-8")
    r = run(["bash", str(HERE / "g0-run-child.sh"), "G0_0B0", "RUN-M0-TEST",
             "SANDBOX_CONTAINER", str(art), "--",
             sys.executable, "-c", "import sys; sys.exit(3)"])
    check("child 가 3 으로 끝나면 래퍼도 3 으로 끝난다", r.returncode == 3,
          f"returncode={r.returncode}")
    man = Path(str(art) + ".manifest.json")
    if man.is_file():
        import json
        m = json.loads(man.read_text(encoding="utf-8"))
        check("manifest 의 exit_code 가 3 이다", m.get("exit_code") == 3, str(m.get("exit_code")))
    else:
        check("manifest 가 생성된다", False)

print("\n[7] M0-6 — README 의 probe 수와 실행 명령")

rd = (HERE / "README.md").read_text(encoding="utf-8")
check("README 가 87 probe 로 적혀 있다", "87 probe" in rd)
check("README 에 86 probe 표기가 남아 있지 않다", "86 probe" not in rd)

print("\n" + "=" * 70)
print(f"통과 {PASS}건 · 실패 {FAIL}건")
sys.exit(1 if FAIL else 0)
