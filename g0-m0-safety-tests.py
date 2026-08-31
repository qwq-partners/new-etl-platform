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
import re
import subprocess
import sys
import json as _json_mod
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

# partitions=8 은 **하네스 상한**은 통과하지만 봉투가 승인하지 않으면 여전히 죽는다.
# 승인된 봉투를 주면 인자 검증을 지나 pyspark 부재로 실패해야 한다 — 통과가 불가능한
# 경로만 시험하면 "거부한다" 는 것만 알고 "허용한다" 는 것은 모른다.
with tempfile.TemporaryDirectory() as _td:
    _ep = Path(_td) / "env.json"
    _ep.write_text(_json_mod.dumps({"envelopes": {"DBX": {
        "approved_by": "test", "max_partitions": 8, "max_concurrent_sessions": 9,
        "max_active_runs": 1, "target_touch_allowed": True}}}), encoding="utf-8")
    _EA = {"ORA_PW": "x", "G0_RUN_ID": "rid-b", "G0_SOURCE_ENVELOPE": str(_ep),
           "G0_LEASE_DIR": str(Path(_td) / "lease")}
    r = run([sys.executable, str(B0), *BASE, "--partitions", "8"], env=_EA)
    check("승인된 봉투에서 partitions=8 은 인자·봉투 검증을 통과한다 "
          "(이후 pyspark 부재로 실패해도 2 가 아니다)",
          r.returncode != 2, f"returncode={r.returncode} {(r.stdout + r.stderr)[-300:]}")
    r = run([sys.executable, str(B0), *BASE, "--partitions", "9"], env=_EA)
    check("승인된 봉투여도 하네스 상한 8 은 넘지 못한다", r.returncode == 2,
          f"returncode={r.returncode}")

src = B0.read_text(encoding="utf-8")
check("MAX_PARTITIONS 가 코드에 있다", "MAX_PARTITIONS = 8" in src)
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

    # ── 9차 조치 7(P0-07 후반부) — allowlist 는 **위치**만 보인다 ──────
    # 파일이 패키지 밖에 있다는 것은 승인 주체·소유권·서명을 증명하지 않는다. 서명 체계를
    # 여기서 만들 수는 없으므로 할 수 있는 둘만 한다 — 승인자를 적게 강제하고, 그것이
    # 자가기재라는 사실을 증거에 남긴다. **못 하는 것을 한 척하지 않는다.**
    with tempfile.TemporaryDirectory() as td:
        bare = Path(td) / "bare.txt"
        bare.write_text("FREEPDB1\n", encoding="utf-8")
        r = run([sys.executable, str(CE), "--suite", str(suite), "--out", os.devnull],
                env={"CE_ENV_ALLOWLIST": str(bare)})
        out = r.stdout + r.stderr
        check("attestation 없는 allowlist → exit 2", r.returncode == 2,
              f"returncode={r.returncode}")
        check("네 항목을 이름으로 지목한다",
              all(k in out for k in
                  ("approved_by", "approved_at", "contact", "environment_is")), out[-300:])
        check("증명하지 못하는 것을 함께 말한다", "서명이 아니라 자가기재다" in out,
              out[-300:])
        check("접속 전에 죽는다 — preflight 까지 가지 않는다",
              "preflight" not in out, out[-300:])

        ok = Path(td) / "ok.txt"
        ok.write_text("#@ approved_by: 시험자\n#@ approved_at: 2026-08-31\n"
                      "#@ contact: t@example.com\n#@ environment_is: 폐기용 컨테이너\n"
                      "FREEPDB1\n", encoding="utf-8")
        r = run([sys.executable, str(CE), "--suite", str(suite), "--out", os.devnull],
                env={"CE_ENV_ALLOWLIST": str(ok)})
        out = r.stdout + r.stderr
        # **양성 대조.** attestation 을 채우면 allowlist 게이트를 지나야 한다. 이 뒤에서
        # 멈추는 것은 이 컨테이너에 oracledb 가 없기 때문이지 allowlist 때문이 아니다.
        check("attestation 을 채우면 allowlist 게이트를 지난다",
              "CE_ENV_ALLOWLIST" not in out, out[-300:])
        check("승인 자가기재를 화면에 적는다", "승인 자가기재" in out, out[-300:])
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
# **래퍼는 M1 계약도 요구한다**: G0_SOURCE_ID · 경로에 RUN_ID · 기존 산출물 비덮어쓰기.
import json as _json

RID = "RUN-M0-TEST"
WRAP_ENV = {"G0_SOURCE_ID": "TESTSTBY"}

with tempfile.TemporaryDirectory() as td:
    art = Path(td) / f"a-{RID}.json"
    r = run(["bash", str(HERE / "g0-run-child.sh"), "G0_0B0", RID,
             "SANDBOX_CONTAINER", str(art), "--",
             sys.executable, "-c",
             f"open({str(art)!r},'w').write('{{}}'); import sys; sys.exit(3)"],
            env=WRAP_ENV)
    check("child 가 3 으로 끝나면 래퍼도 3 으로 끝난다", r.returncode == 3,
          f"returncode={r.returncode}\n{r.stderr[-300:]}")
    man = Path(str(art) + ".manifest.json")
    if man.is_file():
        m = _json.loads(man.read_text(encoding="utf-8"))
        check("manifest 의 exit_code 가 3 이다", m.get("exit_code") == 3, str(m.get("exit_code")))
    else:
        check("manifest 가 생성된다", False)

print("\n[8] M1 — 래퍼가 증거 결속을 강제한다 (8차 M1-2·M1-4)")

with tempfile.TemporaryDirectory() as td:
    art = Path(td) / f"a-{RID}.json"
    # G0_SOURCE_ID 없이
    r = run(["bash", str(HERE / "g0-run-child.sh"), "G0_0B0", RID,
             "SANDBOX_CONTAINER", str(art), "--", sys.executable, "-c", "pass"],
            env={"G0_SOURCE_ID": ""})
    check("G0_SOURCE_ID 없이 → exit 2", r.returncode == 2, f"returncode={r.returncode}")
    check("사유가 G0_SOURCE_ID", "G0_SOURCE_ID" in (r.stdout + r.stderr))

    # 경로에 RUN_ID 가 없으면
    bad = Path(td) / "a.json"
    r = run(["bash", str(HERE / "g0-run-child.sh"), "G0_0B0", RID,
             "SANDBOX_CONTAINER", str(bad), "--", sys.executable, "-c", "pass"],
            env=WRAP_ENV)
    check("산출물 경로에 RUN_ID 가 없으면 → exit 2", r.returncode == 2,
          f"returncode={r.returncode}")
    check("사유가 RUN_ID 경로", "RUN_ID" in (r.stdout + r.stderr))

    # 이미 있는 산출물은 덮지 않는다
    exist = Path(td) / f"b-{RID}.json"
    exist.write_text("{}", encoding="utf-8")
    r = run(["bash", str(HERE / "g0-run-child.sh"), "G0_0B0", RID,
             "SANDBOX_CONTAINER", str(exist), "--", sys.executable, "-c", "pass"],
            env=WRAP_ENV)
    check("기존 산출물이 있으면 → exit 2 (회차 산출물은 불변)", r.returncode == 2,
          f"returncode={r.returncode}")
    check("새 RUN_ID 를 쓰라고 안내한다", "새 RUN_ID" in (r.stdout + r.stderr))

    # manifest 에 M1 필드가 들어간다
    ok = Path(td) / f"c-{RID}.json"
    r = run(["bash", str(HERE / "g0-run-child.sh"), "G0_0B0", RID,
             "SANDBOX_CONTAINER", str(ok), "--",
             sys.executable, "-c", f"open({str(ok)!r},'w').write('{{}}')"],
            env=WRAP_ENV)
    m2 = Path(str(ok) + ".manifest.json")
    if m2.is_file():
        d = _json.loads(m2.read_text(encoding="utf-8"))
        check("manifest 에 source_id 가 있다", d.get("source_id") == "TESTSTBY", str(d.get("source_id")))
        check("manifest 에 harness_digest 가 있다",
              isinstance(d.get("harness_digest"), str) and len(d["harness_digest"]) == 64,
              str(d.get("harness_digest"))[:40])
        check("manifest 에 started_at·ended_at 이 있다",
              bool(d.get("started_at")) and bool(d.get("ended_at")))
        check("manifest 에 overwrote_existing 이 있다", "overwrote_existing" in d)
    else:
        check("manifest 가 생성된다(M1 필드 확인)", False, r.stderr[-300:])

print("\n[10] 9차 조치 6 — 원천 안전 봉투 (P0-04)")

sys.path.insert(0, str(HERE))
import g0_source_envelope as _envl  # noqa: E402

_DEF = _envl.load()
check("기본 봉투의 max_partitions 는 1이다",
      _DEF["envelopes"]["default"]["max_partitions"] == 1,
      str(_DEF["envelopes"]["default"].get("max_partitions")))
check("기본 봉투는 대상 질의를 허용하지 않는다",
      _DEF["envelopes"]["default"]["target_touch_allowed"] is False)
check("기본 봉투는 미승인으로 표시된다",
      str(_DEF["envelopes"]["default"]["approved_by"]).startswith("UNAPPROVED"))

# **핵심 회귀**: 세션 상한이 파티션 상한에서 유도되면 그 검사는 죽은 코드다.
# 두 값이 독립인 봉투에서만 세션 검사가 파티션 검사와 **따로** 걸린다.
_INDEP = {"envelopes": {"S": {"approved_by": "owner", "max_partitions": 4,
                             "max_concurrent_sessions": 2, "max_active_runs": 1,
                             "target_touch_allowed": True}}}
_v = _envl.check_request("S", partitions=3, wants_target_touch=True, doc=_INDEP)
check("세션 검사가 파티션 검사와 독립으로 걸린다(죽은 분기가 아니다)",
      any("동시 세션" in x for x in _v) and not any("승인 상한" in x and "num-partitions" in x
                                                   for x in _v),
      str(_v))

# 모순된 봉투 자체를 거부한다 — 이것이 P0-04 가 잡은 `MAX_CONCURRENT_SESSIONS=12` 다.
_DEAD = {"envelopes": {"S": {"approved_by": "owner", "max_partitions": 8,
                            "max_concurrent_sessions": 12, "max_active_runs": 1,
                            "target_touch_allowed": True}}}
check("max_concurrent_sessions=12 · max_partitions=8 봉투를 모순으로 거부한다",
      any("스스로 모순" in x for x in
          _envl.check_request("S", partitions=1, wants_target_touch=True, doc=_DEAD)))

# 봉투가 허용하는 요청은 통과한다 — 통과가 불가능한 검사는 검사가 아니다.
_OK = {"envelopes": {"S": {"approved_by": "owner", "max_partitions": 2,
                          "max_concurrent_sessions": 3, "max_active_runs": 1,
                          "target_touch_allowed": True}}}
check("승인된 봉투 안의 요청은 위반이 없다",
      _envl.check_request("S", partitions=2, wants_target_touch=True, doc=_OK) == [],
      str(_envl.check_request("S", partitions=2, wants_target_touch=True, doc=_OK)))
check("전용 봉투가 없으면 default 로 떨어졌다는 사실을 위반으로 적는다",
      any("전용 봉투가 없어" in x for x in
          _envl.check_request("UNKNOWN_SRC", partitions=1, wants_target_touch=False)))

with tempfile.TemporaryDirectory() as td:
    _ld = Path(td)
    _l1 = _envl.acquire("S", "run-1", doc=_OK, lease_dir=_ld)
    try:
        _envl.acquire("S", "run-2", doc=_OK, lease_dir=_ld)
        check("같은 원천에 두 번째 회차가 붙지 못한다", False, "두 번째 acquire 가 통과했다")
    except _envl.EnvelopeError as e:
        check("같은 원천에 두 번째 회차가 붙지 못한다", "run-1" in str(e), str(e)[:120])
    _envl.release(_l1)
    try:
        _l2 = _envl.acquire("S", "run-2", doc=_OK, lease_dir=_ld)
        check("앞 회차가 놓으면 다음 회차가 붙는다", True)
        _envl.release(_l2)
    except _envl.EnvelopeError as e:
        check("앞 회차가 놓으면 다음 회차가 붙는다", False, str(e)[:120])

print("\n[11] 9차 조치 6 — B0·B1 이 봉투 없이 붙지 않는다")

# B0 는 봉투 위반에서 **spark 를 띄우기 전에** 죽어야 한다.
r = run([sys.executable, str(B0), *BASE], env={"ORA_PW": "x", "G0_RUN_ID": "rid-1"})
check("B0 가 미승인 봉투에서 exit 2 로 죽는다", r.returncode == 2, f"rc={r.returncode}")
check("B0 가 봉투 위반이라고 말한다", "봉투" in (r.stdout + r.stderr),
      (r.stdout + r.stderr)[-300:])
check("B0 가 pyspark 를 부르기 전에 죽는다", "pyspark" not in (r.stdout + r.stderr).lower(),
      (r.stdout + r.stderr)[-300:])

# run-id 가 없으면 봉투 이전에 죽는다 — lease 를 익명으로 쥐지 않는다.
r = run([sys.executable, str(B0), *BASE], env={"ORA_PW": "x", "G0_RUN_ID": ""})
check("B0 가 run-id 없이 붙지 않는다", r.returncode == 2 and "run_id" in r.stdout,
      (r.stdout + r.stderr)[-300:])

# 래퍼의 source_id 와 스크립트의 기대 신원이 다르면 죽는다.
r = run([sys.executable, str(B0), *BASE],
        env={"ORA_PW": "x", "G0_RUN_ID": "rid-1", "G0_SOURCE_ID": "OTHER"})
check("B0 가 G0_SOURCE_ID 불일치를 거부한다",
      r.returncode == 2 and "source_id" in r.stdout, (r.stdout + r.stderr)[-300:])

# 래퍼가 회차·원천 신원을 환경으로 넘긴다.
_w = (HERE / "g0-run-child.sh").read_text(encoding="utf-8")
check("래퍼가 G0_RUN_ID 를 export 한다", 'export G0_RUN_ID="$RUN_ID"' in _w)

# 도달 불가능한 세션 상한 상수를 코드에서 없앴다.
for _f in ("g0-0b0-spark-smoke.py", "g0-0b1-connection-provider/run-g0-0b1.py"):
    _src = (HERE / _f).read_text(encoding="utf-8")
    # **상수 선언**이 없어야 한다. 주석 안의 언급(왜 없앴는지 설명)은 세지 않는다.
    check(f"{_f} 에 MAX_CONCURRENT_SESSIONS 상수 선언이 없다",
          not re.search(r"(?m)^MAX_CONCURRENT_SESSIONS\s*=", _src))

print("\n[9] M0-6 — README 의 probe 수와 실행 명령")

rd = (HERE / "README.md").read_text(encoding="utf-8")
check("README 가 87 probe 로 적혀 있다", "87 probe" in rd)
check("README 에 86 probe 표기가 남아 있지 않다", "86 probe" not in rd)

print("\n[12] 9차 P2-1 — A SQL 의 '대상 테이블 접촉' 목록이 실제와 같은가")

# 이 저장소는 **손으로 센 숫자를 세 번 틀렸다**(HANDOFF §2 의 c_expected 표). 접촉
# 건수도 같은 종류다 — 8차까지 "네 건" 이라고 적혀 있었고 실제로는 다섯 건이었다.
# 그래서 숫자를 고치는 데서 멈추지 않고 **다시 어긋나면 시험이 실패하게** 만든다.
import re as _re

_asql = (HERE / "g0-0a-capability-inventory.sql").read_text(encoding="utf-8")
_body = _asql.split("-- 안전 규칙", 1)[1] if "-- 안전 규칙" in _asql else _asql
_head = _asql.split("-- 안전 규칙", 1)[0]

_TARGET = "&TARGET_OWNER..&TARGET_TABLE"
_actual = {}
for m in _re.finditer(r"p_(?:scalar|stmt)\s*\(\s*'([^']+)'\s*,(.*?)\);", _body, _re.S):
    name, sql = m.group(1), m.group(2)
    if _TARGET in sql:
        bounds = [int(x) for x in _re.findall(r"ROWNUM\s*<?=\s*(\d+)", sql)]
        _actual[name] = max(bounds) if bounds else None

# 머리말이 선언한 목록 — `--         · 이름   설명` 꼴
# 이름은 백틱으로 감싸여 있고, 행 수 표기에 굵게(`**`)가 붙기도 한다.
_declared = dict(_re.findall(r"^--\s+·\s+`?([^`\s]+)`?\s+.*?최대 (\d+)행", _head, _re.M))
_declared = {k: int(v) for k, v in _declared.items()}

check("머리말이 접촉 문장을 하나도 빠짐없이 적는다",
      set(_declared) == set(_actual),
      f"머리말만={sorted(set(_declared) - set(_actual))} 실제만={sorted(set(_actual) - set(_declared))}")
check("건수 표기가 실제와 같다", f"**{'다섯' if len(_actual) == 5 else len(_actual)} 건**" in _head
      or f"**다섯 건**" in _head and len(_actual) == 5,
      f"실제 {len(_actual)}건")
check("문장별 최대 행 수가 머리말과 같다",
      all(_declared.get(k) == v for k, v in _actual.items()),
      str({k: (v, _declared.get(k)) for k, v in _actual.items() if _declared.get(k) != v}))
check("합계 최대 행 수 표기가 맞다",
      f"최대 {sum(v for v in _actual.values() if v)}행" in _head,
      f"실제 합계 {sum(v for v in _actual.values() if v)}행")
# **빈 집합으로 통과하지 않는다.**
check("접촉 문장이 실제로 있다", len(_actual) > 0, str(len(_actual)))
# 옛 표기가 남아 있지 않은가 (M0-6 과 같은 형태의 잔존 검사)
check("'한 건뿐' 이라는 옛 표기가 남아 있지 않다",
      "ROWNUM = 1 한 건뿐" not in _asql)

print("\n" + "=" * 70)
print(f"통과 {PASS}건 · 실패 {FAIL}건")
sys.exit(1 if FAIL else 0)
