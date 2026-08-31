#!/usr/bin/env python3
"""절차서 dry-run lint — **문서대로 실행하면 되는가** (9차 조치 2).

9차 교차 리뷰 P0-06. `g0-0-runbook.md` 는 *"이 문서만 따라가면 된다"* 고 적힌 실행 문서인데
**문서대로 하면 첫 wrapper 호출에서 exit 2 로 죽었다.** 8건이 어긋나 있었고 그중 다섯은 전부
같은 원인이다 — **M0·M1 이 새 필수값을 만들었는데 절차서를 같이 고치지 않았다.**

  M1-2  래퍼가 `G0_SOURCE_ID` 를 요구 → 절차서에 export 없음
  M0-4  B0 가 `--expect-db-unique-name` 을 요구 → 절차서 명령에 없음
  M1-4  산출물 경로에 `RUN_ID` 필수 → B1·CE 경로에 없음
  M0-5  CE 가 `CE_ENV_ALLOWLIST` 를 요구 → 절차서에 export 없음
  M4    정규화 명령이 `$DB_UNIQUE_NAME` 을 쓰는데 **정의하는 줄이 없음**

**계약을 강하게 만들 때마다 절차서가 뒤처진다.** 사람이 그때그때 맞추는 것으로는 안 된다 —
그래서 기계가 대조한다. 이 lint 는 절차서의 bash 블록을 파싱해 다음을 본다.

  1. 쓰이는 `$VAR` 가 **그 앞에서** export/대입되는가 (또는 알려진 외부 입력인가)
  2. 래퍼 호출의 산출물 경로에 `RUN_ID` 가 들어가는가
  3. 각 도구가 **코드에서** 필수라고 선언한 인자·환경변수가 명령에 있는가
  4. 참조하는 파일이 저장소에 실재하는가
  5. 문서가 가리키는 판본 문서가 현행인가

3번의 "코드에서" 가 요점이다. 필수 목록을 이 파일에 베껴 적으면 그것도 같이 뒤처진다.
**실물 스크립트를 읽어서** 뽑는다.

    python3 g0-runbook-lint.py
"""
from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent
RUNBOOK = ROOT / "g0-0-runbook.md"

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
# 절차서에서 실행 블록을 뽑는다
# ─────────────────────────────────────────────────────────────────────
def bash_blocks(md: str) -> list[tuple[int, str]]:
    """```bash 블록만. 인용(`>`) 안의 예시 블록도 포함한다 — 운영자는 그것도 복사한다."""
    out, line_no = [], 1
    for m in re.finditer(r"```bash\n(.*?)```", md, re.S):
        line_no = md[: m.start()].count("\n") + 1
        body = "\n".join(re.sub(r"^>\s?", "", ln) for ln in m.group(1).splitlines())
        out.append((line_no, body))
    return out


def assigned_vars(block: str) -> set[str]:
    """이 블록이 정의하는 변수. `export A=…` · `A=…` · `read … A` · `for A in`."""
    v: set[str] = set()
    v |= set(re.findall(r"^\s*export\s+([A-Za-z_][A-Za-z0-9_]*)=", block, re.M))
    # `export A=1 B=2`
    for m in re.finditer(r"^\s*export\s+(.+)$", block, re.M):
        for tok in m.group(1).split():
            if "=" in tok:
                v.add(tok.split("=", 1)[0])
    v |= set(re.findall(r"^\s*([A-Za-z_][A-Za-z0-9_]*)=", block, re.M))
    v |= set(re.findall(r"read\s+(?:-\w+\s+)*(?:-p\s+'[^']*'\s+)?([A-Za-z_][A-Za-z0-9_]*)", block))
    v |= set(re.findall(r"\bexport\s+([A-Za-z_][A-Za-z0-9_]*)\b", block))
    return v


def used_vars(block: str) -> set[str]:
    v = set(re.findall(r'\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?', block))
    return v


# shell·환경이 주는 것들. 절차서가 정의할 의무가 없다.
AMBIENT = {
    "PWD", "HOME", "PATH", "EDITOR", "SHELL", "USER", "PYTHONPATH", "TMPDIR",
    "PIPESTATUS", "RC", "1", "2",
    # 절차서 앞부분(탐색 회차)에서 세우는 것
    "SPARK_HOME",
}


def t_vars_defined_before_use() -> None:
    print("\n[1] 쓰이는 변수가 그 앞에서 정의되는가")
    md = RUNBOOK.read_text(encoding="utf-8")
    known = set(AMBIENT)
    bad: list[str] = []
    # **줄 단위로 본다.** 블록 단위로 보면 같은 블록 안에서 정의하고 바로 쓰는 정상 코드가
    # 전부 오탐이 된다 — 첫 판이 그랬다.
    for line_no, blk in bash_blocks(md):
        for off, line in enumerate(blk.splitlines()):
            # 같은 줄의 대입을 먼저 반영한다 — `export EVID=… && mkdir -p "$EVID"` 는
            # 정상 shell 이고 오탐으로 잡으면 안 된다.
            known |= assigned_vars(line)
            for u in sorted(used_vars(line)):
                if u in known or u.isdigit():
                    continue
                bad.append(f"{RUNBOOK.name}:{line_no + off} 에서 ${u} 를 쓰는데 "
                           f"앞에서 정의되지 않는다")
    check("정의되지 않은 변수를 쓰지 않는다", not bad, "; ".join(bad[:6]))


# ─────────────────────────────────────────────────────────────────────
# 필수 인자·환경변수를 **실물 스크립트에서** 뽑는다
# ─────────────────────────────────────────────────────────────────────
def required_argparse_flags(py: pathlib.Path) -> set[str]:
    """`ap.add_argument("--x", …, required=True)` 를 실물에서 읽는다."""
    src = py.read_text(encoding="utf-8")
    out = set()
    for m in re.finditer(r'add_argument\(\s*"(--[a-z0-9-]+)"(.*?)\)\s*\n', src, re.S):
        if "required=True" in m.group(2):
            out.add(m.group(1))
    return out


def b0_hard_required() -> set[str]:
    """B0 는 argparse 밖에서도 필수를 강제한다(M0-4). 그 사실을 코드에서 읽는다."""
    src = (ROOT / "g0-0b0-spark-smoke.py").read_text(encoding="utf-8")
    out = set()
    if "expect_db_unique_name" in src and "--expect-db-unique-name" in src:
        out.add("--expect-db-unique-name")
    return out


def shell_required_env(sh: pathlib.Path) -> set[str]:
    """`: "${VAR:?…}"` 와 `${VAR:-}` + 빈값 검사로 요구되는 환경변수."""
    src = sh.read_text(encoding="utf-8")
    out = set(re.findall(r'\$\{([A-Za-z_][A-Za-z0-9_]*):\?', src))
    # g0-run-child.sh 의 G0_SOURCE_ID 처럼 직접 검사하는 형태
    for m in re.finditer(r'([A-Z_][A-Z0-9_]*)="\$\{([A-Z_][A-Z0-9_]*):-\}"', src):
        var = m.group(2)
        if re.search(rf'{re.escape(var)} 가 없다|{re.escape(m.group(1))}" \]', src):
            out.add(var)
    return out


def t_required_flags_present() -> None:
    print("\n[2] 도구가 필수라고 선언한 인자가 명령에 있는가")
    md = RUNBOOK.read_text(encoding="utf-8")
    all_bash = "\n".join(b for _, b in bash_blocks(md))

    specs = {
        "g0-0b0-spark-smoke.py": required_argparse_flags(ROOT / "g0-0b0-spark-smoke.py")
                                 | b0_hard_required(),
        "g0-normalize.py": required_argparse_flags(ROOT / "g0-normalize.py"),
        "runner.py": required_argparse_flags(ROOT / "g0-0c-counterexamples" / "runner.py"),
    }
    for script, flags in specs.items():
        if script not in all_bash:
            check(f"{script} 가 절차서에 있다", False, "명령이 사라졌다면 이 lint 를 고쳐라")
            continue
        # 그 스크립트를 부르는 명령 조각만 본다(다음 빈 줄 또는 다음 명령까지).
        idx = all_bash.index(script)
        frag = all_bash[idx: idx + 900]
        missing = sorted(f for f in flags if f not in frag)
        check(f"{script}: 필수 인자 {sorted(flags)}", not missing, f"없는 것 {missing}")


def t_required_env_present() -> None:
    print("\n[3] 스크립트가 요구하는 환경변수가 export 되는가")
    md = RUNBOOK.read_text(encoding="utf-8")
    exported: set[str] = set(AMBIENT)
    for _, blk in bash_blocks(md):
        exported |= assigned_vars(blk)

    for sh in ("g0-run-child.sh", "g0-0b1-connection-provider/run.sh"):
        need = shell_required_env(ROOT / sh)
        missing = sorted(v for v in need if v not in exported)
        check(f"{sh}: 요구 환경변수 {sorted(need)}", not missing, f"export 없는 것 {missing}")

    # CE runner 의 외부 allowlist(M0-5)
    ce = (ROOT / "g0-0c-counterexamples" / "runner.py").read_text(encoding="utf-8")
    if "CE_ENV_ALLOWLIST" in ce:
        check("CE_ENV_ALLOWLIST 를 export 한다", "CE_ENV_ALLOWLIST" in exported,
              "runner 가 이것 없이는 접속 전에 멈춘다(M0-5)")


def t_artifact_paths_have_run_id() -> None:
    print("\n[4] 래퍼 산출물 경로에 RUN_ID 가 들어가는가 (M1-4)")
    md = RUNBOOK.read_text(encoding="utf-8")
    all_bash = "\n".join(b for _, b in bash_blocks(md))
    calls = re.findall(r'g0-run-child\.sh\s+(\S+)\s+"\$RUN_ID"\s+"\$PROFILE"\s+(\S+)', all_bash)
    check("래퍼 호출을 찾았다", len(calls) >= 4, f"{len(calls)}건")
    for child, art in calls:
        # $EVID 는 정의가 ~/g0/evidence/$RUN_ID 이므로 RUN_ID 를 포함한다.
        ok = ("RUN_ID" in art) or ("EVID" in art) or ("B1_OUT" in art)
        check(f"{child} 산출물 경로에 RUN_ID", ok,
              f"{art} — 고정 이름은 여러 회차의 별칭이 된다")


def t_referenced_files_exist() -> None:
    print("\n[5] 절차서가 가리키는 파일이 실재하는가")
    md = RUNBOOK.read_text(encoding="utf-8")
    all_bash = "\n".join(b for _, b in bash_blocks(md))
    # 확장자 뒤에 단어 문자가 오면 안 된다 — `.tgz.sha512` 가 `.sh` 로 잡히던 오탐.
    refs = set(re.findall(r'(?<![\w/.$-])((?:\./)?[\w./-]+\.(?:py|sh|sql|md|json|yaml|lock))(?![\w])',
                          all_bash))
    missing = []
    for r in sorted(refs):
        if "$" in r or "<" in r or r.endswith("-run.sql"):
            continue                     # 실행 중 만들어지는 것
        if r.startswith("/"):
            # **저장소 밖 절대 경로는 여기서 검사하지 않는다.** CE allowlist 는 패키지 밖에
            # 있어야 한다는 것이 M0-5 의 요건이다 — 없다고 실패시키면 요건과 반대로 간다.
            continue
        cand = [ROOT / r.lstrip("./"),
                ROOT / "g0-0b1-connection-provider" / r.lstrip("./"),
                ROOT / "g0-0c-counterexamples" / r.lstrip("./")]
        if not any(c.is_file() for c in cand):
            missing.append(r)
    check("참조 파일이 전부 실재한다", not missing, f"없는 것 {missing}")


def t_no_stale_version_pointer() -> None:
    print("\n[6] 현행이 아닌 판본 문서를 가리키지 않는가")
    md = RUNBOOK.read_text(encoding="utf-8")
    archs = sorted(p.name for p in ROOT.glob("etl-platform-target-architecture-v1.2*.md"))

    def ver(n):
        return [int(x) for x in re.findall(r"\d+", n.split("-v")[-1])]
    current = max(archs, key=ver)
    stale = [a for a in archs if a != current and a in md]
    check(f"현행 A({current})만 가리킨다", not stale, f"낡은 참조 {stale}")


def t_sql_placeholder_guard() -> None:
    print("\n[7] SQL 자리표시자를 그대로 넘기지 않는가")
    for f in ("g0-0a-capability-inventory.sql", "g0-0c-fence-facts.sql"):
        src = (ROOT / f).read_text(encoding="utf-8")
        ph = re.findall(r"^DEFINE\s+(\w+)\s*=\s*'(SCHEMA_NAME|TABLE_NAME|DBUNIQUENAME)'",
                        src, re.M)
        if ph:
            md = RUNBOOK.read_text(encoding="utf-8")
            check(f"{f} 의 자리표시자를 절차서가 경고한다",
                  "자리표시자" in md and "DEFINE" in md,
                  f"{[p[0] for p in ph]} 가 자리표시자인데 절차서가 그 사실을 안 말한다")
            check(f"{f} 를 원본 그대로 넘기지 않는다",
                  f"g0-sqlplus.sh {f}" not in md,
                  "원본을 그대로 넘기면 SCHEMA_NAME.TABLE_NAME 을 질의한다")


def main() -> int:
    print("=" * 70)
    print("절차서 dry-run lint — 9차 조치 2 (P0-06)")
    print("=" * 70)
    for t in (t_vars_defined_before_use, t_required_flags_present, t_required_env_present,
              t_artifact_paths_have_run_id, t_referenced_files_exist,
              t_no_stale_version_pointer, t_sql_placeholder_guard):
        t()
    print("\n" + "=" * 70)
    print(f"통과 {PASS}건 · 실패 {len(FAIL)}건")
    for f in FAIL:
        print(f"  - {f}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
