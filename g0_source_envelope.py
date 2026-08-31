"""원천 안전 봉투와 회차 lease (9차 조치 6 · P0-04).

9차 리뷰가 지적한 것 셋.

  · 보호가 **반환 행 수**에 치우쳐 있다. `ROWNUM <= n` 은 empty/sparse heap 에서 읽는
    block 수·wall-clock·physical read 의 상한이 아니다.
  · `MAX_CONCURRENT_SESSIONS = 12` 가 **도달 불가능한 분기**였다 — `partitions ≤ 8` 이
    이미 강제되므로 추정 세션은 최대 9다. 안전 검사처럼 보이지만 죽은 코드다.
  · **한 Spark read 의 partition 상한은 source 전체의 안전 budget 이 아니다.**
    여러 회차가 동시에 붙으면 아무것도 보장하지 않는다.

그래서 둘을 둔다.

  **봉투**  `g0-source-envelope.json` 이 원천별 승인값을 선언한다. 하네스가 스스로 정하는
            값이 아니라 **원천 소유자가 승인한 정책**이며, 승인 전에는 `approved_by` 가
            그 사실을 적는다. 요청이 봉투를 넘으면 **원천에 붙기 전에** 죽는다.
  **lease** 같은 원천에 동시 회차가 붙는 것을 막는다. 파일 기반이라 한 호스트 안에서만
            유효하다 — **그 한계를 숨기지 않는다**(`limits` 참조). 여러 호스트에서 도는
            회차는 이것으로 막히지 않으며, 그때는 조직 절차가 유일한 방어다.
"""
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import time

_BUNDLED = pathlib.Path(__file__).resolve().parent / "g0-source-envelope.json"

# **승인된 봉투는 저장소 밖에 둘 수 있다.** M0-5 가 CE allowlist 를 `/etc/g0/` 에 둔 것과
# 같은 이유다 — 원천 소유자의 승인은 **사이트 정책**이지 저장소 내용이 아니다. 저장소에
# 승인값을 커밋하면, 저장소를 복제한 누구나 그 승인을 들고 다니게 된다.
# 지정하지 않으면 번들된 파일을 쓰고, 그것의 `default` 는 미승인이라 아무것도 통과하지 않는다.
ENVELOPE_FILE = pathlib.Path(os.environ["G0_SOURCE_ENVELOPE"]).expanduser() \
    if os.environ.get("G0_SOURCE_ENVELOPE") else _BUNDLED

# lease 디렉터리. 호스트 하나 안에서만 유효하다.
LEASE_DIR = pathlib.Path(os.environ.get("G0_LEASE_DIR", "/tmp/g0-source-lease"))

LIMITS = (
    "이 lease 는 **한 호스트 안에서만** 유효하다(파일 기반). 다른 호스트·다른 컨테이너에서 "
    "같은 원천에 붙는 회차는 막지 못한다. 원천 전체의 동시성 보장은 DB 측 장치(profile "
    "resource limit·Resource Manager)나 조직 절차만 할 수 있으며 그것은 이 저장소 밖이다."
)


class EnvelopeError(RuntimeError):
    """봉투 위반. 호출자는 **원천에 붙기 전에** 이것으로 죽어야 한다."""


def load(path: pathlib.Path | None = None) -> dict:
    # 매번 환경을 다시 본다. 모듈 로드 시점에 굳히면 시험이 서로를 오염시키고,
    # 무엇보다 **어느 파일을 읽었는지**가 호출 시점에 결정되어야 정직하다.
    p = path or (pathlib.Path(os.environ["G0_SOURCE_ENVELOPE"]).expanduser()
                 if os.environ.get("G0_SOURCE_ENVELOPE") else _BUNDLED)
    return json.loads(p.read_text(encoding="utf-8"))


def provenance() -> dict:
    """**어느 봉투가 이 회차를 지배했는가.**

    `harness_digest` 는 저장소에 번들된 봉투를 센다. `$G0_SOURCE_ENVELOPE` 로 덮으면
    실제로 적용된 정책은 그 digest 에 들어 있지 않다 — 그러면 digest 가 "이 정책으로
    쟀다" 를 말하지 못한다. 그래서 실제로 읽은 파일의 경로와 digest 를 증거에 남긴다.
    """
    p = (pathlib.Path(os.environ["G0_SOURCE_ENVELOPE"]).expanduser()
         if os.environ.get("G0_SOURCE_ENVELOPE") else _BUNDLED)
    try:
        sha = hashlib.sha256(p.read_bytes()).hexdigest()
    except OSError:
        sha = "MISSING"
    return {"path": str(p), "sha256": sha, "bundled": p == _BUNDLED,
            "note": ("번들 봉투다 — harness_digest 가 이 내용을 센다" if p == _BUNDLED else
                     "**저장소 밖 봉투다** — harness_digest 는 이 내용을 세지 않는다. "
                     "적용된 정책은 위 sha256 이 가리킨다")}


def envelope_for(source_id: str, doc: dict | None = None) -> tuple[dict, bool]:
    """(봉투, 그 원천 전용인가). 전용이 없으면 `default` 로 떨어진다."""
    d = doc or load()
    env = d.get("envelopes", {})
    if source_id and source_id in env:
        return env[source_id], True
    if "default" not in env:
        raise EnvelopeError("g0-source-envelope.json 에 default 봉투가 없다")
    return env["default"], False


def check_request(source_id: str, *, partitions: int, wants_target_touch: bool = True,
                  doc: dict | None = None) -> list[str]:
    """요청이 봉투 안인가. 위반 목록을 돌려준다 — 비어 있지 않으면 붙지 마라."""
    env, specific = envelope_for(source_id, doc)
    V: list[str] = []

    mp = int(env.get("max_partitions", 1))
    if partitions > mp:
        V.append(f"--num-partitions={partitions} 가 원천 {source_id!r} 의 승인 상한 {mp} 을 "
                 f"넘는다. **파티션 하나가 원천 세션 하나다** — 이 값은 하네스가 아니라 "
                 f"원천 소유자가 g0-source-envelope.json 에서 정한다")

    ms = int(env.get("max_concurrent_sessions", mp + 1))
    est = partitions + 1                      # executor 파티션 + driver
    if est > ms:
        V.append(f"예상 동시 세션 {est}(파티션 {partitions} + driver 1)가 승인 상한 {ms} 를 "
                 f"넘는다")
    # **상한이 도달 가능한지 자체를 본다.** 9차 P0-04 가 잡은 것이 이것이다 —
    # max_partitions=8 에 max_concurrent_sessions=12 면 그 분기는 죽은 코드다.
    if ms > mp + 1:
        V.append(f"봉투가 스스로 모순이다: max_concurrent_sessions={ms} 가 "
                 f"max_partitions+1={mp + 1} 보다 크다 — **세션 검사가 어떤 입력에서도 걸리지 "
                 f"않는다.** 안전 검사처럼 보이는 죽은 코드를 두지 않는다(9차 P0-04)")

    if wants_target_touch and not env.get("target_touch_allowed", False):
        V.append(f"원천 {source_id!r} 의 봉투가 대상 테이블 질의를 허용하지 않는다"
                 f"(target_touch_allowed=false). 열려면 access plan 또는 검증된 index path 와 "
                 f"함께 원천 소유자가 승인해야 한다(9차 M5c)")

    if not specific:
        V.append(f"원천 {source_id!r} 전용 봉투가 없어 default 를 썼다. default 의 "
                 f"approved_by 는 미승인이다 — **사내 원천에는 전용 봉투를 만들어라**")
    elif str(env.get("approved_by", "")).startswith("UNAPPROVED"):
        V.append(f"원천 {source_id!r} 의 봉투가 아직 승인되지 않았다(approved_by=UNAPPROVED)")
    return V


def statement_timeout(source_id: str, doc: dict | None = None) -> int:
    env, _ = envelope_for(source_id, doc)
    return int(env.get("statement_timeout_seconds", 30))


# ── lease ────────────────────────────────────────────────────────────
def acquire(source_id: str, run_id: str, doc: dict | None = None,
            lease_dir: pathlib.Path | None = None) -> pathlib.Path:
    """같은 원천에 동시 회차가 붙는 것을 막는다.

    `O_EXCL` 로 만든다 — 존재 확인 후 생성하면 그 사이에 다른 회차가 들어온다.
    """
    env, _ = envelope_for(source_id, doc)
    cap = int(env.get("max_active_runs", 1))
    d = lease_dir or LEASE_DIR
    d.mkdir(parents=True, exist_ok=True)

    live = [p for p in d.glob(f"{_safe(source_id)}.*.lease") if _alive(p)]
    if len(live) >= cap:
        holders = [json.loads(p.read_text(encoding="utf-8")).get("run_id") for p in live]
        raise EnvelopeError(
            f"원천 {source_id!r} 에 이미 회차 {holders} 가 붙어 있다(상한 {cap}). "
            f"**한 회차의 partition 상한은 회차가 겹치면 아무것도 보장하지 않는다**(9차 P0-04). "
            f"먼저 끝내거나 원천 소유자가 max_active_runs 를 올려야 한다")

    path = d / f"{_safe(source_id)}.{_safe(run_id)}.lease"
    fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump({"source_id": source_id, "run_id": run_id, "pid": os.getpid(),
                   "acquired_at": time.time(), "limits": LIMITS}, fh, ensure_ascii=False)
    return path


def release(path: pathlib.Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def _safe(s: str) -> str:
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in str(s))


def _alive(p: pathlib.Path) -> bool:
    """lease 를 쥔 프로세스가 아직 사는가. 죽었으면 그 lease 는 쓰레기다."""
    try:
        pid = int(json.loads(p.read_text(encoding="utf-8")).get("pid", -1))
    except (OSError, ValueError, json.JSONDecodeError):
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        p.unlink(missing_ok=True)        # 죽은 회차의 lease 를 치운다
        return False
    except PermissionError:
        return True                      # 다른 사용자의 살아 있는 프로세스
