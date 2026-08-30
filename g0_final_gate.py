"""최종 G0 게이트의 **입구**. G0-0 레코드는 여기서 항상 거절된다.

8차 교차 리뷰 M3-4 의 조치다. 7차 P0-04 에서 `record_type` 을 `g0_0_evidence` 로 갈라
놓았지만, **그 구분을 강제하는 코드가 없었다** — 이름만 다르고 아무도 확인하지 않았다.
8차 §12 필수 회귀 시험 마지막 줄이 그것이다: "G0-0 record를 final G0 gate에 입력하면
항상 거부".

## 이 파일이 하지 않는 것

**G0-1~G0-5 를 집계하지 않는다.** 그 산출물이 아직 하나도 없기 때문이다. 실물 없는
aggregator 를 미리 쓰면 그것이 무엇을 집계하는지 스스로도 말할 수 없고, 나중에 실물이
나왔을 때 코드를 고치는 대신 실물을 코드에 맞추게 된다. `aggregate()` 는 그래서
`NotImplementedError` 다 — **미구현을 미구현이라고 적는 것이 이 함수의 내용이다.**

## 이 파일이 하는 것

입구 하나. 어떤 레코드가 최종 G0 게이트의 입력이 될 자격이 있는가.

    admit(record) -> (bool, [사유…])

세 가지를 본다.

1. `record_type == "g0_evidence"` — `g0_0_evidence` 는 **무조건** 거절이다.
   completeness 가 COMPLETE 든 아니든, 계약 위반이 0건이든 아니든 상관없다.
2. `gate_eligible is True` — G0-0 스키마에서 이 값은 `const false` 라 바꿀 수단이 없다.
3. `g0-final-contract.json` 의 항목이 전부 있는가 — 부분 레코드로 게이트를 통과하지 못한다.

사용:

    python3 g0_final_gate.py <record.json>      # exit 0 통과 / 1 거절 / 2 실행 전 조건 미비
"""
from __future__ import annotations

import json
import pathlib
import sys

CONTRACT_FILE = pathlib.Path(__file__).resolve().parent / "g0-final-contract.json"
FINAL_RECORD_TYPE = "g0_evidence"


def load_contract(path: pathlib.Path | None = None) -> dict:
    p = path or CONTRACT_FILE
    return json.loads(p.read_text(encoding="utf-8"))


def contract_items(contract: dict | None = None) -> list[str]:
    c = contract or load_contract()
    return [str(i["item"]) for i in c.get("items", []) if isinstance(i, dict) and "item" in i]


def covered_items(contract: dict | None = None) -> list[dict]:
    """G0-0 이 덮는다고 계약이 선언한 항목."""
    c = contract or load_contract()
    return [i for i in c.get("items", []) if i.get("g0_0") == "COVERED"]


def not_covered_items(contract: dict | None = None) -> list[dict]:
    c = contract or load_contract()
    return [i for i in c.get("items", []) if i.get("g0_0") == "NOT_COVERED"]


def resolve(record: dict, where: str):
    """`a.b` 와 `a[*].b` 만 지원하는 최소 경로 해석기.

    반환은 값 목록이다. 비어 있으면 그 항목은 이 레코드에 없다. `[*]` 는 dict 의 값들을
    훑는다 — `children` 이 배열이 아니라 child 이름을 키로 하는 객체이기 때문이다.
    """
    cur: list = [record]
    for part in where.split("."):
        nxt: list = []
        star = part.endswith("[*]")
        key = part[:-3] if star else part
        for c in cur:
            if not isinstance(c, dict):
                continue
            v = c.get(key)
            if v is None:
                continue
            if star:
                nxt.extend(v.values() if isinstance(v, dict) else
                           (v if isinstance(v, list) else [v]))
            else:
                nxt.append(v)
        cur = nxt
    return [v for v in cur if v not in (None, "", [], {})]


def admit(record: dict, contract: dict | None = None) -> tuple[bool, list[str]]:
    """이 레코드가 최종 G0 게이트의 입력이 될 자격이 있는가."""
    reasons: list[str] = []
    if not isinstance(record, dict):
        return False, ["레코드가 객체가 아니다"]

    rt = record.get("record_type")
    if rt != FINAL_RECORD_TYPE:
        reasons.append(
            f"record_type={rt!r} — 최종 게이트는 {FINAL_RECORD_TYPE!r} 만 받는다. "
            f"G0-0 레코드(g0_0_evidence)는 **무조건** 거절이다: G0-0 completed 는 G0 PASS 가 "
            f"아니며 G0 PASS 는 G0-1 ∧ G0-2 ∧ G0-3 ∧ G0-4 ∧ same_lock(G0-5) 이다")
        # 여기서 멈추지 않는다. 나머지 사유도 함께 보여야 왜 부분 레코드인지 드러난다.

    if record.get("gate_eligible") is not True:
        reasons.append(f"gate_eligible={record.get('gate_eligible')!r} — 게이트 입력은 "
                       f"스스로 true 를 선언해야 한다")

    c = contract or load_contract()
    missing = [it for it in contract_items(c) if not resolve(record, it)]
    if missing:
        reasons.append(f"최종 계약 항목 누락 {missing} — 부분 레코드는 게이트 입력이 아니다")

    return (not reasons), reasons


def aggregate(*_args, **_kwargs):
    """G0-1~G0-5 집계기. **아직 없다.**"""
    raise NotImplementedError(
        "최종 G0 aggregator 는 구현되지 않았다. G0-1(DDL)·G0-2(판정 SQL)·G0-3(canonical "
        "hash 벡터)·G0-4(제출 경로) 산출물이 하나도 없기 때문이다. 실물 없이 집계기를 쓰면 "
        "나중에 실물을 집계기에 맞추게 된다 — 그 방향은 증거가 아니다.")


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__.strip().splitlines()[0], file=sys.stderr)
        print("사용: python3 g0_final_gate.py <record.json>", file=sys.stderr)
        return 2
    p = pathlib.Path(argv[1])
    if not p.is_file():
        print(f"[fatal] 파일이 없다: {p}", file=sys.stderr)
        return 2
    try:
        rec = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        print(f"[fatal] JSON 이 아니다: {e}", file=sys.stderr)
        return 2
    ok, reasons = admit(rec)
    print(json.dumps({"admitted": ok, "record_type": rec.get("record_type"),
                      "reasons": reasons}, ensure_ascii=False, indent=1))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
