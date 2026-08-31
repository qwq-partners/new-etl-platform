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

**게이트는 닫혀 있다**(9차 조치 9 / P1-04 · §5-1). `GATE_OPEN = False` 인 동안
`admit()` 은 어떤 레코드에도 True 를 주지 않는다. 이유는 단순하다 — 들여보낼 곳이 없다.
G0-1~G0-5 산출물이 하나도 없고 `aggregate()` 는 `NotImplementedError` 다. 열어 둘 이유가
없는 문을 열어 놓고 위조를 하나씩 막는 것보다 문을 닫는 것이 근본이다.

9차 판정 §5-1 이 실증한 것이 그 위조다. 전부 쓰레기 문자열인 레코드가 `admitted = True,
reasons = []` 로 승인됐다. 값의 schema·digest 를 안 보기 때문만이 아니라, 그 앞에
**`admit()` 이 계약의 `where` 를 쓰지 않고 `item` 이름을 경로로 해석했기** 때문이다.
`hash_vector_result (V-01~V-16)` 이라는 이름의 키 하나면 그 항목이 충족됐다.

값 검증(schema·digest)은 **여기서 미리 만들지 않는다.** 최종 레코드의 실물이 없는 상태에서
검증기를 쓰면 나중에 실물을 검증기에 맞추게 된다 — `aggregate()` 를 구현하지 않은 것과
같은 이유다. 문은 실물이 생길 때 그 실물에 맞춰 연다.

닫힌 채로도 사유는 전부 모은다. 거절 하나로 끝내면 그 레코드가 **왜** 자격이 없는지가
안 보이고, 그러면 게이트를 여는 날 무엇을 고쳐야 하는지도 모른다.

1. `GATE_OPEN` — 닫혀 있으면 무조건 거절이다.
2. `record_type == "g0_evidence"` — `g0_0_evidence` 는 **무조건** 거절이다.
   completeness 가 COMPLETE 든 아니든, 계약 위반이 0건이든 아니든 상관없다.
3. `gate_eligible is True` — G0-0 스키마에서 이 값은 `const false` 라 바꿀 수단이 없다.
4. `g0-final-contract.json` 의 항목이 **`where` 가 가리키는 자리에** 전부 있는가.
   `where` 가 null 인 항목은 위치가 아직 정해지지 않았다는 뜻이고, 그런 항목은 어떤
   레코드로도 충족되지 않는다.

사용:

    python3 g0_final_gate.py <record.json>      # exit 0 통과 / 1 거절 / 2 실행 전 조건 미비
"""
from __future__ import annotations

import json
import pathlib
import sys

CONTRACT_FILE = pathlib.Path(__file__).resolve().parent / "g0-final-contract.json"
FINAL_RECORD_TYPE = "g0_evidence"

# 최종 G0 게이트는 **닫혀 있다**(9차 조치 9). 이 값을 True 로 바꾸려면 그 전에 전부
# 참이어야 한다: (a) G0-1~G0-5 산출물이 실재하고, (b) `aggregate()` 가 그 실물에 맞춰
# 구현됐고, (c) 계약의 모든 항목에 `where` 가 정해졌고, (d) 값의 schema·digest 검증이
# 실물 레코드에 대고 세워졌다. 넷 중 하나라도 아니면 여는 것은 이르다 —
# **닫힌 문은 위조를 전부 막지만, 열린 문은 우리가 생각해 낸 위조만 막는다.**
GATE_OPEN = False


def load_contract(path: pathlib.Path | None = None) -> dict:
    p = path or CONTRACT_FILE
    return json.loads(p.read_text(encoding="utf-8"))


def contract_items(contract: dict | None = None) -> list[str]:
    """계약 항목의 **식별자** 목록. 표시 이름(`label`)도 위치(`where`)도 아니다."""
    c = contract or load_contract()
    return [str(i["item"]) for i in c.get("items", []) if isinstance(i, dict) and "item" in i]


def item_locations(contract: dict | None = None) -> list[tuple[str, str | None]]:
    """`(식별자, where)` 목록. `where` 가 None 이면 **위치 미정**이다(9차 조치 9).

    `item` 을 경로로 쓰지 않는다 — 그것이 §5-1 이 실증한 위조 경로였다.
    """
    c = contract or load_contract()
    return [(str(i["item"]), (str(i["where"]) if i.get("where") else None))
            for i in c.get("items", []) if isinstance(i, dict) and "item" in i]


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

    if not GATE_OPEN:
        reasons.append(
            "최종 G0 게이트가 닫혀 있다(GATE_OPEN=False) — G0-1~G0-5 산출물이 하나도 없고 "
            "aggregate() 도 구현되지 않았다. 들여보낼 곳이 없으므로 **어떤 레코드도** "
            "승인하지 않는다. 여는 조건은 g0_final_gate.GATE_OPEN 주석에 있다")

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

    # **`where` 가 경로의 유일한 권위다**(9차 조치 9 / §5-1). `item` 을 경로로 해석하면
    # 계약이 표시용으로 적어 둔 이름과 같은 키 하나로 항목이 충족된다.
    c = contract or load_contract()
    missing: list[str] = []
    undefined: list[str] = []
    for name, where in item_locations(c):
        if where is None:
            undefined.append(name)
        elif not resolve(record, where):
            missing.append(f"{name}@{where}")
    if undefined:
        reasons.append(f"위치가 정해지지 않은 계약 항목 {undefined} — `where` 가 null 인 "
                       f"항목은 어떤 레코드로도 충족되지 않는다. 그 위치는 G0-1~G0-5 실물이 "
                       f"생길 때 정한다")
    if missing:
        reasons.append(f"최종 계약 항목 누락 {missing} — 부분 레코드는 게이트 입력이 아니다")

    # 닫혀 있으면 사유가 비어도 통과시키지 않는다. 두 조건을 곱으로 두는 이유는,
    # 사유 수집 로직이 나중에 바뀌어도 문이 열리는 일이 없게 하기 위해서다.
    return (GATE_OPEN and not reasons), reasons


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
