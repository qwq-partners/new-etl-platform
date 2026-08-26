#!/usr/bin/env python3
"""CE08 — mid-sweep crash와 cursor non-advance

**미구현 스텁이다.** 이 파일은 fixture·주입·정리를 소유해야 하며, 구현 전에는
runner에 INJECTION_NOT_OBSERVED를 보고해 suite가 PASS로 넘어가지 않게 한다.

구현 시 반드시 지킬 것
  1. suite.yaml의 object_prefix를 모든 객체 이름에 붙인다.
  2. 마지막에 반드시 정리하고 leftover_objects를 정확히 보고한다.
  3. injection_observed는 **서버 측 관측**(오류 코드·행 상태·타이밍)으로만 true.
     로그 문구나 "예외가 안 났다"는 근거가 아니다.
  4. 결과는 마지막 줄에 SCENARIO_RESULT <json> 한 줄로 출력한다.
"""
import json, sys

def main() -> int:
    cfg = json.loads(sys.argv[sys.argv.index("--suite") + 1]) if "--suite" in sys.argv else {}
    result = {
        "outcome": "INJECTION_NOT_OBSERVED",
        "injection_observed": False,
        "injection_evidence": [],
        "fixture": {"objects_created": [], "rows_written": 0},
        "observations": [
            {"name": "implemented", "value": False,
              "note": "스텁이다. scenario.yaml의 steps를 구현하기 전까지 suite는 PASS가 될 수 없다."},
            {"name": "object_prefix", "value": cfg.get("object_prefix", "")},
        ],
        "cleanup": {"attempted": False, "succeeded": True, "leftover_objects": []},
    }
    print("SCENARIO_RESULT " + json.dumps(result, ensure_ascii=False))
    return 0

if __name__ == "__main__":
    sys.exit(main())
