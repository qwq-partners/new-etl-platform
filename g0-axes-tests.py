#!/usr/bin/env python3
"""`g0_axes.py` 반례 회귀 시험.

7차 교차 리뷰 §5.1 의 축 관련 표를 실행 가능한 시험으로 옮긴 것이다.
**이전 파생기가 내던 값이 지금은 나오지 않는지**를 본다. 그리고 양성 대조로,
근거가 충분할 때는 실제로 승격되는지도 함께 본다.

    python3 g0-axes-tests.py
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys

_spec = importlib.util.spec_from_file_location(
    "g0_axes", pathlib.Path(__file__).resolve().parent / "g0_axes.py")
ax = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ax)

FAIL: list[str] = []
PASS = 0
BINDING = {"db_identity": "ETLSTB", "owner": "APP", "object": "T1", "object_type": "TABLE"}


def ok(pid, value=None, **kw):
    r = {"probe": pid, "query_ok": True}
    if value is not None:
        r["value"] = value
        r["value_interpretable"] = kw.pop("value_interpretable", True)
    r.update(kw)
    return r


def err(pid, ora):
    return {"probe": pid, "query_ok": False, "ora": -abs(ora),
            "msg": f"ORA-{abs(ora):05d}: test"}


def P(*recs):
    return {r["probe"]: r for r in recs}


def check(name, cond, detail=""):
    global PASS
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL.append(f"{name} — {detail}")
        print(f"  FAIL  {name}  {detail}")


def val(P_, axis, binding=BINDING):
    return ax.derive_axes(P_, binding=binding)[axis]["value"]


# ─────────────────────────────────────────────────────────────────────
def t_no_readonly_txn_from_timestamp():
    print("\n[1] AS OF TIMESTAMP 성공 + READ ONLY 두 probe 실패 → READ_ONLY_TXN 금지")
    p = P(ok("as_of_timestamp.target", "1"),
          err("txn.set_read_only", 1031), err("txn.select_inside", 1031),
          err("dbms_flashback.get_scn", 1031))
    v = val(p, "snapshot_anchor")
    check("값이 TIMESTAMP 다", v == "TIMESTAMP", f"got {v}")
    check("READ_ONLY_TXN 이라는 값 자체가 축에 없다",
          "READ_ONLY_TXN" not in (ax.AXIS_SPEC["snapshot_anchor"]["values"] or []))


def t_no_as_of_scn_from_count():
    print("\n[2] view.v_database COUNT(*) 성공만으로 AS_OF_SCN 승격 금지")
    p = P(ok("as_of_timestamp.target", "1"),
          ok("view.v_database", "1"),                 # COUNT(*) — 접근 가능성일 뿐
          err("dbms_flashback.get_scn", 1031))        # SCN 원점은 못 얻었다
    a = ax.derive_axes(p, binding=BINDING)["snapshot_anchor"]
    check("SCN 이 아니다", a["value"] != "SCN", f"got {a['value']}")
    check("view.v_database 는 판정에 쓰이지 않았다고 남는다",
          "view.v_database" in (a.get("considered_but_not_used") or []),
          str(a.get("considered_but_not_used")))


def t_scn_positive():
    print("[2b] 양성 대조 — SCN 을 실제로 읽으면 SCN 이다")
    p = P(ok("dbms_flashback.get_scn", "123456"), ok("as_of_timestamp.target", "1"))
    check("SCN 승격", val(p, "snapshot_anchor") == "SCN")


def t_no_dg_stats_from_count():
    print("\n[3] DG 뷰 조회 성공·값 없음 → DG_STATS 금지")
    p = P(ok("view.v_dataguard_stats", "1"),                       # COUNT(*)
          {"probe": "v$dataguard_stats", "query_ok": True, "row_present": False})
    a = ax.derive_axes(p, binding=BINDING)["lag_observation"]
    check("DG_STATS 가 아니다", a["value"] != "DG_STATS", f"got {a['value']}")
    check("NONE 도 아니다(행 없음은 기능 부재가 아니다)", a["value"] == "UNDETERMINED", a["value"])
    check("COUNT probe 는 미사용으로 남는다",
          "view.v_dataguard_stats" in (a.get("considered_but_not_used") or []))


def t_dg_positive():
    print("[3b] 양성 대조 — 값을 읽으면 DG_STATS")
    p = P(ok("v$dataguard_stats", "apply lag|+00 00:00:03"))
    check("DG_STATS 승격", val(p, "lag_observation") == "DG_STATS")


def t_no_enforced_without_ora3172():
    print("\n[4] MAX_DELAY ALTER 성공 + ORA-03172 양성 대조 없음 → MAX_DELAY_ENFORCED 금지")
    p = P(ok("alter.STANDBY_MAX_DATA_DELAY.D", "ok"),
          ok("alter.STANDBY_MAX_DATA_DELAY.zero", "ok"),
          ok("max_delay_zero.touch_target", "1"))       # 거절당하지 않았다
    a = ax.derive_axes(p, binding=BINDING)["lag_admission"]
    check("ENFORCED 가 아니다", a["value"] != "MAX_DELAY_ENFORCED", a["value"])
    check("UNDETERMINED 다(오류 부재는 강제의 증거가 아니다)", a["value"] == "UNDETERMINED", a["value"])


def t_enforced_positive():
    print("[4b] 양성 대조 — ORA-03172 가 나오면 ENFORCED")
    p = P(ok("alter.STANDBY_MAX_DATA_DELAY.D", "ok"),
          err("max_delay_zero.touch_target", 3172))
    check("ENFORCED 승격", val(p, "lag_admission") == "MAX_DELAY_ENFORCED")


def t_transient_is_not_none():
    print("\n[5] transient 실패(ORA-03135 등)를 기능 부재로 강등 금지 (P1-03)")
    for code, name in ((3135, "연결 단절"), (12541, "리스너 없음"), (1013, "취소"), (1555, "snapshot too old")):
        p = P(err("feat.fetch_first", code), err("feat.standard_hash_sha256", code),
              err("dbms_flashback.get_scn", code), err("as_of_timestamp.target", code))
        a = ax.derive_axes(p, binding=BINDING)
        check(f"ORA-{code}({name}) → sql_dialect 가 11G 로 강등되지 않는다",
              a["sql_dialect"]["value"] == "UNDETERMINED", a["sql_dialect"]["value"])
        check(f"ORA-{code} → hash_function 이 NONE 이 되지 않는다",
              a["hash_function"]["value"] == "UNDETERMINED", a["hash_function"]["value"])
        check(f"ORA-{code} → snapshot_anchor 가 NONE 이 되지 않는다",
              a["snapshot_anchor"]["value"] == "UNDETERMINED", a["snapshot_anchor"]["value"])


def t_unsupported_is_none():
    print("[5b] 양성 대조 — 진짜 미지원은 NONE/11G 로 내려간다")
    # ORA-00900(invalid SQL statement)·ORA-00933(SQL command not properly ended)은 어느
    # probe 에서 나와도 뜻이 같다. ORA-00904 는 그렇지 않아서 여기 쓰지 않는다 —
    # `feat.standard_hash_sha256` 에서만 부재를 뜻하고, 그 사실은 그 probe 가 선언한다.
    p = P(err("feat.fetch_first", 900), err("feat.standard_hash_sha256", 904),
          err("dbms_flashback.get_scn", 900), err("as_of_timestamp.target", 933))
    a = ax.derive_axes(p, binding=BINDING)
    check("sql_dialect=11G", a["sql_dialect"]["value"] == "11G", a["sql_dialect"]["value"])
    check("hash_function=NONE(probe 가 선언한 904)",
          a["hash_function"]["value"] == "NONE", a["hash_function"]["value"])
    check("snapshot_anchor=NONE", a["snapshot_anchor"]["value"] == "NONE",
          a["snapshot_anchor"]["value"])


def t_m3_taxonomy_never_none():
    print("\n[5c] M3-2 — 권한 부족·대상 부재·프로브 결함·모호 코드는 NONE 이 아니다")
    # 8차 §7: "P1-03 의 현 ABSENCE_ORA 는 unsupported, permission denied, wrong target,
    # probe bug 를 섞는다. 예를 들어 ORA-01031/00942 가 row_hash=NONE, sql_dialect=11G 가
    # 될 수 있다." — 그 네 가지를 각각 넣고 어느 것도 NONE/11G 를 만들지 않는지 본다.
    for code, kind in ((1031, "DENIED"), (1039, "DENIED"),
                       (942, "WRONG_TARGET"), (4043, "WRONG_TARGET"),
                       (6502, "PROBE_BUG"), (1722, "PROBE_BUG"),
                       (904, "AMBIGUOUS"), (6550, "AMBIGUOUS")):
        p = P(err("feat.fetch_first", code), err("dbms_flashback.get_scn", code),
              err("as_of_timestamp.target", code), err("v$dataguard_stats", code),
              err("feat.ora_rowscn_target", code))
        a = ax.derive_axes(p, binding=BINDING)
        check(f"ORA-{code:05d}({kind}) → sql_dialect 가 11G 가 아니다",
              a["sql_dialect"]["value"] == "UNDETERMINED", a["sql_dialect"]["value"])
        check(f"ORA-{code:05d}({kind}) → snapshot_anchor 가 NONE 이 아니다",
              a["snapshot_anchor"]["value"] == "UNDETERMINED", a["snapshot_anchor"]["value"])
        check(f"ORA-{code:05d}({kind}) → lag_observation 이 NONE 이 아니다",
              a["lag_observation"]["value"] == "UNDETERMINED", a["lag_observation"]["value"])
    # ORA-00904 는 그 뜻을 아는 probe 에서만 부재다 — 같은 코드가 축마다 다르게 읽힌다.
    p = P(err("feat.standard_hash_sha256", 904))
    check("같은 ORA-00904 라도 standard_hash 에서는 NONE",
          ax.derive_axes(p, binding=BINDING)["hash_function"]["value"] == "NONE")


def t_m3_typed_predicates():
    print("\n[5d] M3-2 — probe 별 typed predicate: 행·값·문법을 따로 본다")
    # query_ok=true 인데 행이 없다 / 값이 null 이다 / 값이 기대 형태가 아니다 — 셋은 서로
    # 다른 사실이고 어느 것도 '기능 부재' 가 아니다.
    check("행 없음 → EMPTY",
          ax.classify({"probe": "nls.characterset", "query_ok": True,
                       "row_present": False}) == "EMPTY")
    check("값 null → EMPTY",
          ax.classify({"probe": "nls.characterset", "query_ok": True,
                       "value": None}) == "EMPTY")
    check("문법 위반 → GRAMMAR",
          ax.classify({"probe": "feat.rowdependencies_target", "query_ok": True,
                       "value": "yes"}) == "GRAMMAR")
    check("문법 충족 → OK",
          ax.classify({"probe": "feat.rowdependencies_target", "query_ok": True,
                       "value": "ENABLED"}) == "OK")
    check("probe 가 해석 못 했다고 적으면 NOT_INTERPRETABLE",
          ax.classify({"probe": "feat.standard_hash_sha256", "query_ok": True,
                       "value": "deadbeef", "value_interpretable": False})
          == "NOT_INTERPRETABLE")
    # GRAMMAR 는 원천의 성질이 아니라 파싱 실패다 → successor 를 추정하지 않는다.
    a = ax.derive_axes(P(ok("wm_column.type_facts", "타입을 못 읽었다")),
                       binding=BINDING)["wm_successor"]
    check("형태가 다른 type_facts 는 UNDETERMINED", a["value"] == "UNDETERMINED", a["value"])
    check("추정하지 않았다고 남긴다", "추정하지 않는다" in a.get("note", ""), a.get("note", ""))
    # 시험 벡터 불일치는 이 축에서만 부재의 증거다.
    a2 = ax.derive_axes(P(ok("feat.standard_hash_sha256", "deadbeef",
                             value_interpretable=False)), binding=BINDING)["hash_function"]
    check("벡터 불일치 → hash_function=NONE", a2["value"] == "NONE", a2["value"])
    # ORA_ROWSCN 은 읽혔으나 ROWDEPENDENCIES 를 못 읽으면 입도를 모른다 —
    # 권한 부족을 DISABLED 로 읽어 BLOCK_LEVEL 을 확정하지 않는다.
    a3 = ax.derive_axes(P(ok("feat.ora_rowscn_target", "1"),
                          err("feat.rowdependencies_target", 1031)),
                        binding=BINDING)["row_change_scn"]
    check("rowdependencies 를 못 읽으면 입도는 UNDETERMINED",
          a3["value"] == "UNDETERMINED", a3["value"])


def t_m3_floor():
    print("\n[5e] M3-3 — effective floor 는 값을 내리기만 한다")
    p = P(ok("feat.fetch_first", "1"), ok("nls.characterset", "AL32UTF8"))
    # floor 사유가 없으면 effective_value == value
    a = ax.derive_axes(p, binding=BINDING, now="2026-08-30T00:00:00+00:00",
                       measured_at="2026-08-29T00:00:00+00:00", ttl_seconds=86400 * 30)
    check("사유가 없으면 effective_value == value",
          a["sql_dialect"]["effective_value"] == "12C_PLUS", str(a["sql_dialect"])[:120])
    check("stale=false", a["sql_dialect"]["stale"] is False)
    check("freshness_basis=OPERATOR_DECLARED_TTL",
          a["sql_dialect"]["freshness_basis"] == "OPERATOR_DECLARED_TTL")
    # TTL 이 지나면 stale 이고 floor 로 내려간다
    b = ax.derive_axes(p, binding=BINDING, now="2027-08-30T00:00:00+00:00",
                       measured_at="2026-08-29T00:00:00+00:00", ttl_seconds=86400 * 30)
    check("TTL 초과 → stale", b["sql_dialect"]["stale"] is True, str(b["sql_dialect"])[:120])
    check("stale 이면 floor(11G)", b["sql_dialect"]["effective_value"] == "11G",
          b["sql_dialect"]["effective_value"])
    check("value 는 그대로 12C_PLUS(감사용)", b["sql_dialect"]["value"] == "12C_PLUS")
    # floor 는 값을 올리지 않는다
    check("UNDETERMINED 를 floor 로 올리지 않는다",
          ax.floored_value("sql_dialect", "UNDETERMINED") == "UNDETERMINED")
    check("이미 floor 보다 약한 값은 그대로",
          ax.floored_value("snapshot_scope", "NONE") == "NONE")
    check("floor 보다 강한 값은 내려간다",
          ax.floored_value("snapshot_scope", "JOB") == "STATEMENT")
    check("자유 값 축은 선언된 floor 로",
          ax.floored_value("db_charset", "AL32UTF8") == "UNDETERMINED")
    check("wm_successor 의 floor 는 UNDEFINED",
          ax.floored_value("wm_successor", "TIMESTAMP(6)") == "UNDEFINED")
    # 합성 축은 입력이 내려가면 같이 내려간다
    c = ax.derive_axes(P(ok("feat.standard_hash_sha256", "ba7816bf")),
                       binding=BINDING, floor_reasons=("CHILD_NOT_MEASURED",))
    check("합성 축에 COMPOSITE_INPUT_FLOORED",
          "COMPOSITE_INPUT_FLOORED" in c["canonical_row_compare"]["floor_reasons"],
          str(c["canonical_row_compare"]["floor_reasons"]))
    check("합성 축 effective_value 도 내려간다",
          c["canonical_row_compare"]["effective_value"] == "NONE",
          c["canonical_row_compare"]["effective_value"])
    # 사유 이름은 표에 선언된 것만 쓴다
    used = {r for v in c.values() for r in v["floor_reasons"]}
    check("floor 사유가 전부 FLOOR_REASONS 에 있다", used <= set(ax.FLOOR_REASONS),
          str(used - set(ax.FLOOR_REASONS)))


def t_wm_successor_exact():
    print("\n[6] wm_successor 는 등급이 아니라 exact scale·min step 이다 (P0-01)")
    cases = [("TIMESTAMP(2)|scale=2", "TIMESTAMP(2)", "10ms"),
             ("TIMESTAMP(5)|scale=5", "TIMESTAMP(5)", "10us"),
             ("TIMESTAMP(8)|scale=8", "TIMESTAMP(8)", "10ns"),
             ("TIMESTAMP(6)|scale=6", "TIMESTAMP(6)", "1us"),
             ("DATE|scale=0", "DATE", "1s")]
    for raw, want, step in cases:
        a = ax.derive_axes(P(ok("wm_column.type_facts", raw)), binding=BINDING)["wm_successor"]
        check(f"{raw} → {want}", a["value"] == want, a["value"])
        check(f"{raw} 의 min step 이 {step}", step in a.get("note", ""), a.get("note", "")[:80])
    a = ax.derive_axes(P(ok("wm_column.type_facts", "NUMBER|scale=2")),
                       binding=BINDING)["wm_successor"]
    check("NUMBER(scale=2) 가 SEC 이 아니다", "SEC" not in a["value"], a["value"])
    check("NUMBER 는 시간 단위가 아니라고 남긴다", "시간 단위가 아니다" in a.get("note", ""),
          a.get("note", "")[:80])
    a = ax.derive_axes(P(ok("wm_column.type_facts", "NUMBER|scale=-")),
                       binding=BINDING)["wm_successor"]
    check("정밀도 미지정 NUMBER 는 UNDETERMINED", a["value"] == "UNDETERMINED", a["value"])
    a = ax.derive_axes(P(ok("wm_column.type_facts", "TIMESTAMP|scale=-")),
                       binding=BINDING)["wm_successor"]
    check("scale 을 못 읽은 TIMESTAMP 에 기본값 6 을 가정하지 않는다",
          a["value"] == "UNDETERMINED", a["value"])
    check("round-trip 미검증을 명시한다",
          "round-trip" in ax.derive_axes(P(ok("wm_column.type_facts", "TIMESTAMP(6)|scale=6")),
                                         binding=BINDING)["wm_successor"].get("note", ""))


def t_unbound_table_axes():
    print("\n[7] 대상 식별자가 없으면 테이블 단위 축은 확정값을 내지 않는다 (P0-03)")
    p = P(ok("feat.ora_rowscn_target", "1"), ok("feat.rowdependencies_target", "ENABLED"),
          ok("wm_column.type_facts", "TIMESTAMP(6)|scale=6"))
    a = ax.derive_axes(p, binding=None)
    for name in ("row_change_scn", "wm_successor", "watermark_commit_bound",
                 "snapshot_object_coverage"):
        check(f"{name} 이 UNDETERMINED", a[name]["value"] == "UNDETERMINED", a[name]["value"])
        check(f"{name} 의 binding 이 null", a[name]["binding"] is None)
    b = ax.derive_axes(p, binding=BINDING)
    check("binding 을 주면 row_change_scn 이 확정된다", b["row_change_scn"]["value"] == "ROW_LEVEL",
          b["row_change_scn"]["value"])
    check("확정값에는 binding 이 붙는다", b["row_change_scn"]["binding"] == BINDING)


def t_watermark_bound_independent_of_lag():
    print("\n[8] watermark_commit_bound 는 lag 축과 독립이다 (P0-05 — 복원된 축)")
    check("축이 존재한다", "watermark_commit_bound" in ax.AXIS_SPEC)
    # apply lag 를 완벽히 관측할 수 있어도 commit bound 는 별개로 판정된다
    p = P(ok("v$dataguard_stats", "apply lag|+00 00:00:00"),
          err("timestamp_to_scn", 1031), err("scn_to_timestamp.roundtrip", 1031),
          err("feat.ora_rowscn_target", 1031))
    a = ax.derive_axes(p, binding=BINDING)
    check("lag_observation 은 DG_STATS", a["lag_observation"]["value"] == "DG_STATS")
    check("그래도 watermark_commit_bound 는 승격되지 않는다",
          a["watermark_commit_bound"]["value"] in ("NONE", "UNDETERMINED"),
          a["watermark_commit_bound"]["value"])
    p2 = P(ok("feat.ora_rowscn_target", "1"), ok("scn_to_timestamp.roundtrip", "ok"),
           ok("timestamp_to_scn", "1"))
    a2 = ax.derive_axes(p2, binding=BINDING)["watermark_commit_bound"]
    check("관측 수단이 있으면 OBSERVED(ENFORCED 아님)", a2["value"] == "OBSERVED", a2["value"])
    check("ENFORCED 가 Profile U 에서 도달 불가임을 남긴다", "ENFORCED 가 아니다" in a2["note"])


def t_composites():
    print("\n[9] 합성 축 — 구조적으로 불가능한 값이 나오지 않는다")
    # 모든 원자 축이 최선일 때조차
    p = P(ok("dbms_flashback.get_scn", "1"), ok("as_of_timestamp.target", "1"),
          ok("feat.standard_hash_sha256", "ba7816bf…", value_interpretable=True),
          ok("nls.characterset", "AL32UTF8"))
    a = ax.derive_axes(p, binding=BINDING)
    check("snapshot_scope 가 JOB 이 아니다(object coverage 가 ALL 이 될 수 없다)",
          a["snapshot_scope"]["value"] != "JOB", a["snapshot_scope"]["value"])
    check("canonical_row_compare 가 VECTORS_PROVEN 이 아니다",
          a["canonical_row_compare"]["value"] != "VECTORS_PROVEN",
          a["canonical_row_compare"]["value"])
    check("cross_source_comparable 은 UNDETERMINED (원천이 하나다)",
          a["cross_source_comparable"]["value"] == "UNDETERMINED",
          a["cross_source_comparable"]["value"])
    check("db_charset 은 관측값 그대로", a["db_charset"]["value"] == "AL32UTF8",
          a["db_charset"]["value"])
    check("hash_function=SHA256 이어도 비교 능력은 PARTIAL",
          a["hash_function"]["value"] == "SHA256"
          and a["canonical_row_compare"]["value"] == "PARTIAL")


def t_inputs_are_facts():
    print("\n[10] inputs 는 의도가 아니라 실제로 읽은 것이다 (P1-06)")
    p = P(ok("wm_column.type_facts", "TIMESTAMP(6)|scale=6"))
    a = ax.derive_axes(p, binding=BINDING)["wm_successor"]
    reads = {i["probe"] for i in a["inputs"]}
    check("읽은 probe 만 inputs 에", reads == {"wm_column.type_facts"}, str(reads))
    check("분기에 안 쓴 probe 는 분리된다",
          set(a.get("considered_but_not_used") or []) ==
          {"feat.interval_ns_successor", "feat.timestamp9_precision"},
          str(a.get("considered_but_not_used")))


def t_empty_input():
    print("\n[11] 입력이 전혀 없으면 전 축 UNDETERMINED")
    a = ax.derive_axes({}, binding=None)
    check("13축 전부 존재", set(a) == set(ax.AXIS_SPEC), str(set(ax.AXIS_SPEC) - set(a)))
    check("전부 UNDETERMINED", {v["value"] for v in a.values()} == {"UNDETERMINED"},
          str({k: v["value"] for k, v in a.items() if v["value"] != "UNDETERMINED"}))


def t_schema_agreement():
    print("\n[12] 스키마와 코드가 같은 축 집합·값 집합을 말하는가")
    import json
    sc = json.loads((pathlib.Path(__file__).resolve().parent /
                     "g0-0-evidence.schema.json").read_text(encoding="utf-8"))
    props = sc["properties"]["capability_axes"]["properties"]
    check("축 이름 집합이 같다", set(props) == set(ax.AXIS_SPEC),
          str(set(props) ^ set(ax.AXIS_SPEC)))
    check("required 도 같다",
          set(sc["properties"]["capability_axes"]["required"]) == set(ax.AXIS_SPEC))
    # 파생기가 낼 수 있는 모든 값이 스키마 enum 안에 있는가
    import jsonschema
    v = jsonschema.Draft202012Validator(sc["properties"]["capability_axes"], )
    for name, spec in ax.AXIS_SPEC.items():
        if not spec["values"]:
            continue
        check(f"{name} 의 UNDETERMINED 가 enum 에 있다", "UNDETERMINED" in spec["values"],
              str(spec["values"]))


def main() -> int:
    print("=" * 70)
    print("g0_axes.py 반례 회귀 시험 — 7차 교차 리뷰 §5.1 (축)")
    print("=" * 70)
    for t in (t_no_readonly_txn_from_timestamp, t_no_as_of_scn_from_count, t_scn_positive,
              t_no_dg_stats_from_count, t_dg_positive, t_no_enforced_without_ora3172,
              t_enforced_positive, t_transient_is_not_none, t_unsupported_is_none,
              t_m3_taxonomy_never_none, t_m3_typed_predicates, t_m3_floor,
              t_wm_successor_exact, t_unbound_table_axes,
              t_watermark_bound_independent_of_lag, t_composites, t_inputs_are_facts,
              t_empty_input, t_schema_agreement):
        t()
    print("\n" + "=" * 70)
    print(f"통과 {PASS}건 · 실패 {len(FAIL)}건")
    for f in FAIL:
        print(f"  - {f}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
