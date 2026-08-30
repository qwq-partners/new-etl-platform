# ETL Platform v2.0 — 8차 Codex 교차 리뷰

- 리뷰 일자: 2026-08-30
- 사용자 지정 기준 커밋: `9cba9209409d67df13405593e970fe76c2987366` (`main`)
- 검토 방식: 저장소 정적 검토 + 격리된 반례 실행 + Oracle/Spark 공식 문서 대조
- Oracle·Spark·사내 원천에 대한 실제 G0-0 실행: **없음**
- 실행 코드·스키마 변경: 없음. 결과 커밋은 이 검토서와 상태·안전 안내 문서만 갱신한다.

> 최초 요청서 4행은 기준 커밋을 `65846ec`로 적었지만 사용자가 지정한 HEAD는 `9cba920`이었다.
> 두 커밋 사이에는 `README.md`와 8차 요청서만 바뀌어 실행 코드 판정에는 영향이 없었다.
> 이번 문서 갱신에서 요청서의 기준 커밋을 `9cba920`으로 바로잡았다.
>
> 검토 완료 직전 저장소 HEAD가 외부 작업에 의해 `7e0872b`로 전진한 것을 확인했다.
> `9cba920..7e0872b` 변경은 `.gitignore`, `HANDOFF.md`, `README.md`,
> `etl-platform-transfer-guide.md`뿐이며 이 리뷰가 검증한 실행 코드·evidence contract는 바뀌지 않았다.
> 따라서 판정 기준은 요청대로 `9cba920`을 유지한다.

---

## 0. 최종 판정

### 0.1 한 줄 판정

**단일 privilege-zero core + 원천별 capability overlay라는 방향은 GO다. 그러나 `9cba920`을 G0-0 실행 준비 완료판, 신뢰 가능한 증거 정규화판, Profile U 규범 동결판으로 받아들이는 것은 NO-GO다.**

7차 P0 6건의 재판정은 다음과 같다.

| 판정 | 건수 |
|---|---:|
| CLOSED | **0** |
| PARTIAL | **2** |
| OPEN | **4** |

수정은 분명 진전했다. 특히 `g0_0_evidence`/`gate_eligible=false` 분리, CE child return code 반영,
`lag_observation`/`lag_admission` 분리, `watermark_commit_bound` 복원은 유지할 가치가 있다.
그러나 검증 도구가 검증 대상보다 아직 느슨하다.

### 0.2 의사결정표

| 결정 | 판정 | 이유 |
|---|---|---|
| Dagster OSS + 얇은 Control Plane 방향 | **GO 유지** | 이번 결함은 주로 Oracle snapshot/evidence plane 문제이며 오케스트레이터 선택과 독립이다. |
| 권한 0 core + 선택적 overlay | **GO 유지** | 권한을 정합성 전제로 삼지 않는 방향은 맞다. |
| 7차 P0 6건 종결 선언 | **NO-GO** | CLOSED 0 / PARTIAL 2 / OPEN 4. |
| `9cba920` 상태로 사내 원천 G0-0 전체 실행 | **NO-GO** | exit-code 유실, B0 파티션 무상한, identity 선차단 부재, B1 실행 불가가 있다. |
| 최소 안전 패치 후 원시 probe 수집 | **조건부 GO** | A/B0/B1은 pinned runtime·부하 승인·외부 wrapper가 필요하다. C00은 scan 승인, C01~09는 외부 allowlist의 폐기 환경만 허용한다. |
| 현 normalizer 결과를 capability로 수용 | **NO-GO** | 얕은·혼합·과거 산출물을 `MEASURED`와 고등급 축으로 승격할 수 있다. |
| B1 `coverage=PROVEN` 수용 | **NO-GO** | 실행 모드 불일치, 순환적 stack 추정, terminal failure 미검증. |
| G0 PASS 또는 완전한 G0-0 PASS 선언 | **NO-GO** | 레코드는 의도적으로 `gate_eligible=false`; 최종 G0 aggregator도 없다. |
| capability 9축 및 A/P v2.0 동결 | **NO-GO** | 독립 축과 composition이 미정이고 현 overlay가 자기모순이다. |
| DBA 권한 요청 보류 | **GO** | 아직 승인된 후보가 없고 core가 권한에 의존하면 안 된다. |
| “FLASHBACK은 받아도 순이익이 아니다”라는 실체 판정 | **NO-GO** | 수동 grant와 선택적 runtime 활성화 비용을 섞었고, AS OF TIMESTAMP의 공통 snapshot 이득을 누락했다. |
| 비의미적 플랫폼 골격/UI/Dagster spike | **조건부 GO** | correctness grade를 고정하지 않고 BEST_EFFORT만 표시하는 Phase 0 작업은 병행 가능하다. |
| production 구현·이관 | **NO-GO** | §6 핵심 의미와 evidence plane이 열려 있고 플랫폼 코드는 아직 0줄이다. |

---

## 1. 실제로 재현한 반례

### 1.1 B1 `failclosed_task`는 실행할 수 없다

`run.sh`는 `--mode failclosed_task`를 호출하지만 실행기의 choices는
`coverage|failclosed|initstatement`뿐이다.

실제 격리 실행 결과:

```text
run-g0-0b1.py: error: argument --mode: invalid choice: 'failclosed_task'
```

즉 SparkSession을 만들기 전 argparse에서 종료한다. 설령 choice만 추가해도
`run-g0-0b1.py`의 결과 판정은 `mode == "failclosed"`만 expected-failure로 처리하므로
`failclosed_task`는 일반 `ERROR`가 된다.

### 1.2 B0의 안전 거부는 exit 0이다

실제 격리 실행:

```text
--probe-rows 100001
PROBE {"probe":"args.probe_rows","ok":false,...}
process exit = 0
```

`main()`은 2를 return하지만 entry point가 `sys.exit(main())`를 호출하지 않는다.
읽기 자체는 막았지만 자동화는 성공으로 오독한다.

### 1.3 `derive_axes`의 양성 근거 없는 승격

격리된 순수 함수 반례에서 다음이 재현됐다.

| 입력 | 실제 출력 | 안전한 출력 |
|---|---|---|
| `as_of_timestamp.target={query_ok:false, ora:942}`, READ ONLY probe 없음 | `snapshot_read=NONE` | `UNDETERMINED` |
| AS OF와 SCN probe가 `query_ok=true,row_present=false` | `AS_OF_SCN` | `UNDETERMINED` |
| READ ONLY select가 `query_ok=true,row_present=false` | `READ_ONLY_TXN` | `UNDETERMINED` |
| ROWSCN·dependency가 성공이나 row 없음 | `BLOCK_LEVEL` | `UNDETERMINED` |
| charset query 성공이나 row 없음 | `OTHER` | `UNDETERMINED` |

핵심 원인은 공통 `ok()`가 `query_ok`만 보고 `row_present`, `value_interpretable`,
값 grammar와 양성 대조를 보지 않는 것이다.

### 1.4 coverage 최소치 우회

다음 형태의 synthetic payload를 조합했을 때 다섯 child가 모두 `MEASURED`가 됐다.

```text
B0: S0.fake × 2 + S1.fake × 2
B1: verdict.coverage=PROVEN + 임의의 비어 있지 않은 by_path/preamble_ok_by_path
C00: fence.fake 한 건(query_ok=true)
CE: pass=true + 내용 없는 scenario 객체 9개
A: manifest_ok가 false가 아닌 임의 summary + 일부 probe
```

이는 “한 줄짜리 산출물”만 막았을 뿐 exact manifest, known ID, unique ID, run sentinel,
producer exit, runtime/source binding을 검증하지 않기 때문이다.

### 1.5 B1 analyzer의 false positive

synthetic trace에서 TASK로 **추정된** trace는 존재하지만 task preamble failure가 없고
`failclosed_task` 결과 status가 `ERROR`인 경우에도 다음이 나왔다.

```text
task_path_preamble_failed=false
failclosed_answer=YES
coverage=PROVEN
exit=0
```

`task_failed`를 계산하지만 verdict 술어에 사용하지 않기 때문이다.

### 1.6 invalid rerun 뒤 낡은 final이 남는다

schema-invalid 재실행은 `.invalid`만 쓰고 같은 `--out` 경로의 과거 valid 파일을 남긴다.
고정 파일명 `g0-0-evidence.json`을 “현재 결과”로 읽는 소비자는 실패한 최신 회차 대신
낡은 성공 회차를 사용할 수 있다.

---

## 2. 7차 P0 6건 재판정

### P0-01 capability derivation — **PARTIAL**

좋아진 점:

- AS OF probe 하나만 성공할 때 `READ_ONLY_TXN`으로 내리던 일부 오류가 고쳐졌다.
- `V$DATABASE` COUNT를 SCN 원점으로 직접 쓰는 경로는 normalizer에서 빠졌다.
- lag 관측과 admission이 분리됐다.

남은 차단점:

1. `AS_OF_SCN`은 exact acquired SCN을 대상 객체에 실행하지 않고도 나온다.
2. `READ_ONLY_TXN`은 재발행 `ORA-01453` 양성 대조가 없어도 같은 effective grade다.
3. 일부 snapshot probe만 “부재”여도 나머지 probe가 없는데 `NONE`이 될 수 있다.
4. `DG_STATS`는 `COUNT(*) >= 1`만으로 나오며 실제 lag 값·`DATUM_TIME`을 읽지 않는다.
5. `TIMESTAMP(1|2|4|5|7|8)`을 대부분 `US`로 잘못 분류하고, `NUMBER(p,s)`를 시간 `SEC`로 표현한다.
6. `row_hash=SHA256`은 함수 test vector만 증명한다. canonical row comparison은 G0-3 전까지 별도 축이어야 한다.

필수 정정:

- probe별 typed predicate를 둔다: query 성공, required row, interpretable value, value grammar,
  positive control을 각각 명시한다.
- snapshot을 최소 `anchor_kind`, `anchor_acquisition`, `object_coverage`,
  `engine_propagation`, `snapshot_scope`로 분리한다.
- 현 `derive_axes` 결과는 coverage/binding이 완결되기 전 모두 audit-only `value`로 두고
  `effective_value=UNDETERMINED`로 floor한다.

### P0-02 strict aggregation — **OPEN**

exact한 7차 한 줄 반례는 일부 막았고 CE child nonzero exit 처리도 고쳐졌다.
그러나 현재 기준은 “최소 모양”일 뿐 “그 실행의 완결성”이 아니다.

- A: `manifest_ok`가 false만 아니면 되며 expected/emitted equality, known ID set,
  end sentinel, producer exit를 보지 않는다.
- `by_id()`는 duplicate를 last-wins로 조용히 덮고 첫 summary를 취한다. 두 run을 이어 붙이면 혼합된다.
- B0/B1/C00/CE에는 normalizer가 검증하는 child schema가 없다.
- CE runner 자체의 child return code 수정은 **CLOSED**로 인정한다. 다만 top normalizer가 CE schema를 다시 검증하지 않는다.

필수 정정:

- A/B0/B1/C00 각각 child schema와 exact manifest를 만든다.
- unknown/duplicate/missing ID, summary 복수, sentinel·exit·digest 부재, concatenated run을 거부한다.
- `MEASURED`는 child schema PASS + semantic manifest PASS의 conjunction으로만 만든다.

### P0-03 source/run/version binding — **OPEN**

- `executed_at`은 child 측정 시각이 아니라 normalization 시각이다.
- current `versions.lock`을 사후 hash할 뿐 child가 실행 시 기록한 digest와 비교하지 않는다.
- `--target/--wm` fallback으로 옛 로그를 다른 객체에 붙일 수 있다.
- `target.identity`는 서버가 확인한 identity가 아니라 SQL substitution literal의 echo다.
- B0/B1/CE가 가진 runtime·environment 정보를 top record가 버린다.
- `--profile CORP_POC`와 `versions.lock profile: LOCAL_WSL`을 비교하지 않는다.
- old local B1 JSON을 `CORP_POC`로 relabel할 수 있다.

필수 정정:

모든 child가 스스로 다음을 기록하고 aggregator가 equality를 검증해야 한다.

```text
run_id
producer_started_at / producer_finished_at / producer_exit
source db_unique_name / role / instance / target object manifest
profile
versions_lock_digest_at_execution
actual Spark/Scala/JDK/OJDBC/Oracle runtime fingerprint
harness/source/config digest
raw artifact digest
```

CLI identity override는 제거하거나 child identity와 exact-match일 때만 허용한다.

### P0-04 G0-0와 final G0 경계 — **PARTIAL**

잘된 부분:

- `record_type=g0_0_evidence`
- `scope=CAPABILITY_INVENTORY`
- `gate_eligible=false` const
- schema 설명에서 final G0가 아님을 명시

남은 부분:

- `not_covered`는 임의의 한 항목으로도 schema-valid다.
- `artifacts`는 빈 객체여도 된다.
- final `g0_evidence` schema와 G0-1~G0-5 sole aggregator가 없다.
- README와 normalizer docstring은 여전히 `g0_evidence`라고 부른다.
- `watermark_commit_bound`를 항상 `UNDETERMINED`로 만들고, UNDETERMINED 축이 있으면 exit 3이므로
  현재 코드에서 exit 0은 사실상 도달 불가다.
- exit 3은 “정상 측정 결과로 capability가 없음/미확정”과 “child 실행 불완전”을 섞는다.

권고:

- process success, measurement completeness, capability grade, final-gate eligibility를 네 값으로 분리한다.
- final gate consumer는 `record_type=g0_evidence`만 수용하고 G0-0 record는 무조건 거부한다.
- `not_covered`는 exact enum/set으로 고정하고 final G0 계약과의 차집합을 자동 검사한다.

### P0-05 independent axes and composition — **OPEN**

`lag_observation`/`lag_admission` 분리는 맞고 `watermark_commit_bound`를 되살린 것도 맞다.
그러나 다음은 여전히 합성 축이다.

- `snapshot_read`: object privilege, anchor source, object coverage, connection propagation, scope
- `row_hash`: SHA-256 함수 존재와 canonical row equality
- `charset_class`: 한 DB의 charset과 cross-source comparability
- `wm_granularity`: DB type, round-trip losslessness, typed successor

overlay는 스스로 모순된다.

- §3.1은 `bound_kind` 복원을 선언하지만 §7은 다시 `lag_visibility`로 대체한다.
- §3은 `lag_observation`/`lag_admission`을 정의하지만 §5는 존재하지 않는 `lag_visibility`를 사용한다.
- §3.1은 composition이 미정이라고 하면서 §7은 대체 관계를 확정형으로 쓴다.
- 표에는 9축이 있는데 머리말은 7축이라 한다.

판정:

**최종 축과 composition을 실측 뒤 확정하는 것은 가능하다. 단, 실측 전에 raw fact contract와 child binding은 고정해야 하며 현 provisional `derive_axes`를 publish 입력으로 사용하면 안 된다.**

### P0-06 B1 task fail-closed — **OPEN**

세 결함이 각각 독립적으로 차단한다.

1. `failclosed_task` mode 자체가 실행 불가다.
2. `Trace.classify()`가 “추정이며 raw stack이 권위”라고 적으면서도 같은 추정을
   injection actuator와 coverage/fail-closed proof 양쪽에 쓴다. 판정이 순환적이다.
3. analyzer는 `task_failed`와 task-specific result status를 verdict에 쓰지 않는다.

또한 관측된 trace를 스스로 denominator로 삼기 때문에 provider bypass나 trace write loss가
있어도 남은 `n/n`으로 “모든 physical connection”을 주장할 수 있다.

안전한 B1 재설계:

- `.option("connectionProvider", "g0b1tracer")`로 provider를 명시하고 Basic을 전역 disable하지 않는다.
- `declared_scenario`와 `observed_stack_guess`를 분리한다. stack guess는 진단용이며 주입·PASS를 제어하지 않는다.
- schema-only, task-only, metadata-only를 독립 프로세스/실행으로 분리한다.
- task-only는 가능하면 driver/executor JVM을 분리하고 executor에만 무조건 failure token을 건다.
- trace proxy가 create/execute/close와 active-count를 기록하고 trace loss를 evidence-invalid로 만든다.
- fail-closed PASS는 injection token 관측 + 의도한 action/job terminal failure + 이후 business SQL/row 0
  + 모든 driver/executor sentinel/digest 존재를 모두 요구한다.

---

## 3. 새로 발견한 차단 결함

### 3.1 runbook이 producer exit를 잃는다 — **P0**

`g0-0-probe-README.md`의 `sqlplus | tee`, `spark-submit | tee` 예시는 `set -o pipefail`이나
`PIPESTATUS[0]` 검사를 하지 않는다. 따라서 SQLPlus/Spark가 실패해도 tee 성공을 shell 성공으로
기록한다. A의 `WHENEVER SQLERROR EXIT FAILURE`도 이 wrapper에서 무력화된다.

필수 정정:

- executable wrapper에 `set -euo pipefail`을 사용한다.
- producer exit, tee exit, end sentinel을 별도 필드로 보존한다.
- 사용자가 복사하는 README 예시와 CI 실행기가 동일 wrapper를 사용하게 한다.

### 3.2 B0는 source connection 수가 무상한이다 — **P0 (production-adjacent)**

`--probe-rows`에는 상한이 있지만 `--partitions`에는 상한이 없고 그대로 `numPartitions`가 된다.
행 상한은 connection 수와 aggregate source work의 상한이 아니다. 또한 대상 읽기가 DB role/identity
hard preflight보다 먼저 일어난다.

필수 정정:

- 코드상 `1 <= partitions <= approved_cap`을 강제한다. 기본은 1 또는 매우 작은 값으로 둔다.
- 별도 한-connection preflight가 expected DB unique name/role을 확인한 뒤에만 target query를 허용한다.
- source별 세션 budget과 run-level maximum을 wrapper가 함께 검증한다.

### 3.3 A의 “ROWNUM이므로 scan-safe” 표현은 과하다 — **P0 (생산라인 연계 원천)**

A는 예상 role/DB mismatch를 기록만 하고 계속한다. 대상 접근은 문서가 말하는 3회보다 많고,
`ROWNUM <= n`은 반환 행을 제한할 뿐 empty/sparse heap에서 읽는 block 수를 하드 제한하지 않는다.

조건부 실행 요건:

- target 접촉 전 독립 identity preflight에서 mismatch 시 즉시 종료
- 대상 객체별 실행계획/통계 기반 cost budget 또는 DBA/운영자 승인
- statement timeout과 source-side connection cap
- “전체 scan 없음” 대신 “반환 행 제한, I/O 상한은 미증명”으로 문구 정정

### 3.4 C00 completion과 C01~09 환경 guard — **P0/P1**

- C00은 `WHENEVER SQLERROR CONTINUE`이고 PL/SQL block 뒤의 PROMPT가 무조건 찍힌다.
  compile failure도 end banner와 shell success처럼 보일 수 있다.
- C01~09의 “DISPOSABLE”은 같은 editable `suite.yaml`이 주장하고 같은 파일이 기대 identity를 정한다.
  DB server가 non-production임을 독립 증명하지 않는다.
- CE08 child는 공통 session budget wrapper를 우회해 직접 connection을 연다.

따라서 C00은 external manifest/exit wrapper 전까지 complete evidence로 수용하지 말고,
C01~09는 사내 CMDB/환경 registry 같은 **외부 allowlist**가 승인한 폐기 DB에서만 실행해야 한다.

---

## 4. Oracle·Spark 사실 검증

### 4.1 ORA-08181과 ORA-08180 — **정정 필요**

활성 문서는 “같은 SCN 재사용”을 ORA-08181의 표적이라고 쓰고, 일부 후보 문장은
timestamp→SCN mapping 부재도 ORA-08181로 쓴다. 공식 정의는 다르다.

- ORA-08181: supplied SCN이 valid SCN bounds 밖이다.
- ORA-08180: 지정 시간에 대응하는 snapshot을 찾지 못했다.

같은 과거 SCN을 재사용하는 것 자체가 ORA-08181 원인은 아니다. 오래된 image 소실은 보통
retention/undo 계열과 분리해 다뤄야 한다.

근거: [Oracle ORA-08181](https://docs.oracle.com/en/error-help/db/ora-08181/),
[Oracle error messages](https://docs.oracle.com/en/database/oracle/oracle-database/18/errmg/ORA-07500.html).

### 4.2 AS OF TIMESTAMP의 3초 방향 — **정정 필요**

문서의 “±3초”는 틀리다. timestamp Flashback Query는 지정 시각보다 **최대 3초 이전** 시점으로
해석될 수 있다. 반면 `SCN_TO_TIMESTAMP` 함수 자체는 “usual precision 3 seconds”인 근사이며
hard bound/방향 보장으로 쓰면 안 된다.

근거: [Oracle 11.2 Flashback guidance](https://docs.oracle.com/cd/E24693_01/appdev.11203/e17125.pdf),
[SCN_TO_TIMESTAMP](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/SCN_TO_TIMESTAMP.html).

### 4.3 object FLASHBACK의 가치가 빠졌다 — **설계 판정 수정 필요**

현재 grant verdict는 “FLASHBACK만 받으면 AS OF TIMESTAMP뿐”인 사실을 곧바로 순이익 부재로
연결한다. 그러나 같은 timestamp literal을 모든 Spark partition query에 bind하면 여러 connection을
**같은 flashback anchor**에 묶을 수 있다. 3초 이전 mapping은 anchor precision 문제이지
cross-connection common snapshot 자체가 사라지는 문제가 아니다.

따라서 다음을 분리해야 한다.

```text
grant 존재             != runtime 활성화
anchor_kind            = SCN | TIMESTAMP | NONE
anchor_precision       = EXACT_SCN | TIMESTAMP_UP_TO_3S_EARLIER | UNDETERMINED
object_coverage        = 대상 객체/LOB별
engine_propagation     = 모든 실제 connection에 동일 literal 적용 여부
snapshot_scope         = STATEMENT | CONNECTION | JOB
```

object grant는 passive이고, undo/DDL/standby 부하는 overlay를 실제 사용할 때 생긴다.
따라서 **권한 요청 보류는 유지**하되 “받아도 순이익이 아니다”는 문장을
“G0·object 수·DDL·LOB·retention·Spark 전파 실증 전에는 활성화/요청하지 않는다”로 낮춰야 한다.

정확한 권한 규칙은 대상 객체 `SELECT`/`READ`와 object `FLASHBACK` 또는 system
`FLASHBACK ANY TABLE`의 조합이다. 최소권한 정책에서는 object grant가 맞지만 유일한 SQL 권한은 아니다.

근거: [Oracle SELECT prerequisites](https://docs.oracle.com/en/database/oracle/oracle-database/18/sqlrf/SELECT.html),
[Oracle Flashback privilege guide](https://docs.oracle.com/en/database/oracle/oracle-database/19/adfns/flashback.html).

### 4.4 Spark provider 선택 — **정정 필요**

Spark는 다수 provider가 같은 driver/options를 처리할 때 JDBC option `connectionProvider`로
명시 선택하도록 문서화한다. 따라서 Basic을 JVM 전체에서 disable하는 것이 필수라는 주장은 과하다.

명시 선택이 더 안전하다.

```python
.option("connectionProvider", "g0b1tracer")
```

근거: [Spark 4.2 JDBC options](https://spark.apache.org/docs/4.2.0/sql-data-sources-jdbc.html),
[JdbcConnectionProvider API](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/jdbc/JdbcConnectionProvider.html).

### 4.5 B0 S4 classifier — **P1**

`DBMS_SESSION.SLEEP`은 18c/19c에는 있으나 12.2 package catalog에는 없다. 최소 11.2 core에서
version-dependent measurement failure가 나는 것은 예상해야 한다. 현재 classifier는 generic 문자열
`DBMS_SESSION`을 timeout marker보다 먼저 검사한다. driver가 SQL text를 error에 echo하면 진짜 timeout도
setup failure가 된다.

SQL text substring 대신 exact SQLCODE/category를 쓰고, setup failure / cancel / timeout / transport error를
서로 배타적으로 분류해야 한다.

근거: [Oracle 12.2 DBMS_SESSION](https://docs.oracle.com/en/database/oracle/oracle-database/12.2/arpls/DBMS_SESSION.html),
[Oracle 18c DBMS_SESSION.SLEEP](https://docs.oracle.com/en/database/oracle/oracle-database/18/arpls/DBMS_SESSION.html).

---

## 5. 권한 후보 37건과 P1-11

### 5.1 닫힌 부분

기계적 cardinality는 맞는다.

- summary 37건
- detail 37건
- 적대 검증 9건
- 미검증 28건
- 9개 기각과 R1~R9의 대응

“28건은 승인도 기각도 아니다”라는 표시도 명확하다.

### 5.2 아직 재현 불가능한 부분

P1-11은 **PARTIAL**이다.

- 저장소에 raw `journal.jsonl` 또는 immutable export가 없다.
- journal digest, entry ID/offset, timestamp, prompt, model/tool configuration, 9건 선택 규칙이 없다.
- 1,302줄 문서에 직접 source URL이 0건이고 `[...]` 절단 표기가 다수 남아 있다.
- 문서 스스로 primary-source URL·edition·access time이 없다고 인정한다.

따라서 이 문서는 읽고 재심사할 수 있는 working reconstruction이지, “journal에서 verbatim 복원됐음”을
감사할 수 있는 원자료는 아니다.

필수 보강:

- immutable journal export + SHA-256
- candidate별 source entry ID/offset
- 원 prompt·selection protocol·model/runtime metadata
- CONFIRMED claim마다 direct primary URL, Oracle edition/version, access time
- 37개를 “grant 후보”가 아니라 `options/claims`로 이름 변경
- `grant_ddl_writes_primary`와 `runtime_path_writes_source`를 분리

### 5.3 보류 판정의 올바른 근거

보류는 다음 근거로 충분히 정당하다.

1. core가 권한에 의존해서는 안 된다.
2. 현재 승인 판정을 받은 옵션은 0건이다.
3. 28건은 아직 검증되지 않았다.
4. G0-0으로 이미 가진 권한조차 확인하지 않았다.

반대로 “9건이 모두 기각됐으므로 37건은 순이익이 없다”는 추론은 성립하지 않는다.

---

## 6. `effective_value`, stale, lifecycle

스키마 설명은 `effective_value`를 authoritative 값이라고 하지만 구현은 모든 축에
`effective_value=value`를 넣는다. `stale`과 axis-level `measured_at`은 optional이고 생성되지 않는다.
summary와 exit 판정도 `effective_value`가 아니라 `value`를 읽는다.

필수 불변식:

```text
child incomplete/failed/unbound     => dependent effective_value = floor
stale = true                        => effective_value = floor
profile/runtime/source mismatch     => effective_value = floor
value                               => 과거 관측값, audit/display only
effective_value                     => validator/publish가 읽는 유일한 값
```

schema는 각 axis에 `value`, `effective_value`, `measured_at`, `stale`, freshness basis를 모두 요구해야 한다.
old-high/new-transient, old-high/new-denied, old-high/expired 회귀 시험이 필요하다.

---

## 7. P1-03 / P1-07 / P1-09를 미뤄도 되는가

| 항목 | raw 수집 전 | semantic normalization 전 | publish/동결 전 |
|---|---|---|---|
| P1-03 SQLCODE taxonomy | **미뤄도 됨** — raw SQLCODE 보존 | **필수** | 필수 |
| P1-07 LOB retention 분리 | raw metadata 수집은 가능 | Flashback/LOB 축은 floor | **Flashback/LOB 활성화 전 필수** |
| P1-09 child schema/sentinel/manifest | 외부 wrapper가 있으면 제한적 수집 가능 | **필수** | 필수 |
| final 9축/composition | raw fact vocabulary만 먼저 고정 | 측정 분포를 보고 설계 가능 | **필수** |

P1-03의 현 `ABSENCE_ORA`는 unsupported, permission denied, wrong target, probe bug를 섞는다.
예를 들어 ORA-01031/00942가 `row_hash=NONE`, `sql_dialect=11G`가 될 수 있다.
이는 raw 수집 안전성은 해치지 않지만 capability·grant·drift 판정을 오염시킨다.

P1-07은 일반 UNDO, BasicFiles/SecureFiles, PCTVERSION/RETENTION mode, column coverage,
필요 horizon, aged AS OF positive test를 분리해야 한다. 이 전에는 `retention_guarantee=true`를 만들지 않는다.

---

## 8. 로컬 PoC H/D/X 판정

### 8.1 H

다음은 H로 유효하다.

- partial classpath에서 source compile/API linkage
- manifest/ServiceLoader unit behavior
- CE runner의 Python-level control flow
- synthetic analyzer/normalizer negative tests

단 H는 Oracle/ADG/Spark runtime 보증이 아니다.

### 8.2 D

“같은 vanilla Spark version”만으로 전이하면 안 된다. 최소 다음 equivalence predicate가 필요하다.

```text
exact Spark distribution/build + full classpath
Scala/JDK bytecode level
OJDBC jar digest
provider jar/source/config digest
deployment mode(driver/executor topology)
datasource path(DSv1/DSv2)와 query/options
profile/source identity
```

현재 local evidence는 partial Maven classpath compile/ServiceLoader 사실만 전이 가능하다.
schema/task/metadata coverage, fail-closed, connection peak는 전이할 수 없다.

### 8.3 X

현재 X 정의 “원리적으로 측정 불가”는 틀렸다.

- METADATA: 다른 DSv2 scenario로 측정 가능
- F-13: 반복·대기 실행으로 측정 가능
- scale: 적절한 인프라에서 측정 가능
- platform behavior: 코드가 생기면 측정 가능
- ADG/license behavior: 사내 환경에서 측정 가능

`LOCAL_OUT_OF_SCOPE`, `NOT_INDUCED`, `NO_TEST_ARTIFACT`, `REQUIRES_CORP_ENV`로 바꾸고
진짜 operationalize할 수 없는 주장에만 `UNMEASURABLE`을 쓴다.

### 8.4 로컬 증거 누수 경로

- normalizer가 caller의 `--profile`을 믿는다.
- child evidence에 profile/run/source/lock digest가 없다.
- normalization 당시 lock을 사후 hash한다.
- local B1 evidence에는 raw `javap`/ServiceLoader output과 artifact hash가 없다.

`gate_eligible=false`가 직접 G0 PASS는 막지만 문서·수동 capability 등록까지 막지는 못한다.
profile과 exact runtime binding을 소비자까지 강제해야 한다.

---

## 9. simplification-decision §6 공백에 대한 답

이 항목들은 G0-0 결과를 기다려 자동으로 닫히지 않는다. 설계 결정을 먼저 내려야 한다.

### 9.1 fence 전달 가능성

**결정:** Profile U에서 다중 JDBC connection을 Job 단위 공통 snapshot으로 선언하지 않는다.

- `SET TRANSACTION READ ONLY`는 connection scope다.
- 공통 SCN/timestamp literal을 모든 객체·connection에 적용했다는 증거가 없으면
  `snapshot_scope=JOB`은 금지한다.
- 다중 partition 추출은 `snapshot_scope=CONNECTION` 또는 `STATEMENT`,
  `upsert_consistency=BEST_EFFORT`다.
- Full의 target `INSERT OVERWRITE` commit이 atomic해도 source read image가 atomic해지는 것은 아니다.

JOB scope를 얻는 경로는 명시적으로 제한한다.

1. 공통 AS OF SCN/timestamp literal + 전 object 권한/지원 + 전 physical connection 전파 실증
2. commit-ordered CDC/snapshot export 같은 별도 source capability

### 9.2 `STANDBY_MAX_DATA_DELAY D`

**결정:** D는 source monitor session 속성이 아니라 Run/Job freshness 정책이며 모든 실제 extraction
connection에서 적용·검증돼야 한다.

- monitor는 source health 관측/사전 admission 용도다.
- provider가 각 data connection에 D를 적용하고 ORA-03172 양성 대조가 있어야 `lag_admission=ENFORCED`다.
- 서로 다른 D를 가진 Job이 monitor connection 하나를 공유하는 것은 허용하되, 그 monitor의 D를
  Job enforcement 증거로 사용하지 않는다.
- 모든 경로를 덮지 못하면 effective lag admission은 `NONE/UNDETERMINED`다.

### 9.3 `safety_lag`, `clock_skew`, `overlap`

**결정:** Profile U에서는 측정 불가능한 clock 항을 correctness proof에서 제거한다.

- client↔standby RTT/clock offset probe는 관측 가능하지만 primary commit time bound가 아니다.
- primary↔standby clock witness가 없으면 `clock_skew_max`를 보증값으로 만들지 않는다.
- overlap은 관측된 late-arrival distribution과 운영 비용으로 정한 risk-control이다.
- `overlap_basis=OBSERVED_QUANTILE|OPERATOR_CONSERVATIVE`, observation window와 exceedance를 공시한다.
- overlap 밖 late commit 가능성이 남으므로 BEST_EFFORT + reconciliation을 강제한다.

즉 “근거 없는 보수 상수”를 큰 숫자로 잡아 ZERO_GAP처럼 보이게 하지 않는다.

### 9.4 ORA-03172 자기증폭 루프

**결정:** 이 항목은 G0 결과를 기다리지 않고 A v2.0의 필수 정책으로 확정한다.

- source-scoped circuit breaker
- centralized token-bucket/concurrency admission
- full jitter backoff(동일 next_eligible_at 금지)
- source admission/identity/auth 계열은 `spark.task.maxFailures=1`
- Spark 내부 retry가 아니라 Control Plane이 retry budget 소유
- breaker open 중 신규 source read 0, Hold 종료 후 단일 bounded catch-up
- ORA-03172 storm, 500-run synchronized failure, half-open probe 1개 양성 시험

### 9.5 B1과 세션 budget

provider 호출 횟수는 concurrent peak가 아니다. close instrumentation과 active-count transition이 없으면
session lease/budget을 증명할 수 없다. `(instance_name, sid, sessionid)`와 provider connection UUID를
함께 기록하고, close 누락과 trace loss를 failure로 만든다.

---

## 10. 문서 상태 오류

다음은 기능보다 먼저 고쳐야 운영자가 잘못된 절차를 실행하지 않는다.

1. A executable은 87 probe인데 README/local plan/simplification은 86이라고 쓴다.
2. README는 B1을 “실제 Spark 4.2.0에 대고 빌드”했다고 하고 뒤에서는 실제 Spark jar build 전이라고 한다.
   정확한 표현은 “partial Maven classpath compile/API linkage 완료, full distribution runtime 미실행”이다.
3. README는 `g0_0_evidence`를 다시 `g0_evidence`라고 부른다.
4. 7차 assessment는 path-specific injection 구현 완료와 미구현을 동시에 적는다.
5. overlay는 7축/9축, `lag_visibility` 폐기/사용, `bound_kind` 복원/대체를 동시에 적는다.
6. “G0-0 한 번도 미실행”은 “사내 원천/full sequence 미실행”으로 한정해 B1 partial compile·SPI linkage 기록과 구분한다.
7. 요청서 기준 커밋을 사용자 지정판 `9cba920`과 일치시킨다. 이번 문서 갱신에서 반영했다.

---

## 11. 최소 수정 순서

### M0 — 실행 안전성, 먼저

1. wrapper `pipefail` + producer exit/sentinel 보존
2. B0 `sys.exit(main())`
3. B0/B1 partitions·sessions 하드 상한
4. target 접촉 전 fail-closed DB identity preflight
5. C01~09 외부 disposable-environment allowlist
6. README 86→87와 실행 명령 정정

### M1 — child evidence contract

1. A/B0/B1/C00 개별 schema
2. run ID, source, profile, runtime/lock/harness digest, start/end, exit, exact manifest
3. duplicate/unknown/concatenated run 거부
4. run-specific immutable artifact path

### M2 — B1 재작성

1. explicit `connectionProvider`
2. 실제 `failclosed_task` 실행 경로
3. stack guess와 actuator 분리
4. schema/task/metadata 독립 scenario
5. terminal failure token·business SQL 0·trace completeness 판정

### M3 — normalizer

1. child schema 검증 후에만 aggregation
2. typed probe predicate와 SQLCODE taxonomy
3. PARTIAL/FAILED/unbound/stale의 effective floor
4. exact `not_covered`, final G0 schema/aggregator 분리
5. stale final alias 문제 제거

### M4 — 사실·규범 문서 정정

1. AS OF TIMESTAMP 3초 방향과 common snapshot 가치
2. ORA-08180/08181
3. Spark provider 선택
4. grant hold 근거와 passive grant/runtime 활성화 분리
5. overlay 내부 모순과 H/D/X 재분류

### M5 — 그 뒤 G0-0

1. disposable lab에서 B1·CE negative tests
2. pinned 사내 runtime에서 A/B0/B1
3. C00은 별도 scan 승인
4. raw immutable evidence 보존
5. 측정 분포로 axes/composition 확정
6. A v2.0/P v2.0 개정

---

## 12. 필수 회귀 시험

### Normalizer/contract

- missing/duplicate/unknown probe ID
- summary 복수·concatenated runs
- summary 존재 + end sentinel 부재 + child exit nonzero
- old log/new lock, A/B source swap, profile relabel
- query_ok=true + row 없음/null/malformed value
- unsupported/denied/empty/transient/wrong-target/probe-bug SQLCODE
- stale high, new transient, expiry
- invalid rerun 뒤 old final을 current로 읽지 않음
- G0-0 record를 final G0 gate에 입력하면 항상 거부

### B1

- explicit provider selection with Basic enabled
- schema-only failure token → action terminal failure + business SQL 0
- multi-JVM task-only injection → schema 성공, executor 실패, row 0
- DSv2 metadata-only injection
- TASK trace 있으나 terminal success → NOT_PROVEN
- missing sentinel/result/trace → NOT_PROVEN
- MIXED/UNKNOWN only → NOT_PROVEN
- trace write failure → evidence invalid
- create/close/active peak 일치

### Source safety

- partitions cap+1이 target 접촉 전 거부되고 nonzero exit
- wrong DB/role이 target 접촉 전 거부
- producer fail + tee success에서도 wrapper nonzero
- C00 compile failure가 completion PASS가 되지 않음
- CE production-like identity가 외부 allowlist 없이는 거부

### Architecture

- multi-connection READ ONLY가 JOB scope로 승격되지 않음
- 동일 AS OF TIMESTAMP literal의 전 connection/object 전파
- ORA-03172 500-run storm에서 breaker open 후 신규 source read 0
- jitter로 next eligible 분산
- overlap exceedance가 BEST_EFFORT 계약과 reconciliation으로 노출

---

## 13. 최종 권고

이 단계에서 다시 1,500줄 규범을 먼저 패치하지 않는 것이 맞다. 그러나 “실측 전 문서 변경 금지”를
증거 도구 결함까지 미루는 근거로 쓰면 안 된다. **지금 고칠 것은 규범 enum이 아니라 측정기의 안전성과
증거 결속이다.**

권고하는 다음 한 묶음은 다음과 같다.

1. M0+M1만 작은 semantic patch로 닫는다.
2. B1을 stack-driven injection에서 scenario-driven injection으로 바꾼다.
3. synthetic negative suite가 전부 fail-closed임을 확인한다.
4. 그 뒤에만 사내 G0-0 raw 측정을 시작한다.
5. Profile U의 첫 구현 범위는 `FULL_BEST_EFFORT`와 `INCREMENTAL_BEST_EFFORT + reconciliation`으로 제한한다.
6. `snapshot_scope=JOB`, `ZERO_GAP`, Flashback overlay는 별도 capability가 실증될 때만 활성화한다.

이렇게 하면 Dagster 전환·Job Wizard·Source System 관리 UI 같은 플랫폼 가치 작업은 멈추지 않으면서,
확인되지 않은 Oracle 보증을 새 플랫폼의 기반에 굳히지 않을 수 있다.
