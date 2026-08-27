# ETL Platform v2.0 저장소 7차 교차 리뷰

- 검토일: 2026-08-27
- 요청 기준판: `538ec311ade9603f057bf410ae64220c8c3effb6` (`main`, 검토 시작 시 clean, 추적 파일 66개)
- 검토 대상: capability overlay, 권한 요청 판정, 감축 결정, A v1.2.3.1, P 8차, G0-0 증거 스키마·정규화기·A/B0/B1/C00/C01~C09 실행물
- 방법: 문서 간 계약 대조, 실행물 정적 검사, 반례 구성, Oracle·Spark 1차 출처 대조
- 중요한 한계: **G0-0은 한 번도 실행되지 않았다.** 이 문서의 실행물 판정은 정적 검토이며 Oracle·Spark 실측값을 가정하지 않는다.
- 작업 중 새 커밋 `c2fa93b13a5527e4fc13434f2ce7dc633f3dc9a4`가 원격과 로컬 `main`에 추가되었다. 본 판정의 권위는 요청한 `538ec31`이며, 새 커밋의 4파일 delta는 §8에서 별도 재확인했다.

---

## 0. 최종 판정

**전체 방향은 GO지만, capability 규격 동결과 현재 G0-0 실행은 NO-GO다.**

| 대상 | 판정 | 이유 |
|---|---|---|
| Dagster OSS + 얇은 Control Plane 방향 | **GO 유지** | 선언형 JobSpec, 중앙 Hold/Catch-up, source protection, lineage·reconciliation, Kafka 알림의 운영 가치는 Oracle 보증 등급과 독립적으로 남는다 |
| 단일 privilege-zero core + 원천별 overlay | **GO, 모델 수정 조건** | Profile O/U 이중 제품보다 맞는 방향이다. 다만 현재 7축은 독립 성질을 합치고 scope·조합 규칙이 없어 그대로 동결할 수 없다 |
| `etl-platform-v2.0-capability-overlay.md` 동결 | **NO-GO** | `snapshot_read`, `lag_visibility`, `row_hash`가 실제로 증명하지 않은 능력을 표시할 수 있고 `bound_kind`를 잘못 제거한다 |
| DBA 권한 요청 | **지금 제출 보류에 동의** | G0-0 미실행, 37개 중 28개 미검증이므로 현재 요청서를 내지 않는 것은 타당하다 |
| “FLASHBACK은 받아도 순이익이 아니다”라는 일반 결론 | **부분 기각** | 권한 자체와 기능 활성화 비용을 합쳤고, 여러 Spark connection에 공통 snapshot을 주는 유일한 선택 가치를 누락했다 |
| G0-0 실행물 준비 완료 | **NO-GO** | 정상·부분·조작 입력에서 거짓 `MEASURED`/`PROVEN` 및 exit 0이 가능한 P0가 있다 |
| “G0-0은 G0 전체가 아니다” 경계 | **문서상 확인 / 실행상 미폐쇄** | `not_covered`와 설명은 옳지만 schema·normalizer·exit code가 경계를 강제하지 않는다 |
| A v1.2.3.1 / P v1 | **Profile O 참고판으로 보존** | 현재 조직 제약의 구현 규범으로는 사용할 수 없다. G0-0 증거 계약을 먼저 고친 뒤 A/P v2.0으로 올려야 한다 |

현재 가장 위험한 것은 Oracle 사실의 미확정 자체가 아니다. **미확정을 확정값으로 바꾸는 코드가 이미 있다는 것**이다. 이 저장소의 규율인 “확인하지 못한 것은 미확인”을 실행물부터 위반한다.

---

## 1. P0 — G0-0 실행 전에 반드시 닫을 결함

### P0-01. `derive_axes()`가 반대 증거가 있어도 capability를 승격한다

`g0-normalize.py:96-104`의 `snapshot_read` 파생은 세 경로가 잘못됐다.

1. `as_of_timestamp.target` 성공 + `view.v_database` 성공이면 `AS_OF_SCN`이다. 그러나 A의 `view.v_database`는 `V$DATABASE`에서 `COUNT(*)` 한 행을 읽을 수 있는지만 본다(`g0-0a-capability-inventory.sql:268-269`). SCN을 읽지도 않고, 그 SCN으로 대상 object를 `AS OF SCN` 조회하지도 않는다.
2. `AS OF TIMESTAMP`만 성공하고 SCN 원점이 없으면 `READ_ONLY_TXN`이다. `txn.set_read_only`와 `txn.select_inside`가 둘 다 실패해도 같은 결과다.
3. `txn.set_read_only.reissue`에서 ORA-01453이 나와야 한다는 양성 대조(`g0-0-probe-README.md:131-133`)를 파생기가 전혀 읽지 않는다.

정적 반례:

```text
as_of_timestamp.target.query_ok = true
dbms_flashback.get_scn.query_ok = false
view.v_database.query_ok = false
txn.set_read_only.query_ok = false
txn.select_inside.query_ok = false
=> 현재 결과: READ_ONLY_TXN
=> 정답: AS_OF_TIMESTAMP_OBJECT_ACCESS 또는 UNDETERMINED. READ_ONLY_TXN은 아님
```

`lag_visibility`도 같은 오류다(`g0-normalize.py:134-140`). `V$DATAGUARD_STATS COUNT(*)`가 실행되면 실제 lag row·값이 없어도 `DG_STATS`, `ALTER SESSION` 문장이 수락되면 ORA-03172 양성 대조가 없어도 `MAX_DELAY_ONLY`가 된다.

필수 정정:

- `AS_OF_SCN`: `SCN 획득 → 같은 standby의 모든 extract object에 그 정확한 SCN으로 조회 성공 → 2개 이상 physical connection에서 같은 anchor 성공`을 하나의 composite probe로 증명한다.
- `AS_OF_TIMESTAMP`: 고정된 timestamp literal을 같은 object-set·다중 connection에 적용한 별도 capability로 둔다.
- `READ_ONLY_TXN`: `SET TRANSACTION`, 내부 SELECT, 재발행 ORA-01453, 실제 data path 적용을 모두 요구한다.
- `DG_STATS`: 실제 lag 값과 `DATUM_TIME`을 해석할 수 있어야 한다.
- `MAX_DELAY_ENFORCED`: 정상 대조 성공과 lag 상태에서 ORA-03172 양성 대조를 모두 요구한다.
- 예상된 “권한 없음/구문 미지원”과 timeout·접속 단절·대상 오기입을 구분한다. 후자는 `NONE`이 아니라 `UNDETERMINED`다.

### P0-02. 정규화기는 불완전·조작 산출물을 `MEASURED`로 만든다

다음 입력이 모두 통과 가능하다.

| 입력 | 현재 판정 | 근거 |
|---|---|---|
| B0에 JSON probe 한 줄만 존재 | `MEASURED` | `g0-normalize.py:248-252` |
| B1 파일이 `{"verdict":{"coverage":"PROVEN"}}`뿐 | `MEASURED` | `g0-normalize.py:255-265` |
| C00에 `fence.summary` 한 줄만 존재 | `MEASURED` | `g0-normalize.py:269-275` |
| CE 파일이 `suite_verdict.pass=true`, scenario 0개 | `MEASURED` | `g0-normalize.py:278-285` |
| A summary가 없지만 일부 probe가 존재 | coverage는 `PARTIAL`, 축은 확정값 가능 | `g0-normalize.py:234-237,300` |
| schema 검증 오류 | 파일을 쓴 뒤 stderr만 출력, exit 0 가능 | `g0-normalize.py:310-331` |

A 계약은 exit 0 + `probe_run_end` + `manifest_ok=true`를 모두 요구하지만(`g0-0-probe-README.md:102-105`), `jsonl()`은 sentinel을 버리고 프로세스 exit code를 받지 않는다. concatenated log에서는 첫 summary와 마지막-wins probe가 다른 회차에서 섞일 수도 있다.

CE runner도 child process의 `returncode`를 검사하지 않는다(`g0-0c-counterexamples/runner.py@538ec31:279-323`). 시나리오가 통과 모양의 `SCENARIO_RESULT`를 찍은 뒤 exit 1이어도 suite PASS 후보가 된다.

필수 정정:

- A/B0/B1/C00/C-suite 각각 별도 schema, `run_id`, 시작·종료 sentinel, expected manifest, exit code, source identity, runtime digest를 요구한다.
- child 하나라도 `PARTIAL|FAILED|NOT_RUN`이면 top-level은 `COMPLETE`나 gate-eligible이 될 수 없다.
- schema 검증 불가·위반은 파일을 최종 경로에 쓰지 않고 nonzero로 종료한다.
- parser는 duplicate probe id와 복수 summary를 거부한다.
- CE는 `returncode == 0`과 payload를 함께 요구한다.

### P0-03. 증거가 측정 대상·시각·실행 판본에 묶이지 않는다

overlay는 세 축을 `(source_db, table)` 단위로 상속한다고 하지만(`etl-platform-v2.0-capability-overlay.md:68-75`), schema의 `source`는 optional이고 내부 필드도 전부 optional이다(`g0-evidence.schema.json:21-30`). normalizer는 `target_owner`, `target_table`, `wm_column`을 한 번도 채우지 않는다(`g0-normalize.py:239-245`).

따라서 `ROWDEPENDENCIES=ENABLED`인 테이블 A에서 얻은 `ROW_LEVEL` 결과를 같은 DB의 테이블 B에 적용하는 것을 증거가 막지 못한다.

판본 binding도 사후 부착이다. normalizer가 **정규화 시점의** `versions.lock`을 hash할 뿐, B0/B1/C가 실제로 실행한 Spark·OJDBC·Oracle·scenario config와 비교하지 않는다. old log를 Spark Y가 적힌 새 lock과 함께 정규화할 수 있다. `executed_at`도 probe 시각이 아니라 정규화 시각이다.

필수 정정:

- 모든 child artifact가 실행 시작 시 `versions_lock_digest`와 실제 runtime fingerprint를 스스로 기록한다.
- aggregator는 다섯 child의 digest·profile·source/fixture identity가 맞는지 비교한다.
- table capability에는 정규화된 `(db_identity, owner, object, object_type, column)`과 extract object manifest digest를 필수로 둔다.
- `measured_at`은 child가 기록하고 aggregator는 바꾸지 않는다. `normalized_at`을 별도 필드로 둔다.
- current capability는 immutable `SourceCapabilityRevision`으로 저장하고 Job release·contract가 그 revision/digest를 pin한다.

### P0-04. `g0_evidence`라는 이름으로 서로 다른 두 계약을 정의한다

P §8.1은 최종 G0 증거에 `ddl_digest`, `verdict_sql_digest`, `canonical_hash_spec_digest`, `hash_vector_result`, `submission_path_result`, `source_kind`, `oracle_env`를 요구한다(`etl-platform-poc-test-plan-v1.md:510-514`). 현재 schema는 같은 `record_type=g0_evidence`, 같은 `schema_version=1.0.0`을 사용하면서 그 필드를 허용하지 않고 `additionalProperties:false`다.

즉 현재 schema는 G0-0 부분 레코드만 표현할 수 있고, 최종 G0 레코드는 오히려 거부한다. `not_covered`도 다섯 항만 나열해 `source_kind`, `oracle_env{nls_nchar_characterset,max_string_size}`, 동일 lock 실증(G0-5)을 빠뜨린다. 임의 dummy 한 항과 빈 `artifacts`도 schema-valid다.

필수 정정:

- 부분 레코드를 `g0_0_evidence`/별도 schema로 분리한다.
- 항상 `gate_eligible=false`, `scope=CAPABILITY_INVENTORY`, `not_covered`를 고정 enum으로 둔다.
- 최종 `g0_evidence`는 G0-1~G0-5 aggregator만 생성한다.
- `not_covered`는 설명용 자유 텍스트가 아니라 schema가 요구하는 정확한 항목 집합으로 만든다.
- CLI exit code를 `0=valid artifact written`, `3=incomplete`, `4=invalid`로 분리하고, **부분 레코드 exit 0을 G0 PASS로 해석할 수 없게** 한다.

### P0-05. capability overlay가 독립인 정합성 축을 합친다

가장 큰 모델 오류는 overlay `:94`다. `bound_kind/bound_evidence`를 `lag_visibility`로 대체한다. Data Guard apply lag와 `commit_time - watermark_value`는 독립이다. apply lag가 0이어도 오래된 `UPDATE_DT`를 가진 transaction이 뒤늦게 commit하면 overlap 밖 누락이 생긴다. A `:811,815-820`이 두 값을 따로 다룬 이유가 사라진다.

또한 현재 축은 다음을 합친다.

- `snapshot_read`: object FLASHBACK 권한, SCN 원점, ADG 지원, Spark connection 전파, snapshot scope
- `lag_visibility`: lag **관측**과 lag **admission 강제** — 우열 관계가 아니라 동시에 존재 가능한 성질
- `row_hash`: SHA-256 함수 존재와 cross-engine canonical row hash 검증
- `charset_class`: 한 DB의 charset과 두 source 간 comparable 여부

최소 모델은 다음처럼 분리해야 한다.

| 축 | 예시 값 | 판정 단위 |
|---|---|---|
| `snapshot_anchor` | `SCN / TIMESTAMP / NONE` | account + DB |
| `snapshot_object_coverage` | manifest digest + `ALL / PARTIAL / NONE` | object-set, LOB는 column/segment |
| `snapshot_scope` | `JOB / CONNECTION / STATEMENT` | Spark runtime + Job read path |
| `lag_observation` | `DG_STATS / NONE` | DB/account |
| `lag_admission` | `MAX_DELAY_ENFORCED / NONE` | connection path |
| `watermark_commit_bound` | `ENFORCED / OBSERVED / NONE` | table + watermark column |
| `hash_function` | `SHA256 / NONE` | DB/runtime |
| `canonical_row_compare` | `VECTORS_PROVEN / PARTIAL / NONE` | mapping + object-set + engines |
| `wm_successor` | exact datatype/scale/round-trip result | column + OJDBC/Spark/runtime |

그 위에 별도 composition function을 둔다. 예를 들어 `snapshot_scope=JOB`은 공통 anchor + 모든 object coverage + 모든 data connection 적용이 모두 참일 때만 나온다. `canonical_row_compare=VECTORS_PROVEN`은 G0-3 전에는 절대 나오지 않는다.

### P0-06. B1이 task-path fail-closed를 증명하지 않고도 `PROVEN`이 될 수 있다

`fail=all`은 provider가 처음 호출된 schema connection에서 즉시 예외를 던진다(`Preamble.java:41-46`). 각 Spark step은 다시 schema resolution에서 막혀 task connection에 도달하지 못할 수 있다. 그런데 analyzer는 failclosed result가 존재하고 status가 `BROKEN/PARTIAL`이 아니면 `YES`로 둔다(`analyze-trace.py:127-152`). failclosed의 TASK trace가 0이어도 `PROVEN` 가능하다.

추가 문제:

- `MIXED` trace 한 건을 SCHEMA와 TASK 양쪽 관측으로 센다(`analyze-trace.py:103-106`).
- DSv2 `METADATA` 경로는 유발하지 않으면서 README의 첫 문장은 “모든 물리 connection”을 전제로 한다. 문서 후반의 제외 고지는 좋지만 normalizer에는 scope 구분이 없다.
- B1 `Preamble`은 `SET TRANSACTION READ ONLY`를 실행하지 않는다. 따라서 B1 통과는 snapshot capability 증거가 아니다.
- 실행 프리앰블은 `NLS_NUMERIC_CHARACTERS = '. '`를 사용한다(`Preamble.java:94`, `run-g0-0b1.py:77`). A가 요구하는 `'.,'`와 다르다.
- 향후 READ ONLY를 추가할 때 현재 identity SELECT 뒤에 단순히 붙이면 안 된다. Oracle은 `SET TRANSACTION`이 transaction의 첫 statement여야 한다. identity 확인 뒤 `COMMIT/ROLLBACK`으로 경계를 닫고, barrier 뒤 `SET TRANSACTION READ ONLY`로 snapshot을 열거나 task가 단일 SELECT뿐이면 `STATEMENT` scope로 정직하게 표시해야 한다.

필수 정정:

- `fail=schema|task|metadata|after_preamble`처럼 경로별 주입점을 만들고 각 경로를 실제로 유발한다.
- 경로별 최소 trace 수, 실패 trace, job terminal, 업무 SQL 0을 manifest로 검사한다.
- `MIXED`는 수동 판정 전 양쪽 PASS로 세지 않는다.
- provider reachability, session assertion, read-only transaction, common snapshot을 서로 다른 verdict로 낸다.
- pinned Spark patch마다 build·ServiceLoader·provider selection·upgrade gate를 반복한다. `JdbcConnectionProvider`는 Spark가 `DeveloperApi`/`Unstable`로 표시한 API다.

---

## 2. 요청한 네 가지 초점에 대한 직접 답

### 2.1 capability 축과 `derive_axes`는 타당한가

**방향은 타당하지만 현재 정의·파생은 동결 불가다.**

좋은 점:

- 버전 추정보다 기능 직접 probe를 우선한다.
- 전 입력 부재와 `manifest_ok=false`에서 축을 `UNDETERMINED`로 두는 기본 방향은 맞다.
- SHA-256 `abc` 양성 벡터, raw artifact digest, probe id provenance를 남긴다.
- DB 축과 table 축을 나누려는 시도는 맞다.

필수 보완:

1. P0-05의 독립 축으로 재분리한다.
2. 각 축의 scope와 composition rule을 schema로 강제한다.
3. `query_ok`, `row_present`, `value_interpretable`, expected SQLCODE를 모두 판정 입력으로 쓴다.
4. `NONE`과 `UNDETERMINED`를 분리한다. transient error는 기능 부재가 아니다.
5. `measured_at`, `expires_at`, `stale`, source/object binding, capability revision을 필수화한다.
6. stale 값은 **감사용 previous value**로 보존하되 effective capability는 floor/unknown으로 내린다. 안전성에 쓰는 expired high grade를 계속 실행에 사용하면 fail-closed가 아니다.

`wm_granularity` 구현도 수정해야 한다. 현재 `TIMESTAMP(1|2|4|5|7|8)`을 모두 `US`로 보내고, `NUMBER(p,s)`를 시간 단위 `SEC`로 표시한다(`g0-normalize.py:142-160`). successor는 등급 enum이 아니라 `datatype`, exact scale, min step, overflow, Oracle→OJDBC→Spark→Control round-trip 결과로 저장해야 한다.

`row_hash=SHA256`도 너무 강하다. G0-0A가 증명하는 것은 함수 한 개의 가용성뿐이다. NULL framing, NFC, NUMBER/TIMESTAMP 직렬화, LOB 경로를 포함한 **행 내용 비교 능력은 G0-3 V-01~V-16을 통과한 뒤** 활성화해야 한다.

### 2.2 권한 요청 보류와 FLASHBACK 논증은 충분한가

**“지금 요청서를 내지 않는다”는 결론은 충분하다. “FLASHBACK은 순이익이 아니다”라는 일반화는 충분하지 않다.**

Oracle은 Flashback Query에 대상 object의 `FLASHBACK`과 `SELECT/READ`가 필요하고, `DBMS_FLASHBACK` 사용에는 별도 `EXECUTE`가 필요하다고 명시한다. 또한 timestamp query는 지정 시각보다 **최대 3초 앞이 아니라 이전 시점으로** 매핑될 수 있다고 명시한다. 현재 문서의 `±3초`와 “방향성이 없다”는 표현은 정정해야 한다.

고정 timestamp literal을 모든 partition query에 주입하면 common snapshot 후보가 된다. 이것이 실제 ADG·object-set·다중 connection에서 성립하는지는 G0 composite probe로 확정해야 하지만, 처음부터 `READ_ONLY_TXN`으로 접어 버릴 이유는 없다. connection-local READ ONLY가 해결하지 못하는 병렬 Spark snapshot skew를 줄이는 고유 가치가 있다.

권한과 활성화도 분리해야 한다.

```text
grant 존재
  != feature 자동 활성화
  != UNDO/LOB retention 변경 요청
  != production primary 설정 변경
```

권고 판정은 다음이다.

- 상태: `NO_SUBMIT_PENDING_G0` — 임시 hold
- G0에서 기존 grant를 발견해도 default OFF
- object-set coverage, 고정 timestamp/SCN 다중 connection, 예상 실행시간 대비 일반 undo, LOB별 aged-AS-OF 양성 시험, DDL 패턴을 통과한 Job에만 opt-in
- primary `UNDO_RETENTION` 변경 없이 기존 budget으로 안 되면 그 Job의 capability를 활성화하지 않는다
- 후보 37개 전부를 `grant SQL / scope / benefit / prerequisites / runtime cost / primary change / evidence / verdict` 37행 matrix로 남긴 뒤 최종 판정

사실 정정도 필요하다.

- ORA-08181은 “같은 SCN 재사용”이 아니라 valid SCN 경계를 벗어난 값이다. 오래된 정상 SCN의 주 위험은 undo/history 계열이다.
- ORA-01466 예시는 문서에 명시된 table 구조 변경 DDL로 한정한다. index rebuild를 일반 트리거로 단정하지 않는다.
- ABMR은 일반 standby read의 corruption-contingent 위험이지 FLASHBACK grant 고유 비용이 아니다.
- 운영 부담은 10,000 Job 수가 아니라 distinct queried object × owner/source 경계 × schema churn × revoke 빈도로 계산한다.

### 2.3 `G0-0 != G0` 경계는 정확한가

**설명은 정확하지만 계약은 그렇지 않다.**

확인한 좋은 경계:

- schema description과 normalizer 주석이 G0-0을 부분집합이라고 명시한다.
- LOCAL_WSL 결과를 설계 근거로 쓰지 말라는 경고가 있다.
- `not_covered`에 G0-1~4 핵심 산출물 다섯을 적었다.
- raw artifact digest를 보존한다.

그러나 P0-02~04 때문에 schema-valid/exit-0가 그 경계를 보장하지 않는다. 특히 같은 `g0_evidence` 이름 충돌은 후속 G0 조립을 막는다. 경계의 최종 합격식은 다음이어야 한다.

```text
G0-0 completed  => capability inventory artifact only
G0 PASS         => G0-1 && G0-2 && G0-3 && G0-4 && same_lock(G0-5)
G0-0 completed  != G0 PASS
```

이 관계를 prose가 아니라 record type, schema condition, aggregator, exit code 네 곳에서 동일하게 강제해야 한다.

### 2.4 `simplification-decision.md` §6의 공백에 대한 답

§6은 여섯 bullet처럼 보이지만 `safety_lag/clock_skew`가 중복되어 **고유 공백은 다섯 개**다.

| 공백 | 판정 | A/P v2.0에서의 답 |
|---|---|---|
| global fence 전달 소멸 | **확인** | Profile U 기본에는 전달할 job-global fence가 없다고 선언한다. `snapshot_scope=CONNECTION|STATEMENT`; Full 병렬 읽기는 비원자 snapshot을 공시하거나 단일 data connection으로 낮춘다. 고정 AS OF TIMESTAMP/SCN이 composite probe를 통과한 object-set만 `JOB`으로 승격한다 |
| Source당 monitor 1개와 Job별 D | **문제 위치 정정** | monitor의 D를 executor에 “전달”하지 않는다. D는 모든 physical data connection에 각각 설정한다. Source 보호용 admission cap과 소비자 freshness SLO를 분리한다. monitor는 직렬 probe 또는 source breaker의 half-open probe일 뿐이다 |
| `safety_lag/clock_skew/overlap` 조달 부재 | **확인** | privilege-zero에서 hard minimum을 만들 수 없다. correctness 식에서 제거하고 `overlap_basis=OBSERVED`, 표본 기간·분위수·margin·last_measured_at를 공개한다. 이 값은 BEST_EFFORT tuning이며 preventive guarantee를 만들지 않는다 |
| B1 미실증 | **확인 + 현재 판정기 P0** | path-specific fail injection, exact Spark build, metadata 유발, close/peak trace를 통과하기 전에는 engine capability를 `UNDETERMINED`로 둔다. 로컬 build 성공도 사내 patched distribution 증거가 아니다 |
| ORA-03172 자기증폭 | **확인** | retry locus를 Control 하나로 고정한다. ORA-03172는 Spark task retry 0, source별 full/decorrelated jitter, retry budget, adaptive concurrency, source-scoped breaker, 저비용 half-open probe를 사용한다. breaker 동안 신규 Run은 Hold하고 incremental은 한 번의 bounded catch-up으로 합친다 |

§6에 추가해야 할 공백은 최소 다섯 개다.

1. **watermark commit bound가 사라졌는데 lag 축으로 대체했다.** 두 축을 복원·분리한다.
2. **Full 60%의 snapshot contract가 없다.** `SINGLE_CONNECTION_ATOMIC`과 `PARALLEL_NONATOMIC`의 선택·SLO·사용자 표시가 필요하다.
3. **무권한 source protection의 실체가 없다.** `GV$SESSION`·DB hard cap 없이 Control이 볼 수 있는 것은 자기 token과 client close뿐이다. `SELF_LIMITED_ONLY`, 보수적인 linger reservation, legacy writer 격리, source별 connection/QPS/bytes budget을 명시한다.
4. **capability drift가 진행 중 계약과 과거 증거에 어떻게 적용되는지 없다.** revision pin, effective downgrade, 신규 Hold, in-flight continue/fence 규칙이 필요하다.
5. **보증이 낮아진 뒤에도 플랫폼 전환 가치가 무엇인지 명시되지 않았다.** 아래 §4의 최소 가치 게이트로 답해야 한다.

---

## 3. P1/P2 추가 결함

### P1

| ID | 결함 | 최소 정정 |
|---|---|---|
| P1-01 | `capability_axes` 자체가 schema required가 아니고 key/value가 자유 문자열이다. overlay가 요구하는 `measured_at`, `stale`는 per-axis `additionalProperties:false` 때문에 저장할 수 없다 | 7축 또는 개정 축을 명시 property/enum으로 고정하고 lifecycle 필드를 schema화 |
| P1-02 | `stale=true`여도 이전 고등급을 유지한다(`overlay:83-85`) | 표시용 prior value와 effective capability를 분리. 안전성·publish 판정에는 `UNKNOWN/floor` 적용 |
| P1-03 | 실패 SQLCODE taxonomy가 없어 ORA-03135도 11g/기능 없음으로 강등될 수 있다 | unsupported/denied/empty/transient/wrong-target 분류표 + 재측정 정책 |
| P1-04 | `row_hash` scope가 overlay §5 DB/table 표에서 누락됐다 | 함수는 DB/runtime, canonicalizable column-set은 mapping/object 단위로 분리 |
| P1-05 | `charset_class=AL32UTF8` 하나로 `cross_source_comparable`을 주장한다 | NCHAR charset·NLS·normalization·mapping/vector digest를 composition 입력으로 사용 |
| P1-06 | `derived_from`이 실제 사용한 probe가 아니라 의도한 probe 목록이다. timestamp·interval/NLS/ORA_HASH는 목록에는 있으나 분기에 사용되지 않는다 | 실제 읽은 evidence id만 기록하고 미사용 probe는 `considered_but_not_used`로 분리 |
| P1-07 | LOB history를 ordinary undo와 하나의 `retention_guarantee` boolean으로 합친다(A `:843`) | UNDO tablespace와 LOB column/segment의 BasicFiles/SecureFiles·PCTVERSION/RETENTION mode를 분리하고 required horizon의 aged AS OF 양성 시험 요구 |
| P1-08 | B0 S4는 어떤 예외든 timeout 보호 성공처럼 기록한다. package 부재·권한 오류도 `ok=true`가 될 수 있다 | SQLCODE/elapsed expected predicate를 명시하고 ORA/timeout 종류가 맞을 때만 PASS |
| P1-09 | C00는 block compile 실패를 process 실패로 보장하지 않고, summary 한 줄만 있어도 normalizer가 `MEASURED`로 만든다 | end sentinel·expected probe manifest·개별 query success 조건을 추가 |
| P1-10 | CE artifact hash가 `suite.yaml`을 의도적으로 제외해 required scenario·budget·version·pass rule이 코드 hash에 묶이지 않는다 | code digest와 suite config digest를 별도로 기록해 둘 다 evidence에 bind |
| P1-11 | grant verdict의 37개 후보·9개 판정 원자료가 저장소에 없어 재현할 수 없다 | 37행 판정 matrix와 1차 출처 URL·판본·검증 일시 추가 |
| P1-12 | README는 “DBA 협조 불가 확정”, 감축 문서는 “비critical 요청 가능”이라 제약이 다르다 | “정합성 전제 불가; 비critical 요청은 가능하나 현재 보류·절대 가정하지 않음”으로 통일 |

### P2

- `g0-0-probe-README.md:65`는 C00 기본 실행 산출물이 0건이라지만 SQL은 skipped record와 summary를 낸다. “대상 table query 0건”으로 고친다.
- B0/B1의 `--probe-rows`/`--limit`은 0·음수·과대값 상한 검증이 없다. production-safe label을 유지하려면 hard maximum을 둔다.
- `COUNTEREXAMPLE_REPRODUCED`와 `MITIGATION_FAIL`도 suite `pass=true`에 포함된다. harness 완주 의미로는 가능하지만 `execution_complete`와 `mitigation_holds`를 별도 필드로 나눠 설계 PASS 오독을 막는다.
- `versions.lock`의 `UNSET` 검사는 YAML 파싱이 아니라 문서 전체 문자열 검색이다. 주석에 `UNSET`이 계속 있어 모두 채워도 경고가 사라지지 않는다.
- artifact `sha256` 형식과 `lines >= 0`이 schema로 제한되지 않는다.

---

## 4. 현재 Airflow 대비 가치와 Dagster 전환 판정

Oracle 보증이 `BEST_EFFORT + 탐지·복구`로 내려가도 다음 가치는 남는다.

- 10,000개 Job을 소스 파일 10,000개가 아니라 versioned JobSpec + 공통 template/bundle로 관리
- SourceSystem/ConnectionRevision, column mapping, Full/Append/Merge wizard, schema discovery를 한 제어면에서 관리
- 중앙 Hold/DRAIN, 종료 시점 1회 catch-up, backfill, retry/adjudication을 일관된 API로 제공
- source별 weighted budget과 burst 500의 공정성·보호 정책
- contract/attempt/commit lineage, reconciliation, 설명 가능한 data-quality 결과
- Dagster UI의 run graph/log/asset 관측과 Control UI의 Job CRUD를 분리

따라서 **Dagster 선택 자체는 여전히 합리적**이다. 다만 다음 네 조건이 PoC에서 서야 전환 이익이 있다.

1. 80% common Job이 하나의 declarative definition/bundle로 표현되고 Job별 Python/DAG 파일을 만들지 않는다.
2. sensor `run_key` 제출 경로가 burst 500에서 중복 0·누락 0·허용 지연을 만족한다.
3. Control이 Dagster scheduler/retry를 재구현하지 않고, source protection·Job metadata·data contract에만 집중한다.
4. Airflow 공존 이관기 writer fencing과 rollback runbook이 실제로 작동한다.

반대로 위 네 가지를 통과하지 못하고 Control이 다시 scheduler, retry, run state, UI 대부분을 구현해야 한다면 “Airflow를 벗어난다”는 이유만으로는 전환 비용을 정당화하기 어렵다.

---

## 5. 수정 후 필요한 최소 회귀 시험

### 5.1 normalizer 단위 반례

| 시험 | 기대 |
|---|---|
| AS OF TIMESTAMP 성공, READ ONLY 두 probe 실패 | `READ_ONLY_TXN` 금지 |
| `view.v_database COUNT(*)` 성공, exact AS OF SCN 미실행 | `AS_OF_SCN` 금지 |
| DG view query 성공·row/value 없음 | `DG_STATS` 금지 |
| MAX_DELAY ALTER 성공·ORA-03172 양성 대조 없음 | `MAX_DELAY_ENFORCED` 금지 |
| A summary/sentinel/exit 중 하나 없음 | A `FAILED`, 축 전부 `UNDETERMINED` |
| B0 한 줄, B1 fabricated PROVEN, C00 summary-only, CE empty pass | 각각 `FAILED` |
| A/B/C의 lock digest 또는 source identity 불일치 | aggregation 거부 |
| table A와 B의 evidence 교차 적용 | schema/consumer가 거부 |
| `TIMESTAMP(2)`, `(5)`, `(8)`, `NUMBER(10,2)`, unconstrained NUMBER | exact successor/round-trip 결과 없이는 HALF_OPEN 금지 |
| schema validation library 없음 또는 violation 1건 | nonzero, 최종 artifact 미생성 |

### 5.2 B1 경로 시험

- coverage: SCHEMA·TASK·실제 사용하는 METADATA 각각 직접 관측
- fail injection: 각 경로별 독립 실패가 해당 경로의 업무 SQL 전에 job을 죽임
- `MIXED/UNKNOWN`: raw stack 수동 판정 전 PASS 기여 0
- session assertion, lag admission, transaction scope, connection open/close/peak를 별도 verdict로 기록
- retry/speculation/executor loss에서 새 connection과 close 누수 측정
- exact pinned Spark/OJDBC와 artifact digest 일치

### 5.3 FLASHBACK opt-in 시험

- 고정 timestamp literal을 다중 physical connection과 전체 extract object-set에서 조회
- 획득한 정확한 SCN을 같은 standby에서 전체 object-set에 재사용
- ordinary row와 LOB별 required horizon aged query 양성·음성 대조
- 실제 운영 DDL 종류별 ORA-01466 분류
- grant 존재, runtime 활성화, retention 변경 요청을 서로 독립 판정

---

## 6. 권고 실행 순서

1. **현재 normalizer를 gate producer로 실행하지 않는다.** raw probe 개발용 실행과 정규 증거 생성은 분리한다.
2. P0-02~04를 먼저 고쳐 evidence envelope·child manifest·binding을 fail-closed로 만든다.
3. P0-01·05의 axis 모델과 파생기를 표 기반 pure function으로 다시 쓰고 §5.1 반례를 자동화한다.
4. B1을 path-specific fail-closed 하네스로 고친 뒤 pinned Spark에서 build/실행한다.
5. 가장 덜 민감한 사내 DR source에서 A → B0 → B1을 실행한다. C00 full-scan 계열은 별도 승인 전 실행하지 않는다.
6. C01~C09는 폐기 가능한 writable Oracle에서만 실행하고, child returncode·suite config digest 수정 뒤 증거로 받는다.
7. 결과로 overlay distribution을 본 뒤 A/P v2.0을 개정한다. 대규모 상태·API 편집은 그때 한다.
8. grant 37행 matrix를 완성한 뒤 object-set별 optional FLASHBACK 요청의 ROI를 다시 판정한다.

이 순서는 “실측 전 대규모 규범 개정 금지” 원칙을 유지한다. 단, **측정기를 바로잡는 semantic patch는 실측의 전제**이므로 미룰 수 없다.

---

## 7. 1차 출처로 정정한 사실

- Oracle Flashback Query는 object의 `FLASHBACK` + `SELECT/READ`를 요구하고, `DBMS_FLASHBACK`은 별도 `EXECUTE`가 필요하다. timestamp는 지정값보다 최대 3초 이전 시점으로 매핑될 수 있다: [Oracle Flashback Technology](https://docs.oracle.com/en/database/oracle/oracle-database/26/adfns/flashback.html), [Oracle 11.2 Application Developer's Guide](https://docs.oracle.com/cd/E24693_01/appdev.11203/e17125.pdf)
- ORA-08181은 supplied SCN이 valid bounds 밖일 때다: [Oracle ORA-08181](https://docs.oracle.com/en/error-help/db/ora-08181/)
- read-only transaction은 한 connection의 transaction-level snapshot이며 `SET TRANSACTION`은 첫 statement여야 한다: [Oracle 11.2 Data Concurrency and Consistency](https://docs.oracle.com/cd/E11882_01/server.112/e40540/consist.htm), [Oracle SET TRANSACTION](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SET-TRANSACTION.html)
- BasicFiles LOB의 `PCTVERSION/RETENTION`과 SecureFiles의 `MAX/MIN/AUTO/NONE`은 성질이 다르다: [Oracle 11.2 SecureFiles and LOB Guide](https://docs.oracle.com/cd/E11882_01/appdev.112/e18294.pdf)
- `STANDBY_MAX_DATA_DELAY`는 session-specific query admission이고 초과 시 ORA-03172다: [Oracle Data Guard standby management](https://docs.oracle.com/database/121/SBYDB/manage_ps.htm)
- Spark `JdbcConnectionProvider`는 `DeveloperApi`/`Unstable`이며 provider 선택은 pinned runtime에서 검증해야 한다: [Spark 3.5.9 JdbcConnectionProvider](https://spark.apache.org/docs/3.5.9/api/scala/org/apache/spark/sql/jdbc/JdbcConnectionProvider.html), [Spark JDBC options](https://spark.apache.org/docs/3.5.6/sql-data-sources-jdbc.html)

---

## 8. 검토 중 추가된 `c2fa93b` delta 판정

새 커밋은 기준판을 바꾸지 않으므로 별도 확인만 했다.

| 변경 | 판정 |
|---|---|
| `etl-platform-local-poc-plan.md` 신설 | LOCAL_WSL이 하네스 동작만 증명하고 사내 설계 근거가 아니라는 경계는 좋다 |
| B1 `run.sh`의 `max_delay=none` | 로컬 PRIMARY에서 ADG 전용 ALTER 때문에 coverage 전체가 실패하는 것을 분리하는 합리적 개발 편의다. 사내 ADG 증거에는 사용할 수 없다 |
| CE standby check를 `NOT_CHECKED`로 표기 | 표현은 정직해졌다. 그러나 `guard_passed=true`로 실행을 계속하는 구조는 그대로라 P0/P1 경계 문제를 닫지 않는다 |
| 로컬 계획 S6의 `verdict.coverage == PROVEN` | 현재 analyzer의 task-path fail-closed 허점 때문에 아직 합격 기준으로 사용할 수 없다 |

따라서 `c2fa93b`는 본 리뷰의 P0를 해소하지 않는다.

---

## 9. 결론

이 저장소는 문제를 숨기지 않고 미확정과 미실행을 명시해 온 점이 강하다. 이번 결함도 아키텍처가 과도해서 생긴 것이 아니라, **설명용 경계를 실행 계약으로 옮기는 마지막 단계가 덜 닫힌 것**에 가깝다.

다음 산출물은 A/P v2.0이 아니라 먼저 다음 네 개여야 한다.

1. `g0_0_evidence` 별도 schema
2. child manifest + strict aggregator
3. table-driven capability derivation + 반례 unit test
4. path-specific B1 fail-closed harness

이 네 개가 서면 G0-0A/B를 실행할 수 있고, 그 결과로 capability overlay와 권한 요청을 훨씬 짧고 강하게 확정할 수 있다.
