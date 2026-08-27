# G0-0C — counterexample harness (CE01~CE09)

`g0-0c-fence-facts.sql`(C00)이 **읽기 전용 fact collector** 라면, 이 패키지는
**상태를 만들어 반례를 재현하는 harness** 다. 쓰기·동시성·프로세스 강제 종료를 한다.

> **폐기 가능한 쓰기 가능 환경에서만 돌린다.** 운영 원천에서는 한 줄도 실행하지 않는다.
> runner 가 `environment_guard` 를 통과시키지 못하면 시나리오는 기동조차 하지 않는다.

---

## 1. 이 패키지가 지키는 규칙

| 규칙 | 강제 지점 |
|---|---|
| **신원은 서버에서 읽는다** | `runner.preflight()` 가 `CE_DSN` 으로 1회 접속해 `DB_UNIQUE_NAME`·`CURRENT_SCHEMA`·`DATABASE_ROLE` 을 읽는다. 운영자가 손으로 적는 값은 **교차 확인용**일 뿐 근거가 아니다 |
| 대상 환경이 아니면 실행하지 않는다 | `runner.enforce_guard()` — 서버가 돌려준 값과 정확 일치, 빈 값 금지, 운영 식별자 패턴 차단. 위반 시 exit 2 |
| 시나리오도 자기 접속 DB 를 재확인한다 | `_ce.Ora.verify_schema()` — `DB_UNIQUE_NAME ≠ 기대값`, `DATABASE_ROLE ≠ PRIMARY`, 금지 패턴 매치면 한 줄도 쓰지 않는다 |
| 자격증명 실패로 계정이 잠기지 않는다 | preflight 1회 실패 → 즉시 SUITE_ABORT. 시나리오 9개가 각각 로그온을 시도하는 경로가 없다 |
| 시간 축이 세션 TZ 에 흔들리지 않는다 | 접속 직후 `ALTER SESSION SET TIME_ZONE = DBTIMEZONE`, 시간 비교는 `CAST(SYSTIMESTAMP AS TIMESTAMP)` 로 통일 |
| 모든 객체는 `object_prefix` 로 시작한다 | `_ce.Fixture.name()` — 벗어나면 예외 |
| 끝나면 지운다. 남으면 그대로 보고한다 | `_ce.Fixture.__exit__()` → `USER_OBJECTS` 재조회. 잔여가 있으면 suite 는 PASS 불가 |
| 비밀번호는 argv·로그·예외에 남기지 않는다 | 환경변수만 읽고, 예외 메시지는 `_ce.scrub()` 통과 |
| 틀린 비밀번호를 재시도하지 않는다 | 접속 1회 실패 시 즉시 `Unavailable` (계정 잠금 방지) |
| 증거 없는 판정을 주장하지 않는다 | 시나리오와 runner 양쪽에서 강등. runner 는 `injection_observed is True`(동일성)**이면서** `injection_evidence` 가 1건 이상이고 각 항목의 `kind` 가 허용값일 때만 passing outcome 을 인정한다 |
| runner 가 자기 출력을 검증한다 | `validate_evidence()` 가 `evidence.schema.json` 으로 검사하고, 위반이 있으면 suite 는 PASS 가 아니다 |

## 2. 준비

```bash
pip install oracledb jsonschema
```

`oracledb` 가 없으면 preflight 가 신원을 확인할 수 없으므로 **실행을 거부한다**(exit 2).
`jsonschema` 가 없으면 evidence 를 검증하지 못했다는 사실이 결함으로 기록되고 PASS 가 되지 않는다.

접속 정보는 **환경변수로만** 넘긴다.

| 변수 | 필수 | 설명 |
|---|---|---|
| `CE_USER` | ✔ | 접속 계정. `suite.yaml` 의 `allowed_schema` 와 같아야 한다 |
| `CE_PASSWORD` | ✔ | 비밀번호(또는 wallet 사용). **argv 금지** |
| `CE_DSN` | ✔ | 폐기용 primary 의 easy connect / tnsnames alias |
| `CE_STANDBY_DSN` | — | ADG standby. 없으면 관련 관측만 건너뛴다 |
| `CE_DOC_PATH` | ✔ | CE09 의 공시 검사 대상 문서(`etl-platform-target-architecture-v1.2.3.1.md`) 경로. **이 패키지 tarball 에는 그 문서가 들어 있지 않다** — 지정하지 않으면 CE09 가 자기 판정 기준을 평가하지 못해 `INCONCLUSIVE` 가 되고 suite 는 절대 PASS 하지 못한다 |

`suite.yaml` 에서 채울 것: `expected_primary_db_unique_name`,
`expected_standby_db_unique_name`, `allowed_schema`, `versions.*`.
비워 두면 runner 가 실행을 거부한다.

## 3. 실행

```bash
python3 runner.py --suite suite.yaml --dry-run
```

```bash
python3 runner.py --suite suite.yaml --out evidence.json --observed-env '{"primary_db_unique_name":"ETLPOC_PRI","standby_db_unique_name":"ETLPOC_STB","schema":"ETL_CE"}'
```

`--observed-env` 는 **선택**이다. 주면 preflight 가 서버에서 읽은 값과 일치하는지
교차 확인하고, 어긋나면 중단한다. 생략해도 신원은 서버에서 직접 읽는다.

`CE_STANDBY_DSN` 을 주면 standby 신원도 서버에서 확인한다. 없으면 그 이름은
**미검증 선언값**으로 기록된다(`environment.standby_verified = false`).

종료 코드: **0** suite PASS · **2** 가드·preflight 실패(SUITE_ABORT) · **3** PASS 아님 · **4** 내부 오류

DB 가 없거나 자격증명이 틀리면 **exit 2 에서 멈춘다** — 시나리오는 한 개도 실행되지 않는다.

## 4. 결과 값 다섯

| outcome | 뜻 | PASS? |
|---|---|---|
| `COUNTEREXAMPLE_REPRODUCED` | 반례가 재현됐다 — 설계 결함 확인 | ✔ |
| `MITIGATION_HOLDS` | 완화책이 실제로 막았다 | ✔ |
| `MITIGATION_FAIL` | 완화책이 있었으나 막지 못했다 | ✔ |
| `INJECTION_NOT_OBSERVED` | 주입의 서버 측 근거가 없다 | ✘ |
| `INCONCLUSIVE` | 환경·타이밍 때문에 판정 불가 | ✘ |

**반례가 재현되지 않은 실행은 `MITIGATION_HOLDS` 가 아니다.** 완화책이 막은 것과
주입이 읽기 순서와 겹치지 않은 것은 다르다. 전자만 `MITIGATION_HOLDS` 이고 후자는
`INCONCLUSIVE`(또는 주입 자체가 안 됐으면 `INJECTION_NOT_OBSERVED`)로 떨어진다.
1회 음성은 부재의 증거가 아니므로 반복 실행 횟수를 함께 기록하라.

`injection_observed` 는 **서버가 보여 준 것**으로만 true 가 된다:
ORA 오류 코드 / 독립 세션이 센 행 수의 변화 / `ORA_ROWSCN` / 서버 타임스탬프 /
SIGKILL 이후 서버에 남은 상태. "예외가 안 났다"·클라이언트 로그는 근거가 아니다.

## 5. 아홉 시나리오

| id | 무엇을 깨는가 | 주입 방식 | 서버 측 증거 |
|---|---|---|---|
| **CE01** | 타입별 `successor(M)` 의 정의·단조·왕복 | 9개 타입의 M·successor·최대값 저장 | 서버 비교 결과, overflow ORA 코드 |
| **CE02** | CAS 이후 동률 watermark late commit | 세션 A 가 `wm=M` 을 미commit 유지 → CAS 뒤 commit | 독립 세션의 `wm=M` 행 수 1→2 |
| **CE03** | 미래 일자 outlier 가 MAX 인 상태 | `SYSTIMESTAMP + 30일` 행 1건 | 서버가 판정한 미래 행 수·간격 |
| **CE04** | NULL watermark 의 세 값 논리 | NULL wm 행 4건 + 빈/전량 NULL 테이블 | `창 안 + 창 밖 < 전체` 의 차이 |
| **CE05** | 두 축 Merge 의 단일 축 seal | INSERT_DT 만 전진 / UPDATE_DT 만 전진하는 두 군 | 축별 행 수, 전략별 회수율 |
| **CE06** | partition 당 connection 의 스냅샷 불일치 | 4 connection 읽는 도중 partition key 이동 | 독립 세션이 확인한 UPDATE 반영 |
| **CE07** | shard 별 독립 cursor | marker–대상–marker 순차 commit 으로 SCN 사이에 행을 심는다 | `ORA_ROWSCN` 이 두 cursor 사이 |
| **CE08** | mid-sweep crash 와 cursor 전진 | 자식 프로세스를 **SIGKILL** | 죽은 뒤 남은 cursor 값 + 절반짜리 staging |
| **CE09** | census 대조의 탐지 공백 | transient insert→delete, same-PK 재삽입 | `ORA_ROWSCN` 상승 + payload 변경 |

## 6. 이 harness 가 덮지 못하는 것

명시해 둔다 — 여기서 나온 결과로 아래를 대신 주장하면 안 된다.

0. **구버전 Oracle.** 이 harness 는 `python-oracledb` **thin 모드 고정**이다(`init_oracle_client` 호출이 없다). thin 모드는 구버전 서버에 붙지 못하므로, 이 프로젝트가 코어 성립 조건으로 잡은 **최저 버전(11.2)에서는 CE01~CE09 를 하나도 재현할 수 없다.** 그 버전대를 재려면 thick 모드(Instant Client)로 바꾸거나 별도 하네스가 필요하다.
1. **OJDBC·Spark 계층.** 이 harness 는 python-oracledb thin 이다. CE01 의 왕복 손실 중
   드라이버 정밀도 탓인 것은 `driver_scoped: true` 로 표시하고 설계 결함으로 승격하지 않는다.
   CE01·CE06·CE08 은 `spark_layer_pending: true` 를 남긴다. **G0-0B1**(커스텀
   `JdbcConnectionProvider` tracer)이 서기 전에는 그 계층이 미검증이다.
2. **F-13(유휴 정지).** 1회 실행으로 관측되지 않는다. C00 을 며칠에 걸쳐 반복해야 한다.
3. **운영 규모.** 여기 fixture 는 수백 행이다. 10k Job / 40k Run 규모의 경합은 G1 소관이다.
4. **타이밍 의존.** CE06·CE07 은 실행마다 결과가 달라질 수 있다. 1회 음성은 부재의 증거가
   아니다 — 반복 실행 횟수와 함께 기록하라.
5. **구성된 상태와 자연 발생의 구분.** CE07 은 어긋난 shard cursor 를 스윕으로 자연
   발생시키지 않고 직접 구성한다. 따라서 보이는 것은 "어긋난 cursor 는 행을 잃는다"이지
   "독립 cursor 는 반드시 어긋난다"가 아니다.
6. **CE08 의 crash 모델.** 자식은 각 partition 을 commit 한 뒤 죽는다. 관측되는 잔여물은
   미commit 조각이 아니라 **commit 된 부분 진행 상태**다. 미commit 조각이 남는 경로는
   별도 실험이 필요하다.

---

## 부록 — 2026-08-27 증거 형식 변경 (7차 교차 리뷰 조치)

| 필드 | 왜 생겼나 |
|---|---|
| `scenarios[*].child_returncode` | **runner 가 자식 프로세스의 종료 코드를 읽지 않았다**(P0-02). 통과 모양의 `SCENARIO_RESULT` 를 찍은 뒤 exit 1 로 죽어도 suite PASS 후보가 됐다. 이제 0 이 아니면 그 시나리오는 `INCONCLUSIVE` 로 강등된다. `-1` 은 '실행하지 않았다'(dry-run·entrypoint 부재)를 뜻한다 |
| `suite_config_sha256` | `artifact_sha256` 은 순환 참조를 피하려고 `suite.yaml` 을 제외하고 계산한다. 그래서 **필수 시나리오·budget·pass rule 이 어떤 digest 에도 묶이지 않았다**(P1-10). code digest 와 config digest 를 따로 남긴다 |

두 필드 모두 `evidence.schema.json` 의 `required` 다. 옛 증거 파일은 이 스키마를 통과하지 못한다 —
그것이 의도다. 증거 형식이 바뀌면 이전 증거는 그 판본의 것이며 새 판본의 근거로 재사용하지 않는다.
