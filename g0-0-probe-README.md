# G0-0 Executable Probe — 실행 안내와 결과 해석

- 작성일: 2026-08-25
- 근거: `etl-platform-v2.0-codex-cross-review.md` §7.1(“**G0-0을 먼저 실행한다. 문서 개정과 병행하지 않는다**”)·§7.2 결과 분기표·§7.4
- 산출물(2026-08-25 재검증 2차 반영) — **실행 순서 A → B0 → (B1) → C00 → C01~C09**
  - **G0-0A** `g0-0a-capability-inventory.sql` — 운영계에서 제한적으로 가능한 read-only capability inventory(대상 테이블 스캔 없음)
  - **G0-0B0** `g0-0b0-spark-smoke.py` — stock Spark smoke probe. **provider tracer가 아니다**
  - **G0-0B1** `g0-0b1-connection-provider/` — 커스텀 `JdbcConnectionProvider`가 schema·task 경로를 실제로 덮는지, 프리앰블 실패 시 job이 정말 죽는지(fail-closed) 증명하는 tracer. **이것이 Profile U 세션 단언 모델의 성립 조건이다.** 빌드·실행법은 그 디렉터리의 README 참조
  - **G0-0C00** `g0-0c-fence-facts.sql` — 운영계 read-only fact collector
  - **G0-0C01~C09** `g0-0c-counterexamples/` — **stateful counterexample harness**(쓰기 fixture·다중 connection·crash 주입이 필요하므로 폐기용 환경 전용). 운영 규칙·환경변수·시나리오별 증거 형태는 `g0-0c-counterexamples/README.md` 에 있다
- **폐기**: 이전 `g0-0-probe.sql`은 실행 차단 결함 5건(잘못된 SHA-256 기대값 · 미정의 `&LIMIT_ROWS` · 대상 테이블 전체 scan · 비밀번호 argv 노출 · **하드코딩된 `"query_ok":true`로 인한 apparent success**)으로 삭제했다. 실행하지 마라.
- 이 두 스크립트의 출력이 A v2.0 / P v2.0의 **여러 분기를 결정한다.** 실측 전에 규범 문서를 대규모로 고치면 재작업이 크다.

---

## 1. 안전 규칙 (실행 전 반드시 확인)

| # | 규칙 | 이유 |
|---|---|---|
| 1 | **읽기 전용이다.** DDL·DML·job 생성이 한 줄도 없다 | 운영 원천을 건드리지 않는다 |
| 2 | **잘못된 비밀번호를 절대 시도하지 않는다** | `FAILED_LOGIN_ATTEMPTS` 소진 = 계정 잠금 = 전체 파이프라인 정지 |
| 3 | `ALTER SESSION`만 쓰고 `ALTER SYSTEM`은 없다 | 자기 세션에만 영향 |
| 4 | 비밀번호는 **환경변수로만** 전달한다(`--password-env`) | 프로세스 목록·로그 노출 방지 |
| 5 | Spark probe는 **운영에서 쓸 pinned Spark·Oracle JDBC 버전**으로 실행한다 | 버전이 다르면 결과가 규범 근거가 되지 못한다 |
| 6 | 데이터 사실 측정은 **G0-0C로 분리**했고 기본이 `EXACT_MODE = N`(표본)이다 | G0-0A의 `wm_column.leading_valid_visible`를 확인하기 전에 전수 모드로 돌리지 않는다 |
| 7 | 모든 판정은 **실제 `SQLCODE`**에서 나온다. 성공을 가정한 리터럴 출력이 없다 | 이전 판의 apparent-success 결함 재발 방지 |

---

## 2. 실행

```bash
# (0-a) 이 블록에서 쓰는 변수를 먼저 정의한다. 정의 없이 쓰면 빈 문자열로 접속을 시도한다.
export ORA_USER='ETL_USER'                     # 접속 계정
export ORA_TNS='//dbhost:1521/SVC'             # easy connect 또는 tnsnames alias
export URL="jdbc:oracle:thin:@$ORA_TNS"        # B0/B1 이 쓰는 JDBC URL
export USER="$ORA_USER"                        # B1 run.sh 의 두 번째 인자
export CE_DOC_PATH="$PWD/etl-platform-target-architecture-v1.2.3.1.md"   # CE09 공시 검사 대상(필수)
# 대상 스키마·테이블·워터마크 컬럼은 각 SQL 파일 상단 DEFINE 을 직접 편집한다:
#   g0-0a-capability-inventory.sql  : TARGET_OWNER / TARGET_TABLE / WM_COLUMN / EXPECT_ROLE / EXPECT_DBUNAME
#   g0-0c-fence-facts.sql           : TARGET_OWNER / TARGET_TABLE / WM_COLUMN / ACK_FULL_SCAN
# EXPECT_DBUNAME 을 모르면 먼저 한 줄로 확인한다:
#   SELECT SYS_CONTEXT('USERENV','DB_UNIQUE_NAME'), SYS_CONTEXT('USERENV','DATABASE_ROLE') FROM DUAL;

# (0) 비밀번호를 명령줄 인자로 절대 넘기지 않는다. /nolog + stdin CONNECT를 쓴다.
read -rs -p "ETL password: " ORA_PW; export ORA_PW

# (1) G0-0A — 안전한 capability inventory (대상 테이블 스캔 없음)
sqlplus -S /nolog <<EOF | tee g0-0a-evidence-$(date +%Y%m%d).log
CONNECT $ORA_USER/$ORA_PW@//host:1521/service
@g0-0a-capability-inventory.sql
EXIT
EOF

# (2) G0-0B0 — stock Spark smoke. 운영과 같은 pinned Spark·ojdbc 버전으로.
spark-submit --jars /path/ojdbc11.jar g0-0b0-spark-smoke.py \
  --url "jdbc:oracle:thin:@//host:1521/service" \
  --user "$ORA_USER" --password-env ORA_PW \
  --table SCHEMA.TABLE --wm UPDATE_DT \
  | tee g0-0b0-evidence-$(date +%Y%m%d).log

# (3) G0-0C00 — fence fact collector. **G0-0A의 wm_column.leading_valid_visible 를 먼저 확인**하고,
#     ACK_FULL_SCAN=N(기본)이면 **대상 테이블 질의가 하나도 실행되지 않는다** —
#     Q1·Q2·Q4 는 물론 Q3 도 게이트 뒤에 있다(SAMPLE 은 표본 추출이지 I/O 절감이 아니다).
#     즉 기본 설정으로는 **대상 테이블 질의가 0건**이다(skipped 레코드와 summary 는 나온다).
#     승인 후 ACK_FULL_SCAN=Y 로 실행하라.
sqlplus -S /nolog <<EOF | tee g0-0c00-evidence-$(date +%Y%m%d).log
CONNECT $ORA_USER/$ORA_PW@//host:1521/service
@g0-0c-fence-facts.sql
EXIT
EOF

# (2.5) G0-0B1 — provider tracer. **B0 다음, C00 앞이다.**
cd g0-0b1-connection-provider
export SPARK_HOME=/opt/spark OJDBC_JAR=/path/ojdbc11.jar
./build.sh                      # 실제 Spark 판본에 대고 컴파일. 실패도 측정 결과다.
./run.sh "$URL" "$USER" SCHEMA.TABLE ETLPOC_STB PHYSICAL_STANDBY 300
# role 은 공백 없이 '_' 로 넘긴다(JVM 인자에서 공백이 잘린다).
cd ..

# (4) G0-0C01~C09 — **폐기용 쓰기 가능 환경에서만**. 운영계에서 실행 금지.
#     runner 가 CE_DSN 으로 직접 접속해 DB_UNIQUE_NAME 을 확인한다(운영자 자기신고 아님).
#     접속 정보는 환경변수로만 넘긴다(argv 금지). 자세한 것은 패키지 README 참조.
cd g0-0c-counterexamples
export CE_USER=ETL_CE CE_DSN='host:1521/etlpoc'
# CE_DOC_PATH 는 **필수**다. 없으면 runner 가 preflight 에서 exit 2 로 중단한다
# (CE09 가 공시 검사로 HOLDS/FAIL 을 가르는데, 그 문서가 이 패키지 tarball 에 없다).
export CE_DOC_PATH="${CE_DOC_PATH:-$PWD/../etl-platform-target-architecture-v1.2.3.1.md}"
read -rs -p 'CE password: ' CE_PASSWORD && export CE_PASSWORD && echo
# suite.yaml 의 expected_*_db_unique_name / allowed_schema / versions 를 먼저 채운다.
python3 runner.py --suite suite.yaml --dry-run          # 가드·계획만
python3 runner.py --suite suite.yaml \
  --observed-env '{"primary_db_unique_name":"...","standby_db_unique_name":"...","schema":"..."}' \
  --out evidence.json
unset CE_PASSWORD
# exit 0 = suite PASS / 2 = 환경 가드 실패 / 3 = PASS 아님 / 4 = 내부 오류
# --observed-env 값은 **실제 접속에서 읽은 것**을 넣는다:
#   SELECT SYS_CONTEXT('USERENV','DB_UNIQUE_NAME'), SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL

unset ORA_PW
```

로그에서 `{"probe":...}` / `PROBE {...}` 라인만 뽑아 `g0_evidence.account_privs`로 등록한다.
**G0-0A는 exit code 0과 `{"probe_run_end":"G0-0A"}` sentinel, 그리고 `probe_summary.manifest_ok = true`를 셋 다 확인해야 유효하다** — 하나라도 없으면 결과 전체를 폐기한다.

**여러 번 실행할 것**: 부하가 다른 시간대(정각 burst 중/한산할 때)에 최소 2회. `S5`/§9의 ORA-03172는 lag가 있을 때만 나오므로 **양성 증거를 얻으려면 lag가 큰 시간대**가 필요하다.

---

## 3. 기록 규칙 — “오류 부재”는 증거가 아니다

리뷰 NEW-19의 지적대로, 각 probe는 세 가지를 **분리해** 기록한다.

| 필드 | 뜻 | 오해하기 쉬운 점 |
|---|---|---|
| `query_ok` | 문장이 오류 없이 실행됐다 | 실행됐다고 capability가 있는 것은 아니다 |
| `row_present` | 행이 돌아왔다 | **0행은 정상 결과일 수 있다**(예: LOB 없는 테이블의 `ALL_LOBS`) |
| `value_interpretable` | 값이 규격이 기대한 형태다 | 값이 있어도 의미가 다를 수 있다(할당값 ≠ 실제 강제) |

특히 두 쌍을 절대 섞지 말 것:

- **profile 할당값 ≠ 실제 강제** — `USER_RESOURCE_LIMITS`가 값을 줘도 `RESOURCE_LIMIT=TRUE`인지는 증명하지 못한다 → `ASSIGNED_LIMIT_EVIDENCE`로만 기록.
- **LOB storage attribute ≠ historical retention 보장** — `ALL_LOBS.RETENTION`은 저장 속성일 뿐 과거 버전 가용성·ORA-01555 회피를 뜻하지 않는다.

---

## 4. 결과 → 설계 분기 (리뷰 §7.2 요약)

| probe 결과 | 필수 설계 결과 |
|---|---|
| `as_of_timestamp.target` 성공 ∧ 실제 extract object 전부에 권한 ∧ **다중 physical connection이 같은 시점 공유** | 그 object-set에만 `COMMON_FLASHBACK_SNAPSHOT` capability. **Profile 전체 승격·ZERO_GAP 자동 복원 금지** |
| `dbms_flashback.get_scn` 성공 ∧ 그 SCN의 `AS OF`가 같은 standby에서 성공 | current-SCN acquisition capability 별도 등록. grant 존재만으로 활성화 금지 |
| `alter.STANDBY_MAX_DATA_DELAY.D` 실패 **또는** ORA-03172 양성 대조 실패 | `read_admission = UNBOUNDED`. bounded admission이 필수인 Job은 publish No-Go |
| `txn.set_read_only` 실패(또는 `txn.set_read_only.reissue`가 ORA-01453이 **아님**) | multi-statement snapshot 비활성 → `SINGLE_QUERY`/`PER_STATEMENT`만 허용 |
| `sync_with_primary` 실패(ORA-03173) | `RECEIVED_REDO_APPLY_BARRIER` 비활성 |
| USERENV role/routing 단언 실패 | ConnectionRevision read-path **No-Go**(best-effort 강등 실행 금지) |
| `S2.schema_bypass = true` | schema query를 같은 assertion·budget 경로에 넣기 전 ConnectionRevision **No-Go**. `customSchema` 단독 합격 금지 |
| `standard_hash.*`/`compose_decompose.nfc` 벡터 불일치 | PK coverage만 유지하고 content-sensitive 계약은 No-Go 또는 제외 공개 |
| `utl_raw.*`/`dbms_crypto.hash_raw` 실패 | 해당 hash 경로 비활성. `UTL_I18N`/SQL built-in과 **하나의 capability로 합치지 않는다** |
| `user_resource_limits`/`user_password_limits` 성공 | `ASSIGNED_LIMIT_EVIDENCE`만 기록. 실제 강제는 `UNKNOWN` 유지 |
| `all_lobs.*` 성공 | `LOB_STORAGE_ATTRIBUTE_ONLY`로 기록. 0행은 non-LOB 테이블의 정상 결과 |
| `v$*`/`dba_*` 성공 | 그 capability만 활성화. **Profile O 일괄 전환 금지**(synonym/`V_$` grant·PDB 위치·`CONTAINER_DATA` 범위를 함께 기록) |
| `ora_rowscn.*` 실패·미확인 | sample capability 비활성 또는 `UNVERIFIED`. fence 근거로 승격 금지 |

---

## 5. 데이터 사실 3종 — fence 반례를 실측으로 바꾼다 (G0-0C)

`g0-0c-fence-facts.sql`이 돌려주는 세 숫자가 리뷰의 fence 공격을 “이론”에서 “이 테이블의 사실”로 바꾼다.

| 값 | ≥ 1이면 | 대응 |
|---|---|---|
| `rows_at_max_wm` | **비어 있지 않은 테이블이면 ≥ 1은 자명하다** — 이 값 단독은 아무것도 증명하지 않는다. `max_wm`의 다일차 정지 관측과 **함께** 볼 때만 F-01의 영구 누락 조건이 성립한다 | `MAX(wm) + typed_successor` seal. **단 같은 snapshot에서 이미 보이는 동률만 봉인하며, 유휴 상태의 late commit(F-02)과 F-13은 남는다** |
| `null_wm_rows` | NULL watermark 행이 **영구 제외**된다 | NULL 정책(거부 또는 full-reconcile)을 publish validator에 |
| `future_wm_rows` | 미래 timestamp가 cursor를 밀 수 있다. **단 기준이 standby `SYSTIMESTAMP`이므로 cross-clock 문제의 독립 증거는 아니다** | `eligible_max`(future outlier 제외) + 배제분 quarantine·재평가. raw MAX를 그대로 쓰면 NEW-08이 오히려 악화된다 |

`max_wm`은 1회 실행으로 판정되지 않는다 — **하루 2~3회, 최소 3일** 반복해 전진 여부를 기록해야 F-13(유휴 정지)이 관측된다. 변하지 않는 구간이 있으면 `NO_SOURCE_PROGRESS` 결과와 backoff가 필요하다.

---

## 6. 이 probe가 **증명하지 못하는 것**

정직하게 적어 둔다. 아래는 G0-0으로 확정되지 않으며 G1 또는 별도 수단이 필요하다.

1. **mid-query lag 재평가** — 장시간 fetch 중 한도가 다시 평가되는지. 자연 lag가 커지는 시간대에 장시간 추출을 걸어 관측하는 수밖에 없고, **음성 결과는 결론을 주지 못한다**.
2. **실제 세션 총수** — `GV$SESSION`이 없으면 플랫폼이 만든 세션만 센다. 이관기 Airflow·운영 스크립트·타 시스템은 정의상 보이지 않는다.
3. **in-doubt 분산 트랜잭션 부재** — 관측도 반증도 불가.
4. **잔존 세션 0** — 클라이언트 close 관측은 “close했는데 서버에 남는 경우”를 정의상 보지 못한다.
5. **clone 구분** — 이름을 재사용한 복제본은 USERENV tuple로 구분되지 않는다.
6. **profile 실제 강제** — 값을 읽어도 `RESOURCE_LIMIT=TRUE`인지는 모른다.

---

## 7. 실행 후 할 일

1. 산출물을 `g0_evidence` 레코드로 정규화한다. **도구가 있다** — 손으로 하지 마라.

```bash
python3 g0-normalize.py --report-id "$(date -u +G0-0-%Y%m%dT%H%M%SZ)" --profile CORP_POC \
    --a g0-0a.log --b0 b0.json --b1 g0-0b1-connection-provider/g0-0b1-evidence.json \
    --c00 c00.log --c-suite g0-0c-counterexamples/evidence.json \
    --versions-lock versions.lock --out g0-0-evidence.json
```

계약은 `g0-0-evidence.schema.json` 이고 도구가 자기 출력을 그것으로 검증한다.
`--report-id` 는 **회차마다 달라야 한다** — F-13(유휴 정지)과 ORA-03172 양성 대조처럼
여러 회차를 조립해야 하는 측정이 있다.

정규화기가 하는 일 중 중요한 것:
  · `manifest_ok=false` 면 G0-0A 결과 전체를 폐기하고 모든 capability 축을 `UNDETERMINED` 로 둔다
  · capability 축을 **실제 probe id 에서 파생**하고 `derived_from` 에 근거를 남긴다(사람이 재판정 가능)
  · G0-0 이 덮지 못하는 G0 항목을 `not_covered` 에 명시한다 — G0-0 은 G0 전체가 아니다
  · `profile=LOCAL_WSL` 이면 "하네스 동작 확인용이며 설계 근거가 아니다" 를 경고에 박는다
2. §4 분기표대로 **capability 목록을 확정**한다 — 이것이 A v2.0의 `ConnectionRevision capability overlay` 초기값이 된다.
3. 그 다음에야 A v2.0 / P v2.0 규범 개정을 시작한다.
4. 남은 세 산출물(리뷰 §7.4): exact preamble spike · ~~Spark connection-path tracer~~ (**철회** — B0 의 S1c·S2·S3 는 stock 경로 관측일 뿐 provider 가 3경로를 덮는지는 증명하지 못한다. G0-0B1 이 그 역할이다) 구 서술: Spark connection-path tracer · fence 반례 harness. 앞의 둘은 이 probe의 `S1c`·`S2`·`S3`가 이미 상당 부분 덮는다.
