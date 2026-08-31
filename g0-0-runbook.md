# G0-0 실행 절차서 (runbook)

> 절차가 계획서·검토서·각 README 에 흩어져 있어 한곳으로 모은 것이다. **이 문서만 따라가면 된다.**
>
> 대상: `etl-platform-local-poc-plan.md` 의 S0~S8. 사내 PoC 도 §4 부터는 같다.
>
> 2026-08-27 기준. 그날의 조치(증거 봉투 fail-closed · 축 재설계 · 경로별 주입)가 **실행 순서를
> 바꿨다** — §1 을 먼저 읽어라.

---

## 0. 이 문서가 없던 동안 무엇이 문제였나

절차가 세 곳에 나뉘어 있었다. 계획서는 **전략**(무엇을 왜 그 순서로), 각 README 는 **도구 사용법**,
검토서는 **정정**을 말한다. 셋을 동시에 펴 놓고 실행해야 했고, 그러다 보니

- `versions.lock` 을 **언제** 확정해야 하는지 아무도 말하지 않았다 (§1 — 지금은 이것이 실패 사유다)
- S3 판정을 예외 메시지로 하라던 낡은 문장이 계획서에 남아 있었다 (2026-08-27 정정)
- `g0-run-child.sh` 없이 돌리면 집계가 통째로 거부된다는 사실이 계약 문서에만 있었다

---

## 1. ★ 가장 중요한 제약 — `versions.lock` 을 먼저 확정한다

증거 계약(`g0-child-contract.md`)이 **실행 시점**의 `versions.lock` digest 를 각 산출물에 박고,
집계기가 **집계 시점** digest 와 대조한다. 하나라도 다르면 **집계 전체를 거부**한다(exit 4).

그런데 lock 의 `oracle:` 항목은 **Oracle 에 붙어 봐야 안다.** 순서가 맞물린다.

### 그래서 회차를 둘로 나눈다

```
┌─ 탐색 회차 (RECON) ─────────────────────────────────────────┐
│  목적: 판본을 알아낸다. **이 회차의 산출물은 증거가 아니다.** │
│  S1 Spark · S2 빌드 · S4 Oracle 기동 · S4.5 전제물          │
│  → 알아낸 값으로 versions.lock 을 채운다                     │
└──────────────────────────────────────────────────────────────┘
                          ↓  lock 확정 (여기서 digest 가 고정된다)
┌─ 증거 회차 (RUN-…) ─────────────────────────────────────────┐
│  목적: 증거를 만든다. **run_id 하나로 전부 묶는다.**         │
│  S5 A·C00 · S6 B1 · S7 B0 · S8 CE → 정규화                  │
└──────────────────────────────────────────────────────────────┘
```

**탐색 회차 산출물을 증거 회차에 섞지 마라.** 섞으면 집계기가 거부한다 — 그게 계약의 목적이다.

lock 을 중간에 고쳐야 할 일이 생기면 **증거 회차를 처음부터 다시 돌린다.** 이미 만든 증거는
그 판본의 것이며 새 판본의 근거로 재사용하지 않는다.

---

## 2. S0 — 작업 사본과 도구 확인

```bash
# ext4 위에서. /mnt/c 에서는 실행하지 않는다 — drvfs/9p 는 파일 락·fsync 의미가 다르다.
mkdir -p ~/g0 && cd ~/g0
git clone <this-repo> repo && cd repo

# 파이썬 의존 — jsonschema 는 4.x 여야 한다(3.x 는 Draft 2020-12 미지원).
python3 -m pip install --user 'jsonschema>=4' oracledb
python3 -c "import jsonschema, oracledb; print('ok')"
```

`fs.inotify.max_user_instances` 는 **지금 손대지 않는다.** k3d 를 위한 것이고 k3d 는 S9 이후다.

### S0.5 — ★ 도구가 이 호스트에서 도는지 먼저 확인한다

Oracle 도 Spark 도 필요 없다. **여기서 실패하면 뒤 단계의 결과를 믿을 수 없다.**

```bash
python3 g0-harness-manifest.py     #  저장소 전 파일이 선언돼 있는가(9차 조치 5)
python3 g0-0a-probe-manifest.py    #  A probe 목록이 SQL 과 같은가(9차 조치 3)
python3 g0-normalize-tests.py      # 193건 — 증거 봉투 fail-closed
python3 g0-axes-tests.py           # 127건 — capability 축 파생·SQLCODE taxonomy·floor
python3 g0-b1-analyzer-tests.py    #  43건 — B1 판정기
python3 g0-m0-safety-tests.py      #  51건 — 실행 안전성(M0)
python3 g0-0b1-connection-provider/g0-b1-wiring-tests.py   # 16건 — **B1 종단 배선**
python3 g0-runbook-lint.py         #  19건 — **이 문서를 그대로 실행할 수 있는가**
# 여덟 다 exit 0 이어야 한다. **건수는 참고값이다** — 판정은 종료 코드로 한다
# (여기 적힌 숫자가 늘어나 있으면 그건 시험이 늘어난 것이지 실패가 아니다).
```

> **아래 셋이 9차 조치로 새로 생겼다.** 앞의 넷과 성격이 다르다.
>
> `g0-0a-probe-manifest.py`(조치 3) — A 의 87개 probe 목록이 **SQL 과 같은가**. 목록을 손으로
> 적지 않고 SQL 에서 생성하므로, SQL 을 고치고 재생성하지 않으면 여기서 멈춘다.
>
> `g0-b1-wiring-tests.py`(조치 1) — 앞의 넷은 판정기에 **합성 입력**을 넣어 판정 규칙을
> 시험한다. 그것만으로는 **launcher 가 그 입력을 실제로 만드는가**를 아무도 안 본다 —
> 9차 P0-03 이 그 틈에서 나왔다. 배선 시험은 실물 `run.sh` 를 돌려 실물 `analyze-trace.py`
> 에 넣고, **배선을 일부러 깨면 실패하는지**까지 확인한다.
>
> `g0-runbook-lint.py`(조치 2) — **이 문서 자신을 검사한다.** 9차 P0-06 에서 *"이 문서만
> 따라가면 된다"* 고 적힌 절차서가 **문서대로 하면 첫 wrapper 호출에서 죽는** 상태였다.
> 여덟 건 중 다섯이 같은 원인이다 — M0·M1 이 새 필수값을 만들 때 절차서를 같이 안 고쳤다.
> lint 는 필수 인자·환경변수 목록을 **실물 스크립트에서 읽어** 이 문서와 대조하므로,
> 계약이 강해지면 절차서가 자동으로 뒤처진 것으로 잡힌다.

---

## 3. 탐색 회차 — 판본을 알아낸다

### S1 — Spark

```bash
cd ~/g0
curl -LO https://archive.apache.org/dist/spark/spark-<VER>/spark-<VER>-bin-hadoop3.tgz
curl -LO https://archive.apache.org/dist/spark/spark-<VER>/spark-<VER>-bin-hadoop3.tgz.sha512
sha512sum -c spark-<VER>-bin-hadoop3.tgz.sha512      # **반드시 확인한다**
tar -xzf spark-<VER>-bin-hadoop3.tgz
export SPARK_HOME=~/g0/spark-<VER>-bin-hadoop3
$SPARK_HOME/bin/spark-submit --version                # 판본·Scala·JDK 를 적어 둔다
```

> **사내 판본이 정해졌으면 그것을 쓴다.** 2026-08-27 회차가 4.2.0·3.5.9(2.12/2.13) 세 판본에서
> 빌드·배선을 확인했지만 그것은 보험이지 대체가 아니다.

### S2 — B1 빌드 (최속 신호)

```bash
cd ~/g0/repo/g0-0b1-connection-provider
./build.sh            # exit 0 + META-INF/services 등록 출력
./run-tests.sh        # Java 주입 매트릭스 29건
```

`bad path element` 경고 19건은 **정상**이다 — derby·scala-compiler 의 MANIFEST `Class-Path` 에서
온다(`build.sh` 주석 참조). 빌드 실패 징후로 읽지 마라.

**실패하면 그것이 첫 측정 결과다** — 그 판본에서 SPI 시그니처가 다르거나, 이 호스트의 JDK 가
`--release 17` 을 못 맞춘다는 뜻이다.

### S3 — SPI 배선 (Oracle 없이)

도달 불가 URL 로 두 회차를 돌린다. **판정은 추적 라인 수로 한다** — 예외 메시지로 하면
Spark 4.2.0 에서 오판한다(§7 참조).

```bash
export OJDBC_JAR=~/g0/ojdbc11-<VER>.jar ORA_PW='unused-no-server'
TR=$(mktemp -d)
$SPARK_HOME/bin/spark-submit --master 'local[2]' \
  --jars "$PWD/g0-0b1-tracer.jar,$OJDBC_JAR" \
  --driver-class-path "$PWD/g0-0b1-tracer.jar:$OJDBC_JAR" \
  --conf "spark.driver.extraJavaOptions=-Dg0b1.run=wiring -Dg0b1.trace.dir=$TR" \
  run-g0-0b1.py --url 'jdbc:oracle:thin:@//127.0.0.1:1/NO_SUCH' --user NOBODY \
    --password-env ORA_PW --table X.Y --provider g0b1tracer \
    --scenario schema_only --limit 10 --trace-dir "$TR"
cat "$TR"/g0-0b1-trace-*.jsonl | wc -l     # ≥1 이어야 한다
```

> **2026-08-30 정정(8차 M4-3) — 선택 수단이 바뀌었다.** 이전 판은
> `--conf spark.sql.sources.disabledJdbcConnProviderList=basic` 으로 **경쟁자를 JVM 전역에서
> 꺼서** 우리 provider 가 유일 후보가 되게 했다. Spark 가 문서화한 수단은 그것이 아니라
> **JDBC 옵션 `connectionProvider` 로 지목하는 것**이다(`--provider g0b1tracer` 가 그 옵션을
> 싣는다). 실제로 2026-08-27 회차에서 Spark 가 던진 예외 자체가 그렇게 말하고 있었다 —
> "Use 'connectionProvider' option to select a specific provider"(`g0-0-s1-s3-results.md` §3-F4).
> 그 문장을 읽고도 전역 비활성화로 갔던 것이 과한 조치였다.
>
> 전역 비활성화가 더 나쁜 이유는 **범위**다. 같은 JVM 의 **다른 JDBC 소스까지** provider 를
> 잃는다. 반면 `connectionProvider` 옵션은 이 DataFrame 하나에만 걸린다.

**음성 대조는 conf 가 아니라 provider 옵션으로 한다.** 같은 명령을 `--provider ''` 로 한 번 더
돌린다 → provider 후보가 2개가 되어 job 이 죽거나(jar 가 classpath 에 있을 때) 추적 라인이
**0** 이다(jar 가 안 올라갔을 때). 둘은 다른 사건이므로 구분해서 기록한다 — §3-F4 참조.

> **전역 비활성화는 진단 fallback 으로만 남긴다.** `DISABLE_BASIC=basic` 을 주면 `run.sh` 가
> 그 conf 를 싣는다. Kerberos 원천이면 `basic` 만으로 부족하다 — 내장 `OracleConnectionProvider`
> 의 `canHandle` 이 `BasicConnectionProvider` 와 정확히 배타적이라 후보가 다시 2개가 된다
> → `DISABLE_BASIC=basic,oracle`. **이 값을 규범에 고정하지 마라**: 사내 원천의 인증 방식에
> 따라 달라지고, `connectionProvider` 옵션을 쓰면 애초에 필요 없다.

### S4 — Oracle

```bash
docker run -d --name ora -p 1521:1521 -e ORACLE_PASSWORD=... gvenzl/oracle-free:<TAG>
docker logs -f ora        # "DATABASE IS READY TO USE" 를 기다린다
```

> **`23.26.x`·`latest` 태그는 23ai 가 아니라 26ai 다**(빌드 스크립트 주석: `23.26.0 and beyond
> calls it 26ai`). 19c 개연 논증에 쓰려면 21c/23ai 로 2차 회차가 필요하다.

붙어서 판본을 읽는다. **여기서 얻은 값이 lock 에 들어간다.**

```sql
SELECT SYS_CONTEXT('USERENV','DB_UNIQUE_NAME'), SYS_CONTEXT('USERENV','DATABASE_ROLE') FROM DUAL;
SELECT * FROM product_component_version;
SELECT value FROM nls_database_parameters WHERE parameter IN
  ('NLS_CHARACTERSET','NLS_NCHAR_CHARACTERSET');
-- 계획서 §3 의 S6 분기 확인: PRIMARY 에서 이 ALTER 가 거부되는가?
ALTER SESSION SET STANDBY_MAX_DATA_DELAY = 300;
```

마지막 줄이 거부되면 `run.sh` 6번째 인자에 `none` 을 준다 — 안 그러면 coverage 회차가 통째로
failclosed 로 변질돼 fail-closed 실험과 구분이 안 된다.

### S4.5 — 실행 전제물 셋

1. **ojdbc jar** — Maven Central. `sha256` 과 MANIFEST `Implementation-Version` 을 확인한다
2. **sqlplus** — instantclient. `sqlplus -v`
3. **fixture 테이블** — `TARGET_OWNER.TARGET_TABLE` 과 watermark 컬럼

### S4.6 — ★ 원천 안전 봉투 (9차 조치 6)

**B0·B1 은 봉투 없이는 원천에 붙지 않는다.** `g0-source-envelope.json` 의 `default` 는
`approved_by = UNAPPROVED` 이고 `target_touch_allowed = false` 이므로, 그대로 두면 두
스크립트가 **spark-submit 직후 exit 2 로 죽는다.** 이것은 결함이 아니라 9차 리뷰 §7 의
단계적 해제(M5c)다 — 대상 테이블 질의는 원천 소유자 승인이 있어야 열린다.

```bash
# 승인값은 **저장소 밖**에 둔다. M0-5 가 CE allowlist 를 /etc/g0/ 에 둔 것과 같은 이유로,
# 원천 소유자의 승인은 사이트 정책이지 저장소 내용이 아니다. 저장소에 커밋하면 복제한
# 누구나 그 승인을 들고 다닌다.
sudo install -d -m 0755 /etc/g0
sudo cp g0-source-envelope.json /etc/g0/source-envelope.json
sudo $EDITOR /etc/g0/source-envelope.json   # envelopes 에 대상 DB_UNIQUE_NAME 항목을 추가
export G0_SOURCE_ENVELOPE=/etc/g0/source-envelope.json
```

지정하지 않으면 저장소에 번들된 파일을 쓰고, 그 `default` 는 미승인이라 아무것도 통과하지
않는다. 어느 파일이 실제로 적용됐는지는 **산출물에 남는다**(B0 의 `envelope` step, B1 의
`envelope` 필드에 경로와 `sha256`). `harness_digest` 는 번들 파일만 세므로, 저장소 밖 봉투를
쓰면 적용된 정책이 그 digest 밖에 있다는 사실도 같이 적힌다.

넣어야 하는 것.

| 키 | 뜻 | 승인 없이는 |
|---|---|---|
| `max_partitions` | 이 원천에 동시에 열 수 있는 executor connection 수 | 1 |
| `max_concurrent_sessions` | 위 + driver. **`max_partitions + 1` 보다 크면 거부한다** | 2 |
| `max_active_runs` | 같은 원천에 동시에 붙는 G0-0 회차 수 | 1 |
| `statement_timeout_seconds` | 쿼리 하나의 벽시계 상한 | 30 |
| `target_touch_allowed` | 대상 테이블 질의 허용 여부 | `false` |
| `approved_by` | 승인한 사람. `UNAPPROVED` 로 시작하면 거부한다 | — |

`max_concurrent_sessions > max_partitions + 1` 인 봉투는 **봉투 자신이 모순**이라고 거부한다.
그런 값에서는 세션 검사가 어떤 입력에서도 걸리지 않기 때문이다 — 걸리지 않는 상한을
안전장치라고 부르지 않는다(9차 P0-04).

**동시 회차는 lease 가 막는다.** 파일 기반(`$G0_LEASE_DIR`, 기본 `/tmp/g0-source-lease`)이라
**한 호스트 안에서만** 유효하다. 다른 호스트에서 같은 원천에 붙는 회차는 막지 못하며,
그때는 DB 측 profile resource limit 이나 조직 절차가 유일한 방어다. 이 한계를 숨기지 않는다.

### ★ lock 확정

```bash
cd ~/g0/repo
$EDITOR versions.lock       # profile: LOCAL_WSL 로. runtime·oracle 을 실측값으로.
sha256sum versions.lock     # 이 값이 모든 증거에 박힌다
```

`UNSET` 이 **값 자리에** 남아 있으면 정규화기가 경고한다(주석의 `UNSET` 은 세지 않는다).
경고가 곧 실패는 아니지만, 그 항목에 의존하는 측정은 미확정으로 남는다.

---

## 4. 증거 회차 — `run_id` 하나로 묶는다

> **2026-08-31(9차 조치 2) — 이 블록이 불완전했다.** M0·M1 이 새 필수값을 만들었는데
> 절차서를 같이 고치지 않아, **문서대로 실행하면 첫 wrapper 호출에서 exit 2 로 죽었다.**
> 운영자 실수가 아니라 문서 결함이다. 지금은 `g0-runbook-lint.py` 가 이 블록과 아래 명령을
> 대조해 누락을 잡는다.

```bash
cd ~/g0/repo
export RUN_ID="RUN-$(date +%Y%m%d)-01"
export PROFILE=LOCAL_WSL            # 사내면 CORP_POC
export ORA_USER=ETL_PROBE
read -rs -p 'Oracle password: ' ORA_PW && export ORA_PW && echo
export URL="jdbc:oracle:thin:@//localhost:1521/FREEPDB1"
export TGT_OWNER=ETL_PROBE TGT_TABLE=G0_TARGET WM=UPDATE_DT
export OJDBC_JAR=~/g0/ojdbc11-<VER>.jar

# ★ 대상 원천의 DB_UNIQUE_NAME. **S4 에서 서버가 밝힌 값을 그대로 쓴다.**
#   래퍼가 G0_SOURCE_ID 로 요구하고(M1-2), 집계기가 --source-id 로 대조하고,
#   B1·B0 가 기대 신원으로 단언한다. 세 곳이 **같은 값**이어야 한다.
export DB_UNIQUE_NAME=ETLSTB
export G0_SOURCE_ID="$DB_UNIQUE_NAME"

export EVID=~/g0/evidence/$RUN_ID && mkdir -p "$EVID"
```

> **`$EVID` 경로에 `$RUN_ID` 가 들어 있는 것이 중요하다.** M1-4 가 산출물 경로에 `RUN_ID` 를
> 요구한다 — 고정 이름 하나가 여러 회차의 별칭이 되면 이전 회차가 덮였는지 사후에 말할 수
> 없다. 아래 모든 산출물을 `$EVID` 아래에 둔다.

> **모든 child 를 `g0-run-child.sh` 로 감싼다.** 그러지 않으면 manifest 사이드카가 없어 집계기가
> 그 child 를 `FAILED` 로 두고 전체를 거부한다(exit 4). 이것은 버그가 아니라 계약이다 —
> 산출물이 "언제·어느 판본으로 만들어졌는지" 를 스스로 말하지 못하면 증거가 아니다.

> ### ⚠ sqlplus 는 반드시 `g0-sqlplus.sh` 로 돌린다
>
> 계약이 **실행 명령을 manifest 에 기록**한다. 그래서 `bash -c "sqlplus … CONNECT $USER/$PW@…"`
> 형태로 쓰면 **비밀번호가 manifest 파일에 그대로 남는다.** 안전 규칙 §3.1-3 을 계약이 뒤에서
> 깨는 셈이다 — 이 runbook 초안이 실제로 그랬고, 돌려 보다가 발견했다.
>
> `g0-sqlplus.sh` 는 비밀번호를 **stdin 으로만** 넘긴다. argv 에도 프로세스 목록에도 manifest 에도
> 남지 않는다. (래퍼에도 `user/pw@host` redaction 을 넣어 뒀지만 그건 심층 방어이지 해결책이 아니다.)

### S5 — G0-0A (87 probe) · C00

```bash
export ORA_DSN="//localhost:1521/FREEPDB1"     # jdbc: 접두사 없는 형태

# ★ 신원 preflight 를 **먼저** 돌린다(9차 조치 6 · P0-05). DUAL 만 읽으며,
#   붙은 DB 가 대상이 아니면 **대상에 아무 질의도 하지 않고** 비영 종료한다.
sed -e "s/^DEFINE EXPECT_DBUNAME .*/DEFINE EXPECT_DBUNAME = '$DB_UNIQUE_NAME'/" \
    -e "s/^DEFINE EXPECT_ROLE .*/DEFINE EXPECT_ROLE    = 'PHYSICAL STANDBY'/" \
    g0-0a-preflight.sql > "$EVID/g0-0a-preflight-run.sql"
./g0-sqlplus.sh "$EVID/g0-0a-preflight-run.sql" "$EVID/g0-0a-preflight.log" \
  || { echo "preflight 실패 — 본 probe 를 돌리지 않는다" >&2; exit 1; }

./g0-run-child.sh G0_0A "$RUN_ID" "$PROFILE" "$EVID/g0-0a.log" -- \
  ./g0-sqlplus.sh "$EVID/g0-0a-run.sql" "$EVID/g0-0a.log"
```

> ### ⚠ SQL 변수 전달 — **이 문서가 틀리게 적고 있었다**(9차 P0-06-8)
>
> 이전 판은 *"스크립트가 프롬프트로 묻는다"* 고 했다. **묻지 않는다.**
> `g0-0a-capability-inventory.sql:42-47` 과 `g0-0c-fence-facts.sql:23-32` 는 파일 안에서
> **자리표시자 값으로 `DEFINE` 한다** — `TARGET_OWNER = 'SCHEMA_NAME'`,
> `EXPECT_DBUNAME = 'DBUNIQUENAME'` 같은 것들이다.
>
> 결과가 둘이다. 앞의 wrapper `.sql` 에서 `DEFINE` 을 해도 **본문의 `DEFINE` 이 그것을 덮는다.**
> 그리고 아무것도 안 하면 존재하지 않는 `SCHEMA_NAME.TABLE_NAME` 을 질의하고
> `DBUNIQUENAME` 과 신원을 대조한다.
>
> **지금 유일하게 맞는 방법**: 그 `DEFINE` 블록을 **직접 편집한 사본**을 만들어 그것을 넘긴다.
>
> ```bash
> sed -e "s/^DEFINE TARGET_OWNER .*/DEFINE TARGET_OWNER   = '$TGT_OWNER'/" \
>     -e "s/^DEFINE TARGET_TABLE .*/DEFINE TARGET_TABLE   = '$TGT_TABLE'/" \
>     -e "s/^DEFINE WM_COLUMN .*/DEFINE WM_COLUMN      = '$WM'/" \
>     -e "s/^DEFINE EXPECT_DBUNAME .*/DEFINE EXPECT_DBUNAME = '$DB_UNIQUE_NAME'/" \
>     g0-0a-capability-inventory.sql > "$EVID/g0-0a-run.sql"
> grep -n '^DEFINE' "$EVID/g0-0a-run.sql"    # **눈으로 확인한다. 자리표시자가 남아 있으면 멈춘다**
> ```
>
> 그 사본을 `$EVID` 에 두는 이유는 **어떤 값으로 쟀는지가 증거의 일부**이기 때문이다.
>
> **이것은 임시 방편이다.** 변수 전달을 제대로 고치는 것은 9차 조치 6(identity fail-fast)에서
> 한다 — 그때 이 헤더를 어차피 다시 쓴다. Oracle 없이 SQL 변경을 검증할 수 없으므로
> **지금 고치지 않는다**(검증 못 한 SQL 을 절차서에 넣는 것이 이 리뷰가 잡은 병이다).

> **preflight 를 래퍼로 감싸지 않는 이유.** 이것은 증거가 아니라 **게이트**다. 통과하면
> 본 probe 회차가 증거를 만들고, 실패하면 그 회차는 시작되지 않는다. 증거 회차에 게이트
> 산출물을 섞으면 집계기가 "계약에 없는 child" 로 거부한다(M1-3). 로그는 `$EVID` 에 남긴다 —
> **무엇을 확인하고 들어갔는지**는 회차의 일부다.

**네 조건이 다 서야 통과다** — `exit 0` ∧ `probe_run_end` sentinel ∧ `manifest_ok=true` ∧
`emitted == expected`. 하나라도 빠지면 집계기가 `PARTIAL` 이상으로 두지 않는다.

C00 은 `ACK_FULL_SCAN=N`(기본)이면 **대상 테이블 질의가 0건**이다. 산출물이 0건인 것은
아니다 — skipped 3건 + summary 가 나온다.

```bash
# C00 도 같은 방식으로 DEFINE 을 채운 사본을 만든다(위 경고 참조).
./g0-run-child.sh G0_0C00 "$RUN_ID" "$PROFILE" "$EVID/g0-0c00.log" -- \
  ./g0-sqlplus.sh "$EVID/g0-0c00-run.sql" "$EVID/g0-0c00.log"
```

### S6 — ★★ B1 본실행 (세 회차)

```bash
cd g0-0b1-connection-provider
# B1_OUT 로 산출물 경로를 지정한다(9차 조치 2). 그러지 않으면 run.sh 가 자기 디렉터리에
# 고정 이름으로 쓰고, 그 경로에는 RUN_ID 가 없어 **래퍼가 거부한다**(M1-4).
export B1_OUT="$EVID/g0-0b1-evidence.json"
../g0-run-child.sh G0_0B1 "$RUN_ID" "$PROFILE" "$B1_OUT" -- \
  ./run.sh "$URL" "$ORA_USER" "$TGT_OWNER.$TGT_TABLE" "$DB_UNIQUE_NAME" PHYSICAL_STANDBY none
cd ..
```

`run.sh` 가 **세 회차**를 돈다 — `coverage` → `failclosed_schema` → `failclosed_task`.
세 번째가 **task 경로 fail-closed 의 유일한 증거**다. `fail=all` 하나로는 schema 에서 막혀
task connection 이 열리지도 않는다.

판정은 `verdicts` 다섯 항으로 나온다.

| verdict | 뜻 |
|---|---|
| `provider_reachability` | provider 가 SCHEMA·TASK 에서 불렸는가 |
| `session_assertion` | 모든 connection 이 프리앰블을 받았는가 |
| `fail_closed` | 실패하면 죽는가 (경로별로 주입이 **닿았을 때만** 판정) |
| `read_only_transaction` | `NOT_IMPLEMENTED` — 이 하네스가 시험하지 않는다 |
| `common_snapshot` | `NOT_IMPLEMENTED` |

**`PROVEN` 은 snapshot capability 의 증거가 아니다.**

### S7 — B0 (대조군)

```bash
./g0-run-child.sh G0_0B0 "$RUN_ID" "$PROFILE" "$EVID/g0-0b0.log" -- \
  bash -c "$SPARK_HOME/bin/spark-submit --jars '$OJDBC_JAR' g0-0b0-spark-smoke.py \
    --url '$URL' --user '$ORA_USER' --password-env ORA_PW \
    --table '$TGT_OWNER.$TGT_TABLE' --wm '$WM' \
    --expect-db-unique-name '$DB_UNIQUE_NAME' > '$EVID/g0-0b0.log' 2>&1"
```

### S8 — ★ CE01~CE09 (폐기용 쓰기 가능 환경 **전용**)

**운영계에서 실행 금지.** runner 가 `CE_DSN` 으로 직접 붙어 `DB_UNIQUE_NAME` 을 확인한다.

```bash
cd g0-0c-counterexamples
$EDITOR suite.yaml     # expected_*_db_unique_name / allowed_schema / object_prefix / versions
export CE_USER=ETL_CE CE_DSN='localhost:1521/FREEPDB1'
export CE_DOC_PATH="$PWD/../etl-platform-target-architecture-v1.2.4.md"     # 필수 (현행 A)
# ★ 패키지 **밖** 의 환경 allowlist. 이것이 없으면 runner 가 접속 전에 멈춘다(M0-5).
#   저장소 안에 두지 마라 — "이 환경은 폐기용이다" 를 저장소가 스스로 주장하면 안 된다.
export CE_ENV_ALLOWLIST=/etc/g0/ce-allowlist.yaml
read -rs -p 'CE password: ' CE_PASSWORD && export CE_PASSWORD && echo

python3 runner.py --suite suite.yaml --dry-run      # 가드·계획만. 먼저 이것부터
# 경로에 RUN_ID 가 있어야 한다(M1-4). CE 는 **다른 환경의 증거**이므로 corporate source
# 회차와 같은 회차로 묶지 마라 — 9차 P0-07, 조치 7 에서 scope 를 분리한다.
../g0-run-child.sh G0_0C_SUITE "$RUN_ID" "$PROFILE" "$EVID/ce-evidence.json" -- \
  python3 runner.py --suite suite.yaml \
    --observed-env '{"primary_db_unique_name":"...","standby_db_unique_name":"...","schema":"..."}' \
    --out "$EVID/ce-evidence.json"
unset CE_PASSWORD
# runner 가 이미 $EVID 에 직접 쓴다 — 예전 cp 줄은 없는 파일을 복사하고 있었다
cd ..
```

`--observed-env` 는 **실제 접속에서 읽은 값**을 넣는다. 운영자 신고값이 아니다.

판정을 두 축으로 읽어라.

- `execution_complete` — 하네스가 돌았는가
- `mitigation_holds` — **설계가 버텼는가**

`COUNTEREXAMPLE_REPRODUCED`·`MITIGATION_FAIL` 도 완주로는 정상이지만 설계로는 실패다.

> `CE_STANDBY_DSN` 을 **설정하지 마라.** 두 번째 컨테이너를 standby 인 척 물리면
> `standby_verified=true` 라는 거짓이 만들어진다 — 가드를 끄는 것보다 나쁘다.
> 설정하지 않으면 `guard_checks` 에 `NOT_CHECKED` 와 그 이유가 남는다(그게 정직한 상태다).

### 정규화

```bash
python3 g0-normalize.py \
  --report-id "NORM-$(date +%Y%m%d)-01" --run-id "$RUN_ID" --profile "$PROFILE" \
  --versions-lock versions.lock \
  --a "$EVID/g0-0a.log" --b0 "$EVID/g0-0b0.log" \
  --b1 "$EVID/g0-0b1-evidence.json" --c00 "$EVID/g0-0c00.log" \
  --c-suite "$EVID/ce-evidence.json" \
  --target-owner "$TGT_OWNER" --target-table "$TGT_TABLE" --wm-column "$WM" \
  --source-id "$DB_UNIQUE_NAME" \
  --out "$EVID/g0-0-evidence.json"
```

`--target-*` 를 주지 않으면 **테이블 단위 축 4개가 전부 `UNDETERMINED`** 가 된다. 묶이지 않은
확정값을 만들지 않는 것이 계약이다.

**`--out` 경로에 `$RUN_ID` 가 들어가야 한다**(8차 M3-5). 고정 이름 하나를 계속 덮어쓰면
그 이름은 여러 회차의 별칭이 되고, 무효한 재실행이 있어도 소비자는 이전 회차를 계속
'현재' 로 읽는다. 이미 있는 경로에는 쓰지 않는다 — 정말 덮어야 하면 `--allow-overwrite` 를
명시한다(증거 생성에는 쓰지 마라).

### 무엇을 '현재' 로 읽는가

정규화기는 `--out` 옆에 `g0-0-evidence.current.json` **포인터**를 쓴다(`--current` 로 위치를
바꿀 수 있다). 소비자는 회차 파일이 아니라 이 포인터를 읽는다.

| `status` | 뜻 |
|---|---|
| `VALID` | `path` 가 가리키는 회차가 현재 유효하다. `sha256` 로 대조하라 |
| `INVALIDATED` | **마지막 정규화가 거부됐다.** 이전 회차를 현재로 읽지 마라 — 그것은 이 회차가 무효라는 사실을 반영하지 않는다 |

`previous` 에 직전 포인터가 접혀 있어 무엇을 대체했는지 볼 수 있다. `VALID` 여도
`gate_eligible` 은 언제나 `false` 다 — 이 포인터는 "가장 최근 유효한 G0-0 레코드" 를
가리킬 뿐 G0 PASS 를 뜻하지 않는다.

### 값 두 개: `value` 와 `effective_value`

`capability_axes[*]` 는 값을 둘 싣는다. **판정·publish 는 `effective_value` 만 읽는다.**

- `value` — 그때 관측한 것. 감사·표시용이다.
- `effective_value` — 실행에 쓰는 값. `floor_reasons` 가 비어 있지 않으면 축의 floor 로
  내려가 있다. 사유는 child 미완결·unbound·stale·신선도 근거 부재·비권위 profile 이며,
  뜻은 `g0_axes.FLOOR_REASONS` 가 권위다.

`--capability-ttl-days`(기본 30)가 신선도 기준이다. **측정 분포로 정한 값이 아니라 운영자
선언값**이며 레코드의 `freshness.basis` 가 그렇게 적는다. `0` 을 주면 TTL 미선언이 되고,
그러면 신선도를 판정할 수 없으므로 모든 확정값이 floor 로 내려간다 — 모르는 것을
신선하다고 가정하지 않는다.

### 이 레코드는 G0 게이트에 못 들어간다

```bash
python3 g0_final_gate.py "$EVID/g0-0-evidence.json"   # 항상 exit 1
```

`record_type=g0_0_evidence` 는 최종 게이트가 **무조건** 거절한다. `gate_eligible` 을 손으로
`true` 로 고쳐도 마찬가지다. 덮은 항목과 못 덮은 항목의 목록은 `g0-final-contract.json` 이
권위이고, 레코드의 `covered`/`not_covered` 는 그 계약에서 나온다.

---

## 5. 종료 코드를 읽는 법

| 도구 | 0 | 2 | 3 | 4 | 5 |
|---|---|---|---|---|---|
| `g0-normalize.py` | 유효·완결 | 전제 미비 | 불완전(레코드는 씀) | **무효 — 최종 경로에 안 씀** | — |
| `analyze-trace.py` | `PROVEN` | 전제 미비 | `NOT_PROVEN` | — | `MEASUREMENT_FAILED` |
| CE `runner.py` | suite PASS | 환경 가드 실패 | PASS 아님 | 내부 오류 | — |

**exit 4 는 "측정이 나빴다"가 아니라 "계약을 못 지켰다"다.** stderr 의 `[contract]` 줄이
무엇이 어긋났는지 정확히 말한다. `<out>.rejected.json` 에 거부본이 남는다.

**exit 5 는 exit 3 과 다르다.** 3 은 "덮지 못했다", 5 는 "측정하지 못했다". 섞지 마라.

**`g0-normalize.py` 의 exit 3 은 측정 완결성만 말한다**(8차 M3). "capability 를 못 정했다"
는 exit 에 섞지 않고 레코드의 `outcome` 이 따로 낸다 — 네 값이 서로 다른 질문에 답한다.

| 필드 | 질문 |
|---|---|
| `outcome.process` | 정규화가 계약·schema 를 통과했는가 (exit 4 와 짝) |
| `outcome.measurement` | 다섯 child 가 전부 MEASURED 인가 (exit 3 과 짝) |
| `outcome.capability` | 13축의 **effective_value** 중 확정값이 몇인가 |
| `outcome.final_gate` | 최종 G0 게이트 입력 자격 — 언제나 `REJECTED_BY_CONTRACT` |

측정이 완결돼도(`exit 0`) capability 가 `UNGRADED` 일 수 있다. 그것은 실패가 아니라
결과다 — 고칠 것이 없다.

---

## 6. 자주 걸리는 곳

| 증상 | 원인 | 처치 |
|---|---|---|
| `exit 4` + `manifest 사이드카가 없다` | `g0-run-child.sh` 없이 돌렸다 | 그 child 를 다시 돌린다 |
| `exit 4` + `versions_lock_digest 불일치` | lock 을 child 실행 **후에** 고쳤다 | **증거 회차 전체를 다시 돈다**(§1) |
| `exit 4` + `run_id 불일치` | 서로 다른 회차 산출물을 섞었다 | `$RUN_ID` 를 통일해 다시 돈다 |
| `exit 4` + `산출물이 실행 후 변경됐다` | 로그를 편집하거나 덮어썼다 | 원본으로 되돌리거나 다시 돈다 |
| B1 추적 0건 | conf 누락, 또는 jar 미탑재 | **Spark 4.2.0 은 원인을 감춘다**(§7). conf·jar 를 눈으로 확인 |
| manifest `command` 에 비밀번호가 보인다 | sqlplus 를 `bash -c` 로 직접 감쌌다 | **그 manifest 와 로그를 폐기하고** `g0-sqlplus.sh` 로 다시 돈다. 비밀번호 교체도 검토하라 |
| B1 `fail_closed: NOT_TESTED` | 주입이 그 경로에 닿지 않았다 | `failclosed_task` 회차가 돌았는지 확인 |
| 축이 전부 `UNDETERMINED` | A 가 `FAILED`/`PARTIAL` 이거나 `--target-*` 누락 | coverage 부터 본다 |
| CE `SUITE_ABORT` | `suite.yaml` 의 expected 값이 비었다 | **채우기 전에는 돌지 않는 것이 정상이다** |

---

## 7. 이 회차가 증명하지 못하는 것

계획서 §1 의 등급표가 권위다. 요약하면

- **H**(하네스 사실) — 87 probe 완주 · B1 빌드·배선 · CE01~09 최초 실행
- **D**(조건부, 동일 Spark 판본에서만) — 경로 커버리지 · fail-closed 성립 · connection 개수 ·
  CE 의 **존재 증명**(빈도로는 이전 금지)
- **X**(원리적 불가) — ADG lag 일체 · `PHYSICAL STANDBY` 양성 분기 · 원천 실제 capability ·
  canonical hash 벡터 · **METADATA 경로**(하네스가 유발조차 하지 않는다) · 규모 · 플랫폼 자체

**로컬 회차는 반드시 `--profile LOCAL_WSL` 로 정규화한다.** 그러면 증거에 "이것은 하네스 동작
확인용이며 설계 주장의 근거가 아니다" 가 박힌다.

그리고 `gate_eligible` 은 **언제나 `false`** 다. schema 의 `const` 이므로 도구가 그 값을 바꿀
방법이 없다.

```
G0-0 completed  ≠  G0 PASS
G0 PASS = G0-1 ∧ G0-2 ∧ G0-3 ∧ G0-4 ∧ same_lock(G0-5)
```

---

## 8. 참고 문서

| 문서 | 무엇을 말하는가 |
|---|---|
| `etl-platform-local-poc-plan.md` | **전략** — 무엇을 왜 그 순서로. §1 증거 등급표가 핵심 |
| `g0-child-contract.md` | **계약** — manifest 형식, 집계기가 강제하는 것, 종료 코드 |
| `g0-run-child.sh` / `g0-sqlplus.sh` | 실행 래퍼. 후자는 비밀번호를 stdin 으로만 넘긴다 |
| `g0-0-probe-README.md` | 안전 규칙 · 결과 → 설계 분기표 |
| `g0-0b1-connection-provider/README.md` | B1 의 질문 셋 · verdict 읽는 법 · 경로별 주입 |
| `g0-0c-counterexamples/README.md` | CE 환경변수 · 종료 코드 · 시나리오별 증거 형태 |
| `etl-platform-v2.0-codex-seventh-review-assessment.md` | **정정** — 왜 이 절차가 지금 형태인가 |
| `g0-0-s1-s3-results.md` | S1~S3 첫 실행 기록(다른 호스트) |
