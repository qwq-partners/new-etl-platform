# 사내 반입 안내

이 저장소를 사내 환경으로 가져가 G0-0 를 실행하기 위한 절차다.

> **핵심**: 저장소만 가져가면 **아무것도 실행되지 않는다.** 산출물은 전부 Oracle 클라이언트·Spark·python 패키지에 의존하고, 폐쇄망이면 그것들이 `pip`/Maven 으로 들어오지 않는다. §3 의 의존물을 함께 반입해야 한다.

---

## 1. 먼저 정할 것 — 사내 망이 GitHub 에 닿는가

| | A. 닿는다 | B. 닿지 않는다(폐쇄망) |
|---|---|---|
| 저장소 | `git clone` (§2.1) | **`git bundle`** (§2.2) |
| 의존물 | `pip install` · Maven | 전부 수동 반입 (§3) |
| 결과 회수 | `git push` | 역방향 bundle (§4) |

**모르면 B 로 준비한다.** A 로 밝혀지면 B 의 준비물은 그대로 버리면 되지만, 반대는 반입 절차를 다시 밟아야 한다.

---

## 2. 저장소 가져오기

### 2.1 GitHub 에 닿는 경우

저장소는 **private** 이므로 인증이 필요하다. 셋 중 하나.

```bash
# ① SSH (권장 — 계정 단위 권한이라 저장소 목록 제한을 받지 않는다)
git clone git@github.com:qwq-partners/new-etl-platform.git

# ② HTTPS + fine-grained PAT
#    주의: 토큰이 이 저장소를 selected repositories 에 포함해야 한다.
#    포함되지 않으면 403(push) / 404(read) 가 난다 — "저장소 없음"이 아니라 "안 보임"이다.
git clone https://github.com/qwq-partners/new-etl-platform.git

# ③ gh CLI
gh repo clone qwq-partners/new-etl-platform
```

사내 프록시가 있으면 `git config --global http.proxy` 를 먼저 건다. SSH 는 22번이 막혔을 때 `ssh.github.com:443` 으로 우회할 수 있다.

```
# ~/.ssh/config
Host github.com
  HostName ssh.github.com
  Port 443
```

### 2.2 폐쇄망 — `git bundle`

**tarball 보다 낫다.** 단일 파일이고, 커밋 이력이 그대로 오고, 무결성을 검증할 수 있고, 나중에 증분만 다시 보낼 수 있다.

밖에서:

```bash
git bundle create new-etl-platform.bundle --all
git bundle verify new-etl-platform.bundle
sha256sum new-etl-platform.bundle
```

실측: **약 0.9 MB** (커밋 18개 / 파일 72개 전부 포함).

안에서:

```bash
sha256sum new-etl-platform.bundle          # 밖에서 적어 온 값과 대조
git bundle verify new-etl-platform.bundle
git clone new-etl-platform.bundle new-etl-platform
cd new-etl-platform && git log --oneline | head -3
```

> **검증됨**: 이 절차로 빈 디렉터리에 clone 해 커밋 18개·파일 72개가 원본과 같은 HEAD 로 복원되는 것을 확인했다.

#### 이후 갱신은 증분으로

```bash
# 밖 — 사내가 가진 마지막 커밋 이후만
git bundle create update.bundle <사내_HEAD>..main
# 안
git bundle verify update.bundle && git pull update.bundle main
```

---

## 3. 의존물 — 이것 없이는 한 줄도 안 돈다

| 산출물 | 필요한 것 |
|---|---|
| G0-0A · C00 | **sqlplus** (Instant Client Basic + SQL*Plus 패키지) |
| G0-0B1 **빌드** | **JDK** + **`$SPARK_HOME/jars`** — `build.sh` 가 Maven 없이 `javac` 로 직접 컴파일한다 |
| G0-0B0 · B1 **실행** | Spark 배포판 + **ojdbc jar**(`$OJDBC_JAR` 필수) |
| G0-0C01~C09 | `oracledb`(thin) + `jsonschema` + **폐기 가능한 쓰기 가능 Oracle** |
| `g0-normalize.py` | `jsonschema` — 없으면 "검증 못 했다"를 결함으로 기록하고 **exit 4** 다 |

### 3.1 python 패키지 (실측)

밖에서 받아 옮긴다.

```bash
pip download --dest ./wheels oracledb jsonschema
```

**약 7.8 MB / 10개 파일**(`oracledb` 2.4MB · `cryptography` 4.5MB · 나머지 소형).
`cryptography`·`cffi`·`rpds_py` 는 **플랫폼별 wheel** 이므로 **사내와 같은 OS·python 버전에서 받아야 한다.** 다르면 `--platform`/`--python-version`/`--only-binary=:all:` 로 맞춰 받는다.

안에서:

```bash
pip install --no-index --find-links ./wheels oracledb jsonschema
```

### 3.2 Spark · JDK · ojdbc · sqlplus

용량이 커서 별도 반입이 필요하다.

- **Spark 배포판** — `spark-<ver>-bin-hadoop3.tgz` (4.2.0 기준 **약 530 MB**)
- **JDK** — 사내 표준 판본. `build.sh` 는 `TARGET_RELEASE`(기본 17)로 바이트코드 레벨을 맞춘다. **빌드 JDK 가 실행 JVM 보다 높으면 ServiceLoader 가 조용히 실패**하고 그 실패는 "추적 0건"으로만 나타난다
- **ojdbc jar** — Maven Central `com.oracle.database.jdbc:ojdbc11`
- **sqlplus** — Oracle Instant Client (Basic + SQL*Plus)

받는 즉시 **판본과 sha256 을 `versions.lock` 에 적는다.** 이 파일의 digest 가 모든 증거에 `versions_lock_digest` 로 박히고, `UNSET` 이 남아 있으면 그 항목에 의존하는 측정은 미확정으로 처리된다.

### 3.3 반입 전 점검

```bash
# 사내에서 이것들이 다 되면 실행 준비가 된 것이다
java -version && javac -version
python3 -V && python3 -c "import oracledb, jsonschema; print('ok')"
$SPARK_HOME/bin/spark-submit --version
unzip -p "$OJDBC_JAR" META-INF/MANIFEST.MF | grep -i implementation-version
which sqlplus
```

---

## 4. 결과를 밖으로 가져오기

G0-0 산출물에는 **원천 식별자가 들어간다**(`DB_UNIQUE_NAME`·`INSTANCE_NAME`·스키마·테이블·SID). 반출 전에 확인한다.

```bash
python3 g0-normalize.py --report-id "$(date -u +G0-0-%Y%m%dT%H%M%SZ)" --profile CORP_POC \
    --a g0-0a.log --b0 b0.json --b1 g0-0b1-connection-provider/g0-0b1-evidence.json \
    --c00 c00.log --c-suite g0-0c-counterexamples/evidence.json \
    --versions-lock versions.lock --out g0-0-evidence.json

# 무엇이 나가는지 눈으로 확인한다
python3 -c "import json;d=json.load(open('g0-0-evidence.json'));print(json.dumps(d['source'],ensure_ascii=False,indent=1))"
```

반출 채널이 git 이면 역방향 bundle 을 쓴다.

```bash
git add -A && git commit -m "G0-0 실측 결과"
git bundle create result.bundle <반입시_HEAD>..HEAD
```

밖에서:

```bash
git bundle verify result.bundle && git pull result.bundle main
```

> `.gitignore` 가 `evidence.json`·`evidence-*.json`·`*evidence*.log` 를 무시한다. **의도한 것이다** — 원천 식별자가 든 실행 산출물을 자동으로 커밋하지 않는다. 반출할 것은 내용을 확인한 뒤 `git add -f` 로 명시 추가한다.

---

## 5. 반입 후 첫 실행 순서

`g0-0-probe-README.md` 와 `etl-platform-local-poc-plan.md` 가 상세를 갖고 있다. 요약하면:

```
(0) 변수 정의 · 대상 스키마/테이블을 각 SQL 상단 DEFINE 에 기입
(1) G0-0A   sqlplus  — exit 0 ∧ probe_run_end sentinel ∧ manifest_ok=true ∧ emitted=87, 넷 다
(2) G0-0B0  stock Spark 경로 관측
(2.5) G0-0B1  ./build.sh  → ./run.sh   ← **여기가 최대 미지수**
(3) G0-0C00 ACK_FULL_SCAN 승인 후에만 (기본값이면 대상 테이블 질의 0건)
(4) G0-0C01~C09  **폐기 가능한 환경에서만.** CE_DOC_PATH 필수
(5) g0-normalize.py 로 g0_0_evidence 레코드 생성
```

주의할 것 둘.

- **`--profile CORP_POC` 로 정규화한다.** `LOCAL_WSL` 로 하면 "하네스 동작 확인용이며 설계 근거가 아니다"가 증거에 박힌다.
- **잘못된 비밀번호를 시도하지 않는다.** 계정 잠금은 전체 파이프라인 정지다. 하네스는 접속 1회 실패 시 중단하도록 돼 있지만, 반입 직후 접속 정보를 손으로 시험할 때가 가장 위험하다.

---

## 6. 변경관리에서 물어볼 만한 것

미리 답을 준비해 두면 승인이 빠르다.

| 질문 | 답 |
|---|---|
| 원천에 쓰기를 하는가 | G0-0A·B0·B1·C00 은 **읽기 전용, DDL·DML 0줄**. C01~C09 만 쓰기이며 **폐기 가능한 별도 환경 전용**이고 가드가 통과하지 못하면 한 줄도 실행되지 않는다 |
| 대상 테이블을 얼마나 읽는가 | A 는 `ROWNUM=1` 몇 건. B0·B1 은 `--limit`/`--probe-rows` 상한(하드 최대 100,000). C00 은 `ACK_FULL_SCAN=Y` 로 **명시 승인**하기 전에는 대상 테이블 질의가 0건 |
| 비밀번호는 어떻게 다루는가 | 환경변수 또는 stdin 만. **argv·URL·로그·추적 파일에 넣지 않는다** |
| 실패하면 어떻게 되는가 | 신원이 기대와 다르면 읽지 않고 예외를 던진다(fail-closed). 자격증명 오류를 만나면 남은 단계를 실행하지 않는다 |
| 어떤 권한이 필요한가 | 대상 테이블 `SELECT` 만. 추가 권한 요청은 **현재 보류**이며 어떤 설계도 그것을 가정하지 않는다 |
