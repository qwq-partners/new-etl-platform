# 사내 반입 안내

이 저장소를 사내 환경으로 가져가기 위한 절차다. G0-0 실행 절차는 포함하지만,
**현재 커밋은 사내 원천 실행 준비 완료판이 아니다.**

> **2026-08-30 실행 차단**: 8차 교차 리뷰에서 M0 실행 안전성과 M1 child evidence contract가
> 닫히기 전에는 현재 A/B0/B1을 사내 원천에 실행하지 않는다고 판정했다.
> §2~§3의 저장소·의존물 반입은 진행할 수 있지만, §4~§5는 M0/M1 수정과 회귀 검증을 마친 뒤의
> 목표 절차다. 상세 근거는 `etl-platform-v2.0-codex-eighth-cross-review.md`를 본다.

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

2026-08-30 확인값: **약 1.0 MB** (기준 HEAD `7e0872b`, 커밋 21개 / 추적 파일 74개 포함).
이 수치는 문서 추가에 따라 달라질 수 있으므로 반입 시 생성한 bundle의 sha256과 `git bundle verify` 결과를 기준으로 삼는다.

안에서:

```bash
sha256sum new-etl-platform.bundle          # 밖에서 적어 온 값과 대조
git bundle verify new-etl-platform.bundle
git clone new-etl-platform.bundle new-etl-platform
cd new-etl-platform && git log --oneline | head -3
```

> **절차 검증 범위**: 과거 dry-run에서 빈 디렉터리 clone과 HEAD 일치를 확인했다.
> 현재 반입본은 반드시 반입 시점의 commit ID·`git bundle verify`·sha256으로 다시 확인한다.

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
# 사내에서 이것들이 다 되면 의존물 반입이 끝난 것이다.
# 이것만으로 G0-0 실행 승인이 되지는 않는다.
java -version && javac -version
python3 -V && python3 -c "import oracledb, jsonschema; print('ok')"
$SPARK_HOME/bin/spark-submit --version
unzip -p "$OJDBC_JAR" META-INF/MANIFEST.MF | grep -i implementation-version
which sqlplus
```

---

## 4. 결과를 밖으로 가져오기

G0-0 산출물에는 **원천 식별자가 들어간다**(`DB_UNIQUE_NAME`·`INSTANCE_NAME`·스키마·테이블·SID). 반출 전에 확인한다.

> **현재 normalizer 결과는 판정 증거로 수용하지 않는다.** child별 run/source/profile/runtime/lock 결속,
> exact manifest, completion/exit 상태와 stale/effective floor를 M1/M3에서 보강하기 전에는 raw 산출물을
> 변경하지 않고 보관한다. 아래 정규화 명령은 그 보강이 완료된 뒤의 목표 사용법이다.

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

## 5. M0/M1 수정 후 첫 실행 순서

`g0-0-probe-README.md` 와 `etl-platform-local-poc-plan.md` 가 상세를 갖고 있다. 요약하면:

**STOP 조건**: producer exit를 보존하는 외부 wrapper, target 접촉 전 identity hard preflight,
B0 partition/session 하드 상한, B1 explicit provider·경로별 독립 검증, child evidence binding,
C01~C09 외부 disposable-environment allowlist 중 하나라도 없으면 실행하지 않는다.

```
(0) M0/M1 회귀 검증 통과 · 변수 정의 · 외부 실행 wrapper와 승인 범위 고정
(1) G0-0A   sqlplus  — exit 0 ∧ probe_run_end sentinel ∧ manifest_ok=true ∧ emitted=87, 넷 다
(2) G0-0B0  stock Spark 경로 관측 — partition/session cap과 process exit 확인
(2.5) G0-0B1  explicit connectionProvider로 schema/task/metadata 경로를 각각 실행
(3) G0-0C00 ACK_FULL_SCAN 승인 후에만 (기본값이면 대상 테이블 질의 0건)
(4) G0-0C01~C09  **외부 allowlist가 확인한 폐기 가능한 환경에서만.** CE_DOC_PATH 필수
(5) child contract 검증 후 g0-normalize.py 로 gate_eligible=false인 g0_0_evidence 생성
```

주의할 것 둘.

- 실행자가 넘긴 **`--profile CORP_POC` 문자열만으로 사내 증거가 되지 않는다.** M1의 child binding과 runtime/source 확인을 통과해야 한다. `LOCAL_WSL` 결과는 하네스 회귀 검증용일 뿐 설계 근거가 아니다.
- **잘못된 비밀번호를 시도하지 않는다.** 계정 잠금은 전체 파이프라인 정지다. 하네스는 접속 1회 실패 시 중단하도록 돼 있지만, 반입 직후 접속 정보를 손으로 시험할 때가 가장 위험하다.

---

## 6. 변경관리에서 물어볼 만한 것

미리 답을 준비해 두면 승인이 빠르다.

| 질문 | 답 |
|---|---|
| 원천에 쓰기를 하는가 | A·B0·B1·C00의 의도는 읽기 전용이지만 **현재 실행 안전성은 미승인**이다. C01~C09는 쓰기이며 저장소 내부 guard가 아니라 **외부 allowlist가 확인한 폐기 환경**에서만 허용한다 |
| 대상 테이블을 얼마나 읽는가 | 현재 `ROWNUM`·row count는 결과 행 상한이지 Oracle I/O 하드 상한이 아니다. B0는 partition/session 하드 상한도 없다. 실행 전 SQL plan·partition 수·동시 세션·scan budget을 별도로 승인해야 한다 |
| 비밀번호는 어떻게 다루는가 | 환경변수 또는 stdin 만. **argv·URL·로그·추적 파일에 넣지 않는다** |
| 실패하면 어떻게 되는가 | **현재는 fail-closed를 보장하지 않는다.** A는 target 접촉 전 identity 선차단이 없고, B0는 오류 출력 뒤 exit 0이 가능하며, `producer \| tee`는 producer exit를 잃을 수 있다. M0 wrapper와 hard preflight 수정 후 재검증한다 |
| 어떤 권한이 필요한가 | 대상 객체 조회의 최소 전제는 `SELECT`/`READ`다. 나머지 probe는 계정에 이미 있는 capability를 측정하며 실패할 수 있다. 추가 권한 요청은 **현재 보류**이고 어떤 설계도 이를 가정하지 않는다 |
