# Codex 교차 리뷰 요청 — 무권한(DBA 비협조) 재설계 범위 v2.0

당신은 대규모 데이터 플랫폼(Dagster, Spark, Iceberg, Oracle Data Guard, Kubernetes)을 운영해 본 principal architect다. 지금 하는 일은 **설계 리뷰가 아니라 재설계 방향의 타당성 검증**이다 — 프로젝트의 전제 하나가 바뀌었고, 그 결과로 제안된 새 방향이 옳은지, 무엇을 놓쳤는지 판정해야 한다.

리뷰 대상 저자의 편을 들지 마라. 이 프로젝트는 이미 다섯 차례 교차 리뷰를 거쳤고, **매번 "문서로 확인되지 않은 것을 확인된 것처럼 쓴 문장"이 결함으로 잡혔다.** 같은 유형을 여섯 번째로 찾는 것이 이 리뷰의 핵심 가치다.

---

## 0. 배경 (5줄)

- 신규 ETL 플랫폼: Dagster OSS + 얇은 Java Control Plane(PostgreSQL), Oracle physical standby(ADG) → Spark → Iceberg/Polaris. 규모 10,000 Job / 40,000 Run/일 / 정시 burst 500.
- v1.1 → v1.2.3.1까지 다섯 번의 교차 리뷰와 정정을 거쳐 의미 규격이 상당히 단단해졌다.
- **2026-08-24, 전제가 하나 무너졌다: Oracle 원천 DBA 협조를 받을 수 없다(확정).**
- v1.2.3.1은 DBA를 깊게 전제한다 — 원천 테이블 생성(`ETL_HEARTBEAT`), 스케줄러 job(트랜잭션 나이 kill), PL/SQL 배포(`etl_assert_standby()`·`ETL_CANON`), `V$`/`DBA_*` SELECT 권한, profile 등록 증거, undo/LOB retention 설정, DG 시험 인스턴스와 장애 주입.
- 그래서 **보장 등급·fence·검증 oracle을 다시 짜는 범위 제안서**를 썼다. 그 제안서가 이번 리뷰 대상이다.

---

## 1. 읽어야 할 파일

작업 디렉토리에 다음이 있다. **1번은 전부, 2·3번은 지시된 절을 반드시 읽어라.** skim 금지.

| # | 파일 | 줄 | SHA-256 |
|---|---|---:|---|
| 1 | `etl-platform-v2.0-unprivileged-redesign-scope.md` — **이번 리뷰 대상** | 195 | `AEFFE9C257642CAF1F504E7A0F6D736BA77D921B1D9CD42F304B0BFC22F4FFE6` |
| 2 | `etl-platform-target-architecture-v1.2.3.1.md` — 현행 규격(무엇이 죽는지 대조용) | 1,738 | `FA1C760F5A0B7F4456D0C222A42375DEBBE0F28E219AD70D2DB7B7D96100A7A1` |
| 3 | `etl-platform-poc-test-plan-v1.md` 8차 — PoC 시험·합격 기준서 | 649 | `D31B79DF6D8881E8DB7335CEBA84E16217AF959183354C49FCE267CD9CAFEE69` |

2번에서 반드시 볼 절: **6.1**(SourceCapability·`db_identity`), **7.2 5·13번**(publish rule), **10.2**(Guard 6·8번), **11.1~11.5**(정책 계층·connection 예산·Source Visibility Fence·읽기 일관성·extract-once), **12.2·12.3**(sweep·Data Reconciliation Audit), **17장**(validator rule), **19장**(게이트·즉시 No-Go), **22장**(DBA 확정 항목).
3번에서 반드시 볼 절: **§2.1**(Phase 0 baseline 입력), **§2.3**(stub 요건), **§3.2**(비교 A·B), **§5.1**(판정 4층), **§6**(즉시 No-Go), **§8.1·§8.3**(G0·게이트).

참고(선택): `etl-platform-v1.2.2-codex-third-cross-review.md`(3차 리뷰, 669줄) — 이 프로젝트가 어떤 유형의 결함으로 반복해 걸렸는지 보여 준다.

---

## 2. 제안서의 주장 요약 (당신이 검증할 대상)

**(A) 제어 모델 전환.** 현행은 *플랫폼이 `V$` 지표를 읽고 판단*(read-then-decide)이며 그 읽기가 이제 불가능하다. 새 모델은 *세션이 접속 직후 제약을 선언하고 서버가 위반 시 거부*(assert-and-let-the-server-refuse)다. 제안서는 이 전환이 **freshness·role·identity 세 축에서는 오히려 강해지고**, 완전성·부하 제어·증거 세 축에서 약해진다고 주장한다.

**(B) 무권한으로 살아남는다고 주장하는 수단** — 전부 Oracle 19c 공식 문서 근거라고 적혀 있다.

| 수단 | 제안서의 주장 |
|---|---|
| `SYS_CONTEXT('USERENV', …)` 14속성 | 권한 전제 없음. `DATABASE_ROLE`·`DB_UNIQUE_NAME`·`CON_NAME`·`CON_ID`·`SERVICE_NAME`·`SERVER_HOST`·`INSTANCE_NAME`·`ISDBA`·`IS_DG_ROLLING_UPGRADE`로 identity·role fence를 **전부 대체** |
| `ALTER SESSION SET STANDBY_MAX_DATA_DELAY = N` | 무권한 설정 가능. 초과 시 **ORA-3172**로 쿼리 거부 = DB가 강제하는 fail-closed staleness 한도 |
| `ALTER SESSION SYNC WITH PRIMARY` | 무권한. 적용 완료까지 블로킹, 미충족 시 **ORA-3173**. 삭제된 `SYNC_COMMIT_GUARD`의 근사치 |
| `SET TRANSACTION READ ONLY` | 무권한. 트랜잭션 수준 읽기 일관성 = `AS OF SCN`의 대체 |
| `STANDARD_HASH(expr,'SHA256')` + `RAWTOHEX`/`HEXTORAW` | 무권한 내장. canonicalization을 **플랫폼 생성 SQL 텍스트**로 옮기면 `ETL_CANON` PL/SQL 불요, `UTL_RAW`도 불요. LOB·객체 타입만 서버 측 해시 불가 |
| `ORA_ROWSCN`·`SCN_TO_TIMESTAMP` | 무권한. `commit − watermark` **표본**(상한 아님) 수집 |
| `ALL_*`·`USER_*` 뷰 | `USER_`는 권한 불요, `ALL_`은 접근 가능 객체 범위 |
| `DBMS_APPLICATION_INFO`·`DBMS_SESSION.SET_IDENTIFIER` | PUBLIC. 단 읽어서 대조할 `GV$SESSION`이 없다 |

**(C) 죽는다고 주장하는 것.** `V$`/`GV$` 전부(SYSDBA만) · `DBA_*` 전부 · **`AS OF SCN`/`AS OF TIMESTAMP`(FLASHBACK 객체 권한 필요 — SELECT만으로 불가)** · `DBMS_FLASHBACK`·`DBMS_CRYPTO`(PUBLIC 아님) · 원천 DDL/job 일체 · profile·undo·LOB retention 증거 · DG 시험 인스턴스 → **G2 게이트 전체**.

**(D) fence 대체 공식.**
```
t0   = SELECT SYSTIMESTAMP FROM <대상 테이블> WHERE ROWNUM = 1   -- DUAL이 아니라 데이터 블록을 읽는 쿼리
high = min( t0 − D − safety_lag ,  MAX(watermark_column) )       -- 두 항 모두 하향 전용
```
`D`는 프리앰블이 선언한 `STANDBY_MAX_DATA_DELAY`이며, 같은 세션이 `t0`를 읽는 순간 apply lag ≤ D가 DB에 의해 보장된다고 주장한다. `MAX(watermark)`는 **high를 낮추는 방향으로만** 참여시켜 미래 일자 오염 row가 high를 밀어 올리는 경로를 없앤다고 한다.

**(E) 결정 4건.** D1 `ZERO_GAP` 등급 **삭제** / D2 보증 **5축** 재편(`source_staleness`·`snapshot_scope`·`upsert_consistency`·`delete_consistency`·`source_admission_control`) / D3 **v2.0** + v1.2.3.1을 Profile O로 보존 / D4 부록 W(협상용 5건)는 전제 아님.

**(F) 정직하게 포기한다고 적은 것.** 원천 측 admission control **0**(무권한 자기 제한 수단 부재) · 이름을 재사용한 **clone 구분 불가**(USERENV에 `DBID`·`GUID` 없음) · C1(in-doubt 분산 트랜잭션) **관측·반증 모두 불가** · `AS OF SCN` truth 비교가 **스냅샷 비교 → 안정 구간 비교로 강등** · 병렬도와 일관성이 **직접 교환 관계**가 됨(`numPartitions > 1` → 스냅샷 N개).

---

## 3. 검증 과제

### A. Oracle 사실 정확성 (최우선)

제안서 §2의 23개 사실 주장을 **Oracle 19c 1차 출처로 독립 검증**하라. 특히 다음은 설계 전체가 걸려 있으므로 문서 인용까지 확인하라.

1. **`ALTER SESSION SET STANDBY_MAX_DATA_DELAY`가 정말 무권한인가?** ALTER SESSION의 권한 전제("You do not need any privileges to perform the other operations of this statement unless otherwise indicated")가 이 파라미터에 적용되는가? 초과 시 정말 ORA-3172인가? **그리고 결정적으로: 이 한도가 쿼리 시작 시점에만 평가되는가, 장시간 실행 중에도 평가되는가?**(제안서는 "미확인, 보수적으로 시작 시점만 가정"이라 적었다 — 이 판단이 맞는가, 아니면 문서에 답이 있는가?)
2. **`AS OF SCN`/`AS OF TIMESTAMP`에 `FLASHBACK` 객체 권한이 정말 필요한가?** 제안서는 Development Guide §20.2.5를 근거로 "SELECT만으로는 불가"라고 단정한다. 이것이 맞다면 설계의 가장 큰 변경(읽기 일관성 모델 교체)이 정당화된다. **틀리다면 제안서 전체가 과잉 재설계다.**
3. `SYS_CONTEXT('USERENV','DATABASE_ROLE')`가 physical standby에서 실제로 `'PHYSICAL STANDBY'`를 돌려주는가? 속성별 권한 게이트가 정말 없는가?
4. `SET TRANSACTION READ ONLY`가 **ADG physical standby(read-only 인스턴스)에서 허용되는가?** 제안서는 이를 핵심 대체 수단으로 삼는데, read-only DB에서 트랜잭션을 여는 것의 제약을 확인했는지 의심스럽다.
5. `ALTER SESSION SYNC WITH PRIMARY`의 전제 조건(SYNC transport·SYNCHRONIZED·real-time apply)과, 그 전제가 **깨졌을 때 조용히 통과하는지 ORA-3173으로 실패하는지**.
6. `ORA_ROWSCN`이 **ADG standby에서 조회 가능한가**, 그리고 그 값이 적용된 redo를 따르는가? `SCN_TO_TIMESTAMP`의 보존 기간 밖 동작(ORA-08181)은?
7. `STANDARD_HASH`가 LOB을 못 받는 것은 맞는가? `UTL_I18N`·`UTL_RAW`·`DECOMPOSE`/`COMPOSE`의 **기본 PUBLIC 여부**(제안서는 `UTL_RAW`를 "미확인"으로 두고 우회했다 — 그 판단이 옳은가?)
8. 무권한 계정이 자기 세션 수·자원을 제한할 방법이 **정말 없는가?**(제안서는 "존재하지 않음"이라 단정한다)

**판정 형식**: 각 항목을 `확인(문서 인용)` / `정정(올바른 사실 + 출처)` / `미확인(문서에 없음)`으로 나누고, **정정된 항목이 설계의 어느 부분을 무너뜨리는지**까지 적어라.

### B. fence 공식의 건전성

제안서 §4.1의 `high = min(t0 − D − safety_lag, MAX(watermark))`를 공격하라.

- `t0`를 대상 테이블 접근 쿼리에서 얻으면 정말 apply-lag 검사가 유발되는가? `SELECT SYSTIMESTAMP FROM <tbl> WHERE ROWNUM=1`이 실제로 데이터 블록을 읽는가(옵티마이저가 테이블 접근을 제거할 가능성)?
- `SYSTIMESTAMP`는 **standby 호스트 시계**이고 `watermark`는 **primary/애플리케이션 시계**다. 두 시계의 차이를 `safety_lag`가 흡수한다는 주장이 성립하는가? 시계차의 방향이 불리하면?
- `MAX(watermark)`를 하향 캡으로 쓰면 **원천이 유휴일 때 window가 영구 정지**한다. 제안서는 "알림을 두되 강제 전진시키지 않는다"고 하는데, 40,000 Run/일 규모에서 이 선택의 운영 결과는?
- `D`를 크게 잡으면 fence가 느슨해지고 작게 잡으면 상시 실패한다. **선택 근거가 문서에 있는가?**
- 이 공식이 **누락 0을 주장하지 않는다는 점**을 제안서가 충분히 명시했는가, 아니면 여전히 완전성을 암시하는가?

### C. 놓친 결함 (최소 5건)

제안서가 다루지 않았지만 DBA 없는 세계에서 무너지는 것을 찾아라. 단서:

- `sessionInitStatement`가 **executor마다** 실행되는데 그중 하나가 실패하면? 프리앰블 5단계 중 일부만 적용된 세션이 생기는가?
- Spark JDBC가 **커넥션 풀을 재사용**할 때 `ALTER SESSION` 설정이 유지되는가? 풀 반환 후 다른 Job이 그 세션을 쓰면?
- `SET TRANSACTION READ ONLY` 트랜잭션이 **얼마나 오래 살 수 있는가**(ORA-01555)와 40k Run/일의 추출 시간 분포가 양립하는가?
- ORA-3172가 **burst 500 정각에 동시 다발**하면 재시도 폭풍이 되는가? 제안서에 그 처리가 있는가?
- 원천 부하 제어가 0인데 **10k Job이 동시에 standby를 때리면** 무슨 일이 일어나는가? 제안서의 "풀이 유일한 choke point"가 실제로 유일한가(Airflow 이관기 병행·운영 스크립트·타 시스템)?
- **G0-0 프로브 자체가 실패**하면(권한이 예상보다 적으면) 어느 결정이 뒤집히는가? 제안서에 그 분기가 있는가?
- Profile O / Profile U **두 문서를 병행 유지하는 비용**(drift)을 제안서가 과소평가했는가?

### D. 결정 4건의 타당성

- **D1(`ZERO_GAP` 삭제)** — 삭제가 맞는가, 아니면 `STANDBY_VISIBLE_SCN` 경로만이라도 남길 수 있는가? 제안서는 `visible_scn` 취득이 `DBMS_FLASHBACK` EXECUTE에 걸려 불가하다고 하는데, **다른 취득 경로는 정말 없는가?**
- **D2(5축)** — 축이 다섯이면 소비자가 이해할 수 있는가? 더 적은 수로 같은 정직성을 얻을 수 있는가? `source_admission_control = SELF_LIMITED_ONLY` 같은 "공시 전용 축"이 실제로 유용한가, 아니면 문서 장식인가?
- **D3(v2.0 + Profile 2종)** — Profile O를 보존하는 것이 실질 가치가 있는가, 아니면 **결코 오지 않을 미래를 위한 유지 비용**인가?
- **D4(부록 W)** — "전제가 아니다"라는 선언만으로 부록이 전제화되는 것을 막을 수 있는가? 과거 이 프로젝트에서 같은 방식이 실패한 전례가 있는가?

### E. 남은 것이 쓸모 있는가 (가장 중요한 질문)

**DBA 협조 없이 만든 이 플랫폼이 실제로 가치가 있는가?** 냉정하게 판정하라.

- 보장이 "탐지·복구형 + DB 강제 신선도 한도"로 줄었을 때, 기존 Airflow/HAflow 대비 **무엇이 나아지는가?**
- 소비자에게 "빠짐없이 들어온다는 보장은 없고, 빠진 것을 찾아 메우는 보장이 있다"고 말하는 것이 **데이터 웨어하우스 계약으로 수용 가능한가?**
- 이 조건에서도 **Dagster + Control Plane 전환을 추진할 이유**가 남는가, 아니면 전환의 정당화가 원천 보장에 걸려 있었는가?
- 만약 "가치 없다"면, **그 결론을 뒷받침하는 최소 조건**(부록 W 중 무엇을 얻으면 가치가 생기는지)을 적어라.

---

## 4. 출력 형식 (한국어, Markdown)

```
# Codex 교차 리뷰: 무권한 재설계 범위 v2.0

## 1. 최종 판정
### 1.1 한 줄 결론
### 1.2 의사결정 요약 (표: 대상 | GO/NO-GO/조건부 | 설명)
    - 제안서의 사실 기반 신뢰도
    - D1~D4 각각에 대한 찬반
    - v2.0 개정 착수 여부
    - 이 플랫폼을 계속 추진할 것인가 (§3-E)

## 2. Oracle 사실 검증표
| # | 제안서 주장 | 판정(확인/정정/미확인) | 1차 출처 | 정정 시 무너지는 설계 부분 |

## 3. fence 공식 공격
## 4. 제안서가 놓친 결함 (NEW-01 …) — 각각 실패 시나리오 + 등급
## 5. 결정 4건 판정
## 6. 남은 것의 가치 (§3-E에 대한 답)
## 7. v2.0 착수 전 필수 정정 (있다면 우선순위대로)
## 부록. 검토 artifact 식별값 (파일 · 줄 수 · SHA-256)
```

---

## 5. 리뷰 규칙

1. **문서로 확인되지 않은 것을 확인된 것처럼 쓰지 마라.** 제안서가 "미확인"이라 적은 항목을 당신이 확정하려면 1차 출처를 대라. 반대로 제안서가 확정했는데 근거가 약하면 그것을 지적하는 것이 이 리뷰의 최대 가치다.
2. **이미 기각된 처방을 재제출하지 마라.** 이전 다섯 라운드에서 기각된 것: catalog gateway/`commit_intent` journal, fencing generation, `run_submission` 테이블, exact recovery journal(RPO 0), activation barrier, 세 clock freshness, version/topology digest, `EXTRACT_ONCE`-only 초기 적재. 다시 제안하려면 **왜 이번엔 다른지**를 적어라.
3. **DBA 협조를 전제한 권고를 하지 마라.** 그것이 이 재설계의 이유다. 부록 W(협상용)에 넣을 항목이라면 그렇게 표시하라.
4. 심각도는 **P0(데이터 손상·실행 불가) / P1(보장 훼손) / P2(문서·운영)** 3단으로.
5. 제안서의 문장을 인용할 때는 **절 번호와 원문**을 함께 적어라.
6. 분량 제한 없음. 짧게 쓰려고 결함을 빠뜨리지 마라.
