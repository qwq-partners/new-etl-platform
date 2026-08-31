# 권한 요청 방향 판정 (2026-08-27, **2026-08-30 사실 정정**)

"ETL 계정에 critical 하지 않은 권한을 요청해 보자"를 조사한 결과다. **결론은 보류다.** 후보 37건 중 9건을 적대적으로 검증했고 **하나도 통과하지 못했다.**

> ## 2026-08-30 — 8차 교차 리뷰 M4 정정
>
> **결론(보류)은 유지된다. 근거 문장 셋이 틀렸다.**
>
> | 무엇 | 이전 | 지금 |
> |---|---|---|
> | 보류의 이유 | "받아도 **순이익이 아니다**" | "실증 전에는 **활성화·요청하지 않는다**" — 순이익 부재를 단정할 근거가 없다(§1·§3) |
> | `AS OF TIMESTAMP` 오차 | "±3초 근삿값" | **최대 3초 이전**. 방향이 있다 — 미래로는 가지 않는다(§3-1) |
> | `ORA-08181` | "같은 SCN 재사용이 이 오류의 표적" | **아니다.** 08181 은 SCN 이 유효 범위 **밖**일 때다. timestamp 매핑 실패는 **ORA-08180** 이고, 오래된 image 소실은 **ORA-01555** 다(§3-2) |
> | 빠져 있던 것 | — | **공통 anchor 의 가치.** 같은 timestamp 리터럴을 모든 partition 쿼리에 bind 하면 여러 connection 이 같은 flashback anchor 에 묶인다. 현 코어는 그것을 낼 수 없다(§3-0) |
>
> **passive grant 와 runtime 활성화를 분리한다.** object grant 자체는 primary 에서의 1회성 변경이고, undo·DDL·standby 부하는 그 overlay 를 **실제로 쓸 때** 생긴다. 이전 판은 둘을 한 덩어리로 계산해 "권한 = 부하"로 읽었다.
>
> **2026-08-31(9차 §5.2) 재정정** — 위 문장을 "딕셔너리 row 하나"라고 썼던 것은 **측정하지 않은 수치**다. `GRANT` 1회가 만드는 것은 metadata 변경 + redo + audit row 이고, 의존 객체 invalidation 도 따라올 수 있다. **일회성인 것은 맞지만 양은 모른다** — 그것이 "작다"는 주장의 근거가 되지 못한다.

---

## 1. 판정

> **DBA 권한 요청을 정합성의 전제로 삼지 않는다. 지금은 요청서를 내지 않는다.**

이유는 "권한을 못 받을 것 같아서"가 아니다. **요청서 자체에 사실 오류가 있었고(§2), 받았을 때의
이득·비용을 아직 실증하지 않았기 때문**이다.

**2026-08-30 정정 — 이전 판의 "받아도 순이익이 아니다"는 과했다.** 순이익이 없다고 **단정**하려면
아래 다섯을 재고 나서야 한다. 하나도 재지 않았다.

```text
G0-0        이미 grant 를 갖고 있는가(그렇다면 요청 자체가 무의미하다)
object 수   extract object-set 전체에 grant 가 필요한가, 그 수는 몇인가
DDL         원천의 파티션 롤링·인덱스 재구성 빈도(ORA-01466 조우율)
LOB·retention  대상 컬럼의 undo·LOB RETENTION 이 추출 시간을 덮는가
Spark 전파  같은 anchor 리터럴이 **모든 물리 connection** 에 실제로 실리는가(G0-0B1)
```

그래서 판정 문장을 이렇게 낮춘다.

> **위 다섯을 실증하기 전에는 `FLASHBACK` overlay 를 활성화하지도, 요청하지도 않는다.**
> 실증 결과가 좋으면 요청은 다시 후보가 된다. 보류는 "영구 기각"이 아니다.

**원자료**: 후보 37건 전체와 검증 9건의 기각 사유 전문은 [`etl-platform-v2.0-grant-request-candidates.md`](etl-platform-v2.0-grant-request-candidates.md) 에 있다. 이 판정은 그 문서 없이는 재현되지 않는다(7차 리뷰 P1-11).

읽는 법에 대한 단서: 검증자에게 "확신이 안 서면 기각"을 지시했으므로 0/9 라는 숫자 자체는 과하게 엄격할 수 있다. 그러나 기각 사유가 **1차 출처와 이 저장소의 자기 문서를 직접 인용**하고 있어, 기본 회의가 아니라 실제 결함이다. 그리고 **37건 중 28건은 검증되지 않았다** — 통과 가능한 것이 그 안에 있을 수 있다.

---

## 2. 요청서에 그대로 썼으면 DBA 앞에서 무너졌을 사실 오류

| 우리가 쓰려던 것 | 실제 |
|---|---|
| `GRANT SELECT ON V$DATAGUARD_STATS` | **ORA-02030 으로 실패한다.** `V$` 는 public synonym 이다. `SYS.V_$DATAGUARD_STATS` 에 grant 해야 한다 |
| "영향 범위 0" | **모든 GRANT 는 primary 에서 실행해야 한다.** standby 는 read-only 라 `ORA-16000` 으로 거부된다. metadata 변경 + redo + audit row 가 발생하고 의존 객체 invalidation 도 따라올 수 있다(1회성이지만 0 은 아니며, **양은 측정하지 않았다** — 9차 §5.2) |
| "`STANDBY_MAX_DATA_DELAY` 가 우리를 자동으로 막아 준다" | **쿼리 시작 시점에만 평가된다.** 정각에 깨끗하게 시작해 20분 동안 apply 를 굶기는 Full 추출은 **절대 self-fail 하지 않는다**. 우리가 걱정하는 바로 그 시나리오를 fence 가 가장 못 잡는다 |
| "`ORA-03172` 가 나면 우리가 원인이다" | `ORA-03172` 는 **apply lag 의 함수이지 우리가 원인인지의 함수가 아니다.** lag 은 end-to-end 라 primary 의 redo 폭증만으로도 뜬다. fence 는 **자기 보호 장치로는 유효하나 자기 유해성 탐지기로는 무효**다 |
| "primary 로 가는 경로는 없다" | **자동 블록 손상 복구(ABMR)** — standby 의 read-only 쿼리가 손상 블록을 만나면 **primary 에 정상 블록을 요청한다**. Full 60% 의 대규모 스캔은 조우 확률이 구조적으로 높다 |
| `D = 0` · `SYNC WITH PRIMARY` 사용 | SYNC transport · 전송상태 SYNCHRONIZED · max protection/availability · real-time apply 가 **전제**다. 사이트가 ASYNC/max performance 면 사용 불가이고, **그것을 확인하려면 다시 `V$` 가 필요하다**(순환) |

---

## 3. `FLASHBACK` 객체 권한 — 이득과 비용

1순위로 걸었던 요청이다. 검증 결과 **되살아난다고 주장한 5개 중 깨끗한 것은 1개**뿐이었다.
아래는 그 뒤 8차 리뷰가 정정한 판을 반영한 것이다.

### 3-0. 먼저, 빠져 있던 **잠재** 이득 — **cross-connection 공통 anchor**

> **2026-08-31(9차 §5.2) 정정** — 이 항을 "이득"이라고 쓴 것은 과하다. **검증할 잠재
> 이득이지 확인된 이득이 아니다.** 아래 세 조건 중 마지막(모든 물리 connection 에
> 리터럴이 실제로 실리는가)은 G0-0B1 이 아직 답하지 않았다. 본문 마지막 줄이 그렇게
> 적고 있었는데 제목과 인용처가 따라오지 않았다.

**이전 판은 이 항을 세지 않았다.** SCN 출처가 없다는 사실에서 곧바로 "순이익 없음"으로 갔는데,
그 사이에 있는 것을 건너뛰었다.

Spark 는 `numPartitions > 1` 이면 partition 마다 **다른 물리 connection** 으로 읽는다. 지금 코어의
유일한 수단인 `SET TRANSACTION READ ONLY` 는 **connection scope** 라 그 connection 안에서만
일관되다 — 즉 코어가 낼 수 있는 최대는 `snapshot_scope = CONNECTION` 이고, 한 회차의 partition
들은 서로 다른 시점을 본다(CE06 이 겨냥하는 것이 이것이다).

`AS OF TIMESTAMP <리터럴>` 은 다르다. **리터럴은 connection 에 매이지 않는다.** 같은 문자열을
모든 partition 쿼리에 bind 하면 같은 DB 안에서 같은 SCN 으로 매핑되므로, 여러 connection 이
**같은 flashback anchor** 에 묶인다. 3초 이전 매핑은 **anchor 의 정밀도** 문제이지 공통 anchor 가
사라지는 문제가 아니다.

그렇다고 `snapshot_scope = JOB` 이 바로 나오지는 않는다. 세 조건이 **모두** 참이어야 한다.

| 조건 | 무엇이 답하는가 |
|---|---|
| 공통 anchor 를 얻는가 | 이 절(`FLASHBACK` + 리터럴) |
| extract object-set **전체**에 그 anchor 가 통하는가 | `snapshot_object_coverage` — object 마다 grant 가 필요하다 |
| **모든 물리 connection** 에 그 리터럴이 실제로 실리는가 | G0-0B1(engine propagation). Spark 가 그렇게 하리라는 것은 가정이지 관측이 아니다 |

**즉 이득은 "가능성"이지 "실측"이 아니다.** 그러나 가능성이 0 이라고 쓴 것은 틀렸다.

### 3-1. SCN 출처가 없다 — 사실. 단, 오차의 성질을 정정한다

`AS OF SCN` 을 쓰려면 SCN 을 얻어야 하는데 `V$DATABASE.CURRENT_SCN` 도
`DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER` 도 **별도 권한**이다. `FLASHBACK` 만 받으면
`AS OF TIMESTAMP` 뿐이다. 여기까지는 그대로다.

**2026-08-30 정정 — "±3초 근삿값"은 틀렸다.** Oracle 은 SCN↔timestamp 를 3초 입도로 매핑하며,
timestamp 로 지정한 flashback query 가 실제로 보는 시점은 **지정 시각보다 최대 3초 이전**이다.
방향이 있다 — **미래로는 가지 않는다.** `±` 는 "3초 뒤 시점을 볼 수도 있다"로 읽히는데 그것은
반개구간 fence 의 상한 판정에서 정반대의 안전 방향이다.

그리고 이것을 `SCN_TO_TIMESTAMP` 의 오차와 **섞지 마라**. 둘은 다른 문장이다.

| 함수·구문 | 오차의 성질 |
|---|---|
| `AS OF TIMESTAMP t` | 실제로 보는 시점 ∈ `[t − 3초, t]` — **보수 방향(과거)이 보장된다** |
| `SCN_TO_TIMESTAMP(s)` | "통상 정밀도 3초"의 **근삿값**. 상한도 방향 보장도 아니다 |

이전 판이 인용한 `g0-0a-capability-inventory.sql` 145행은 **후자**에 대한 경고다. 전자의 근거로
인용한 것이 오독이었다. A §11.3 은 후자를 이미 바르게 쓰고 있다(그 witness 로는 `ZERO_GAP` 을
publish 하지 못한다).

근거: [Oracle Flashback 개발자 가이드](https://docs.oracle.com/en/database/oracle/oracle-database/19/adfns/flashback.html)
— "the actual time queried might be up to 3 seconds earlier than the time you specify".

### 3-2. 새 실패 모드 — **오류 코드를 정정한다**

`SET TRANSACTION READ ONLY` 에는 맞지 않는 오류들이다. 단, 어느 코드가 무엇인지가 틀려 있었다.

| 코드 | 실제 정의 | 우리에게 언제 뜨는가 |
|---|---|---|
| `ORA-01466` | fence 시점 이후 대상 정의가 바뀌었다 | **생산라인 DB 의 파티션 롤링·인덱스 재구성이 그대로 트리거**다. 그대로 유효 |
| `ORA-08180` | "no snapshot found based on specified time" — **시각을 매핑표의 SCN 에 맞추지 못했다** | `AS OF TIMESTAMP` 경로의 고유 실패다. 지정 시각이 매핑 보존 범위 밖일 때 |
| `ORA-08181` | "specified number is not a valid system change number" — **공급된 SCN 이 유효 SCN 범위 밖이다** | SCN 이 애초에 유효 범위 밖일 때. 대표 사례는 **다른 DB 의 SCN** 이다(A §11.4 — identity 대조 없이는 08181 조차 안 날 수 있다) |
| `ORA-01555` | undo 부족으로 과거 image 가 덮였다 | 오래된 anchor 로 오래 읽을 때. `READ ONLY` 에도 있지만 요구량이 늘어난다 |

**"재시도에 같은 SCN 을 재사용하는 설계가 정확히 ORA-08181 의 표적이다"는 틀렸다.** 한 번 유효했던
SCN 은 시간이 지나도 유효 범위 **안**이다 — 범위를 벗어나지 않는다. 같은 anchor 로 다시 읽을 때
실제로 나는 것은 **`ORA-01555`**(그 시점 image 가 덮였다)이거나 **`ORA-01466`**(그 사이 DDL)이다.
08181 은 그 상황의 코드가 아니다.

근거: [ORA-08181](https://docs.oracle.com/en/error-help/db/ora-08181/) — "The supplied SCN was beyond
the bounds of a valid SCN"; [ORA-08180](https://docs.oracle.com/en/error-help/db/ora-08180/) —
"Could not match the time to an SCN from the mapping table".

> **A §11.4 의 P1-12 규칙은 그대로 둔다.** `ORA-01555/01466/08181 → SPARK_FAILED + 같은 계약
> RETRY 금지`는 세 코드 **전부**에 대해 맞는 처리다(어느 것이든 그 anchor 로는 다시 못 읽는다).
> 정정 대상은 규칙이 아니라 **어느 코드가 어느 원인인지에 대한 설명**이다. 다만 `AS OF TIMESTAMP`
> 경로를 쓰기로 하면 `ORA-08180` 이 같은 목록에 들어가야 한다.

### 3-3. 비용 — **passive grant 와 runtime 활성화를 분리한다**

이전 판은 아래 둘을 한 덩어리로 세어 "권한 = 부하"로 읽었다. 갈라야 판단이 선다.

| | passive(권한을 받아 두기만) | runtime(overlay 를 실제로 쓰기) |
|---|---|---|
| 무엇이 생기나 | primary 에서 `GRANT` 1회 — metadata 변경 + redo + audit, 의존 객체 invalidation 가능. **일회성이나 양은 미측정**(9차 §5.2) | undo 요구량 증가, DDL 조우, standby 읽기 부하 |
| 되돌리기 | `REVOKE` | 그 회차를 되돌릴 수 없다 |
| 생산 primary 영향 | 1회성 | **`UNDO_RETENTION` 상향 요구가 여기서 나온다** |

**primary 로 가는 채널은 runtime 쪽에서 열린다.** standby 의 undo 부족에 대한 Oracle 의 권고
대책이 "primary 의 `UNDO_RETENTION` 을 올려라"다 — 우리가 안전의 근거로 인용하려던 문장이 사실은
**standby ETL 부하가 production primary 에 도달하는 경로**다. 생산라인 DB 에서 이건 결정적이며,
**그래서 활성화를 실증 뒤로 미룬다.** 그러나 그것이 "grant 를 받아 두는 것 자체가 위험하다"는
근거는 아니다.

**우리 문서가 이미 벌칙을 정해 뒀다.** P1-12 대로 undo 가 깨지면 **부분 추출까지 버리고 원천을
통째로 다시 읽는다**. "재시도 재독을 없애 부하를 줄인다"는 주장은 **실패 시** 정반대로 뒤집힌다 —
이것은 runtime 비용이며 여전히 유효한 지적이다.

### 3-4. 정확한 권한 규칙

이전 판은 "`FLASHBACK` 객체 권한"만 적었다. 실제 규칙은 **조합**이고, 유일한 경로도 아니다.

```text
대상 객체 READ 또는 SELECT
  ∧ ( 대상 객체 FLASHBACK  또는  시스템 FLASHBACK ANY TABLE )
```

최소권한 정책에서는 object grant 가 맞지만 **유일한 SQL 권한은 아니다** — DBA 가
`FLASHBACK ANY TABLE` 을 제안할 수 있고, 그때는 object 수 문제가 사라지는 대신 범위가 넓어진다.
요청서에 두 경로를 다 적고 조직이 고르게 해야 한다. `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER`
의 `EXECUTE` 는 이것과 **별개**다(§3-1).

근거: [SELECT 전제조건](https://docs.oracle.com/en/database/oracle/oracle-database/18/sqlrf/SELECT.html),
[Flashback 권한 가이드](https://docs.oracle.com/en/database/oracle/oracle-database/19/adfns/flashback.html).

---

## 4. 설계에 반드시 반영해야 할 것 — 자기증폭 루프

권한과 무관하게 남는 실제 위험이다.

```
apply lag ↑ → ORA-03172 로 세션 사망 → 재시도 → 원천 재독 → 부하 ↑ → apply lag ↑
```

여기에 **정시 burst 500건**이 겹치면 상관적 동시 실패가 된다. 500개가 같은 순간에 `ORA-03172` 를 맞고 같은 순간에 재시도한다. **backoff + jitter + circuit breaker 가 없으면 fence 는 부하를 줄이는 게 아니라 접속·재시작 부하로 증폭시킨다.**

→ A v2.0 의 재시도 정책에 이 되먹임 항을 명시적으로 넣어야 한다. 지금 설계에는 없다.

---

## 5. witness 의 진짜 요건 (heartbeat 대안 검토에서 나온 것)

"생산 PDB 를 건드리지 말고 같은 CDB 의 ETL 전용 PDB 에 heartbeat 를 두자"는 안이 **fail-open** 이라 기각됐다.

- 생산 PDB 가 복구되지 않는데 ETL PDB heartbeat 만 계속 전진하면, 모니터는 "신선"이라 보고하고 **오래된 데이터가 최신으로 라벨링되어 하류로 나간다.**
- 반대로 heartbeat 를 관측 대상과 같은 곳에 두면 같이 얼어붙어 강등이 발화한다 — **fail-safe**.

> **증인의 요건은 "같은 redo 스트림에 실려 온다"가 아니라 "관측 대상과 같은 실패 운명을 공유한다"이다.**

이 원칙은 heartbeat 뿐 아니라 모든 freshness 증거에 적용된다.

---

## 6. 그래도 남는 것

1. **Tier 0(권한 불필요) 목록** — 이건 여전히 유효하고, 코어는 여기서만 성립해야 한다. `etl-platform-v2.0-capability-overlay.md` §2 참조.
2. **우리가 먼저 거는 자기제한** — Resource Manager 배치안은 기각됐지만(배치 설계가 틀렸다), **부하 상한을 스스로 걸어야 한다는 방향 자체는 유효**하다. Full 60% 가 대량 전수 읽기라는 사실은 변하지 않는다. 재설계 필요.
3. **G0-0A 로 현황부터 확인** — `view.v_dataguard_stats` / `view.v_database` / `pkg.dbms_flashback` probe 가 **이미 grant 를 받았는지** 알려 준다. 요청하기 전에 이미 있는지 확인하지 않으면 무의미한 요청을 하게 된다.

---

## 7. 남은 일

- 미검증 28건 중 Tier 0·자기제한 계열을 다시 훑는다(권한 요청 계열은 우선순위를 내린다).
- Resource Manager / 프로파일 자원 제한을 **다시 설계**한다 — 기각 사유는 "하지 말라"가 아니라 "배치가 틀렸다"였다.
- `ORA-03172` 되먹임 대책(backoff·jitter·circuit breaker)을 A v2.0 재시도 정책에 반영한다.
- **§1 의 실증 다섯을 G0-0 에 붙인다**(2026-08-30). 셋은 이미 하네스에 있다 —
  `pkg.dbms_flashback`·`view.v_database` probe 가 "이미 grant 를 갖고 있는가"에 답하고(§6-3),
  `as_of_timestamp.target` 이 대상 하나에 대한 anchor 성립을, G0-0B1 이 engine propagation 을
  답한다. **없는 것은 object 수와 DDL 빈도·LOB retention 이며 그것은 원천 DBA 만 답할 수 있다.**
- 이 문서에 남은 미정: `AS OF TIMESTAMP` 를 실제로 쓰기로 하면 `ORA-08180` 을 A §11.4 의
  `ORA-01555/01466/08181` 목록에 넣어야 한다(§3-2). **쓰기로 정하기 전에는 넣지 않는다** —
  쓰지 않는 경로의 오류 코드를 규범에 미리 박으면 그 경로를 채택한 것처럼 읽힌다.
