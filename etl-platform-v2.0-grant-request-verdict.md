# 권한 요청 방향 판정 (2026-08-27)

"ETL 계정에 critical 하지 않은 권한을 요청해 보자"를 조사한 결과다. **결론은 보류다.** 후보 37건 중 9건을 적대적으로 검증했고 **하나도 통과하지 못했다.**

---

## 1. 판정

> **DBA 권한 요청을 정합성의 전제로 삼지 않는다. 지금은 요청서를 내지 않는다.**

이유는 "권한을 못 받을 것 같아서"가 아니다. **받아도 순이익이 아니거나, 요청서 자체가 틀렸기 때문**이다.

**원자료**: 후보 37건 전체와 검증 9건의 기각 사유 전문은 [`etl-platform-v2.0-grant-request-candidates.md`](etl-platform-v2.0-grant-request-candidates.md) 에 있다. 이 판정은 그 문서 없이는 재현되지 않는다(7차 리뷰 P1-11).

읽는 법에 대한 단서: 검증자에게 "확신이 안 서면 기각"을 지시했으므로 0/9 라는 숫자 자체는 과하게 엄격할 수 있다. 그러나 기각 사유가 **1차 출처와 이 저장소의 자기 문서를 직접 인용**하고 있어, 기본 회의가 아니라 실제 결함이다. 그리고 **37건 중 28건은 검증되지 않았다** — 통과 가능한 것이 그 안에 있을 수 있다.

---

## 2. 요청서에 그대로 썼으면 DBA 앞에서 무너졌을 사실 오류

| 우리가 쓰려던 것 | 실제 |
|---|---|
| `GRANT SELECT ON V$DATAGUARD_STATS` | **ORA-02030 으로 실패한다.** `V$` 는 public synonym 이다. `SYS.V_$DATAGUARD_STATS` 에 grant 해야 한다 |
| "영향 범위 0" | **모든 GRANT 는 primary 에서 실행해야 한다.** standby 는 read-only 라 `ORA-16000` 으로 거부된다. 딕셔너리 row + redo 가 발생한다(1회성이지만 0 은 아니다) |
| "`STANDBY_MAX_DATA_DELAY` 가 우리를 자동으로 막아 준다" | **쿼리 시작 시점에만 평가된다.** 정각에 깨끗하게 시작해 20분 동안 apply 를 굶기는 Full 추출은 **절대 self-fail 하지 않는다**. 우리가 걱정하는 바로 그 시나리오를 fence 가 가장 못 잡는다 |
| "`ORA-03172` 가 나면 우리가 원인이다" | `ORA-03172` 는 **apply lag 의 함수이지 우리가 원인인지의 함수가 아니다.** lag 은 end-to-end 라 primary 의 redo 폭증만으로도 뜬다. fence 는 **자기 보호 장치로는 유효하나 자기 유해성 탐지기로는 무효**다 |
| "primary 로 가는 경로는 없다" | **자동 블록 손상 복구(ABMR)** — standby 의 read-only 쿼리가 손상 블록을 만나면 **primary 에 정상 블록을 요청한다**. Full 60% 의 대규모 스캔은 조우 확률이 구조적으로 높다 |
| `D = 0` · `SYNC WITH PRIMARY` 사용 | SYNC transport · 전송상태 SYNCHRONIZED · max protection/availability · real-time apply 가 **전제**다. 사이트가 ASYNC/max performance 면 사용 불가이고, **그것을 확인하려면 다시 `V$` 가 필요하다**(순환) |

---

## 3. `FLASHBACK` 객체 권한이 순이익이 아닌 이유

1순위로 걸었던 요청이다. 결과는 **되살아난다고 주장한 5개 중 깨끗한 것이 1개**뿐이다.

- **SCN 출처가 없다.** `AS OF SCN` 을 쓰려면 SCN 을 얻어야 하는데 `V$DATABASE.CURRENT_SCN` 도 `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER` 도 **별도 권한**이다. `FLASHBACK` 만 받으면 `AS OF TIMESTAMP` 뿐이고, 그건 `SMON_SCN_TIME` 기반 **±3초 근삿값**이다 — 우리 `g0-0a-capability-inventory.sql` 145행이 이미 그렇게 경고하고 있다.
- **새 실패 모드가 세 개 생긴다.** `SET TRANSACTION READ ONLY` 는 맞지 않는 오류들이다.
  - `ORA-01466` — fence 시점 이후 원천에 DDL 이 일어나면 발생. **생산라인 DB 의 파티션 롤링·인덱스 재구성이 그대로 트리거**다.
  - `ORA-08181` — 무효 SCN. 재시도에 같은 SCN 을 재사용하는 설계가 정확히 이 오류의 표적이다.
  - `ORA-01555` — undo 부족. 이건 `READ ONLY` 에도 있지만 요구량이 늘어난다.
- **우리 문서가 이미 벌칙을 정해 뒀다.** P1-12: `ORA-01555/01466/08181 → SPARK_FAILED + 같은 계약 RETRY 금지`. 즉 undo 가 깨지면 **부분 추출까지 버리고 원천을 통째로 다시 읽는다**. "재시도 재독을 없애 부하를 줄인다"는 주장이 실패 시 정반대로 뒤집힌다.
- **primary 로 가는 채널이 여기서 열린다.** standby 의 undo 부족에 대한 Oracle 의 권고 대책이 **"primary 의 `UNDO_RETENTION` 을 올려라"** 다. 우리가 안전의 근거로 인용하려던 문장이 사실은 **standby ETL 부하가 production primary 에 도달하는 경로**다. 생산라인 DB 에서 이건 결정적이다.

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
