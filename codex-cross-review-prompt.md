당신은 대규모 데이터 플랫폼(Dagster, Spark, Iceberg, Oracle Data Guard, Kubernetes)을 운영해 본 principal architect이며, 지금은 **다른 리뷰어가 쓴 아키텍처 리뷰를 메타 리뷰(review of the review)** 하는 역할이다.

현재 디렉토리에 두 파일이 있다. 두 파일을 **전부** 읽어라(각각 977줄, 524줄 — 절대 skim하지 말 것).

1. `etl-platform-target-architecture-v1.md` — 신규 ETL 플랫폼 목표 아키텍처 초안 (Dagster OSS + 얇은 Control Plane, Oracle DR → Spark → Iceberg/Polaris, 10k Job / 40k Run/일 / 정시 burst 500).
2. `etl-platform-target-architecture-v1-review.md` — 그 초안에 대한 상세 리뷰 (97건: P0 2 / P1 37 / P2 37 / P3 21).

## 과제

리뷰 문서(2번)를 초안(1번)에 대조하며 **비판적으로** 검토하라. 리뷰어의 편을 들지 말고, 리뷰가 틀렸거나 과장했거나 놓친 곳을 찾는 것이 목적이다. 다음 7개 축으로 판정한다.

A. **사실 정확성** — 리뷰가 Dagster OSS, Apache Iceberg, Spark JDBC, Oracle(Data Guard/Flashback/Resource Manager/ORA_ROWSCN/V$ 뷰), Apache Polaris, Kubeflow Spark Operator, Kubernetes의 동작에 대해 주장한 내용 중 **틀리거나 버전 의존적이거나 과장된 것**을 모두 찾아라. 각 항목에 대해 "맞음 / 틀림 / 부정확(정정문) / 버전 확인 필요"로 판정하고 근거를 써라. 특히 다음을 반드시 점검하라:
   - DAG-01: 비파티션 schedule의 놓친 tick 처리(최신 1개만 재생, max_catchup_runs는 파티션 전용)
   - DAG-02: schedule 평가 gRPC timeout 기본 60초, max_tick_retries 기본 0
   - DAG-04: in-process executor에서 run resume 불가, run_retries의 retry_on_asset_or_op_failure 기본값
   - DAG-08/SCL-02: QueuedRunCoordinator max_concurrent_runs 기본 10, dequeue 단일 스레드
   - OPS-01: Dagster OSS webserver에 인증/RBAC/감사 부재, --read-only 플래그
   - ORA-01/ORA-05: standby에서 V$DATABASE.CURRENT_SCN의 의미, 적용 SCN을 얻는 올바른 방법, STANDBY_MAX_DATA_DELAY, ADG에서 Flashback Query
   - ORA-02: ORA_ROWSCN 의미(commit SCN 이상 반환), Flashback Query와의 공존 불가, 인덱스 불가
   - ORA-04: Resource Manager의 ADG physical standby 적용 여부
   - ICE-01/02/03/04/05: snapshot 보관과 데이터 파일, partitionOverwriteMode, cherry-pick/publish_changes 제약, CommitMetadata ThreadLocal, compaction과 OCC
   - CON-02: Iceberg append의 동시 commit 성공 여부
   - CRT-06: Spark JDBC Oracle dialect 타입 매핑(NUMBER, DATE→Timestamp, preferTimestampNTZ)
   - CRT-10: Spark Operator Helm 기본값
   - CRT-09: Oracle DEFAULT profile의 FAILED_LOGIN_ATTEMPTS 등

B. **초안 오독** — 리뷰가 "없다/미정의"라고 한 것 중 초안이 실제로 다른 절에서 다루고 있는 것. 초안의 해당 문장을 인용하라.

C. **심각도 보정** — P0/P1 각 항목에 대해 등급이 적절한지(과대/과소) 판정. 특히 P0 2건(CON-01, ORA-01)이 정말 P0인지 독립적으로 판단하라. P2→P1로 올려야 할 것도 찾아라.

D. **리뷰가 놓친 중요 이슈** — 초안에 있지만 리뷰가 전혀 다루지 않은 설계 결함·위험·누락. 최소 5개 이상, 각각 초안 절 인용 + 구체적 실패 시나리오 + 권고. 흔한 일반론은 제외하고 이 초안에 특화된 것만.

E. **리뷰 내부 모순** — 서로 충돌하는 권고(예: 한 곳에서는 원칙 5를 지키라 하고 다른 곳에서는 연속 탐지기를 권고), 중복, 한 finding의 권고가 다른 finding의 문제를 악화시키는 경우.

F. **권고의 실행 가능성** — 비현실적이거나 과도하게 복잡하거나(예: ValidatingAdmissionWebhook, attempt별 WAP branch) 오히려 해로운 권고. 더 단순한 대안이 있으면 제시.

G. **문서 품질** — 구조, 중복, 길이, 저자가 실제로 반영할 수 있는 형태인지.

## 출력 형식 (한국어, Markdown)

```
# Codex 교차 리뷰: etl-platform-target-architecture-v1-review.md

## 1. 총평 (리뷰의 전반적 신뢰도, 5줄 이내)

## 2. 사실 정확성 판정표
| 리뷰 ID | 주장 | 판정 | 근거/정정 |

## 3. 초안 오독
| 리뷰 ID | 리뷰 주장 | 초안의 실제 문장(절) | 영향 |

## 4. 심각도 재판정 (P0/P1 전체 + 올려야 할 P2)
| 리뷰 ID | 리뷰 등급 | 내 판정 | 이유 |

## 5. 리뷰가 놓친 이슈 (NEW-01 ...)
각각: 절 인용 / 실패 시나리오 / 권고 / 제안 등급

## 6. 리뷰 내부 모순·중복

## 7. 비현실적/해로운 권고와 대안

## 8. 문서 품질

## 9. 최종 권고 — 이 리뷰를 초안 저자에게 그대로 전달해도 되는가? 수정해야 할 것은?
```

규칙: 근거 없는 동의 금지. 모르는 것은 "확인 불가"로 표시. 리뷰의 ID(CON-01 등)와 초안의 절 번호를 반드시 인용. 출력은 최종 답변 한 번에 전부 담아라(중간 요약 금지). 길이 제한 없음 — 깊이가 우선이다.
