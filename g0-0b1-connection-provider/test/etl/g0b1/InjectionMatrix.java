package etl.g0b1;

/**
 * {@code Preamble.shouldFail} 매트릭스 시험 (8차 교차 리뷰 M2-3).
 *
 * <p><b>이 시험이 지키는 성질 하나.</b> 주입 대상은 <b>driver 가 선언한 phase</b> 로 정하며
 * {@code Trace.classify} 의 스택 추정은 어디에도 들어가지 않는다. v1 은 추정 경로를 받아
 * 판단했고, 그러면 분류기의 오류가 곧 잘못된 주입이 되며 그 결과로 분류기를 검증할 수도
 * 없다(순환).
 *
 * <p>실행: {@code ./run-tests.sh}
 */
public final class InjectionMatrix {

    private static int pass = 0;
    private static int fail = 0;

    /** {@code g0b1.fail} / {@code g0b1.fail.phase} 를 세우고 선언된 phase 로 물어본다. */
    private static void check(String failMode, String failPhase, String declaredPhase, boolean want) {
        String prevFail = System.getProperty("g0b1.fail");
        String prevPhase = System.getProperty("g0b1.fail.phase");
        set("g0b1.fail", failMode);
        set("g0b1.fail.phase", failPhase);

        boolean got = Preamble.shouldFail(declaredPhase);

        set("g0b1.fail", prevFail);
        set("g0b1.fail.phase", prevPhase);

        String label = String.format("fail=%-6s fail.phase=%-18s declared=%-18s -> %s",
                String.valueOf(failMode), String.valueOf(failPhase),
                String.valueOf(declaredPhase), want);
        if (got == want) {
            pass++;
            System.out.println("  PASS  " + label);
        } else {
            fail++;
            System.out.println("  FAIL  " + label + "   (got " + got + ")");
        }
    }

    private static void set(String k, String v) {
        if (v == null) System.clearProperty(k);
        else System.setProperty(k, v);
    }

    public static void main(String[] args) {
        System.out.println("Preamble.shouldFail 매트릭스 (8차 M2-3 — 선언된 phase 기반)");

        System.out.println("\n[1] 기본은 주입 없음");
        check(null, null, "schema_only", false);
        check("none", null, "schema_only", false);
        check("NONE", null, "partitioned_count", false);
        check("", null, "schema_only", false);

        System.out.println("\n[2] fail=all 은 선언된 phase 와 무관하게 전부");
        check("all", null, "schema_only", true);
        check("all", null, "partitioned_count", true);
        check("all", null, "second_action", true);
        check("all", null, "UNDECLARED", true);
        check("all", null, "BETWEEN_STEPS", true);
        check("all", null, null, true);
        check("ALL", null, "schema_only", true);

        System.out.println("\n[3] fail.phase 는 선언된 phase 와 정확히 일치할 때만");
        check("phase", "partitioned_count", "partitioned_count", true);
        check("phase", "partitioned_count", "schema_only", false);
        check("phase", "partitioned_count", "second_action", false);
        check("phase", "schema_only", "schema_only", true);
        check("phase", "schema_only", "partitioned_count", false);

        System.out.println("\n[4] 선언되지 않은 구간에는 주입하지 않는다");
        // 어느 step 에도 속하지 않는 connection 에 주입하면 무엇을 시험한 것인지 말할 수 없다.
        check("phase", "partitioned_count", "UNDECLARED", false);
        check("phase", "partitioned_count", "BETWEEN_STEPS", false);
        check("phase", "partitioned_count", null, false);

        System.out.println("\n[5] fail.phase 를 주지 않으면 fail-closed — 주입하지 않는다");
        // 지정하지 않았는데 아무 데나 주입하면 그 회차가 무엇을 시험했는지 알 수 없다.
        check("phase", null, "partitioned_count", false);
        check("phase", "", "partitioned_count", false);
        check("phase", "   ", "schema_only", false);

        System.out.println("\n[6] 대소문자·공백");
        check("phase", "PARTITIONED_COUNT", "partitioned_count", true);
        check("phase", "partitioned_count", "PARTITIONED_COUNT", true);
        check("  all  ", null, "schema_only", true);

        System.out.println("\n[7] **경로 이름은 더 이상 주입 키가 아니다**");
        // v1 의 `-Dg0b1.fail=schema|task` 는 분류기 결과와 대조되던 값이다. 그 형태를
        // 계속 받으면 스택 추정이 다시 actuator 로 새어 든다. 이제는 아무것도 하지 않는다.
        check("schema", null, "schema_only", false);
        check("task", null, "partitioned_count", false);
        check("schema,task", null, "schema_only", false);
        check("SCHEMA", null, "schema_only", false);

        System.out.println();
        System.out.println("통과 " + pass + "건 · 실패 " + fail + "건");
        if (fail > 0) System.exit(1);
    }
}
