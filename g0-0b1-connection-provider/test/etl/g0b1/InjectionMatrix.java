package etl.g0b1;

/**
 * `Preamble.shouldFail` 경로별 주입 매트릭스 시험 (7차 교차 리뷰 P0-06, 조치 5).
 *
 * <p>이전에는 {@code fail=all} 하나뿐이라 task 경로의 fail-closed 를 독립적으로 시험할 수
 * 없었다. {@code all} 은 provider 가 처음 불린 connection 에서 던지므로 각 step 이 schema
 * 해석에서 막혀 <b>task connection 을 열지 못한다.</b>
 *
 * <p>실행: {@code ./run-tests.sh}
 */
public final class InjectionMatrix {

    private static int pass = 0;
    private static int fail = 0;

    private static void check(String failMode, String path, boolean want) {
        String prev = System.getProperty("g0b1.fail");
        if (failMode == null) System.clearProperty("g0b1.fail");
        else System.setProperty("g0b1.fail", failMode);
        boolean got = Preamble.shouldFail(path);
        if (prev == null) System.clearProperty("g0b1.fail");
        else System.setProperty("g0b1.fail", prev);

        String label = String.format("fail=%-13s path=%-9s -> %s",
                String.valueOf(failMode), String.valueOf(path), want);
        if (got == want) {
            pass++;
            System.out.println("  PASS  " + label);
        } else {
            fail++;
            System.out.println("  FAIL  " + label + "   (got " + got + ")");
        }
    }

    public static void main(String[] args) {
        System.out.println("Preamble.shouldFail 매트릭스");

        System.out.println("\n[1] 기본은 주입 없음");
        check(null, "SCHEMA", false);
        check("none", "SCHEMA", false);
        check("", "TASK", false);

        System.out.println("\n[2] all 은 전 경로 — MIXED·UNKNOWN 포함");
        for (String p : new String[]{"SCHEMA", "TASK", "METADATA", "MIXED", "UNKNOWN"}) {
            check("all", p, true);
        }

        System.out.println("\n[3] 경로 지정 — 그 경로만");
        check("schema", "SCHEMA", true);
        check("schema", "TASK", false);
        check("task", "TASK", true);
        check("task", "SCHEMA", false);
        check("metadata", "METADATA", true);
        check("metadata", "SCHEMA", false);

        System.out.println("\n[4] 경로를 지정하면 MIXED·UNKNOWN 에는 주입하지 않는다");
        System.out.println("     (어느 경로를 시험한 것인지 말할 수 없는 주입은 증거가 못 된다)");
        check("schema", "MIXED", false);
        check("task", "MIXED", false);
        check("schema", "UNKNOWN", false);
        check("task", "UNKNOWN", false);
        check("schema,task", "MIXED", false);

        System.out.println("\n[5] 조합·대소문자·공백");
        check("schema,task", "SCHEMA", true);
        check("schema,task", "TASK", true);
        check("schema,task", "METADATA", false);
        check("SCHEMA", "SCHEMA", true);
        check(" task , schema ", "TASK", true);

        System.out.println("\n[6] null 경로는 주입 대상이 아니다");
        check("schema", null, false);
        check("all", null, true);

        System.out.println("\n통과 " + pass + "건 · 실패 " + fail + "건");
        if (fail > 0) System.exit(1);
    }
}
