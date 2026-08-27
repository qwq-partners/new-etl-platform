package etl.g0b1;

import java.io.IOException;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * G0-0B1 추적 기록기.
 *
 * <p>executor 는 별도 JVM 이므로 stdout 은 모으기 어렵다. JVM 마다 파일 하나를 열고
 * JSON Lines 로 append 한다. 드라이버 스크립트가 그 디렉터리를 훑어 합친다.
 *
 * <p><b>여기서 무엇을 증명하려 하는지</b>: Spark 는 JDBC 연결을 여러 경로로 연다
 * (schema 해석 / metadata / task). {@code sessionInitStatement} 는 그중 task 경로에서만
 * 실행된다. 커스텀 provider 가 <b>세 경로를 모두</b> 덮는지가 Profile U 세션 단언 모델의
 * 성립 조건이고, 이 파일은 그 판정에 쓸 원자료를 남긴다.
 *
 * <p><b>스택은 분류하지 않고 원문을 남긴다.</b> classify() 의 결과는 편의값일 뿐이며,
 * 판정은 raw_stack 을 사람이 보고 한다. 분류기가 틀려도 증거는 남아야 한다.
 */
final class Trace {
    private static final Object LOCK = new Object();
    private static Path file;

    private Trace() {}

    static synchronized Path file() {
        if (file == null) {
            String dir = System.getProperty("g0b1.trace.dir", System.getProperty("java.io.tmpdir"));
            String jvm = ManagementFactory.getRuntimeMXBean().getName().replaceAll("[^A-Za-z0-9_.@-]", "_");
            // **회차(mode)를 파일명에 넣는다.** coverage 와 failclosed 가 같은 디렉터리에
            // 섞이면 failclosed 의 의도된 실패가 coverage 통계로 합산되어, 완벽한 실행도
            // 영원히 NOT_PROVEN 이 된다.
            file = Paths.get(dir, "g0-0b1-trace-" + run() + "-" + jvm + ".jsonl");
            try {
                Files.createDirectories(file.getParent());
            } catch (IOException ignored) {
                // 디렉터리 생성 실패는 아래 write 에서 다시 드러난다.
            }
        }
        return file;
    }

    /** 이 JVM 이 참여한 실행 회차 이름. run.sh 가 -Dg0b1.run 으로 준다. */
    static String run() {
        String r = System.getProperty("g0b1.run", "unspecified");
        return r.replaceAll("[^A-Za-z0-9_.-]", "_");
    }

    static void line(String json) {
        synchronized (LOCK) {
            try (Writer w = Files.newBufferedWriter(file(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                w.write(json);
                w.write('\n');
            } catch (IOException e) {
                // 추적 실패가 실험을 죽이면 안 되지만 조용히 넘어가서도 안 된다.
                System.err.println("[g0-0b1] trace write failed: " + e);
            }
        }
    }

    /** JSON 문자열 이스케이프. 의존성을 늘리지 않으려고 직접 쓴다. */
    static String q(String s) {
        if (s == null) return "null";
        StringBuilder b = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':  b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n");  break;
                case '\r': b.append("\\r");  break;
                case '\t': b.append("\\t");  break;
                default:
                    if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
                    else b.append(c);
            }
        }
        return b.append('"').toString();
    }

    /**
     * 호출 스택을 Spark 경로로 <b>추정</b>한다. 확정이 아니다 — raw_stack 이 권위다.
     *
     * @return SCHEMA / TASK / METADATA / MIXED / UNKNOWN
     */
    static String classify(StackTraceElement[] st) {
        boolean schema = false, task = false, meta = false;
        for (StackTraceElement e : st) {
            String c = e.getClassName();
            String m = e.getMethodName();
            if (c.contains("JDBCRDD") && "compute".equals(m)) task = true;
            if (c.contains("org.apache.spark.executor.Executor")) task = true;
            if (c.contains("JDBCRDD") && "resolveTable".equals(m)) schema = true;
            if (c.contains("JdbcUtils") && (m.contains("getSchemaOption") || m.contains("getQueryOutputSchema")
                    || m.contains("resolveTable"))) schema = true;
            if (c.contains("JDBCRelation")) schema = true;
            if (c.contains("JDBCTableCatalog") || c.contains("JDBCTable")) meta = true;
            if (c.toLowerCase().contains("databasemetadata")) meta = true;
        }
        int n = (schema ? 1 : 0) + (task ? 1 : 0) + (meta ? 1 : 0);
        if (n > 1) return "MIXED";
        if (task) return "TASK";
        if (schema) return "SCHEMA";
        if (meta) return "METADATA";
        return "UNKNOWN";
    }

    /** 상위 N 프레임을 JSON 배열 문자열로. 분류를 사람이 재판정할 수 있게 남긴다. */
    static String stackJson(StackTraceElement[] st, int n) {
        StringBuilder b = new StringBuilder("[");
        int c = 0;
        for (StackTraceElement e : st) {
            String cn = e.getClassName();
            if (cn.startsWith("etl.g0b1.")) continue;   // 자기 자신 프레임은 뺀다
            if (c > 0) b.append(',');
            b.append(q(cn + "." + e.getMethodName()));
            if (++c >= n) break;
        }
        return b.append(']').toString();
    }
}
