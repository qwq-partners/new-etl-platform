package etl.g0b1;

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;

import org.apache.spark.sql.jdbc.JdbcConnectionProvider;

/**
 * G0-0B1 — Spark 가 여는 <b>모든</b> JDBC connection 을 가로채 세션 프리앰블을 걸고
 * 어느 경로에서 열렸는지 기록하는 tracer.
 *
 * <p><b>왜 필요한가.</b> {@code sessionInitStatement} 는 task 경로에서만 실행된다.
 * schema 해석·metadata 경로의 connection 은 그것을 실행하지 않으므로, 그 경로들은
 * 세션 단언(신원 확인·지연 상한) 밖에서 원천을 읽는다. Profile U 의 보장은 전부
 * "모든 물리 connection 이 프리앰블을 받는다"를 전제하므로, 그 전제가 성립하는지를
 * 실측하지 않으면 그 위의 보장이 전부 미확정이다.
 *
 * <p><b>이 클래스가 답해야 하는 질문 셋.</b>
 * <ol>
 *   <li>커스텀 provider 가 정말 세 경로 모두에서 호출되는가?</li>
 *   <li>프리앰블이 실패했을 때 job 이 정말 죽는가(fail-closed)? 아니면 어떤 경로는
 *       예외를 삼키고 계속 가는가?</li>
 *   <li>한 회차가 여는 물리 connection 은 실제로 몇 개이며 서버 SID 는 몇 개인가?</li>
 * </ol>
 *
 * <p><b>등록</b>: {@code META-INF/services/org.apache.spark.sql.jdbc.JdbcConnectionProvider}
 * <p><b>주의</b>: 내장 {@code BasicConnectionProvider} 도 같은 옵션을 claim 하므로
 * {@code spark.sql.sources.disabledJdbcConnProviderList=basic} 을 함께 줘야 한다.
 * 그러지 않으면 Spark 가 provider 중복으로 실패하거나 우리 것이 선택되지 않는다.
 */
public final class TracingConnectionProvider extends JdbcConnectionProvider {

    private static final String NAME = "g0b1tracer";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean canHandle(Driver driver, scala.collection.immutable.Map<String, String> options) {
        String url = opt(options, "url");
        return url != null && url.startsWith("jdbc:oracle:");
    }

    @Override
    public boolean modifiesSecurityContext(Driver driver, scala.collection.immutable.Map<String, String> options) {
        return false;
    }

    @Override
    public Connection getConnection(Driver driver, scala.collection.immutable.Map<String, String> options) {
        final String connId = UUID.randomUUID().toString();
        final StackTraceElement[] stack = new Throwable().getStackTrace();
        final String path = Trace.classify(stack);
        final String url = opt(options, "url");
        final long t0 = System.nanoTime();

        Connection c = null;
        String openError = null;
        Preamble.Result pr = null;
        String preambleError = null;

        try {
            Properties p = new Properties();
            String user = opt(options, "user");
            String pw = opt(options, "password");
            if (user != null) p.setProperty("user", user);
            if (pw != null) p.setProperty("password", pw);
            c = driver.connect(url, p);
            if (c == null) {
                openError = "driver.connect returned null (URL 을 이 driver 가 받지 않았다)";
            }
        } catch (SQLException e) {
            openError = e.getClass().getName() + ": " + e.getMessage();
        }

        if (c != null) {
            try {
                pr = Preamble.apply(c);
            } catch (SQLException | RuntimeException e) {
                preambleError = e.getClass().getName() + ": " + e.getMessage();
            }
        }

        // 기록은 던지기 **전에** 한다. 예외가 삼켜지든 말든 증거는 남아야 한다.
        emit(connId, path, stack, url, openError, pr, preambleError, t0);

        if (openError != null) {
            throw new RuntimeException("[g0-0b1] connection open 실패: " + openError);
        }
        if (preambleError != null) {
            // fail-closed. 이 예외가 어떤 경로에서 삼켜지는지가 관측 대상이다.
            try {
                c.close();
            } catch (SQLException ignored) {
                // 닫기 실패는 원인 예외를 가리지 않게 무시한다.
            }
            throw new RuntimeException("[g0-0b1] preamble 실패로 connection 거부: " + preambleError);
        }
        return c;
    }

    private static void emit(String connId, String path, StackTraceElement[] stack, String url,
                             String openError, Preamble.Result pr, String preambleError, long t0) {
        String jvm = ManagementFactory.getRuntimeMXBean().getName();
        StringBuilder b = new StringBuilder("{");
        b.append("\"event\":\"connection\"");
        b.append(",\"conn_id\":").append(Trace.q(connId));
        b.append(",\"path_guess\":").append(Trace.q(path));
        b.append(",\"jvm\":").append(Trace.q(jvm));
        b.append(",\"thread\":").append(Trace.q(Thread.currentThread().getName()));
        b.append(",\"url_host\":").append(Trace.q(hostOnly(url)));
        b.append(",\"open_error\":").append(Trace.q(openError));
        b.append(",\"preamble\":").append(Preamble.toJson(pr));
        b.append(",\"preamble_error\":").append(Trace.q(preambleError));
        b.append(",\"elapsed_ms\":").append((System.nanoTime() - t0) / 1000000L);
        b.append(",\"raw_stack\":").append(Trace.stackJson(stack, 18));
        b.append("}");
        Trace.line(b.toString());
    }

    /** URL 에 비밀번호가 섞여 오는 형태가 있으므로 호스트 부분만 남긴다. */
    private static String hostOnly(String url) {
        if (url == null) return null;
        int at = url.indexOf('@');
        return at >= 0 ? url.substring(at) : url;
    }

    /** Scala Map 에서 키를 대소문자 무시로 찾는다. Spark 판본마다 키 정규화가 다르다. */
    private static String opt(scala.collection.immutable.Map<String, String> m, String key) {
        if (m == null) return null;
        scala.Option<String> direct = m.get(key);
        if (direct != null && direct.isDefined()) return direct.get();
        scala.collection.Iterator<scala.Tuple2<String, String>> it = m.iterator();
        while (it.hasNext()) {
            scala.Tuple2<String, String> kv = it.next();
            if (kv._1() != null && kv._1().equalsIgnoreCase(key)) return kv._2();
        }
        return null;
    }
}
