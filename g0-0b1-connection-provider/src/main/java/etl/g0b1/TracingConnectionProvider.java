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
            // Spark 의 BasicConnectionProvider 는 JDBCOptions.asConnectionProperties 로
            // **Spark 전용 키를 뺀 나머지 전부**를 드라이버에 넘긴다. user/password 만 넘기면
            // oracle.jdbc.* 같은 드라이버 속성이 조용히 사라져, 우리 provider 를 켜는 순간
            // 동작이 달라진다. 그래서 같은 규칙으로 복사한다.
            Properties p = driverProps(options);
            c = driver.connect(url, p);
            if (c == null) {
                openError = "driver.connect returned null (URL 을 이 driver 가 받지 않았다)";
            }
        } catch (SQLException e) {
            openError = e.getClass().getName() + ": " + e.getMessage();
        }

        // **주입 여부를 미리 확정해 추적에 남긴다.** 판정기가 "그 경로에 주입이 닿았는가" 를
        // preamble_error 존재로 **추정**하던 것을 사실로 바꾸기 위해서다(7차 리뷰 P0-06).
        // connection open 이 실패해 프리앰블에 도달조차 못 한 경우와, 주입 대상이 아니어서
        // 통과한 경우를 구분할 수 있어야 한다.
        // **선언된 phase 로 주입을 정한다**(8차 M2-3). path 는 아래 emit 의 진단 라벨로만 쓴다.
        final String declaredPhase = Trace.declaredPhase();
        final boolean injectionTarget = Preamble.shouldFail(declaredPhase);

        if (c != null) {
            try {
                pr = Preamble.apply(c, path);
            } catch (SQLException | RuntimeException e) {
                preambleError = e.getClass().getName() + ": " + e.getMessage();
            }
        }

        // 기록은 던지기 **전에** 한다. 예외가 삼켜지든 말든 증거는 남아야 한다.
        emit(connId, path, declaredPhase, stack, url, openError, pr, preambleError, t0, passedKeys(options),
             injectionTarget);

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

    private static void emit(String connId, String path, String declaredPhase,
                             StackTraceElement[] stack, String url,
                             String openError, Preamble.Result pr, String preambleError, long t0,
                             String passedKeysJson, boolean injectionTarget) {
        String jvm = ManagementFactory.getRuntimeMXBean().getName();
        StringBuilder b = new StringBuilder("{");
        b.append("\"event\":\"connection\"");
        b.append(",\"run\":").append(Trace.q(Trace.run()));
        b.append(",\"conn_id\":").append(Trace.q(connId));
        // **path_guess 는 진단 라벨이다**(8차 M2-3). 어떤 판정 술어에도 들어가지 않는다.
        // 판정에 쓰는 것은 아래 declared_phase 다 — driver 가 선언한 사실이다.
        b.append(",\"path_guess\":").append(Trace.q(path));
        b.append(",\"declared_phase\":").append(Trace.q(declaredPhase));
        b.append(",\"jvm\":").append(Trace.q(jvm));
        b.append(",\"thread\":").append(Trace.q(Thread.currentThread().getName()));
        b.append(",\"url_host\":").append(Trace.q(hostOnly(url)));
        b.append(",\"open_error\":").append(Trace.q(openError));
        b.append(",\"preamble\":").append(Preamble.toJson(pr));
        b.append(",\"preamble_error\":").append(Trace.q(preambleError));
        // fail_mode 는 이 회차의 주입 설정, injection_target 은 **이 connection 이 그 대상인가**.
        // 둘을 함께 남겨야 "주입이 이 경로에 닿았다" 를 판정기가 추정 없이 읽는다.
        b.append(",\"fail_mode\":").append(Trace.q(System.getProperty("g0b1.fail", "none")));
        b.append(",\"fail_phase\":").append(Trace.q(System.getProperty("g0b1.fail.phase", "")));
        b.append(",\"injection_target\":").append(injectionTarget);
        b.append(",\"injection_applied\":").append(injectionTarget && preambleError != null);
        b.append(",\"elapsed_ms\":").append((System.nanoTime() - t0) / 1000000L);
        b.append(",\"driver_props_passed\":").append(passedKeysJson);
        b.append(",\"raw_stack\":").append(Trace.stackJson(stack, 18));
        b.append("}");
        Trace.line(b.toString());
    }

    /**
     * Spark 전용 옵션 키. 이것만 빼고 나머지는 드라이버에 그대로 넘긴다.
     * <p><b>판본 의존</b>: Spark 가 옵션을 추가하면 이 목록도 늘어야 한다. 빠뜨리면
     * Spark 전용 키가 드라이버로 새어 드라이버가 거부할 수 있다. 실행 시 passed_keys 를
     * 확인하라 — 값은 남기지 않고 **키 이름만** 남긴다.
     */
    private static final java.util.Set<String> SPARK_ONLY = new java.util.HashSet<>(java.util.Arrays.asList(
            "url", "dbtable", "query", "driver", "partitioncolumn", "lowerbound", "upperbound",
            "numpartitions", "querytimeout", "fetchsize", "batchsize", "isolationlevel",
            "sessioninitstatement", "truncate", "cascadetruncate", "createtablecolumntypes",
            "createtableoptions", "customschema", "pushdownpredicate", "pushdownaggregate",
            "pushdownlimit", "pushdownoffset", "pushdowntablesample", "keytab", "principal",
            "refreshkrb5config", "connectionprovider", "prefertimestampntz", "tabletypes",
            "inferTimestampNTZType".toLowerCase()));

    private static Properties driverProps(scala.collection.immutable.Map<String, String> m) {
        Properties p = new Properties();
        if (m == null) return p;
        scala.collection.Iterator<scala.Tuple2<String, String>> it = m.iterator();
        while (it.hasNext()) {
            scala.Tuple2<String, String> kv = it.next();
            String k = kv._1();
            if (k == null || kv._2() == null) continue;
            if (SPARK_ONLY.contains(k.toLowerCase())) continue;
            p.setProperty(k, kv._2());
        }
        return p;
    }

    /** 드라이버에 넘긴 키 **이름만** 모은다. 값은 절대 남기지 않는다(비밀번호 포함). */
    private static String passedKeys(scala.collection.immutable.Map<String, String> m) {
        StringBuilder b = new StringBuilder("[");
        Properties p = driverProps(m);
        java.util.List<String> ks = new java.util.ArrayList<>(p.stringPropertyNames());
        java.util.Collections.sort(ks);
        for (int i = 0; i < ks.size(); i++) {
            if (i > 0) b.append(',');
            b.append(Trace.q(ks.get(i)));
        }
        return b.append(']').toString();
    }

    /** URL 에 비밀번호가 섞여 오는 형태가 있으므로 호스트 부분만 남긴다. */
    private static String hostOnly(String url) {
        if (url == null) return null;
        // **lastIndexOf** 다. 비밀번호에 '@' 가 들어 있으면 indexOf 는 비밀번호 잔여분을
        // 그대로 남긴다. Oracle thin URL 은 자격증명 다음의 마지막 '@' 가 호스트 구분자다.
        int at = url.lastIndexOf('@');
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
