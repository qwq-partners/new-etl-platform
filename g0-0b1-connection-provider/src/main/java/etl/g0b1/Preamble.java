package etl.g0b1;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 모든 물리 connection 에 거는 세션 단언.
 *
 * <p>Profile U 는 "읽고 나서 판단한다"가 아니라 <b>"단언하고 서버가 거절하게 한다"</b> 로
 * 간다. 그래서 이 프리앰블은 값을 조회해 로그에 남기는 것이 아니라, 기대와 다르면
 * <b>예외를 던져 connection 자체를 실패시킨다</b>(fail-closed).
 *
 * <p>여기서 fail-closed 가 실제로 성립하는지가 G0-0B1 의 핵심 질문이다. Spark 의 어떤
 * 경로는 connection 생성 예외를 삼킬 수 있고, 그러면 그 경로는 fence 밖에서 원천을 읽는다.
 * {@code -Dg0b1.fail=…} 로 일부러 실패시켜 job 이 정말 죽는지 관측한다.
 *
 * <p><b>경로별 주입</b>(2026-08-27, 7차 교차 리뷰 P0-06). {@code fail=all} 하나뿐이던 것을
 * 경로별로 나눴다. 이유는 이렇다 — {@code all} 은 provider 가 <b>처음 불린</b> connection 에서
 * 즉시 던지므로, 각 step 이 schema 해석에서 막혀 <b>task connection 을 아예 열지 못한다.</b>
 * 그 회차의 "전 step 이 실패했다" 는 task 경로의 fail-closed 에 대해 아무것도 말하지 않는다.
 * {@code fail=task} 로 두면 schema 는 정상 통과하고 task 에서만 던지므로 그 경로를 독립적으로
 * 시험할 수 있다.
 *
 * <pre>
 *   -Dg0b1.fail=none              주입 없음(기본)
 *   -Dg0b1.fail=all               모든 경로
 *   -Dg0b1.fail=schema            SCHEMA 경로만
 *   -Dg0b1.fail=task              TASK 경로만
 *   -Dg0b1.fail=metadata          METADATA 경로만(이 하네스는 유발하지 않는다)
 *   -Dg0b1.fail=schema,task       조합
 * </pre>
 *
 * <p><b>MIXED·UNKNOWN 에는 주입하지 않는다</b>(경로를 지정한 경우). 분류기가 갈피를 못 잡은
 * connection 에 주입하면 어느 경로를 시험한 것인지 말할 수 없다. {@code all} 은 예외다.
 */
final class Preamble {

    static final class Result {
        String dbUniqueName, databaseRole, instanceName, sid, serverTime, sessionTz;
        boolean ok;
        String error;
    }

    private Preamble() {}

    private static String prop(String k, String dflt) {
        String v = System.getProperty(k);
        return (v == null || v.isEmpty()) ? dflt : v;
    }

    /**
     * 이 connection 의 경로에 실패를 주입해야 하는가.
     *
     * <p>패키지 가시성으로 열어 둔 이유는 {@code TracingConnectionProvider} 가 <b>주입 여부를
     * 추적에 남겨야</b> 하기 때문이다. 판정기가 "주입이 그 경로에 닿았는가" 를 추정이 아니라
     * 사실로 읽을 수 있어야 한다.
     *
     * @param path {@code Trace.classify} 의 결과. SCHEMA / TASK / METADATA / MIXED / UNKNOWN
     */
    static boolean shouldFail(String path) {
        String fail = prop("g0b1.fail", "none").trim();
        if (fail.isEmpty() || "none".equalsIgnoreCase(fail)) return false;
        if ("all".equalsIgnoreCase(fail)) return true;
        if (path == null) return false;
        // 경로를 지정한 경우 MIXED·UNKNOWN 에는 주입하지 않는다 — 어느 경로를 시험한 것인지
        // 말할 수 없는 주입은 증거를 만들지 못한다.
        if (!"SCHEMA".equals(path) && !"TASK".equals(path) && !"METADATA".equals(path)) return false;
        for (String tok : fail.split(",")) {
            if (tok.trim().equalsIgnoreCase(path)) return true;
        }
        return false;
    }

    /**
     * @throws SQLException 신원이 기대와 다르거나 강제 실패 모드일 때. 호출자는 이것을
     *                      삼키지 말고 그대로 올려 connection 을 실패시켜야 한다.
     */
    static Result apply(Connection c, String path) throws SQLException {
        Result r = new Result();

        // ── 0. 강제 실패 모드 — fail-closed 가 정말 성립하는지 보는 실험용 ──────
        if (shouldFail(path)) {
            String fail = prop("g0b1.fail", "none");
            r.ok = false;
            r.error = "forced failure (g0b1.fail=" + fail + ", path=" + path + ")";
            throw new SQLException("[g0-0b1] 의도적 프리앰블 실패 (path=" + path
                    + ") — 이 예외가 삼켜지는지 관측한다");
        }

        // ── 1. 신원을 서버에게 묻는다 ───────────────────────────────────────
        int timeoutS = Integer.parseInt(prop("g0b1.stmt.timeout", "15"));
        try (Statement s = c.createStatement()) {
            // 프리앰블 전용 타임아웃. Spark 의 queryTimeout 옵션은 여기에 적용되지 않으므로
            // 걸지 않으면 프리앰블이 무한히 매달려 connection 생성 자체가 멈춘다.
            s.setQueryTimeout(timeoutS);
            try (ResultSet rs = s.executeQuery(
                     "SELECT SYS_CONTEXT('USERENV','DB_UNIQUE_NAME'),"
                   + "       SYS_CONTEXT('USERENV','DATABASE_ROLE'),"
                   + "       SYS_CONTEXT('USERENV','INSTANCE_NAME'),"
                   + "       SYS_CONTEXT('USERENV','SID'),"
                   + "       TO_CHAR(SYSTIMESTAMP,'YYYY-MM-DD\"T\"HH24:MI:SS.FF6') FROM DUAL")) {
                if (rs.next()) {
                    r.dbUniqueName = rs.getString(1);
                    r.databaseRole = rs.getString(2);
                    r.instanceName = rs.getString(3);
                    r.sid          = rs.getString(4);
                    r.serverTime   = rs.getString(5);
                }
            }
        }

        // ── 2. 단언. 어긋나면 던진다 — 이것이 이 파일의 존재 이유다 ──────────
        String expectDb   = prop("g0b1.expect.dbuname", null);
        // run.sh 는 공백이 JVM 인자에서 잘리는 것을 피하려고 'PHYSICAL_STANDBY' 형태로 넘긴다.
        // 여기서 '_' 를 공백으로 되돌려 Oracle 이 돌려주는 'PHYSICAL STANDBY' 와 맞춘다.
        String expectRole = prop("g0b1.expect.role", null);
        if (expectRole != null) expectRole = expectRole.replace('_', ' ').trim();
        if (expectDb != null && !expectDb.equalsIgnoreCase(String.valueOf(r.dbUniqueName))) {
            r.ok = false;
            r.error = "DB_UNIQUE_NAME=" + r.dbUniqueName + " != expected " + expectDb;
            throw new SQLException("[g0-0b1] " + r.error + " — 대상 DB 가 아니다. 읽지 않는다.");
        }
        if (expectRole != null && !expectRole.equalsIgnoreCase(String.valueOf(r.databaseRole))) {
            r.ok = false;
            r.error = "DATABASE_ROLE=" + r.databaseRole + " != expected " + expectRole;
            throw new SQLException("[g0-0b1] " + r.error);
        }

        // ── 3. 세션 고정. 시간 축과 지연 상한. ──────────────────────────────
        //     주의: STANDBY_MAX_DATA_DELAY 는 **쿼리 시작 시점에만** 평가된다.
        //     오래 도는 추출은 이것으로 self-fail 하지 않는다(권한 판정서 §2 참조).
        try (Statement s = c.createStatement()) {
            s.setQueryTimeout(timeoutS);
            // **'+00:00' 이지 DBTIMEZONE 이 아니다**(2026-08-30 정정). A §11.3 의
            // sessionInitStatement 가 `TIME_ZONE = '+00:00'` 을 고정하고(P §3.2 TIMESTAMP
            // 실행 규격 ⑤), G0-0A(:136)·G0-0B0(:129,:147) 도 그 값이다. **여기만
            // DBTIMEZONE 이었다.**
            //
            // DBTIMEZONE 은 원천 DB 를 만들 때 정해진 값이라 무엇인지 모른다 — A 는 그것을
            // 등록조차 하지 않는다(§6.1 capability 목록의 'DB 시간대' 는 필드 이름이 없는
            // 산문이고 §22-4 의 미결이다). 그러면 B1 이 재는 세션이 규범이 규정한 세션이
            // 아니게 되고, B1 통과가 규범 세션의 성립을 시사하지 못한다.
            //
            // NLS_NUMERIC_CHARACTERS 의 '. ' → '.,' 와 **정확히 같은 종류의 결함**이다.
            // 7차 리뷰 P0-06 조치 때 NUMBER 축만 보고 TIMESTAMP 축을 놓쳤다.
            // 규범이 세션 값을 고정하는 이유는 canonical row hash 재현성이다(A §12.3).
            s.execute("ALTER SESSION SET TIME_ZONE = '+00:00'");
            // **'.,' 다.** A(§6.1 세션 프리앰블)·P(§3.2 NUMBER 실행 규격 ⑥ 로케일 차단)·G0-0A
            // ·G0-0B0 가 모두 '.,' 를 고정한다. 여기만 '. '(그룹 구분자 공백)였다 —
            // Oracle 이 받는 유효한 값이라 조용히 달랐고, 그러면 B1 이 재는 세션이 규범이
            // 규정한 세션이 아니게 되어 B1 통과가 규범 세션의 성립을 시사하지 못한다.
            // 7차 교차 리뷰 P0-06 에서 지적, 2026-08-27 검토서에서 확정.
            s.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'");
            String d = prop("g0b1.max.delay", null);
            if (d != null) {
                s.execute("ALTER SESSION SET STANDBY_MAX_DATA_DELAY = " + Integer.parseInt(d));
            }
        }
        // **적용 후** 의 세션 타임존을 읽는다. 적용 전 값을 증거로 남기면
        // "프리앰블이 시간축을 고정했다" 는 주장을 뒷받침하지 못한다.
        try (Statement s = c.createStatement()) {
            s.setQueryTimeout(timeoutS);
            try (ResultSet rs = s.executeQuery("SELECT SESSIONTIMEZONE FROM DUAL")) {
                if (rs.next()) r.sessionTz = rs.getString(1);
            }
        }
        r.ok = true;
        return r;
    }

    static String toJson(Result r) {
        if (r == null) return "null";
        return "{\"ok\":" + r.ok
             + ",\"db_unique_name\":" + Trace.q(r.dbUniqueName)
             + ",\"database_role\":" + Trace.q(r.databaseRole)
             + ",\"instance_name\":" + Trace.q(r.instanceName)
             + ",\"sid\":" + Trace.q(r.sid)
             + ",\"server_time\":" + Trace.q(r.serverTime)
             + ",\"session_tz\":" + Trace.q(r.sessionTz)
             + ",\"error\":" + Trace.q(r.error) + "}";
    }
}
