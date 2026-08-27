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
 * {@code -Dg0b1.fail=all} 로 일부러 실패시켜 job 이 정말 죽는지 관측한다.
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
     * @throws SQLException 신원이 기대와 다르거나 강제 실패 모드일 때. 호출자는 이것을
     *                      삼키지 말고 그대로 올려 connection 을 실패시켜야 한다.
     */
    /**
     * @param path 이 connection 이 열린 Spark 경로(SCHEMA/TASK/METADATA/MIXED/UNKNOWN).
     *             경로별 주입에 쓴다.
     */
    static Result apply(Connection c, String path) throws SQLException {
        Result r = new Result();

        // ── 0. 강제 실패 모드 ────────────────────────────────────────────
        //   g0b1.fail = none | all | <경로 목록(쉼표 구분)>
        //   예: -Dg0b1.fail=schema  → schema 경로만 실패시킨다.
        //
        //   **왜 경로별이어야 하는가**: fail=all 은 **첫 provider 호출(대개 schema)에서
        //   즉시 던진다.** 그러면 task connection 이 아예 열리지 않고, 그 상태를
        //   "task 경로도 fail-closed 다" 로 읽으면 증명되지 않은 것을 증명됐다고 하는 것이다
        //   (7차 교차 리뷰 P0-06). schema 를 통과시키고 task 만 실패시켜야
        //   task 경로의 fail-closed 를 실제로 관측할 수 있다.
        String fail = prop("g0b1.fail", "none");
        if (!"none".equalsIgnoreCase(fail)) {
            boolean hit = "all".equalsIgnoreCase(fail);
            if (!hit && path != null) {
                for (String tok : fail.split(",")) {
                    if (tok.trim().equalsIgnoreCase(path)) { hit = true; break; }
                }
            }
            if (hit) {
                r.ok = false;
                r.error = "forced failure (g0b1.fail=" + fail + ", path=" + path + ")";
                throw new SQLException("[g0-0b1] 의도적 프리앰블 실패 (경로 " + path
                                       + ") — 이 예외가 삼켜지는지 관측한다");
            }
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
            s.execute("ALTER SESSION SET TIME_ZONE = DBTIMEZONE");
            // A 규범·G0-0A 와 같은 값이어야 한다. 다르면 B1 의 세션과 A 가 잰 세션이 다른 것이다.
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
