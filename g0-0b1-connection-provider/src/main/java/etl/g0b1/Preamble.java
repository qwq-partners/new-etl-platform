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
    static Result apply(Connection c) throws SQLException {
        Result r = new Result();

        // ── 0. 강제 실패 모드 — fail-closed 가 정말 성립하는지 보는 실험용 ──────
        String fail = prop("g0b1.fail", "none");
        if ("all".equalsIgnoreCase(fail)) {
            r.ok = false;
            r.error = "forced failure (g0b1.fail=all)";
            throw new SQLException("[g0-0b1] 의도적 프리앰블 실패 — 이 예외가 삼켜지는지 관측한다");
        }

        // ── 1. 신원을 서버에게 묻는다 ───────────────────────────────────────
        try (Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT SYS_CONTEXT('USERENV','DB_UNIQUE_NAME'),"
                   + "       SYS_CONTEXT('USERENV','DATABASE_ROLE'),"
                   + "       SYS_CONTEXT('USERENV','INSTANCE_NAME'),"
                   + "       SYS_CONTEXT('USERENV','SID'),"
                   + "       TO_CHAR(SYSTIMESTAMP,'YYYY-MM-DD\"T\"HH24:MI:SS.FF6'),"
                   + "       SESSIONTIMEZONE FROM DUAL")) {
            if (rs.next()) {
                r.dbUniqueName = rs.getString(1);
                r.databaseRole = rs.getString(2);
                r.instanceName = rs.getString(3);
                r.sid          = rs.getString(4);
                r.serverTime   = rs.getString(5);
                r.sessionTz    = rs.getString(6);
            }
        }

        // ── 2. 단언. 어긋나면 던진다 — 이것이 이 파일의 존재 이유다 ──────────
        String expectDb   = prop("g0b1.expect.dbuname", null);
        String expectRole = prop("g0b1.expect.role", null);
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
            s.execute("ALTER SESSION SET TIME_ZONE = DBTIMEZONE");
            s.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '. '");
            String d = prop("g0b1.max.delay", null);
            if (d != null) {
                s.execute("ALTER SESSION SET STANDBY_MAX_DATA_DELAY = " + Integer.parseInt(d));
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
