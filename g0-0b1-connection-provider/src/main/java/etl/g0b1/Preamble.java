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
 * <p><b>주입 대상은 driver 가 선언한 phase 로 정한다</b>(2026-08-30, 8차 M2-3).
 * v1 은 {@code Trace.classify(stack)} 의 추정 경로로 주입 여부를 정했다. 그러면
 * <b>분류기의 오류가 곧 잘못된 주입</b>이 되고, 그 주입 결과로 분류기를 검증할 수도 없다.
 * driver 는 자기가 {@code .schema} 를 부르는지 {@code .count()} 를 부르는지 알고 있으므로,
 * step 을 시작하기 전에 phase 파일에 그 이름을 쓰고 provider 는 그 값만 읽는다.
 *
 * <pre>
 *   -Dg0b1.fail=none                  주입 없음(기본)
 *   -Dg0b1.fail=all                   선언된 phase 와 무관하게 전부
 *   -Dg0b1.fail=phase
 *   -Dg0b1.fail.phase=&lt;phase 이름&gt;   그 phase 로 선언된 동안 열린 connection 만
 * </pre>
 *
 * <p>{@code fail=all} 은 <b>시나리오가 경로를 이미 격리했을 때</b> 쓴다(M2-4) — 예컨대
 * {@code schema_only} 시나리오에서는 그 회차의 모든 connection 이 schema 경로이므로
 * 분류기에 묻지 않고도 그것을 안다. {@code path_guess} 는 추적에 남지만 <b>진단 라벨일 뿐</b>
 * 이며 어떤 판정 술어에도 들어가지 않는다.
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
     * 이 connection 에 실패를 주입해야 하는가.
     *
     * <p>패키지 가시성으로 열어 둔 이유는 {@code TracingConnectionProvider} 가 <b>주입 여부를
     * 추적에 남겨야</b> 하기 때문이다. 판정기가 "주입이 닿았는가" 를 추정이 아니라 사실로
     * 읽을 수 있어야 한다.
     *
     * @param declaredPhase driver 가 phase 파일에 선언한 값. <b>스택 추정이 아니다.</b>
     */
    static boolean shouldFail(String declaredPhase) {
        String fail = prop("g0b1.fail", "none").trim();
        if (fail.isEmpty() || "none".equalsIgnoreCase(fail)) return false;

        // **주입 대상은 driver 가 선언한 phase 로 정한다**(2026-08-30, 8차 M2-3).
        // v1 은 Trace.classify(stack) 의 결과를 받아 판단했다 — 스택 추정이 actuator 를
        // 구동하면 분류기의 오류가 곧 잘못된 주입이 되고, 그 결과로 분류기를 검증할 수도
        // 없다(순환). driver 는 자기가 무엇을 하는 중인지 알고 있으므로 그것을 쓴다.
        //
        //   -Dg0b1.fail=none                주입 없음
        //   -Dg0b1.fail=all                 선언된 phase 와 무관하게 전부
        //   -Dg0b1.fail.phase=<phase 이름>   그 phase 로 선언된 동안 열린 connection 만
        //
        // fail=all 은 시나리오가 경로를 이미 격리했을 때 쓴다(M2-4). 예: schema_only
        // 시나리오에서 fail=all 이면 그 회차의 모든 connection 이 schema 경로다 —
        // **분류기에 묻지 않고도** 그것을 안다.
        if ("all".equalsIgnoreCase(fail)) return true;

        String want = prop("g0b1.fail.phase", "").trim();
        if (want.isEmpty()) return false;      // 지정하지 않았으면 주입하지 않는다(fail-closed)
        return want.equalsIgnoreCase(declaredPhase);
    }

    /**
     * @throws SQLException 신원이 기대와 다르거나 강제 실패 모드일 때. 호출자는 이것을
     *                      삼키지 말고 그대로 올려 connection 을 실패시켜야 한다.
     */
    static Result apply(Connection c, String path) throws SQLException {
        Result r = new Result();

        // ── 0. 강제 실패 모드 — fail-closed 가 정말 성립하는지 보는 실험용 ──────
        // **`path` 는 여기서 쓰이지 않는다.** 진단 라벨로 로그에만 남는다(M2-3).
        String phase = Trace.declaredPhase();
        if (shouldFail(phase)) {
            String fail = prop("g0b1.fail", "none");
            r.ok = false;
            r.error = "forced failure (g0b1.fail=" + fail
                    + ", fail.phase=" + prop("g0b1.fail.phase", "")
                    + ", declared_phase=" + phase + ")";
            throw new SQLException("[g0-0b1] 의도적 프리앰블 실패 (declared_phase=" + phase
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
