package tests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;

public interface PerformanceTest {

    class TestResult {
        public final long resultCount;
        public final long queryDuration;
        public final long fetchDuration;

        public TestResult(long resultCount, long fetchDuration, long queryDuration) {
            this.resultCount = resultCount;
            this.queryDuration = queryDuration;
            this.fetchDuration = fetchDuration;
        }
    }

    class Bucket {
        private final double percentage;
        private final int id;

        Bucket(double percentage, double id) {
            this.percentage = percentage;
            this.id = (int) id;
        }

        public int bucketId() {
            return id >= 0 ? id : (int) Math.floor(Math.random() * 2048);
        }

        public double bucketTotal(int total) {
            return total * percentage;
        }

        @Override
        public String toString() {
            return percentage + "/" + id;
        }

    }

    default void run(String _args[]) throws ClassNotFoundException {
        List<String> args = Arrays.asList(_args);

        // Register JDBC driver.
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        System.out.format("Running with args: %s\n", String.join(" ", args));

        int index = args.indexOf("--create");
        if (index >= 0) {
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + args.get(0))) {
                create(conn);
            } catch (SQLException e) {
                Logger.getLogger(PerformanceTest.class.getName()).severe(e.getMessage());
                e.printStackTrace();
            }
        }

        index = args.indexOf("--load");
        if (index >= 0) {
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + args.get(0))) {
                populate(conn, args.subList(index + 1, args.size()));
                System.out.println("Completed data loading");
            } catch (SQLException e) {
                Logger.getLogger(PerformanceTest.class.getName()).severe(e.getMessage());
                e.printStackTrace();
            }

        }

        index = args.indexOf("--run");
        if (index >= 0) {
            System.out.println("Starting tests...");
            int testIterations = Integer.parseInt(args.get(index + 1));
            int fetchSize = Integer.parseInt(args.get(index + 2));
            boolean countResult = args.contains("--countResult");

            ArrayList<Long> queryDurations = new ArrayList<>();
            ArrayList<Long> fetchDurations = new ArrayList<>();

            System.out.printf("Fetch size: %s\n", fetchSize);

            for (int i = 0; i < testIterations; i++) {
                try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + args.get(0))) {
                    TestResult result = execute(conn, testIterations, fetchSize, countResult);
                    System.out.format("[Test %s/%s] Result count %s; Query Duration %s", i + 1, testIterations, result.resultCount, result.queryDuration);
                    if (countResult) System.out.format(", Fetch duration %s", result.fetchDuration);
                    System.out.format("\n");
                    queryDurations.add(result.queryDuration);
                    if (countResult) fetchDurations.add(result.fetchDuration);
                } catch (SQLException e) {
                    Logger.getLogger(PerformanceTest.class.getName()).severe(e.getMessage());
                }
            }

            System.out.format("Tests completed. Average duration: %s", queryDurations.stream().mapToLong(Long::longValue).average().getAsDouble());
            if (countResult) System.out.format(", Average fetch duration: %s", fetchDurations.stream().mapToLong(Long::longValue).average().getAsDouble());
            System.out.format("\n");
        }
    }

    void create(Connection conn) throws SQLException;

    void populate(Connection conn, List<String> args) throws SQLException;

    default void populateTable(int count, List<Bucket> buckets, PreparedStatement stmt, Consumer<PreparedStatement> setParams) throws SQLException {
        int id = 0;
        for (Bucket bucket : buckets) {
            double bucketTotal = Math.ceil(bucket.bucketTotal(count));
            for (int i = 0; i < bucketTotal; i++) {
                stmt.setInt(1, id++);
                stmt.setInt(2, bucket.bucketId());
                setParams.accept(stmt);
                stmt.addBatch();
                if (i % 10000 == 0) stmt.executeBatch();
            }
            stmt.executeBatch();
        }
    }

    PreparedStatement getTestQuery(Connection conn) throws SQLException;

    default TestResult execute(Connection conn, int count, int fetchSize, boolean countResult) throws SQLException {
        PreparedStatement stmt = getTestQuery(conn);
        stmt.setFetchSize(fetchSize);
        long start = new Date().getTime();
        stmt.execute();
        long queryDuration = new Date().getTime() - start;
        ResultSet result = stmt.getResultSet();
        int resultCount = 0;
        if (countResult) {
            while (result.next()) {
                resultCount++;
            }
        }
        long fetchDuration = new Date().getTime() - start - queryDuration;
        return new TestResult(resultCount, queryDuration, fetchDuration);
    }
}
