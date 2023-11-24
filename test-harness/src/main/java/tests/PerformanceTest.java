package tests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.function.Consumer;
import java.util.logging.Logger;

public interface PerformanceTest {

    class Pair<T1, T2> {

        T1 fst;
        T2 snd;

        public Pair(T1 f, T2 s) {
            fst = f;
            snd = s;
        }
    }

    class TestResult {
        private final List<Long> resultCount = new ArrayList<>();
        private final List<Long> queryDuration = new ArrayList<>();
        private final List<Long> fetchDuration = new ArrayList<>();

        private final List<List<Object>> resultRows = new ArrayList<>();

        private Logger log() {
            return Logger.getLogger(getClass().getName());
        }

        public void store(ResultSet res) throws SQLException {
            ResultSetMetaData meta = res.getMetaData();
            List<Object> row = new ArrayList<>();
            for (int i = 0; i < meta.getColumnCount(); i++) {
                row.add(res.getObject(i + 1));
            }
            resultRows.add(row);
        }

        public void add(long resultCount, long fetchDuration, long queryDuration) {
            getResultCount().add(resultCount);
            getFetchDuration().add(fetchDuration);
            getQueryDuration().add(queryDuration);
        }

        public boolean compare(TestResult otherTestResult) {
            List<List<Object>> other = otherTestResult.resultRows;
            if (resultRows.size() != other.size()) {
                log().severe("[Different sizes] L: " + resultRows.size() + " / R: " + other.size());
                return false;
            }
            for (int i = 0; i < resultRows.size(); i++) {
                List<Object> left = resultRows.get(i);
                List<Object> right = other.get(i);
                if (left.size() != right.size()) {
                    log().severe("[Different sizes] L: " + left.size() + " / R: " + right.size());
                    return false;
                }
                for (int j = 0; j < left.size(); j++) {
                    if (!Objects.equals(left.get(j), right.get(j))) {
                        log().severe("[DIFF @ " + j + "] L: " + left + " / R: " + right);
                        return false;
                    }
                }
            }

            int random = (int) Math.floor(Math.random() * other.size());
            log().info("[MATCH] (Sample row) L: " + resultRows.get(random) + " / R: " + other.get(random));
            log().info("[MATCH] L: " + resultRows.size() + " / R: " + other.size());

            return true;
        }

        public List<Long> getResultCount() {
            return resultCount;
        }

        public List<Long> getQueryDuration() {
            return queryDuration;
        }

        public List<Long> getFetchDuration() {
            return fetchDuration;
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
            boolean countResult = !args.contains("--noCountResults");
            int threads = args.contains("-t") ? Integer.parseInt(args.get(args.indexOf("-t") + 1)) : 1;


            System.out.printf("Fetch size: %s\n", fetchSize);

            List<Pair<Thread, Execution>> executions = new ArrayList<>();

            try {
                for (int i = 0; i < threads; i++) {
                    Execution e = new Execution(getTestQuery(args), "jdbc:ignite:thin://" + args.get(0), fetchSize, testIterations, countResult, false);
                    Thread t = new Thread(e);
                    t.start();
                    executions.add(new Pair<>(t, e));
                    log().info("Started thread " + i);
                }

                StringBuilder res = new StringBuilder();
                res.append(String.format("Test Results:\n| %-5s| %-20s| %-20s| %-20s|\n", "ID", "Test Count", "Averge Query Time", "Average Fetch Time"));
                List<Double> queryTimes = new ArrayList<>();
                List<Double> fetchTimes = new ArrayList<>();

                for (int i = 0; i < threads; i++) {
                    Thread t = executions.get(i).fst;
                    Execution e = executions.get(i).snd;
                    t.join();
                    e.complete();

                    double fetchTime = e.avgFetchTime();
                    double queryTime = e.avgQueryTime();

                    fetchTimes.add(fetchTime);
                    queryTimes.add(queryTime);

                    res.append(String.format("| %-5s", i))
                        .append(String.format("| %-20s", e.avgTestCount()))
                        .append(String.format("| %-20s", queryTime))
                        .append(String.format("| %-20s|\n", fetchTime));
                }
                res.append(String.format("| %-5s", "Cum"))
                    .append(String.format("| %-20s", "NA"))
                    .append(String.format("| %-20s", queryTimes.stream().mapToDouble(Double::doubleValue).average().getAsDouble()))
                    .append(String.format("| %-20s|\n", fetchTimes.stream().mapToDouble(Double::doubleValue).average().getAsDouble()));

                log().info(res.toString());

            } catch (SQLException | InterruptedException e) {
                log().severe(e.getMessage());
            }

        }

        index = args.indexOf("--validate");
        if (index >= 0) {
            System.out.println("Starting validation...");

            String host1 = args.get(index + 1);
            String host2 = args.get(index + 2);


            List<Pair<Thread, Execution>> executions = new ArrayList<>();

            try {
                for (int i = 0; i < 2; i++) {
                    Execution e = new Execution(getTestQuery(args), "jdbc:ignite:thin://" + host1, 1024, 1, true, true);
                    Thread t = new Thread(e);
                    t.start();
                    executions.add(new Pair<>(t, e));
                    log().info("Started host1 thread " + i);
                }

                for (int i = 0; i < 2; i++) {
                    Execution e = new Execution(getTestQuery(args), "jdbc:ignite:thin://" + host2, 1024, 1, true, true);
                    Thread t = new Thread(e);
                    t.start();
                    executions.add(new Pair<>(t, e));
                    log().info("Started host2 thread " + i);
                }

                for (int i = 0; i < 4; i++) {
                    Thread t = executions.get(i).fst;
                    Execution e = executions.get(i).snd;
                    t.join();
                    e.complete();
                    log().info("Thread " + i + " complete.");
                }

                log().info("Threads complete, comparing results");

                TestResult t1 = executions.get(0).snd.result;
                TestResult t2 = executions.get(1).snd.result;
                TestResult t3 = executions.get(2).snd.result;
                TestResult t4 = executions.get(3).snd.result;

                if (!t1.compare(t2)) {
                    log().severe("Host1#1 != Host1#2");
                } else if (!t3.compare(t4)) {
                    log().severe("Host2#1 != Host2#2");
                } else if (!t1.compare(t3)) {
                    log().severe("Host1 != Host2");
                } else {
                    log().info("Validation Successful");
                }

            } catch (SQLException | InterruptedException e) {
                log().severe(e.getMessage());
            }
        }

    }

    default Logger log() {
        return Logger.getLogger(getClass().getName());
    }

    void create(Connection conn) throws SQLException;

    void populate(Connection conn, List<String> args) throws SQLException;

    default String getArgOrDefault(List<String> args, String finder, int offset, String def) {
        int index = args.indexOf(finder);
        if (index >= 0) return args.get(index + offset);
        else return def;
    }

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

    String getTestQuery(List<String> args);

    class Execution implements Runnable {

        private final int iterations;
        private final String query;
        private final int fetchSize;
        private final boolean countResult;
        private final Connection conn;

        private final boolean storeResults;
        TestResult result = new TestResult();


        public Execution(String query, String connUrl, int fetchSize, int iterations, boolean countResult, boolean storeResults) throws SQLException {
            this.query = query;
            this.fetchSize = fetchSize;
            this.countResult = countResult;
            this.iterations = iterations;
            this.storeResults = storeResults;
            conn = DriverManager.getConnection(connUrl);
        }

        public void complete() throws SQLException {
            conn.close();
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < iterations; i++) {
                    PreparedStatement stmt = conn.prepareStatement(query);
                    stmt.setFetchSize(fetchSize);
                    long start = new Date().getTime();
                    stmt.execute();
                    long queryDuration = new Date().getTime() - start;
                    ResultSet result = stmt.getResultSet();
                    int resultCount = 0;
                    if (countResult) {
                        while (result.next()) {
                            if (storeResults) this.result.store(result);
                            resultCount++;
                        }
                    }
                    long fetchDuration = new Date().getTime() - start - queryDuration;
                    this.result.add(resultCount, fetchDuration, queryDuration);
                    stmt.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public double avgTestCount() {
            OptionalDouble d = result.resultCount.stream().mapToLong(Long::longValue).average();
            return d.isPresent() ? d.getAsDouble() : 0D;
        }

        public double avgQueryTime() {
            OptionalDouble d = result.queryDuration.stream().mapToLong(Long::longValue).average();
            return d.isPresent() ? d.getAsDouble() : 0D;
        }

        public double avgFetchTime() {
            OptionalDouble d = result.fetchDuration.stream().mapToLong(Long::longValue).average();
            return d.isPresent() ? d.getAsDouble() : 0D;
        }
    }
}
