package tests;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Joins salesmen table with orders table. Approx k * (n/s) results where k=#orders, s=#salesmen, n=#salesmen in WHERE clause
 * Call args:
 * --create
 * --load (table1 size) (batches) [% bucketId : for i in batches] (table2 size) (batches) [% bucketId : for i in batches]
 * Note: batches == 0 => uniform data distribution
 * Sample: --load 6000 2 0.25 1 0.75 4 6000 3 0.5 1 0.25 0 0.25 5
 * Sample: --load 10000 0 10000 0
 * --run (test count) (fetch size) [--countResult]
 * Default fetch size is 1024
 * Sample: 1 10000 --countResult
 */
public class SalesmenPerformanceTest1 implements PerformanceTest {

    public static void main(String[] args) throws ClassNotFoundException {
        new SalesmenPerformanceTest1().run(args);
    }

    @Override
    public PreparedStatement getTestQuery(Connection conn) throws SQLException {
        return conn.prepareStatement("SELECT * FROM table1 INNER JOIN table2 ON table1.id = table2.salesmenId " +
            "WHERE table2.salesmenId in (1,2,3,4,5)");
    }

    @Override
    public void create(Connection conn) throws SQLException {
        PreparedStatement a = conn.prepareStatement("DROP TABLE IF EXISTS table1; CREATE TABLE table1 (id int, cacheKey int, PRIMARY KEY (id, cacheKey)) WITH \"TEMPLATE=CacheA,AFFINITY_KEY=cacheKey,CACHE_NAME=CacheA\"");
        a.execute();

        PreparedStatement b = conn.prepareStatement("DROP TABLE IF EXISTS table2; CREATE TABLE table2 (id int, cacheKey int, salesmenId int, PRIMARY KEY (id, cacheKey)) WITH \"TEMPLATE=CacheB,AFFINITY_KEY=cacheKey,CACHE_NAME=CacheB\"");
        b.execute();
        System.out.println("Tables created.");
    }

    public List<Bucket> getBuckets(List<String> args) {
        List<Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < args.size() / 2; i++) {
            buckets.add(new Bucket(Double.parseDouble(args.get(2 * i)), Double.parseDouble(args.get(2 * i + 1))));
        }
        if (buckets.isEmpty()) buckets.add(new Bucket(1, -1));
        return buckets;
    }

    @Override
    public void populate(Connection conn, List<String> args) throws SQLException {
        int countA = Integer.parseInt(args.get(0));
        int totalABuckets = Integer.parseInt(args.get(1)) * 2;
        List<Bucket> aBuckets = getBuckets(args.subList(2, 2 + totalABuckets));

        int countB = Integer.parseInt(args.get(2 + totalABuckets));
        int totalBBuckets = Integer.parseInt(args.get(3 + totalABuckets)) * 2;
        List<Bucket> bBuckets = getBuckets(args.subList(4, 4 + totalBBuckets));

        System.out.printf("Starting data loading. Table 1: %s / %s, Table 2: %s / %s\n", countA, aBuckets, countB, bBuckets);
        conn.prepareStatement("DELETE FROM table1").execute();
        conn.prepareStatement("DELETE FROM table2").execute();

        PreparedStatement a = conn.prepareStatement("INSERT into table1 (id, cacheKey) VALUES (?, ?)");
        populateTable(countA, aBuckets, a, stmt -> {});

        PreparedStatement b = conn.prepareStatement("INSERT into table2 (id, cacheKey, salesmenId) VALUES (?, ?, ?)");
        populateTable(countB, bBuckets, b, stmt -> {
            try {
                stmt.setInt(3, (int) Math.floor(Math.random() * countA));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        PreparedStatement check = conn.prepareStatement("SELECT count(*) from table1;");
        check.execute();
        ResultSet set = check.getResultSet();
        if (set.next()) System.out.println("Table1 Count: " + set.getInt(1));

        check = conn.prepareStatement("SELECT count(*) from table2;");
        check.execute();
        set = check.getResultSet();
        if (set.next()) System.out.println("Table2 Count: " + set.getInt(1));
    }

}
