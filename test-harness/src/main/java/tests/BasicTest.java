package tests;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Does a basic cross product of random IDs, upper bounded NxN rows.
 * Call args:
 * --create
 * --load (table1 size) (batches) [% bucketId : for i in batches] (table2 size) (batches) [% bucketId : for i in batches] [--maxId max]
 * Note: batches == 0 => uniform data distribution
 * Sample: --load 6000 2 0.25 1 0.75 4 6000 3 0.5 1 0.25 0 0.25 5
 * Sample: --load 10000 0 10000 0
 * --run (test count) (fetch size) [--countResult] -t [thread count]
 * Default fetch size is 1024
 * Sample: 1 10000 --countResult -t 4
 */
public class BasicTest implements PerformanceTest {

    public static void main(String[] args) throws ClassNotFoundException {
        new BasicTest().run(args);
    }

    @Override
    public String getTestQuery(List<String> args) {
        String join = "INNER JOIN";

        if (args.contains("--leftJoin")) join = "LEFT JOIN";
        else if (args.contains("--rightJoin")) join = "RIGHT JOIN";
        else if (args.contains("--fullJoin")) join = "FULL JOIN";

        String qry = "SELECT * FROM table1 " + join + " table2 ON table1.joinKey = table2.joinKey";
        if (args.contains("--sort")) qry += " ORDER BY table1.id,table2.id ASC";
        return qry;
    }

    @Override
    public void create(Connection conn) throws SQLException {
        PreparedStatement a = conn.prepareStatement("DROP TABLE IF EXISTS table1; CREATE TABLE table1 (id int, cacheKey int, joinKey int, sid int, PRIMARY KEY (id, cacheKey)) WITH \"TEMPLATE=CacheA,AFFINITY_KEY=cacheKey,CACHE_NAME=CacheA\"");
        a.execute();

        PreparedStatement b = conn.prepareStatement("DROP TABLE IF EXISTS table2; CREATE TABLE table2 (id int, cacheKey int, joinKey int, PRIMARY KEY (id, cacheKey)) WITH \"TEMPLATE=CacheB,AFFINITY_KEY=cacheKey,CACHE_NAME=CacheB\"");
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
    public void populate(Connection conn, List<String> args, Integer startId) throws SQLException {
        int countA = Integer.parseInt(args.get(0));
        int totalABuckets = Integer.parseInt(args.get(1)) * 2;
        List<Bucket> aBuckets = getBuckets(args.subList(2, 2 + totalABuckets));

        int countB = Integer.parseInt(args.get(2 + totalABuckets));
        int totalBBuckets = Integer.parseInt(args.get(3 + totalABuckets)) * 2;
        List<Bucket> bBuckets = getBuckets(args.subList(4 + totalABuckets, 4 + totalABuckets + totalBBuckets));

        System.out.printf("Starting data loading. Table 1: %s / %s, Table 2: %s / %s\n", countA, aBuckets, countB, bBuckets);

        if (args.contains("--fresh")) {
            conn.prepareStatement("DELETE FROM table1").execute();
            conn.prepareStatement("DELETE FROM table2").execute();
        }

        int maxId;
        if (args.contains("--maxId")) maxId = Integer.parseInt(args.get(args.indexOf("--maxId") + 1));
        else {
            maxId = 50;
        }

        System.out.printf("Max id: %s\n", maxId);

        PreparedStatement a = conn.prepareStatement("INSERT into table1 (id, cacheKey, joinKey, sid) VALUES (?, ?, ?, ?)");
        AtomicInteger sid = new AtomicInteger(0);
        populateTable(countA, aBuckets, a, stmt -> {
            try {
                stmt.setInt(3, Math.toIntExact((long) (Math.random() * maxId)));
                stmt.setInt(4, sid.getAndIncrement());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, startId);

        PreparedStatement b = conn.prepareStatement("INSERT into table2 (id, cacheKey, joinKey) VALUES (?, ?, ?)");
        populateTable(countB, bBuckets, b, stmt -> {
            try {
                stmt.setInt(3, Math.toIntExact((long) (Math.random() * maxId)));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, startId);

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
