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
 * --load (salesmen size) (batches) [% bucketId : for i in batches] (orders size) (batches) [% bucketId : for i in batches]
 * Note: batches == 0 => uniform data distribution
 * Sample: --load 6000 2 0.25 1 0.75 4 6000 3 0.5 1 0.25 0 0.25 5
 * Sample: --load 10000 0 10000 0
 * --run (test count) (fetch size) [--countResult]
 * Default fetch size is 1024
 * Sample: 1 10000 --countResult
 */
public class SalesmenOrdersTest implements PerformanceTest {

    public static void main(String[] args) throws ClassNotFoundException {
        new SalesmenOrdersTest().run(args);
    }

    @Override
    public String getTestQuery(List<String> args) {
        StringBuilder ids = new StringBuilder();
        int idCount = Integer.parseInt(getArgOrDefault(args, "--idCount", 1, "5"));
        int idUpper = Integer.parseInt(getArgOrDefault(args, "--idMax", 1, "20"));
        for (int i = 0; i < idCount; i++) {
            ids.append(args.contains("--randomIds") ? Math.toIntExact((long) (Math.random() * idUpper)) : i).append(",");
        }
        System.out.println("IDS: " + ids);
        return "SELECT * FROM salesmen INNER JOIN orders ON salesmen.id = orders.salesmenId " +
            "WHERE orders.salesmenId in (" + ids.substring(0, ids.length() - 1) + ")";
    }

    @Override
    public void create(Connection conn) throws SQLException {
        PreparedStatement a = conn.prepareStatement("DROP TABLE IF EXISTS salesmen; CREATE TABLE salesmen (id int, cacheKey int, PRIMARY KEY (id, cacheKey)) WITH \"TEMPLATE=CacheA,AFFINITY_KEY=cacheKey,CACHE_NAME=CacheA\"");
        a.execute();

        PreparedStatement b = conn.prepareStatement("DROP TABLE IF EXISTS orders; CREATE TABLE orders (id int, cacheKey int, salesmenId int, PRIMARY KEY (id, cacheKey)) WITH \"TEMPLATE=CacheB,AFFINITY_KEY=cacheKey,CACHE_NAME=CacheB\"");
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
            conn.prepareStatement("DELETE FROM salesmen").execute();
            conn.prepareStatement("DELETE FROM orders").execute();
        }

        PreparedStatement a = conn.prepareStatement("INSERT into salesmen (id, cacheKey) VALUES (?, ?)");
        populateTable(countA, aBuckets, a, stmt -> {
        }, startId);

        PreparedStatement b = conn.prepareStatement("INSERT into orders (id, cacheKey, salesmenId) VALUES (?, ?, ?)");
        populateTable(countB, bBuckets, b, stmt -> {
            try {
                stmt.setInt(3, (int) Math.floor(Math.random() * countA));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, startId);

        PreparedStatement check = conn.prepareStatement("SELECT count(*) from salesmen;");
        check.execute();
        ResultSet set = check.getResultSet();
        if (set.next()) System.out.println("SalesmenTable Count: " + set.getInt(1));

        check = conn.prepareStatement("SELECT count(*) from orders;");
        check.execute();
        set = check.getResultSet();
        if (set.next()) System.out.println("OrdersTable Count: " + set.getInt(1));
    }

}
