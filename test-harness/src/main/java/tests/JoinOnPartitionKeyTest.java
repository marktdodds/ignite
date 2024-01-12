package tests;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class JoinOnPartitionKeyTest extends BasicTest {

    public static void main(String[] args) throws ClassNotFoundException {
        new JoinOnPartitionKeyTest().run(args);
    }

    @Override
    public String getTestQuery(List<String> args) {
        String join = "INNER JOIN";

        if (args.contains("--leftJoin")) join = "LEFT JOIN";
        else if (args.contains("--rightJoin")) join = "RIGHT JOIN";
        else if (args.contains("--fullJoin")) join = "FULL JOIN";

        String qry = "SELECT * FROM table1 " + join + " table2 ON table1.cacheKey = table2.cacheKey";
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
}
