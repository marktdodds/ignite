package tests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Logger;

public class BasicTest {

	public static void main(String[] args) throws ClassNotFoundException {

		// Register JDBC driver.
		Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

		try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + args[0])) {
			System.out.println("Starting data loading...");
			int populationCount = Integer.parseInt(args[1]);
			create(conn);
			populate(conn, populationCount);
			System.out.println("Completed data loading");
		} catch (SQLException e) {
			Logger.getLogger(BasicTest.class.getName()).severe(e.getMessage());
		}
		try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + args[0])) {
			System.out.println("Starting tests...");
			int testIterations = Integer.parseInt(args[2]);
			test(conn, testIterations);
			System.out.println("Completed tests");
		} catch (SQLException e) {
			Logger.getLogger(BasicTest.class.getName()).severe(e.getMessage());
		}

	}

	public static void create(Connection conn) throws SQLException {
		PreparedStatement a = conn.prepareStatement("DROP TABLE IF EXISTS table1; CREATE TABLE table1 (id int primary key, t1key int) WITH \"TEMPLATE=CacheA,CACHE_NAME=CacheA\"");
		a.execute();
		PreparedStatement b = conn.prepareStatement("DROP TABLE IF EXISTS table2; CREATE TABLE table2 (id int primary key, t2key int) WITH \"TEMPLATE=CacheB,CACHE_NAME=CacheB\"");
		b.execute();
		System.out.println("Tables created.");
	}

	public static void populate(Connection conn, int count) throws SQLException {

		PreparedStatement a = conn.prepareStatement("INSERT into table1 (id, t1key) VALUES (?, ?)");
		PreparedStatement b = conn.prepareStatement("INSERT into table2 (id, t2key) VALUES (?, ?)");

		for (int i = 0; i < count; i++) {
			a.setInt(1, i);
			a.setInt(2, (int) Math.round(Math.random() * count));
			a.addBatch();
			b.setInt(1, i + count);
			b.setInt(2, (int) Math.round(Math.random() * count));
			b.addBatch();
			if (i % 500 == 0) System.out.println("Inserted " + i);
		}
		a.executeBatch();
		b.executeBatch();
	}

	public static void test(Connection conn, int count) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement("SELECT * FROM table1 INNER JOIN table2 ON table1.t1key = table2.t2key");
		ArrayList<Long> durations = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			long start = new Date().getTime();
			stmt.execute();
			Long msDuration = new Date().getTime() - start;
			ResultSet result = stmt.getResultSet();
			int resultCount = 0;
			while (result.next()) {
				count++;
			}
			System.out.format("[Test %s] Result count %s; Duration %s", i, resultCount, msDuration);
			durations.add(msDuration);
		}
		System.out.format("Tests completed. Average duration: %s", durations.stream().mapToLong(Long::longValue).average());
	}

}
