package tests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class BasicTest {

	public static void main(String[] args) throws ClassNotFoundException {

		// Register JDBC driver.
		Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
		int index = Arrays.binarySearch(args, "--load");
		if (index >= 0) {

			try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + args[0])) {
				System.out.println("Starting data loading...");
				int populationCount = Integer.parseInt(args[index + 1]);
				create(conn);
				populate(conn, populationCount);
				System.out.println("Completed data loading");
			} catch (SQLException e) {
				Logger.getLogger(BasicTest.class.getName()).severe(e.getMessage());
			}

		}

		index = Arrays.binarySearch(args, "--run");
		if (index >= 0) {
			System.out.println("Starting tests...");
			int testIterations = Integer.parseInt(args[index + 1]);
			ArrayList<Long> durations = new ArrayList<>();

			for (int i = 0; i < testIterations; i++) {
				try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + args[0])) {
					long result[] = test(conn, testIterations);
					System.out.format("[Test %s/%s] Result count %s; Duration %s\n", i + 1, testIterations, result[0], result[1]);

					durations.add(result[1]);
				} catch (SQLException e) {
					Logger.getLogger(BasicTest.class.getName()).severe(e.getMessage());
				}
			}

			System.out.format("Tests completed. Average duration: %s\n", durations.stream().mapToLong(Long::longValue).average().getAsDouble());
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
			a.setInt(2, (int) Math.round(Math.random() * 1000));
			a.addBatch();
			b.setInt(1, i + count);
			b.setInt(2, (int) Math.round(Math.random() * 1000));
			b.addBatch();
			if (i % 10000 == 0) {
				a.executeBatch();
				a.clearBatch();
				b.executeBatch();
				b.clearBatch();
				System.out.println("Inserted " + i);
			}
		}
	}

	public static long[] test(Connection conn, int count) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement("SELECT * FROM table1 INNER JOIN table2 ON table1.t1key = table2.t2key");
		long start = new Date().getTime();
		stmt.execute();
		long msDuration = new Date().getTime() - start;
		ResultSet result = stmt.getResultSet();
		int resultCount = 0;
		while (result.next()) {
			resultCount++;
		}
		return new long[]{resultCount, msDuration};
	}

}
