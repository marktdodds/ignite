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

public class BasicTest {

	public static void main(String[] args) throws ClassNotFoundException {

		// Register JDBC driver.
		Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
		System.out.format("Running with args: %s\n", String.join(" ", args));
		int index = Arrays.asList(args).indexOf("--load");
		if (index >= 0) {

			try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + args[0])) {
				int populationCountA = Integer.parseInt(args[index + 1]);
				int populationCountB = Integer.parseInt(args[index + 2]);
				double aSkew = Double.parseDouble(args[index + 3]);
				double bSkew = Double.parseDouble(args[index + 4]);
				System.out.println("Starting data loading. Table A: " + populationCountA + ", Table B: " + populationCountB);
				create(conn);
				populate(conn, populationCountA, populationCountB, aSkew, bSkew);
				System.out.println("Completed data loading");
			} catch (SQLException e) {
				Logger.getLogger(BasicTest.class.getName()).severe(e.getMessage());
				e.printStackTrace();
			}

		}

		index = Arrays.asList(args).indexOf("--run");
		if (index >= 0) {
			System.out.println("Starting tests...");
			int testIterations = Integer.parseInt(args[index + 1]);
			int fetchSize = Integer.parseInt(args[index + 2]);
			boolean countResult = Arrays.asList(args).contains("--countResult");
			ArrayList<Long> queryDurations = new ArrayList<>();
			ArrayList<Long> totalDurations = new ArrayList<>();

			for (int i = 0; i < testIterations; i++) {
				try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://" + args[0])) {
					long result[] = test(conn, testIterations, fetchSize, countResult);
					System.out.format("[Test %s/%s] Result count %s; Query Duration %s", i + 1, testIterations, result[0], result[1]);
					if (countResult) System.out.format(", Total duration %s", result[2]);
					System.out.format("\n");
					queryDurations.add(result[1]);
					if (countResult) totalDurations.add(result[2]);
				} catch (SQLException e) {
					Logger.getLogger(BasicTest.class.getName()).severe(e.getMessage());
				}
			}

			System.out.format("Tests completed. Average duration: %s", queryDurations.stream().mapToLong(Long::longValue).average().getAsDouble());
			if (countResult) System.out.format(", Average total duration: %s", totalDurations.stream().mapToLong(Long::longValue).average().getAsDouble());
			System.out.format("\n");
		}
	}

	public static void create(Connection conn) throws SQLException {
//		PreparedStatement a = conn.prepareStatement("DROP TABLE IF EXISTS table1; CREATE TABLE table1 (id int, cacheKey int, t1key int, PRIMARY KEY (id, cacheKey)) WITH \"TEMPLATE=CacheA,CACHE_NAME=CacheA\"");
		PreparedStatement a = conn.prepareStatement("DROP TABLE IF EXISTS table1; CREATE TABLE table1 (id int, cacheKey int, t1key int, PRIMARY KEY (id, cacheKey)) WITH \"TEMPLATE=CacheA,AFFINITY_KEY=cacheKey,CACHE_NAME=CacheA\"");
		a.execute();
//		PreparedStatement b = conn.prepareStatement("DROP TABLE IF EXISTS table2; CREATE TABLE table2 (id int, cacheKey int, t2key int, PRIMARY KEY (id, cacheKey)) WITH \"TEMPLATE=CacheB,CACHE_NAME=CacheB\"");
		PreparedStatement b = conn.prepareStatement("DROP TABLE IF EXISTS table2; CREATE TABLE table2 (id int, cacheKey int, t2key int, PRIMARY KEY (id, cacheKey)) WITH \"TEMPLATE=CacheB,AFFINITY_KEY=cacheKey,CACHE_NAME=CacheB\"");
		b.execute();
		System.out.println("Tables created.");
	}

	public static void populate(Connection conn, int countA, int countB, double aSkew, double bSkew) throws SQLException {

		PreparedStatement a = conn.prepareStatement("INSERT into table1 (id, t1key, cacheKey) VALUES (?, ?, ?)");
		PreparedStatement b = conn.prepareStatement("INSERT into table2 (id, t2key, cacheKey) VALUES (?, ?, ?)");

		ArrayList<Integer> aIds = new ArrayList<>();
		ArrayList<Integer> bIds = new ArrayList<>();
		int maxCount = Math.max(countA, countB);

		aIds.add(0);

		for (float i = 0; i <= 1; i += aSkew) {
			aIds.add(((int) Math.round(Math.random() * 100000) + 1000));
		}

		bIds.add(((int) Math.round(Math.random() * 10000) + 1000));
		for (float i = 0; i <= 1; i += bSkew) {
			bIds.add(((int) Math.round(Math.random() * 100000) + 1000));
		}

		for (int i = 0; i < maxCount; i++) {
			if (i < countA) {
				a.setInt(1, i);
				a.setInt(2, (int) Math.round(Math.random() * 1000));
				a.setInt(3, aIds.get((int) Math.floor(((double) i / maxCount) / aSkew)));
				a.addBatch();
			}

			if (i < countB) {
				b.setInt(1, i + maxCount);
				b.setInt(2, (int) Math.round(Math.random() * 1000));
				b.setInt(3, bIds.get((int) Math.floor(((double) i / maxCount) / bSkew)));
				b.addBatch();
			}

			if (i % 10000 == 0) {
				a.executeBatch();
				a.clearBatch();
				b.executeBatch();
				b.clearBatch();
				System.out.println("Inserted " + i);
			}
		}

		a.executeBatch();
		b.executeBatch();

		PreparedStatement check = conn.prepareStatement("SELECT count(*) from table1;");
		check.execute();
		ResultSet set = check.getResultSet();
		if (set.next()) System.out.println("Table1 Count: " + set.getInt(1));

		check = conn.prepareStatement("SELECT count(*) from table2;");
		check.execute();
		set = check.getResultSet();
		if (set.next()) System.out.println("Table2 Count: " + set.getInt(1));

	}

	public static long[] test(Connection conn, int count, int fetchSize, boolean countResult) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement("SELECT * FROM table1 INNER JOIN table2 ON table1.t1key = table2.t2key");
		stmt.setFetchSize(fetchSize);
		long start = new Date().getTime();
		stmt.execute();
		long msDuration = new Date().getTime() - start;
		ResultSet result = stmt.getResultSet();
		int resultCount = 0;
		if (countResult) {
			while (result.next()) {
				resultCount++;
			}
		}
		long totalDuration = new Date().getTime() - start;
		return new long[]{resultCount, msDuration, totalDuration};
	}

}
