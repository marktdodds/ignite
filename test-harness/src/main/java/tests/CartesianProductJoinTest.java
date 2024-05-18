package tests;

import java.util.List;

public class CartesianProductJoinTest extends BasicTest {

    public static void main(String[] args) throws ClassNotFoundException {
        new JoinOnPartitionKeyTest().run(args);
    }

    @Override
    public String getTestQuery(List<String> args) {
        String join = "INNER JOIN";

        if (args.contains("--leftJoin")) join = "LEFT JOIN";
        else if (args.contains("--rightJoin")) join = "RIGHT JOIN";
        else if (args.contains("--fullJoin")) join = "FULL JOIN";

        String qry = "SELECT * FROM table1 " + join + " table2 ON 1 = 1";
        if (args.contains("--sort")) qry += " ORDER BY table1.id,table2.id ASC";
        return qry;
    }

}
