package utils;


import java.sql.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBUtils {
    private Connection conn = null;
    private String dbName = "instacart";
    private String userName = "admin";
    private String password = "lujunlin1996";
    private String hostname = "instacart.csp9pjssfgc4.us-east-1.rds.amazonaws.com";
    private String port = "3306";

    public void connect() throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://" + hostname + ":" + port + "/" + dbName + "?user=" + userName + "&password=" + password + "&useSSL=false");
        } catch (ClassNotFoundException classException) {
            classException.printStackTrace();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }

    public void createTable(String sql, String tableName) {
        try {
            if (!tableExist(tableName)) {
                Statement statement = conn.createStatement();
                boolean createSucceed = statement.execute(sql);
                if (createSucceed) System.out.println("Create table '" + tableName + "' succeeded.");
            } else {
                System.out.println("Table '" + tableName + "' already exists.");
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }

    public void insertData(ArrayList<String[]> dataList, String tableName) {
        try {
            if (!tableExist(tableName)) {
                System.out.println("Table '" + tableName + "' does not exist.");
                return;
            }
            Statement statement = conn.createStatement();
            for (int i = 0; i < dataList.size(); i ++) {
                StringBuilder sb = new StringBuilder("INSERT INTO " + tableName + " VALUES (");
                String[] data = dataList.get(i);
                for (int j = 0; j < data.length; j ++) {
                    if (isNumberic(data[j])) {
                        sb.append(data[j]);
                    } else {
                        sb.append("'" + data[j] + "'");
                    }
                    if (j < data.length - 1) {
                        sb.append(",");
                    }
                }
                sb.append(");");
                System.out.println(sb.toString());
                statement.executeUpdate(sb.toString());
            }
//            statement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Insert data into table " + tableName + " finished.");
        }
    }

    public void deleteTable(String table) {
        String sql = "DROP TABLE " + table;
        try {
            if (tableExist(table)) {
                Statement statement = conn.createStatement();
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }

    public boolean tableExist(String tableName) {
        try {
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet table = meta.getTables(dbName, null, tableName, new String[] {"TABLE"});
            if (table.next()) return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean isNumberic(String str) {
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher isNumber = pattern.matcher(str);
        return isNumber.matches();
    }

    public void excecuteSql(String sql) throws SQLException {
        try {
            Statement setupStatement = conn.createStatement();
            setupStatement.addBatch(sql);
            setupStatement.executeBatch();
            setupStatement.close();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }

//    public static void main(String[] args) throws SQLException {
//        DBUtils test = new DBUtils();
//        test.connect();
//        String createTable = "CREATE TABLE Test (\n" +
//                "  order_id INT NOT NULL,\n" +
//                "  product_id INT NOT NULL,\n" +
//                "  add_to_cart_order INT NOT NULL,\n" +
//                "  reordered INT NOT NULL,\n" +
//                "  PRIMARY KEY (order_id),\n" +
//                "  UNIQUE (product_id)\n" +
//                ");";
//        test.createTable(createTable, "Test");
//        System.out.println(test.isNumberic("1384617"));
//    }
}
