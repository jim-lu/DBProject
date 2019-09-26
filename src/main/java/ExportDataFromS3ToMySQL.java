import utils.BucketUtils;
import utils.DBUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

public class ExportDataFromS3ToMySQL {
    public static void main(String[] args) throws IOException, SQLException {
        BucketUtils bucketUtilities = new BucketUtils();
        DBUtils DBUtilities = new DBUtils();

        ArrayList<String[]> data = bucketUtilities.downloadObjectsFromS3("my-data-s3-bucket", "Instacart/order_products.csv");
//        for (int i = 0; i < data.size(); i ++) {
//            String[] tmp = data.get(i);
//            for (String str: tmp) {
//                System.out.println(str);
//            }
//            System.out.println();
//        }

//        String createTable = "CREATE TABLE Test (\n" +
//                "  order_id INT NOT NULL,\n" +
//                "  product_id INT NOT NULL,\n" +
//                "  add_to_cart_order INT NOT NULL,\n" +
//                "  reordered INT NOT NULL,\n" +
//                "  PRIMARY KEY (order_id),\n" +
//                "  UNIQUE (product_id)\n" +
//                ");";
        DBUtilities.connect();
//        DBUtilities.createTable(createTable, "Test");
        DBUtilities.insertData(data, "Test");
    }
}
