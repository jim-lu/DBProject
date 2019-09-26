package utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class BucketUtils {
    private final AmazonS3 s3 = new AmazonS3Client();
    private S3Object object;

    public ArrayList<String[]> downloadObjectsFromS3(String bucketName, String key) throws IOException {
        try {
            object = s3.getObject(new GetObjectRequest(bucketName, key));
        } catch (AmazonServiceException serviceException) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + serviceException.getMessage());
            return null;
        } catch (AmazonClientException clientException) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message:    " + clientException.getMessage());
            return null;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
        ArrayList<String[]> list = new ArrayList<String[]>();
        int count = 0;
        boolean firstLine = true;
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
            if (firstLine) {
                firstLine = false;
                continue;
            }
            list.add(line.split(","));
            count ++;
        }
        System.out.println(count);
        return list;
    }

//    public static void main(String[] args) throws IOException {
//        BucketUtils bu = new BucketUtils();
//        bu.downloadObjectsFromS3("my-data-s3-bucket", "Instacart/order_products.csv");
//    }
}
