package tests;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class demo extends BaseTest{
    public static void main(String[] args) {
        DefaultAWSCredentialsProviderChain props = new DefaultAWSCredentialsProviderChain();

        AWSCredentials credentials = props.getCredentials();

        final String AWS_ACCESS_KEY_ID = credentials.getAWSAccessKeyId();
        final String AWS_SECRET_ACCESS_KEY = credentials.getAWSSecretKey();

        SparkSession spark= SparkSession.builder()
                .appName("My Application")
                .config("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
                .config("fs.s3a.secret.key",AWS_SECRET_ACCESS_KEY)
                .master("local")
                .getOrCreate();

        String objectsListFromFolder="s3a://common-metrics-nonprodqa/utility_billing_data_firehose/2023/05/30/*";
        Dataset<Row> df = spark.read().option("inferSchema",true).json(objectsListFromFolder);
        df.show();
//Dataset<Row> df2 = df.filter(df.col("uuid").equalTo("9f7c215c-175d-409f-bba8-e69701cd5b9b"));
        for (Row row:df.collectAsList()) {
            System.out.println(row.getAs("uuid").toString());
        }
        spark.stop();
    }
}
