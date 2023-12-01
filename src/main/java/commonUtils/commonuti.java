package commonUtils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.*;
import java.util.Properties;

public class commonuti {

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
//        DefaultAWSCredentialsProviderChain props = new DefaultAWSCredentialsProviderChain();

//        AWSCredentials credentials = props.getCredentials();// JDBC driver and database URL
        String jdbcUrl = "";
        String username = "";
        String password = "";

        // Register JDBC driver
        Class.forName("");

        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);

        // Establish the connection
        Connection conn = DriverManager.getConnection(jdbcUrl, props);

        // Create a statement
        Statement stmt = conn.createStatement();

// Execute the query
        String sqlQuery = "select * from user_meta_data ;";
        ResultSet resultSet = stmt.executeQuery(sqlQuery);

// Process the results
        while (resultSet.next()) {
            // Access individual columns using resultSet.getXXX(columnIndex) methods
            System.out.println("uuid : "+resultSet.getString("uuid"));

//        final String AWS_ACCESS_KEY_ID = "";
//        final String AWS_SECRET_ACCESS_KEY = "";
//
//        System.out.println(AWS_ACCESS_KEY_ID+"======="+AWS_SECRET_ACCESS_KEY);
//        SparkSession spark= SparkSession.builder()
//                .appName("My Application")
//                .config("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
//                .config("fs.s3a.secret.key",AWS_SECRET_ACCESS_KEY)
//                .master("local")
//                .getOrCreate();

//            String objectsListFromFolder="s3a://bidgelyna-firehose/users_firehose/2023/06/16/00/users_prod-na-2-2-2023-06-16-00-00-25-53757dd7-c707-48fc-b162-5bb709afb5af";
//        Dataset<Row> df = spark.read().option("inferSchema",true).json(objectsListFromFolder);
////        Dataset<Row> df2 = df.filter(df.col("uuid").equalTo("4382b3fa-c9f8-4150-9055-3753fc49599d"));
//        df.show(100);
//        df.printSchema();

        }
    }
}
