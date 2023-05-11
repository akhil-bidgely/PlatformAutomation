package Ingestion;

import PojoClasses.UserFilePOJO;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.thirdparty.jackson.databind.JsonNode;
import com.amazonaws.thirdparty.jackson.databind.ObjectMapper;
import com.opencsv.exceptions.CsvException;
import io.restassured.response.Response;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.parser.ParseException;
import CommonUtils.JsonUtils;
import CommonUtils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.*;
import org.testng.asserts.SoftAssert;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;

import static CommonUtils.JsonUtils.getAuthToken;
import static CommonUtils.Utils.getTimeStampsConsumption;
import static CommonUtils.Utils.processRawFile;
import static Constants.FilePaths.*;
import static Constants.PilotIDs.AMEREN_PILOT_ID;
import static org.apache.spark.sql.functions.*;


public class UserIngestion extends BaseTest{
    String token= "";
    String uuid= "";
    Map<String, String> executionVariables = JsonUtils.getExecutionVariables();
    private static Logger logger = LoggerFactory.getLogger(UserIngestion.class);
    SparkSession spark;

    @BeforeMethod
    public void generateToken() {
        Response response= restUtils.generateToken();
        token=getAuthToken(response);
    }

    @Test(enabled = false)
    public void fileIngestion () throws IOException, ParseException, InterruptedException, java.text.ParseException, CsvException {

        //Internal bucket USERENROLL file upload
        UserFilePOJO userfileInfo= new UserFilePOJO();
        String userTempFilePath=processRawFile(USER_ENROLLMENT_AMI_E,executionVariables,userfileInfo, null);
        Utils.s3UploadFile(userTempFilePath);

        //Calling Partner User API
        /*Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->{
            Response partnerUserIdResponse=  restUtils.getPartnerUserId(token,executionVariables);
            JSONObject obj= new JSONObject(partnerUserIdResponse.asString());
            System.out.println("Length of data : " + obj.getJSONObject("payload").getJSONArray("data").length());
            if(obj.getJSONObject("payload").getJSONArray("data").length() == 1){
                return true;
            }else {
                return false;
            }
        });*/

        Thread.sleep(10000);
//        Awaitility.await().atMost(10,TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS)
//                .until(() ->restUtils.getPartnerUserId(token,executionVariables).getStatusCode() == 200);
        Response partnerUserIdResponse= restUtils.getPartnerUserId(token,executionVariables);
        uuid=JsonUtils.getUuidFromPremiseId(partnerUserIdResponse);

        //Calling Pilot config API
        Response getPilotConfigResponse= restUtils.getPilotConfigs(token,AMEREN_PILOT_ID);
        String timeZone=JsonUtils.getTimeZone(getPilotConfigResponse);

        //Calling User Details API
        Response usersApiResponse= restUtils.getUsers(uuid,token);

        //Validating User Details API response
        ingestionValidations.validateUserDetails(usersApiResponse,userfileInfo,uuid,timeZone,executionVariables,AMEREN_PILOT_ID);

        //Internal bucket METERENROLL file upload
        String meterTempFilePath=processRawFile(METER_ENROLLMENT_AMI_E,executionVariables,null,meterFilePOJO);
        Utils.s3UploadFile(meterTempFilePath);
        Thread.sleep(10000);
//        Awaitility.await().atMost(10,TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS)
//                .until(() ->RestUtils.getMetersApi(baseUrl,uuid,token).getStatusCode() == 200);
        Response metersApiResponse= restUtils.getMetersApi(uuid,token);
        restUtils.printResponseLogInReport(metersApiResponse);
        ingestionValidations.validateMeters(metersApiResponse.asString(),uuid,AMEREN_PILOT_ID,executionVariables,meterFilePOJO);

        //Internal bucket RAW file upload
        String rawTempFilePath=processRawFile(RAW_AMI_E,executionVariables,null,null);
        Utils.s3UploadFile(rawTempFilePath);
        Thread.sleep(10000);
        String t1 = String.valueOf(Instant.now().getEpochSecond());

        //Calling Label TimeStamp API
        Response label= restUtils.getLabelTimeStamp(uuid,token);
        ingestionValidations.validateLableTimeStamp(label.asString());

        //Calling gbJson API
        Response gbJsonApiResponse= restUtils.getGbJsonApi(uuid,token,t1);
        Map<String, String> mapTimestampConsumption = getTimeStampsConsumption(rawTempFilePath);
        ingestionValidations.validateGbJsonConsumption(rawTempFilePath,gbJsonApiResponse.asString(),mapTimestampConsumption);

        //Internal bucket Invoice file upload
        String invoiceTempFilePath=processRawFile(INVOICE_AMI_E,executionVariables,null,null);
        Utils.s3UploadFile(invoiceTempFilePath);
        Thread.sleep(10000);

        //Calling INVOICE API
        Response gbDisaggResponse= restUtils.getGbDisaggResp(uuid,token);
        ingestionValidations.validateGbDisaggResp(gbDisaggResponse.asString());
    }

    @Test(enabled = true)
    public void testFireHose()
    {
        DefaultAWSCredentialsProviderChain props = new DefaultAWSCredentialsProviderChain();
        AWSCredentials credentials = props.getCredentials();
        final String AWS_ACCESS_KEY_ID = credentials.getAWSAccessKeyId();
        final String AWS_SECRET_ACCESS_KEY = credentials.getAWSSecretKey();
        spark= SparkSession.builder()
                .appName("My Application")
                .config("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
                .config("fs.s3a.secret.key",AWS_SECRET_ACCESS_KEY)
                .master("local")
                .getOrCreate();
        //to read the data from S3 Firehose
       // Dataset<Row> df=Utils.getS3FirehoseData(spark,"common-metrics-nonprodqa","/utility_billing_data_firehose/2023/05/04/06/*");
        //Dataset<Row> df=Utils.getS3FirehoseData(spark,"common-metrics-nonprodqa","/utility_billing_data_firehose/2023/05/04/06/*");
        Dataset<Row> df=Utils.getS3FirehoseData(spark,"common-metrics-nonprodqa","/utility_billing_data_firehose/2023/05/09/09/*");
        //1. b90164a7-277d-4d3a-a1fb-d83cc1d33cf9 -Apr26
        //2. c923dca0-54c0-44e0-9c39-2cf80853a1ae -Apr22
        //3. bee45cda-aa01-49d1-a785-613a8eec91e4 -May4
        //4. cd799f82-0b65-4e07-9f23-a411c8c6432b -May 9

       // dfInS3.filter("uuid == 'c923dca0-54c0-44e0-9c39-2cf80853a1ae'").show(50);
       // Dataset<Row> rowS3Dataset = df.filter(df.col("uuid").equalTo("bee45cda-aa01-49d1-a785-613a8eec91e4"))
        Dataset<Row> rowS3Dataset = df.filter(df.col("uuid").equalTo("cd799f82-0b65-4e07-9f23-a411c8c6432b"))
                .filter(df.col("bidgely_generated_invoice").equalTo("false"))
                .orderBy(df.col("billing_start_time"));


        Dataset<Row> rowDatasetNewS3 = rowS3Dataset.withColumn("billing_start_time", from_unixtime(rowS3Dataset.col("billing_start_time").divide(1000), "yyyy-MM-dd"))
                .withColumn("billing_end_time", from_unixtime(rowS3Dataset.col("billing_end_time").divide(1000), "yyyy-MM-dd"));

        System.out.println("========to read the from s3=======");
        rowDatasetNewS3.show(50);

        //To define the schema of the input invoice File
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("customerId", DataTypes.StringType, true),
                DataTypes.createStructField("partnerId", DataTypes.StringType, true),
                DataTypes.createStructField("premiseId", DataTypes.StringType, true),
                DataTypes.createStructField("dataStreamId", DataTypes.StringType, true),
                DataTypes.createStructField("fuelType", DataTypes.StringType, true),
                DataTypes.createStructField("meterCycleCode", DataTypes.StringType, true),
                DataTypes.createStructField("billingStartDate", DataTypes.StringType, true),
                DataTypes.createStructField("billingEndDate", DataTypes.StringType, true),
                DataTypes.createStructField("billingDuration", DataTypes.StringType, true),
                DataTypes.createStructField("chargeName", DataTypes.StringType, true),
                DataTypes.createStructField("chargeType", DataTypes.StringType, true),
                DataTypes.createStructField("consumption", DataTypes.StringType, true),
                DataTypes.createStructField("currencyCost", DataTypes.StringType, true),
                DataTypes.createStructField("dataStreamType", DataTypes.StringType, true)
        });

        //to process the input invoice file
        String filePath=System.getProperty("user.dir")+"/src/test/resources/Ameren/AMI_E/INVOICE_600401000.csv";
        Dataset<Row> df1=spark.read().format("csv").schema(schema) .option("sep","|").load(filePath);


        //to filter based on the billingStartDate and charge type is Total as in Firehose S3 we store only the charge Type Total
        Dataset<Row> rowDatasetInputFile = df1.filter(df1.col("chargeType").equalTo("TOTAL")).orderBy(df1.col("billingStartDate"));

        //to apply the join based on billing start date
        //Dataset <Row> joinedData = rowDatasetNewS3.join(rowDatasetInputFile, rowDatasetInputFile.col("billingStartDate").equalTo(rowDatasetNewS3.col("billing_start_time")));
        Dataset <Row> joinedData = rowDatasetInputFile.join(rowDatasetNewS3, rowDatasetInputFile.col("billingStartDate").equalTo(rowDatasetNewS3.col("billing_start_time")),"left_outer");
        System.out.println("====");
        joinedData.show(100);
        System.out.println("====");
        //input data , s3 data --, billing start, billing end
        //TODO -10 months of data in S3  ,
        //To verify few other fields
        //to get user type -Function
        //to get measurement_type -Function -From the meter file
        //Left Outer Join

        //to get solar -Field from Meter file
        //To define the schema of the input invoice File
        StructType schemaMeter = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("customerId", DataTypes.StringType, true),
                DataTypes.createStructField("partnerId", DataTypes.StringType, true),
                DataTypes.createStructField("premiseId", DataTypes.StringType, true),
                DataTypes.createStructField("dataStreamId", DataTypes.StringType, true),
                DataTypes.createStructField("fuelType", DataTypes.StringType, true),
                DataTypes.createStructField("serviceAgreementStDate", DataTypes.StringType, true),
                DataTypes.createStructField("serviceAgreementEnDate", DataTypes.StringType, true),
                DataTypes.createStructField("ratePlanId", DataTypes.StringType, true),
                DataTypes.createStructField("ratePlanEffectiveDate", DataTypes.StringType, true),
                DataTypes.createStructField("billingCycleCode", DataTypes.StringType, true),
                DataTypes.createStructField("billingCycleEffectiveDate", DataTypes.StringType, true),
                DataTypes.createStructField("solar", DataTypes.StringType, true),
                DataTypes.createStructField("dataStreamType", DataTypes.StringType, true),
                DataTypes.createStructField("label", DataTypes.StringType, true)
        });
        String filePath2=System.getProperty("user.dir")+"/src/test/resources/Ameren/AMI_E/METERENROLL_D_600401000.csv";
        Dataset<Row> df2=spark.read().format("csv").schema(schemaMeter).option("sep","|").load(filePath2);
      //  String solar = df2.select(col("solar")).first().toString();
        String solar=df2.first().getAs("solar");
        boolean solarFieldFromMeterFile = solar.equals("False")?false:true;


        //To verify the field getting in S3 and input file
        joinedData.foreach(row -> {

            //to verify the billing end time
         //   System.out.println(row.getAs("billing_start_time").toString() +"   "+row.getAs("billingStartDate").toString());

            if(row.getAs("billing_start_time")!=null &&
                    row.getAs("billing_start_time").toString().equals(row.getAs("billingStartDate").toString())) {
                SoftAssert softAssert = new SoftAssert();
                logger.info("the billing end time values in s3 and input file is " + row.getAs("billing_end_time").toString() + " " + row.getAs("billingEndDate").toString());
                softAssert.assertEquals(row.getAs("billing_end_time").toString(), row.getAs("billingEndDate").toString());

                //to verify the consumption values
                logger.info("the consumption values in s3 and input file is " + row.getAs("consumption").toString() + " " + row.getAs("consumption_value").toString());
                softAssert.assertEquals(row.getAs("consumption").toString(), row.getAs("consumption_value").toString(), "Consumption Not getting matched"
                        + row.getAs("consumption").toString() + " " + row.getAs("consumption_value"));

                //to verify the cost values
                logger.info("the cost values in s3 and input file is " + row.getAs("cost").toString() + " " + row.getAs("currencyCost").toString());
                softAssert.assertEquals(row.getAs("cost").toString(), row.getAs("currencyCost").toString());

                //to Verify the USER type in s3
                if (row.getAs("dataStreamType").toString().equals("AMI")) {
                    logger.info("verifying the user type in S3 based on Meter type");
                    softAssert.assertEquals(row.getAs("user_type"), Utils.UserTypeAMIMeter);
                } else if (row.getAs("dataStreamType").toString().equals("AMR")) {
                    logger.info("verifying the user type in S3 based on Meter type");
                    softAssert.assertEquals(row.getAs("user_type"), Utils.UserTypeAMRMeter);
                }
                else
                {
                    logger.info("verifying the user type in S3 based on Meter type");
                    softAssert.assertEquals(row.getAs("user_type"), Utils.UserTypeNSMMeter);
                }
                //TODO
                //to verify the Measurement type in s3
                if (row.getAs("fuelType").toString().equals("ELECTRIC")) {
                    logger.info("verifying the measurement type in S3 based on Meter type");
                    softAssert.assertEquals(row.getAs("measurement_type"), "Electricity");
                }
                //to verify the solar field
                logger.info("verifying the solar field in S3 based on Meter File" + row.getAs("solar").toString() + " " + solarFieldFromMeterFile);
                softAssert.assertEquals(Boolean.parseBoolean(row.getAs("solar").toString()), solarFieldFromMeterFile);

                softAssert.assertEquals(row.getAs("bidgely_generated_invoice").toString(), "false");

                //to verify the hid
                softAssert.assertEquals(row.getAs("hid").toString(), "1");

                //to verify the transition bill cycle
                softAssert.assertEquals(row.getAs("transition_bill_cycle").toString(), "false");
                softAssert.assertAll();
            }
            else
            {
                System.out.println("Records not present in S3 bucket");
                System.out.println(row.toString());
            }

        });


    }

    @Test(enabled=true)
    public void testRedshiftData() throws IOException, java.text.ParseException {
       // String query = "select * from utility_billing_data where uuid='bee45cda-aa01-49d1-a785-613a8eec91e4' and bidgely_generated_invoice='false' order by billing_start_time asc";
        String query = "select * from utility_billing_data where uuid='cd799f82-0b65-4e07-9f23-a411c8c6432b' and bidgely_generated_invoice='false' order by billing_start_time asc";
        Response response = restUtils.postRedshiftQuery(query);
        Assert.assertEquals(response.getStatusCode(), 200);
        System.out.println("======");
        System.out.println(response.asString());


        // Extract the response body as a String
        String responseBody = response.getBody().asString();

        // Parse the response body as a JSON array using Jackson
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonArray = mapper.readTree(responseBody);


        //To define the schema of the input invoice File
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("customerId", DataTypes.StringType, true),
                DataTypes.createStructField("partnerId", DataTypes.StringType, true),
                DataTypes.createStructField("premiseId", DataTypes.StringType, true),
                DataTypes.createStructField("dataStreamId", DataTypes.StringType, true),
                DataTypes.createStructField("fuelType", DataTypes.StringType, true),
                DataTypes.createStructField("meterCycleCode", DataTypes.StringType, true),
                DataTypes.createStructField("billingStartDate", DataTypes.StringType, true),
                DataTypes.createStructField("billingEndDate", DataTypes.StringType, true),
                DataTypes.createStructField("billingDuration", DataTypes.StringType, true),
                DataTypes.createStructField("chargeName", DataTypes.StringType, true),
                DataTypes.createStructField("chargeType", DataTypes.StringType, true),
                DataTypes.createStructField("consumption", DataTypes.StringType, true),
                DataTypes.createStructField("currencyCost", DataTypes.StringType, true),
                DataTypes.createStructField("dataStreamType", DataTypes.StringType, true)
        });

        DefaultAWSCredentialsProviderChain props = new DefaultAWSCredentialsProviderChain();
        AWSCredentials credentials = props.getCredentials();
        final String AWS_ACCESS_KEY_ID = credentials.getAWSAccessKeyId();
        final String AWS_SECRET_ACCESS_KEY = credentials.getAWSSecretKey();
        spark = SparkSession.builder()
                .appName("My Application")
                .config("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
                .config("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
                .master("local")
                .getOrCreate();

        //to process the input invoice file
        String filePath = System.getProperty("user.dir") + "/src/test/resources/Ameren/AMI_E/INVOICE_600401000.csv";
        Dataset<Row> df1 = spark.read().format("csv").schema(schema).option("sep", "|").load(filePath);


        //to filter based on the billingStartDate and charge type is Total as in Firehose S3 we store only the charge Type Total
        Dataset<Row> rowDatasetInputFile = df1.filter(df1.col("chargeType").equalTo("TOTAL")).orderBy(df1.col("billingStartDate"));
        System.out.println("Number of records in input file is " + rowDatasetInputFile.count());
        System.out.println("Number of records in Redshift file is " + jsonArray.size());

        rowDatasetInputFile.show(20);

        HashMap<String, JsonNode> hm = new HashMap<String, JsonNode>();
        for (JsonNode jsonNode : jsonArray) {

            String startTime = jsonNode.get("billing_start_time").asText();

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date stDate = dateFormat.parse(startTime);
            String startFormatedDate = dateFormat.format(stDate);
            hm.put(startFormatedDate, jsonNode);
        }

        System.out.println(hm);
/*/

            //transition billing cycle
            //solar
            //bidgely_generated_invoice
            //user type
            //measurement type

            System.out.println("===============================");

*/
        //18 records
        for (Row row : rowDatasetInputFile.collectAsList()) {
            String column1Value = row.getAs("billingStartDate");
          //  int column2Value = row.getAs("column2");
           // JsonNode jsonNode = hm.get(column1Value);
           // System.out.println(column1Value);
            //System.out.println(jsonNode);

            if (hm.containsKey(column1Value)) {
                JsonNode jsonNode=hm.get(column1Value);
                System.out.println(jsonNode);
                String endTime = jsonNode.get("billing_end_time").asText();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                Date enDate = dateFormat.parse(endTime);
                String endFormatedDate = dateFormat.format(enDate);

                Assert.assertEquals(row.getAs("billingEndDate").toString(),endFormatedDate);


                //for consumption value verification
                Double value1 = Double.parseDouble(row.getAs("consumption").toString());
                Double value2 = Double.parseDouble(jsonNode.get("consumption_value").toString().replaceAll("\"",""));
                BigDecimal decimal1 = BigDecimal.valueOf(value1).setScale(1, BigDecimal.ROUND_HALF_UP);
                BigDecimal decimal2 = BigDecimal.valueOf(value2).setScale(1, BigDecimal.ROUND_HALF_UP);
                System.out.println(decimal1 +" "+decimal2);
                Assert.assertEquals(decimal1,decimal2);

                //for currency cost verification
                Double val1 = Double.parseDouble(row.getAs("currencyCost").toString());
                Double val2 = Double.parseDouble(jsonNode.get("cost").toString().replaceAll("\"",""));
                decimal1 = BigDecimal.valueOf(val1).setScale(2, BigDecimal.ROUND_DOWN);
                decimal2 = BigDecimal.valueOf(val2).setScale(2, BigDecimal.ROUND_DOWN);
                Assert.assertEquals(decimal1,decimal2);
                System.out.println(decimal1+"  "+decimal2);

                //for user type verification in redshift
                //to Verify the USER type in s3
                if (row.getAs("dataStreamType").toString().equals("AMI")) {
                    logger.info("verifying the user type in S3 based on Meter type");
                    Assert.assertEquals(jsonNode.get("user_type").toString().replaceAll("\"",""), Utils.UserTypeAMIMeter);
                } else if (row.getAs("dataStreamType").toString().equals("AMR")) {
                    logger.info("verifying the user type in S3 based on Meter type");
                    Assert.assertEquals(jsonNode.get("user_type").toString().replaceAll("\"",""), Utils.UserTypeAMRMeter);
                }
                else
                {
                    logger.info("verifying the user type in S3 based on Meter type");
                    Assert.assertEquals(jsonNode.get("user_type").toString().replaceAll("\"",""), Utils.UserTypeNSMMeter);
                }

                //to verify the Measurement type in s3
                if (row.getAs("fuelType").toString().equals("ELECTRIC")) {
                    logger.info("verifying the measurement type in S3 based on Meter type");
                    Assert.assertEquals(jsonNode.get("measurement_type").toString().replaceAll("\"",""), "Electricity");
                }

                //to verify the hid
                Assert.assertEquals(jsonNode.get("hid").toString().replaceAll("\"",""), "1");

                //to verify the transition bill cycle
                Assert.assertEquals(jsonNode.get("transition_bill_cycle").toString().replaceAll("\"",""), "false");

            } else {
                System.out.println("Following Records not present is the Redshift");
                System.out.println(row.toString());
            }
        }
    }
    }


