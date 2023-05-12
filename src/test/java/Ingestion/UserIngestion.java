package Ingestion;

import Constants.S3Constants;
import Constants.UserType;
import PojoClasses.UserFilePOJO;
import ResponseValidation.IngestionValidations;
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
import java.time.Duration;
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

    Dataset<Row> rowDatasetNewS3;

    Dataset<Row> rowDatasetInvoiceFileTotal;

    Dataset<Row> dfMeterFile;

    JsonNode jsonArrayFromRedshift;




    @BeforeMethod
    public void generateToken() {
        Response response= restUtils.generateToken();
        token=getAuthToken(response);
    }

    @BeforeClass
    public void beforeClassMethod()
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

        //to process the input invoice file
        String filePath=System.getProperty("user.dir")+"/src/test/resources/Ameren/AMI_E/INVOICE_600401000.csv";
        Dataset<Row> dfInvoiceFile = Utils.readInvoiceInputFile(spark, filePath);
        //to filter based on the billingStartDate and charge type is Total as in Firehose S3 we store only the charge Type Total
        rowDatasetInvoiceFileTotal= dfInvoiceFile.filter(dfInvoiceFile.col("chargeType").equalTo("TOTAL")).orderBy(dfInvoiceFile.col("billingStartDate"));

        String filePath2=System.getProperty("user.dir")+"/src/test/resources/Ameren/AMI_E/METERENROLL_D_600401000.csv";
        //To define the schema of the input invoice File
        dfMeterFile =Utils.readMeterInputFile(spark,filePath2);
    }

    @Test(enabled = true)
    public void testFireHoseDataValidation()
    {
       //to compute date and format it in the path format
       //for e.g  /utility_billing_data_firehose/2023/05/09/09/

        String date=Utils.getTodayDate();
        String  utcHour=Utils.getUtcTime();
        date="2023/05/11";
        utcHour="11";
        String bucket=S3Constants.CommonMetricsNonprodqaBucket;
        String path= S3Constants.UtilityBillingDataFirehosePrefix+date+"/"+utcHour+"/*";
        String uuid="1f883911-72e0-49ad-b0f9-23d99fa82e15";

        //to read the data from S3 Firehose
        readS3Data(date, bucket, path, uuid);
        if(rowDatasetNewS3.count()==0)
        {
            Assert.fail("Number of records not found in S3 location for uuid within "+S3Constants.MaxWaitTimeForS3Search+"  "+uuid);
        }


        //to apply the join based on billing start date
        Dataset <Row> joinedData = rowDatasetInvoiceFileTotal.join(rowDatasetNewS3, rowDatasetInvoiceFileTotal.col("billingStartDate").equalTo(rowDatasetNewS3.col("billing_start_time")),"left_outer");
        logger.info("the data after applying the left outer join is ====");
        joinedData.show(100);


        String solar=dfMeterFile.first().getAs("solar");
        boolean solarFieldFromMeterFile = solar.equals("False")?false:true;
        //to validate data between s3 and input file
        ingestionValidations.validateFirehoseS3(joinedData,solarFieldFromMeterFile);
    }



    @Test(dependsOnMethods = "testFireHoseDataValidation")
    public void testCountInFirehoseS3WithInputFile()
    {

        logger.info("the number of records found in input invoice file with charge type total as  "+rowDatasetInvoiceFileTotal.count());
        logger.info("the number of records found in S3 bidgely_generated_invoice as false is "+rowDatasetNewS3.count());
        Assert.assertEquals(rowDatasetNewS3.count(),rowDatasetInvoiceFileTotal.count());
    }

    @Test(enabled=true)
    public void testRedshiftDataValidation() throws IOException, java.text.ParseException {
        String uuid="1f883911-72e0-49ad-b0f9-23d99fa82e15";
        String query = "select * from utility_billing_data where uuid='"+uuid+"' and bidgely_generated_invoice='false' order by billing_start_time asc";
        Response response = restUtils.postRedshiftQuery(query);
        Assert.assertEquals(response.getStatusCode(), 200);
        logger.info(response.asString());


        // Extract the response body as a String
        String responseBody = response.getBody().asString();

        // Parse the response body as a JSON array using Jackson
        ObjectMapper mapper = new ObjectMapper();
        jsonArrayFromRedshift = mapper.readTree(responseBody);


        rowDatasetInvoiceFileTotal.show(20);

        HashMap<String, JsonNode> hm = new HashMap<String, JsonNode>();
        for (JsonNode jsonNode : jsonArrayFromRedshift) {

            String startTime = jsonNode.get("billing_start_time").asText();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date stDate = dateFormat.parse(startTime);
            String startFormatedDate = dateFormat.format(stDate);
            hm.put(startFormatedDate, jsonNode);
        }

        logger.info(hm.toString());
        String solar=dfMeterFile.first().getAs("solar");
        boolean solarFieldFromMeterFile = solar.equals("False")?false:true;
        IngestionValidations.validateRedshiftData(rowDatasetInvoiceFileTotal,hm,solarFieldFromMeterFile);
    }

    @Test(dependsOnMethods = "testRedshiftDataValidation")
    public void testCountInRedshiftWithInputFile()
    {
        logger.info("Number of records in input file is  "+rowDatasetInvoiceFileTotal.count());
        logger.info("Number of records in Redshift file is   "+jsonArrayFromRedshift.size());
        Assert.assertEquals(jsonArrayFromRedshift.size(),rowDatasetInvoiceFileTotal.count());
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
    private void readS3Data(String date, String bucket, String path, String uuid) {
        Instant startTime = Instant.now();  // Record the start time of the loop
        Duration maxDuration = Duration.ofMinutes(S3Constants.MaxWaitTimeForS3Search);  // Set the maximum duration to 10 minutes


        do {
            Dataset<Row> df = Utils.getS3FirehoseData(spark, bucket, path);

            Dataset<Row> rowS3Dataset = df.filter(df.col("uuid").equalTo(uuid))
                    .filter(df.col("bidgely_generated_invoice").equalTo("false"))
                    .orderBy(df.col("billing_start_time"));

            rowDatasetNewS3= rowS3Dataset.withColumn("billing_start_time", from_unixtime(rowS3Dataset.col("billing_start_time").divide(1000), "yyyy-MM-dd"))
                    .withColumn("billing_end_time", from_unixtime(rowS3Dataset.col("billing_end_time").divide(1000), "yyyy-MM-dd"));

            logger.info("the number of records found in  is"+rowDatasetNewS3.count());
            if(rowDatasetNewS3.count()==0)
            {
                path = S3Constants.UtilityBillingDataFirehosePrefix+ date +"/*";
            }
            Instant currentTime = Instant.now();  // Record the current time
            Duration elapsed = Duration.between(startTime, currentTime);  // Calculate the elapsed time

            if (elapsed.compareTo(maxDuration) > 0) {  // Compare the elapsed time to the maximum duration
                // Exit the loop if the elapsed time is greater than the maximum duration
                break;
            }
        } while(rowDatasetNewS3.count()==0);
    }


    @AfterClass
    public void afterClassMethod()
    {
        spark.stop();

    }

}


