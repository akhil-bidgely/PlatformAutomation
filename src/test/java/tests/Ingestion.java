package tests;

import com.amazonaws.thirdparty.jackson.databind.JsonNode;
import com.amazonaws.thirdparty.jackson.databind.ObjectMapper;
import constants.ConstantFile;
import constants.FilePaths;
import dataProviderFile.IngestionsDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import pojoClasses.UserFilePOJO;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import io.restassured.response.Response;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import commonUtils.JsonUtils;
import commonUtils.Utils;
import org.testng.annotations.*;
import responseValidation.IngestionValidations;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static commonUtils.JsonUtils.getAuthToken;
import static commonUtils.Utils.*;
import static constants.ConstantFile.AMEREN_PILOT_ID;
import static constants.FilePaths.METER_ENROLLMENT_AMI_E_PATH;
import static org.apache.spark.sql.functions.from_unixtime;

public class Ingestion extends BaseTest{
    String token= "";
    private static Logger logger = LoggerFactory.getLogger(Ingestion.class);
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

    }

    @Test(alwaysRun = true, dataProvider = "singleMeterDP", dataProviderClass = IngestionsDataProvider.class,priority = 0)
    public void singleMeterIngestion (String scenario,String userFilePath, String meterFilePath, String rawFilePath_1, String invoiceFilePath_1,String model, int gws) throws IOException, java.text.ParseException {

        //Map of variable to be changed in csv files
        Map<String, String> executionVariables = JsonUtils.getExecutionVariables();

        //USERENROLL file upload
//        UserFilePOJO userFilePOJO= new UserFilePOJO();
        String userTempFilePath=processFile(userFilePath,executionVariables,userFilePOJO, meterFilePOJO,executionVariables.get("dataStreamId"));
        Utils.s3UploadFile(userTempFilePath);

        //METERENROLL file upload
        String meterTempFilePath=processFile(meterFilePath,executionVariables,userFilePOJO,meterFilePOJO,executionVariables.get("dataStreamId"));
        Utils.s3UploadFile(meterTempFilePath);

        //RAW file upload
        String rawTempFilePath=processFile(rawFilePath_1,executionVariables,userFilePOJO,meterFilePOJO,executionVariables.get("dataStreamId"));
        Utils.s3UploadFile(rawTempFilePath);

        //Invoice file upload
        String invoiceTempFilePath=processFile(invoiceFilePath_1,executionVariables,userFilePOJO,meterFilePOJO,executionVariables.get("dataStreamId"));
        Utils.s3UploadFile(invoiceTempFilePath);

        //TODO : Add awaitility wait instead of hard wait
//        Thread.sleep(5000);
        Response partnerUserIdResponse= restUtils.getPartnerUserId(token,executionVariables);
        userFilePOJO.setUuid(JsonUtils.getUuidFromPremiseId(partnerUserIdResponse));

        //Calling Pilot config API
        Response getPilotConfigResponse= restUtils.getPilotConfigs(token,AMEREN_PILOT_ID);
        String timeZone=JsonUtils.getTimeZone(getPilotConfigResponse);

        //Calling User Details API
        Response usersApiResponse= restUtils.getUsers(userFilePOJO.getUuid(),token);

        //Validating User Details API response
        ingestionValidations.validateUserDetails(usersApiResponse,userFilePOJO,timeZone,executionVariables,AMEREN_PILOT_ID);

        //Calling Meter API
        Response metersApiResponse= restUtils.getMetersApi(userFilePOJO.getUuid(),token,gws);
        restUtils.printResponseLogInReport(metersApiResponse);
        ingestionValidations.validateMeters(metersApiResponse,scenario,userFilePOJO.getUuid(),AMEREN_PILOT_ID,executionVariables,meterFilePOJO,gws,1,model);

        String t1 = String.valueOf(Instant.now().getEpochSecond());

        //Calling Label TimeStamp API
        Response label= restUtils.getLabelTimeStamp(userFilePOJO.getUuid(),token);
        ingestionValidations.validateLableTimeStamp(label.asString());

        //Calling gbJson API
        Response gbJsonApiResponse= restUtils.getGbJsonApi(userFilePOJO.getUuid(),token,t1,scenario,gws);
        Map<String, String> mapTimestampConsumption = getTimeStampsConsumption(rawTempFilePath);
        ingestionValidations.validateGbJsonConsumption(gbJsonApiResponse,mapTimestampConsumption);

        //Internal bucket Invoice file upload
        t1 = String.valueOf(Instant.now().getEpochSecond());

        //Calling INVOICE API
        Response utilityDataResponse= restUtils.getUtilityData(userFilePOJO.getUuid(),token,t1);
        Map<String, Map<String, Map<String,String>>> mapTimestampCostData = getTimeStampsInvoiceData(invoiceTempFilePath);
        ingestionValidations.validateUtilityData(utilityDataResponse,mapTimestampCostData);

    }

    @Test(enabled = false,alwaysRun = true, dataProvider = "multimeterDP", dataProviderClass = IngestionsDataProvider.class)
    public void multiMeterIngestion (String scenario,String userFilePath, String meterFilePath, String rawFilePath_1, String rawFilePath_2, String invoiceFilePath_1,
                                     String invoiceFilePath_2,String model, int gws) throws IOException, java.text.ParseException {

        //Map of variable to be changed in csv files
        Map<String, String> executionVariables = JsonUtils.getExecutionVariables();
        String dataStreamId1=executionVariables.get("dataStreamId");
        String dataStreamId2= String.valueOf(Long.parseLong(executionVariables.get("dataStreamId"))+1);

        //USERENROLL file upload
        UserFilePOJO userFilePOJO= new UserFilePOJO();
        String userTempFilePath=processFile(userFilePath,executionVariables,userFilePOJO, meterFilePOJO,executionVariables.get("dataStreamId"));
        Utils.s3UploadFile(userTempFilePath);

        //METERENROLL file upload
        String meterTempFilePath=processFile(meterFilePath,executionVariables,null,meterFilePOJO,executionVariables.get("dataStreamId"));
        Utils.s3UploadFile(meterTempFilePath);

        //RAW file upload
        String rawTempFilePath1=processFile(rawFilePath_1,executionVariables,userFilePOJO,meterFilePOJO,dataStreamId1);
        Utils.s3UploadFile(rawTempFilePath1);

        //Invoice file upload
        String invoiceTempFilePath1=processFile(invoiceFilePath_1,executionVariables,userFilePOJO,meterFilePOJO,dataStreamId1);
        Utils.s3UploadFile(invoiceTempFilePath1);

        //RAW file upload
        String rawTempFilePath2=processFile(rawFilePath_2,executionVariables,userFilePOJO,meterFilePOJO, dataStreamId2);
        Utils.s3UploadFile(rawTempFilePath2);

        //Invoice file upload
        String invoiceTempFilePath2=processFile(invoiceFilePath_2,executionVariables,userFilePOJO,meterFilePOJO, dataStreamId2);
        Utils.s3UploadFile(invoiceTempFilePath2);

        //TODO : Add awaitility wait instead of hard wait
//        Thread.sleep(5000);
        Response partnerUserIdResponse= restUtils.getPartnerUserId(token,executionVariables);
        userFilePOJO.setUuid(JsonUtils.getUuidFromPremiseId(partnerUserIdResponse));

        //Calling Pilot config API
        Response getPilotConfigResponse= restUtils.getPilotConfigs(token,AMEREN_PILOT_ID);
        String timeZone=JsonUtils.getTimeZone(getPilotConfigResponse);

        //Calling User Details API
        Response usersApiResponse= restUtils.getUsers(userFilePOJO.getUuid(),token);

        //Validating User Details API response
        ingestionValidations.validateUserDetails(usersApiResponse,userFilePOJO,timeZone,executionVariables,AMEREN_PILOT_ID);

        //Calling Meter API
        Response metersApiResponse= restUtils.getMetersApi(userFilePOJO.getUuid(),token,gws);
        restUtils.printResponseLogInReport(metersApiResponse);
        ingestionValidations.validateMeters(metersApiResponse,scenario,userFilePOJO.getUuid(),AMEREN_PILOT_ID,executionVariables,meterFilePOJO,gws,2,model);

        String t1 = String.valueOf(Instant.now().getEpochSecond());

        //Calling Label TimeStamp API
        Response label= restUtils.getLabelTimeStamp(userFilePOJO.getUuid(),token);
        ingestionValidations.validateLableTimeStamp(label.asString());

        //Calling gbJson API
        Response gbJsonApiResponse= restUtils.getGbJsonApi(userFilePOJO.getUuid(),token,t1,scenario,gws );
        Map<String, String> mapTimestampConsumption = new HashMap<>();
        mapTimestampConsumption.putAll(getTimeStampsConsumption(rawTempFilePath1));
        mapTimestampConsumption.putAll(getTimeStampsConsumption(rawTempFilePath2));
        ingestionValidations.validateGbJsonConsumption(gbJsonApiResponse,mapTimestampConsumption);

        //Internal bucket Invoice file upload
        t1 = String.valueOf(Instant.now().getEpochSecond());

        //Calling INVOICE API
        Response utilityDataResponse= restUtils.getUtilityData(userFilePOJO.getUuid(),token,t1);
        Map<String, Map<String, Map<String,String>>> mapTimestampCostData = new HashMap<>();
        mapTimestampCostData.putAll(getTimeStampsInvoiceData(invoiceTempFilePath1));
        ingestionValidations.validateUtilityData(utilityDataResponse,mapTimestampCostData);
        mapTimestampCostData.clear();
        mapTimestampCostData.putAll(getTimeStampsInvoiceData(invoiceTempFilePath2));
        ingestionValidations.validateUtilityData(utilityDataResponse,mapTimestampCostData);
    }

    @Test(enabled = true,priority = 1)
    public void testFireHoseDataValidation()
    {
        //to compute date and format it in the path format
        //for e.g  /utility_billing_data_firehose/2023/05/09/09/
        readInputFiles();
        String date=Utils.getTodayDate();
        String  utcHour=Utils.getUtcTime();
        String bucket= ConstantFile.CommonMetricsNonprodqaBucket;
        String path= ConstantFile.UtilityBillingDataFirehosePrefix+date+"/"+utcHour+"/";
        String uuid=userFilePOJO.getUuid();
        logger.info("Starting the TC testFireHoseDataValidation for UUID as "+uuid);
        logger.info("the bucket and path is  "+bucket+path);
        logger.info("waiting for the ingestion event sent to Firehose S3 for 5 minutes");
        try {
            Thread.sleep(5 * 60 * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //to read the data from S3 Firehose
        readS3Data( bucket, path, uuid,date);
        if(rowDatasetNewS3.count()==0)
        {
            Assert.fail("Number of records not found in S3 location for uuid within "+ConstantFile.MaxWaitTimeForS3Search+"  "+uuid);
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



    @Test(dependsOnMethods = "testFireHoseDataValidation",priority = 2)
    public void testCountInFirehoseS3WithInputFile()
    {

        logger.info("the number of records found in input invoice file with charge type total as  "+rowDatasetInvoiceFileTotal.count());
        logger.info("the number of records found in S3 bidgely_generated_invoice as false is "+rowDatasetNewS3.count());
        Assert.assertEquals(rowDatasetNewS3.count(),rowDatasetInvoiceFileTotal.count());
    }

    @Test(enabled=true,priority = 4)
    public void testRedshiftDataValidation() throws IOException, java.text.ParseException {
        String uuid=userFilePOJO.getUuid();
        //The current date records will be present in utility_billing_data_firehose table after that the records will be pushed to utility_billing_data table
        String query = "select * from utility_billing_data_firehose where uuid='"+uuid+"' and bidgely_generated_invoice='false' order by billing_start_time asc";
        Response response = restUtils.postRedshiftQuery(query);
        Assert.assertEquals(response.getStatusCode(), 200);
        logger.info(response.asString());


        // Extract the response body as a String
        String responseBody = response.getBody().asString();
        logger.info("the response from redshift is "+responseBody);

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

    @Test(dependsOnMethods = "testRedshiftDataValidation",priority = 3)
    public void testCountInRedshiftWithInputFile()
    {
        logger.info("Number of records in input file is  "+rowDatasetInvoiceFileTotal.count());
        logger.info("Number of records in Redshift file is   "+jsonArrayFromRedshift.size());
        Assert.assertEquals(jsonArrayFromRedshift.size(),rowDatasetInvoiceFileTotal.count());
    }

    private void readInputFiles()
    {
        //DP-1757 as data greater than currentDate-395 Days will be considered for UtilityData firehose streaming
        LocalDate currentDate = LocalDate.now();
        LocalDate dateBefore395Days = currentDate.minusDays(395);


        //to process the input invoice file
        String filePath= FilePaths.INVOICE_AMI_E_PATH;
        Dataset<Row> dfInvoiceFile = Utils.readInvoiceInputFile(spark, filePath);
        //to filter based on the billingStartDate and charge type is Total as in Firehose S3 we store only the charge Type Total
        rowDatasetInvoiceFileTotal= dfInvoiceFile.filter(dfInvoiceFile.col("chargeType").equalTo("TOTAL")).filter((dfInvoiceFile.col("billingStartDate").geq(dateBefore395Days))).orderBy(dfInvoiceFile.col("billingStartDate"));

        rowDatasetInvoiceFileTotal.show(100);
        String filePath2=METER_ENROLLMENT_AMI_E_PATH;
        //To define the schema of the input invoice File
        dfMeterFile =Utils.readMeterInputFile(spark,filePath2);
    }


    private void readS3Data( String bucket, String path, String uuid,String date) {
        Instant startTime = Instant.now();  // Record the start time of the loop
        Duration maxDuration = Duration.ofMinutes(ConstantFile.MaxWaitTimeForS3Search);  // Set the maximum duration to 10 minutes


        do {
            String s3path="s3a://"+bucket+path;
                Dataset<Row> df = Utils.getS3FirehoseData(spark, s3path);

                Dataset<Row> rowS3Dataset = df.filter(df.col("uuid").equalTo(uuid))
                        .filter(df.col("bidgely_generated_invoice").equalTo("false"))
                        .orderBy(df.col("billing_start_time"));

                rowDatasetNewS3 = rowS3Dataset.withColumn("billing_start_time", from_unixtime(rowS3Dataset.col("billing_start_time").divide(1000), "yyyy-MM-dd"))
                        .withColumn("billing_end_time", from_unixtime(rowS3Dataset.col("billing_end_time").divide(1000), "yyyy-MM-dd"));

                logger.info("the number of records found in  is" + rowDatasetNewS3.count());
            if(rowDatasetNewS3.count()==0)
            {
                path = ConstantFile.UtilityBillingDataFirehosePrefix+ date +"/*";
            }
            Instant currentTime = Instant.now();  // Record the current time
            Duration elapsed = Duration.between(startTime, currentTime);  // Calculate the elapsed time

            if (elapsed.compareTo(maxDuration) > 0) {  // Compare the elapsed time to the maximum duration
                // Exit the loop if the elapsed time is greater than the maximum duration
                break;
            }
        } while(rowDatasetNewS3.count()==0);
    }




}
