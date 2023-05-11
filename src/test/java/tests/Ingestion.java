package tests;

import DataProviderFile.IngestionsDataProvider;
import PojoClasses.UserFilePOJO;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import io.restassured.response.Response;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import CommonUtils.JsonUtils;
import CommonUtils.Utils;
import org.testng.annotations.*;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static CommonUtils.JsonUtils.getAuthToken;
import static CommonUtils.Utils.*;
import static Constants.ConstantFile.AMEREN_PILOT_ID;

public class Ingestion extends BaseTest{
    String token= "";

    @BeforeMethod
    public void generateToken() {
        Response response= restUtils.generateToken();
        token=getAuthToken(response);
    }

    @Test(alwaysRun = true, dataProvider = "singleMeterDP", dataProviderClass = IngestionsDataProvider.class)
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

    @Test(alwaysRun = true, dataProvider = "multimeterDP", dataProviderClass = IngestionsDataProvider.class)
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
        mapTimestampCostData.putAll(getTimeStampsInvoiceData(invoiceTempFilePath2));
        ingestionValidations.validateUtilityData(utilityDataResponse,mapTimestampCostData);

    }

    @Test(enabled = false)
    public void testFireHose()
    {
        DefaultAWSCredentialsProviderChain props = new DefaultAWSCredentialsProviderChain();
        AWSCredentials credentials = props.getCredentials();

        final String AWS_ACCESS_KEY_ID = credentials.getAWSAccessKeyId();
        final String AWS_SECRET_ACCESS_KEY = credentials.getAWSSecretKey();
        SparkSession spark = SparkSession.builder()
                .appName("My Application")
                .config("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
                .config("fs.s3a.secret.key",AWS_SECRET_ACCESS_KEY)
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().option("inferSchema",true).json("s3a://bidgelynonprodqa-firehose/user_aggregation_data/2023/04/22/01");

        df.show();
        df.select("uuid").show(false);
    }



}
