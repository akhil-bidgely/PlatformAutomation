package Ingestion;

import PojoClasses.MeterFilePOJO;
import PojoClasses.RawFilePOJO;
import PojoClasses.UserFilePOJO;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.opencsv.exceptions.CsvException;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hamcrest.Matchers;
import org.awaitility.Awaitility;
import org.json.JSONObject;
import org.json.simple.parser.ParseException;
import ServiceHelper.RestUtils;
import CommonUtils.JsonUtils;
import CommonUtils.Utils;
import org.testng.annotations.*;

import java.io.IOException;
import java.sql.Time;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static CommonUtils.JsonUtils.getAuthToken;
import static CommonUtils.Utils.getTimeStampsConsumption;
import static CommonUtils.Utils.processRawFile;
import static Constants.FilePaths.*;
import static Constants.PilotIDs.AMEREN_PILOT_ID;


public class UserIngestion extends BaseTest{
    String token= "";
    String uuid= "";
    Map<String, String> executionVariables = JsonUtils.getExecutionVariables();

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

    @Test
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
