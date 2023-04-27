package Ingestion;

import PojoClasses.MeterFilePOJO;
import PojoClasses.RawFilePOJO;
import PojoClasses.UserFilePOJO;
import com.opencsv.exceptions.CsvException;
import io.restassured.response.Response;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.awaitility.Awaitility;
import org.json.JSONObject;
import org.json.simple.parser.ParseException;
import ServiceHelper.RestUtils;
import CommonUtils.JsonUtils;
import CommonUtils.Utils;
import org.testng.annotations.*;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static CommonUtils.JsonUtils.getRandom10Digits;
import static CommonUtils.Utils.processRawFile;
//import static CommonUtils.Utils.writeCsv;

//@Listeners(ExtentITestListenerClassAdapter.class)
public class UserIngestion extends BaseTest{
    String token= "";
    String uuid= "";
    Map<String, String> jsonDataAsMap = JsonUtils.getJsonDataAsMap("platformQAData.json");

    @BeforeMethod
    public void generateToken() {
        Response response= RestUtils.generateToken(jsonDataAsMap);
        System.out.println(response.getBody().asString());
        JSONObject obj= new JSONObject(response.getBody().asString());
        token=obj.getString("access_token");
    }

    @Test
    public void fileIngestion () throws IOException, ParseException, InterruptedException, java.text.ParseException, CsvException {

        String customerId=getRandom10Digits();
        String partnerUserId=getRandom10Digits();
        String premiseId=getRandom10Digits();
        String dataStreamId=getRandom10Digits();

        //Internal buck et USERENROLL file upload
        UserFilePOJO userfileInfo=userFileAmi.processFile(userFilePOJO,customerId,partnerUserId,premiseId);
        String baseUrl=jsonDataAsMap.get("baseUri");
        String pilotId=jsonDataAsMap.get("pilotId");

        Utils.s3UploadFile(userfileInfo.getFile_abs_path());

        //Calling Partner User API
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> RestUtils.getPartnerUserId(baseUrl,token,partnerUserId).getStatusCode() ==200);
        Response partnerUserIdResponse= RestUtils.getPartnerUserId(baseUrl,token,partnerUserId);
        uuid=JsonUtils.getUuidFromPremiseId(partnerUserIdResponse);

        //Calling Pilot config API
        Response getPilotConfigResponse= RestUtils.getPilotConfigs(baseUrl,token,jsonDataAsMap.get("pilotId"));
        String timeZone=JsonUtils.getTimeZone(getPilotConfigResponse);

        //Calling User Details API
        Response usersApiResponse= RestUtils.getUsers(baseUrl,uuid,token);

        //Validating User Details API response
        ingestionValidations.validateUserDetails(usersApiResponse,userfileInfo,uuid,timeZone);

        //Internal bucket METERENROLL file upload
        MeterFilePOJO meterfileInfo=userFileAmi.processFile(meterFilePOJO,customerId,partnerUserId,premiseId,dataStreamId);
        Utils.s3UploadFile(meterfileInfo.getFile_abs_path());
        Response metersApiResponse= RestUtils.getMetersApi(baseUrl,uuid,token);
        ingestionValidations.validateMeters(metersApiResponse.asString(),uuid,pilotId,meterFilePOJO);

        //Internal bucket RAW file upload
        String rawfile=System.getProperty("user.dir")+"/src/test/resources/AMI_E/RAW_D_900_S_500400306.csv";
        String rawTempFilePath=processRawFile(rawfile,customerId,partnerUserId,premiseId,dataStreamId);
        Utils.s3UploadFile(rawTempFilePath);
        String t1 = String.valueOf(Instant.now().getEpochSecond());
        Response gbJsonApiResponse= RestUtils.getGbJsonApi(baseUrl,uuid,token,t1);



    }
}
