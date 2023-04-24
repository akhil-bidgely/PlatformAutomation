package Ingestion;

import PojoClasses.UserFilePOJO;
import io.restassured.response.Response;
import org.json.JSONObject;
import org.testng.Assert;
import ServiceHelper.RestUtils;
import CommonUtils.JsonUtils;
import CommonUtils.S3Upload;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.Map;

import static CommonUtils.JsonUtils.getRandom10Digits;

public class UserIngestion extends BaseTest{
    String token= "";
    String uuid= "";
    Map<String, String> jsonDataAsMap = JsonUtils.getJsonDataAsMap("platformQAData.json");
    @BeforeMethod
    public void generateToken() throws IOException {
        Response response= RestUtils.generateToken(jsonDataAsMap);
        System.out.println(response.getBody().asString());
        JSONObject obj= new JSONObject(response.getBody().asString());
        token=obj.getString("access_token");
    }

    @Test
    void fileIngestion () throws IOException {

        String account_id=getRandom10Digits();
        String premiseId=getRandom10Digits();
        String user_segment=getRandom10Digits();

        //Internal bucket file upload
        UserFilePOJO fileInfo=userFileAmi.processFile(userFilePOJO,premiseId,account_id,user_segment);
        String baseUrl=jsonDataAsMap.get("baseUri");

        S3Upload.s3UploadFile(fileInfo);

        Response partnerUserIdResponse= RestUtils.getPartnerUserId(baseUrl,token,premiseId);
        uuid=JsonUtils.getUuidFromPremiseId(partnerUserIdResponse);

        Response usersApiResponse= RestUtils.getUsers(baseUrl,uuid,token);
        Assert.assertEquals(usersApiResponse.getStatusCode(),200);

        ingestionValidations.validateParams(usersApiResponse.asString(),fileInfo);

        fileInfo=userFileAmi.processFile(premiseId,account_id,user_segment);
        S3Upload.s3UploadFile(fileInfo);
        Response metersApiResponse= RestUtils.getMetersApi(baseUrl,uuid,token);
    }
}
