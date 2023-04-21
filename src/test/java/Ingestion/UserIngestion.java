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
import java.util.HashMap;
import java.util.Map;

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
    void userFileIngestion () throws IOException {

        //Internal bucket file upload
        UserFilePOJO fileInfo=userFileAmi.processFile(userFilePOJO);
        String baseUrl=jsonDataAsMap.get("baseUri");

        S3Upload.s3UploadFile(fileInfo);

        Response partnerUserIdResponse= RestUtils.getPartnerUserId(baseUrl,token,fileInfo.getPremise_id());
        uuid=JsonUtils.getUuidFromPremiseId(partnerUserIdResponse);

        Response response= RestUtils.getUsers(baseUrl,uuid,token);
        Assert.assertEquals(response.getStatusCode(),200);

        ingestionValidations.validateParams(response.asString(),fileInfo);
    }
}
