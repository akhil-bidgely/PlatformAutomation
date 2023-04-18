package Ingestion;

import io.restassured.response.Response;
import org.testng.Assert;
import restutils.RestUtils;
import utils.JsonUtils;
import utils.S3Upload;
import org.testng.annotations.*;

import java.util.Map;

public class UserIngestion extends BaseTest{

     String uuid= "fdc5bf7a-f60f-45f1-95be-6164f8a5a07d";
     String token= "bee07df3-b265-42b9-acea-b8180fd8c1e5";
    @Test
    void demo1(){
        S3Upload.s3UploadFile();
        Map<String, String> jsonDataAsMap = JsonUtils.getJsonDataAsMap("platformQAData.json");
        //Internal bucket file upload
        Response response= RestUtils.getUsers(jsonDataAsMap.get("baseUri"),uuid,token);
        Assert.assertEquals(response.getStatusCode(),200);
        System.out.println(response.asString());

    }
}
