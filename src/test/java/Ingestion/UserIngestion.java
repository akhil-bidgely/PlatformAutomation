package Ingestion;

import PojoClasses.UserFileAmi;
import io.restassured.response.Response;
import org.json.JSONObject;
import org.testng.Assert;
import restutils.RestUtils;
import utils.JsonUtils;
import utils.S3Upload;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

public class UserIngestion extends BaseTest{
    UserFileAmi userFileAmi = new UserFileAmi();

    String token= "";
    String uuid= "fdc5bf7a-f60f-45f1-95be-6164f8a5a07d";
    Map<String, String> jsonDataAsMap = JsonUtils.getJsonDataAsMap("platformQAData.json");
    @BeforeMethod
    public void generateToken() throws IOException {
        Response response= RestUtils.generateToken(jsonDataAsMap.get("baseUri"));
        System.out.println(response.getBody().asString());
        JSONObject obj= new JSONObject(response.getBody().asString());
        token=obj.getString("access_token");
    }

    private static Logger logger = Logger.getLogger("Mylog");
    @Test
    void userFileIngestion () throws IOException {

        //Internal bucket file upload
        String filePath=userFileAmi.processFile();
        S3Upload.s3UploadFile(filePath);

        Response response= RestUtils.getUsers(jsonDataAsMap.get("baseUri"),uuid,token);
        Assert.assertEquals(response.getStatusCode(),200);

        System.out.println(response.getBody().prettyPrint());
        ingestionValidations.validateParams(response.asString());
    }
}
