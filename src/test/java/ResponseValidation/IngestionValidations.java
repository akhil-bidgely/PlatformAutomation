package ResponseValidation;

import io.restassured.response.Response;
import org.json.JSONObject;
import org.testng.Assert;

public class IngestionValidations {
    public void validateParams(String response){
        JSONObject obj= new JSONObject(response);

        String reqId=obj.getString("requestId");
        Assert.assertNotNull(reqId,"request Id null");

    }
}
