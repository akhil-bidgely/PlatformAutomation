package ResponseValidation;

import PojoClasses.UserFilePOJO;
import io.restassured.response.Response;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;

public class IngestionValidations {
    public void validateParams(String response, UserFilePOJO userFilePOJO){
        JSONObject obj= new JSONObject(response);
        JSONObject utilityTags=obj.getJSONObject("payload").getJSONObject("utilityTags");

        Assert.assertEquals(userFilePOJO.getAccount_id(),utilityTags.getString("customer_id"));
        Assert.assertEquals(userFilePOJO.getPremise_id(),utilityTags.getString("account_number"));

    }
}
