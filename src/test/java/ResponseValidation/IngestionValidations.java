package ResponseValidation;

import CommonUtils.Utils;
import PojoClasses.MeterFilePOJO;
import PojoClasses.UserFilePOJO;
import io.restassured.response.Response;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.asserts.SoftAssert;

import java.util.HashMap;
import java.util.Map;

public class IngestionValidations {
    public void validateUserDetails(Response response, UserFilePOJO userFilePOJO, String uuid, String timeZone){
        SoftAssert softAssert=new SoftAssert();
        JSONObject obj= new JSONObject(response.asString());
        JSONObject payload=obj.getJSONObject("payload");


        Map<String,Object> expectedValueMap = new HashMap<>();
        expectedValueMap.put("uuid",payload.getString("uuid"));
        Utils.assertExpectedValuesWithJsonPath(response,expectedValueMap);

        softAssert.assertEquals(uuid,payload.getString("uuid"),"Validation of UUID");
        softAssert.assertEquals(userFilePOJO.getFirst_name(),payload.getString("firstName"),"Validation of First Name");
        softAssert.assertEquals(userFilePOJO.getLast_name(),payload.getString("lastName"),"Validation of Last Name");
        softAssert.assertEquals("ENABLED",payload.getString("status"),"Validation of status");
        softAssert.assertEquals(userFilePOJO.getEmail(),payload.getString("email"),"Validation of email");
        softAssert.assertEquals("OPT_OUT",payload.getString("notificationUserType"),"Validation of notificationUserType");

        JSONObject homeAccounts=payload.getJSONObject("homeAccounts");
        softAssert.assertEquals(userFilePOJO.getAddress_1(),homeAccounts.getString("address"),"Validation of address line 1");
        softAssert.assertEquals(userFilePOJO.getCity(),homeAccounts.getString("city"),"Validation of city");
        softAssert.assertEquals(userFilePOJO.getPostal_code(),homeAccounts.getString("postalCode"),"Validation of postalCode");
        softAssert.assertEquals(timeZone,homeAccounts.getString("timeZone"),"Validation of timeZone");

        JSONObject utilityTags=payload.getJSONObject("utilityTags");
        softAssert.assertEquals(userFilePOJO.getPartner_user_id(),utilityTags.getString("account_number"),"Validation of account_number");
        softAssert.assertEquals(userFilePOJO.getPartner_user_id()+":"+userFilePOJO.getPremise_id(),utilityTags.getString("account_and_premise_number"),"Validation of account_and_premise_number");
        softAssert.assertEquals(userFilePOJO.getCustomer_id(),utilityTags.getString("customer_id"),"Validation of customer_id");

        softAssert.assertAll();

    }

    public void validateMeters(String response, String uuid, String pilotId, MeterFilePOJO meterFilePOJO){
        SoftAssert softAssert=new SoftAssert();
        JSONObject obj= new JSONObject(response);
        JSONObject jsonObj=obj.getJSONObject("/users/"+uuid+"/homes/1/gws/2/meters/1");
        softAssert.assertEquals(jsonObj.getString("model"),"GreenButton","Validation of Model");
        softAssert.assertEquals(jsonObj.getString("token"),pilotId+":_"+meterFilePOJO.getPartner_user_id()+"_"+meterFilePOJO.getPremise_id()+"_"+meterFilePOJO.getData_stream_id()+"_"+meterFilePOJO.getData_stream_type(),"Validation of token");
        softAssert.assertEquals(jsonObj.getString("id"),meterFilePOJO.getService_type()+"0","Validation of id");
        softAssert.assertAll();
    }
}
