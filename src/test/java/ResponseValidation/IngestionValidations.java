package ResponseValidation;

import CommonUtils.Utils;
import PojoClasses.MeterFilePOJO;
import PojoClasses.UserFilePOJO;
import io.restassured.module.jsv.JsonSchemaValidator;
import io.restassured.response.Response;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.asserts.SoftAssert;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static Constants.Endpoints.GET_USER;
import static Constants.PilotIDs.AMEREN_PILOT_ID;
import static ServiceHelper.RestUtils.BASE_URL;

public class IngestionValidations {
    SoftAssert softAssert=new SoftAssert();
    public void validateUserDetails(Response response, UserFilePOJO userFilePOJO, String uuid, String timeZone,Map<String, String> executionVariables, String AMEREN_PILOT_ID){
        JSONObject obj= new JSONObject(response.asString());
        JSONObject payload=obj.getJSONObject("payload");


        Map<String,Object> expectedValueMap = new HashMap<>();
        expectedValueMap.put("uuid",payload.getString("uuid"));
        Utils.assertExpectedValuesWithJsonPath(response,expectedValueMap);
//        File schema = new File("src/test/resources/Ameren/AMI_E/SchemaFiles/userDetails.json");
//        response.then().body(JsonSchemaValidator.matchesJsonSchema(schema));

        softAssert.assertEquals(uuid,payload.getString("uuid"),"Validation of UUID");
        softAssert.assertEquals(userFilePOJO.getFirst_name(),payload.getString("firstName"),"Validation of First Name");
        softAssert.assertEquals(userFilePOJO.getLast_name(),payload.getString("lastName"),"Validation of Last Name");
        softAssert.assertEquals(userFilePOJO.getPartner_user_id(),payload.getString("partnerUserId"),"Validation of partner UserId");
        softAssert.assertEquals(BASE_URL+GET_USER+uuid,payload.getString("userUrl"),"Validation of user URL");
        softAssert.assertEquals(BASE_URL+GET_USER+uuid+"/endpoints",payload.getString("endPointsUrl"),"Validation of End Points URL");
        softAssert.assertEquals(AMEREN_PILOT_ID,payload.getString("pilotId"),"Validation of Pilot ID");
        softAssert.assertEquals("ROLE_USER",payload.getString("roleId"),"Validation of Role ID");
        softAssert.assertEquals("ENABLED",payload.getString("status"),"Validation of status");
        softAssert.assertEquals("OPT_OUT",payload.getString("notificationUserType"),"Validation of notificationUserType");
        softAssert.assertEquals("support+"+uuid+"@bidgely.com",payload.getString("userName"),"Validation of User Name");
        softAssert.assertEquals("OBTAINED",payload.getString("userConsentStatus"),"userConsentStatus");
        softAssert.assertEquals(userFilePOJO.getEmail(),payload.getString("email"),"Validation of email");

        JSONObject homeAccounts=payload.getJSONObject("homeAccounts");
        softAssert.assertEquals(userFilePOJO.getAddress_1(),homeAccounts.getString("address"),"Validation of address line 1");
        softAssert.assertEquals(userFilePOJO.getCity(),homeAccounts.getString("city"),"Validation of city");
        softAssert.assertEquals(userFilePOJO.getPostal_code(),homeAccounts.getString("postalCode"),"Validation of postalCode");
        softAssert.assertEquals(timeZone,homeAccounts.getString("timeZone"),"Validation of timeZone");

        JSONObject utilityTags=payload.getJSONObject("utilityTags");
        softAssert.assertEquals(executionVariables.get("partnerUserId"),utilityTags.getString("account_number"),"Validation of account_number");
        softAssert.assertEquals(executionVariables.get("partnerUserId")+":"+executionVariables.get("premiseId"),utilityTags.getString("account_and_premise_number"),"Validation of account_and_premise_number");
        softAssert.assertEquals(executionVariables.get("customerId"),utilityTags.getString("customer_id"),"Validation of customer_id");

        softAssert.assertAll();

    }

    public void validateMeters(String response, String uuid, String pilotId, Map<String, String> executionVariables, MeterFilePOJO meterFilePOJO){

        JSONObject obj= new JSONObject(response);
        JSONObject jsonObj=obj.getJSONObject("/users/"+uuid+"/homes/1/gws/2/meters/1");
        softAssert.assertEquals(jsonObj.getString("model"),"GreenButton","Validation of Model");
        softAssert.assertEquals(jsonObj.getString("token"),pilotId+":_"+executionVariables.get("partnerUserId")+"_"+executionVariables.get("premiseId")+"_"+executionVariables.get("dataStreamId")+"_"+meterFilePOJO.getMeter_type(),"Validation of token : ");
        softAssert.assertEquals(jsonObj.getString("id"),meterFilePOJO.getService_type()+"0","Validation of id");
        softAssert.assertAll();
    }

    public void validateLableTimeStamp(String response){

        JSONObject obj= new JSONObject(response);
        Assert.assertTrue(obj.has("first"));
        Assert.assertTrue(obj.has("last"));
        softAssert.assertAll();
    }
    public void validateGbJsonConsumption(String rawTempFilePath,String response,Map<String, String> mapTimestampConsumption) throws IOException {

        JSONArray obj= new JSONArray(response);
        for (Map.Entry<String, String> csvset : mapTimestampConsumption.entrySet()) {
            System.out.println(csvset.getKey() + " = " + csvset.getValue());
            for(int i=0;i<obj.length();i++){
                JSONObject jsonObject=obj.getJSONObject(i);
                long timeInResp= jsonObject.getLong("time");
                long timeIncsv= ((Long.parseLong(csvset.getKey())+23400)-5400);
                if(timeInResp==timeIncsv){
                    Assert.assertEquals(String.valueOf(String.format("%.4f",jsonObject.getFloat("value")/1000)),String.valueOf(String.format("%.4f", Float.valueOf(csvset.getValue()))));
                }
            }
        }
        softAssert.assertAll();
    }

    public void validateGbDisaggResp(String response){

        JSONObject obj= new JSONObject(response);
//        Assert.assertTrue(obj.has("first"));
//        Assert.assertTrue(obj.has("last"));
        softAssert.assertAll();
    }
}
