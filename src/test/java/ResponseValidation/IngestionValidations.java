package ResponseValidation;

import CommonUtils.Utils;
import PojoClasses.InvoicePOJO;
import PojoClasses.MeterFilePOJO;
import PojoClasses.UserFilePOJO;
import io.restassured.response.Response;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.asserts.SoftAssert;
import reporting.ExtentReportManager;
import scala.Int;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static Constants.Endpoints.GET_USER;
import static ServiceHelper.RestUtils.BASE_URL;
import static reporting.Setup.parentExtent;

public class IngestionValidations {
    SoftAssert softAssert=new SoftAssert();
    public void validateUserDetails(Response response, UserFilePOJO userFilePOJO, String timeZone,Map<String, String> executionVariables, String AMEREN_PILOT_ID){
//        JSONObject obj= new JSONObject(response.asString());
//        JSONObject payload=obj.getJSONObject("payload");

        Map<String,Object> expectedValueMap = new HashMap<>();
        expectedValueMap.put("payload.uuid", userFilePOJO.getUuid());
        expectedValueMap.put("payload.firstName",userFilePOJO.getFirst_name());
        expectedValueMap.put("payload.lastName", userFilePOJO.getLast_name());
        expectedValueMap.put("payload.partnerUserId", executionVariables.get("partnerUserId"));
        expectedValueMap.put("payload.userUrl", BASE_URL+GET_USER.replace("{uuid}",userFilePOJO.getUuid()));
        expectedValueMap.put("payload.endPointsUrl", BASE_URL+GET_USER.replace("{uuid}",userFilePOJO.getUuid())+"/endpoints");
        expectedValueMap.put("payload.pilotId", Integer.valueOf(AMEREN_PILOT_ID));
        expectedValueMap.put("payload.roleId", "ROLE_USER");
        expectedValueMap.put("payload.status", "ENABLED");
//        expectedValueMap.put("payload.notificationUserType", "OPT_OUT");
        expectedValueMap.put("payload.userName", "support+"+userFilePOJO.getUuid()+"@bidgely.com");
        expectedValueMap.put("payload.userConsentStatus", "OBTAINED");
        expectedValueMap.put("payload.email", userFilePOJO.getEmail());

        expectedValueMap.put("payload.homeAccounts.address", userFilePOJO.getAddress_1());
        expectedValueMap.put("payload.homeAccounts.city", userFilePOJO.getCity());
        expectedValueMap.put("payload.homeAccounts.postalCode", userFilePOJO.getPostal_code());
        expectedValueMap.put("payload.homeAccounts.timeZone", timeZone);

        expectedValueMap.put("payload.utilityTags.account_number", executionVariables.get("partnerUserId"));
        expectedValueMap.put("payload.utilityTags.account_and_premise_number", executionVariables.get("partnerUserId")+":"+executionVariables.get("premiseId"));
        expectedValueMap.put("payload.utilityTags.customer_id", executionVariables.get("customerId"));
        Utils.assertExpectedValuesWithJsonPath(response,expectedValueMap);
//        File schema = new File("src/test/resources/Ameren/AMI_E/SchemaFiles/userDetails.json");
//        response.then().body(JsonSchemaValidator.matchesJsonSchema(schema));

        softAssert.assertAll();

    }

    public void validateMeters(Response response, String scenario, String uuid, String pilotId, Map<String, String> executionVariables, MeterFilePOJO meterFilePOJO, int gws, int meters, String model){

        for(int i=1;i<=meters;i++){
            String path="/users/"+uuid+"/homes/1/gws/"+gws+"/meters/"+i;

            Map<String,Object> expectedValueMap = new HashMap<>();
            if(scenario.equals("AMI_E") || scenario.equals("AMI_E+AMI_E")){
                expectedValueMap.put(path+".model", "GreenButton");
            }else if(scenario.equals("AMR_E")){
                expectedValueMap.put(path+".model", "GB_MONTH");
            }
            String dataStreamId=String.valueOf(Long.valueOf(executionVariables.get("dataStreamId"))+(i-1));
            expectedValueMap.put(path+".token", pilotId+":_"+executionVariables.get("partnerUserId")+"_"+executionVariables.get("premiseId")+"_"+dataStreamId+"_"+meterFilePOJO.getMeter_type());
            expectedValueMap.put(path+".id", meterFilePOJO.getService_type()+(i-1));
            Utils.assertExpectedValuesWithJsonPath(response,expectedValueMap);
        }
    }

    public void validateLableTimeStamp(String response){

        JSONObject obj= new JSONObject(response);
        Assert.assertTrue(obj.has("first"));
        Assert.assertTrue(obj.has("last"));
        softAssert.assertAll();
    }
    public void validateGbJsonConsumption(Response response,Map<String, String> mapTimestampConsumption) throws IOException {
        JSONArray obj= new JSONArray(response.asString());
        for (Map.Entry<String, String> csvset : mapTimestampConsumption.entrySet()) {
            System.out.println(csvset.getKey() + " = " + csvset.getValue());
//            Map<String,Object> expectedValueMap = new HashMap<>();
            for(int i=0;i<obj.length();i++){
                JSONObject jsonObject=obj.getJSONObject(i);
                long timeInResp= jsonObject.getLong("time");
                long timeIncsv= ((Long.parseLong(csvset.getKey())+23400)-5400);
                if(timeInResp==timeIncsv){
//                    expectedValueMap.put("",String.valueOf(String.format("%.4f", Float.valueOf(csvset.getValue()))));
                    Assert.assertEquals(String.valueOf(String.format("%.4f",jsonObject.getFloat("value")/1000)),String.valueOf(String.format("%.4f", Float.valueOf(csvset.getValue()))));
                }
            }
//            Utils.assertExpectedValuesWithJsonPath(response,expectedValueMap);
        }
        softAssert.assertAll();
    }

    public void validateUtilityData(Response response, Map<String, Map<String, Map<String,String>>> mapTimestampInvoiceData) {

        JSONObject jsonObj = new JSONObject(response.asString());

        for (Map.Entry<String, Map<String, Map<String, String>>> key : mapTimestampInvoiceData.entrySet()) {
            String timeStamp = key.getKey();
            Map<String,Object> expectedValueMap = new HashMap<>();
            JSONObject consolidatedData = jsonObj.getJSONObject(timeStamp);
            Map<String, Map<String, String>> map1 = key.getValue();
            Map.Entry<String, Map<String, String>> firstEntry = map1.entrySet().iterator().next();

            String mapFirstKey= firstEntry.getKey();
            Map<String,String> firstMapBlock = map1.get(mapFirstKey);

            expectedValueMap.put(timeStamp+".billingStartTs", Integer.valueOf(firstMapBlock.get("bc_start_date")));
            expectedValueMap.put(timeStamp+".billingEndTs", Integer.valueOf(firstMapBlock.get("bc_end_date")));
            expectedValueMap.put(timeStamp+".value", Float.valueOf(firstMapBlock.get("kWh_Consumption")));
            expectedValueMap.put(timeStamp+".cost", Float.valueOf(firstMapBlock.get("dollar_cost")));
            expectedValueMap.put(timeStamp+".bidgelyGeneratedInvoice", false);
            expectedValueMap.put(timeStamp+".transitionBillCycle", false);

            if(firstMapBlock.get("meter_type").equals("AMI_E")){
                expectedValueMap.put(timeStamp+".gasValue", "0.0");
                expectedValueMap.put(timeStamp+".gasCost", "0.0");
            }

            JSONArray jsonArray1 = consolidatedData.getJSONArray("invoiceDataList");

            for(int i=0;i<jsonArray1.length();i++) {
                JSONObject jsonObject = jsonArray1.getJSONObject(i);
                String chargeT= getChecker(jsonObject);
                parentExtent.info("Validating  : " + chargeT);
                parentExtent.createNode(chargeT);
                Map<String, String> mapTemp= map1.get(chargeT);
                Map<String,Object> expectedValMap = new HashMap<>();

                expectedValMap.put(timeStamp+".invoiceDataList["+i+"].cost", Float.valueOf(mapTemp.get("dollar_cost")));
                expectedValMap.put(timeStamp+".invoiceDataList["+i+"].consumption", Float.valueOf(mapTemp.get("kWh_Consumption")));
                expectedValMap.put(timeStamp+".invoiceDataList["+i+"].meterId", 1);

                if(firstMapBlock.get("meter_type").equals("AMI_E")){
                    expectedValMap.put(timeStamp+".invoiceDataList["+i+"].measurementType", "Electricity");
                }else if(firstMapBlock.get("meter_type").equals("AMI_G")){
                    expectedValMap.put(timeStamp+".invoiceDataList["+i+"].measurementType", "GAS");
                }
                expectedValMap.put(timeStamp+".invoiceDataList["+i+"].chargeType", mapTemp.get("charge_type"));
                expectedValMap.put(timeStamp+".invoiceDataList["+i+"].estimationType", "ACTUAL");

                Utils.assertExpectedValuesWithJsonPath(response,expectedValMap);
            }
            Utils.assertExpectedValuesWithJsonPath(response,expectedValueMap);
            softAssert.assertAll();
        }
    }
    public String getChecker(JSONObject jsonObject){
        Set<String> set=jsonObject.keySet();

        String checker="";
        if(set.contains("chargeNameString")){
            checker = jsonObject.getString("chargeType") + jsonObject.getString("chargeNameString");
        }else
            checker = jsonObject.getString("chargeType");

        return checker;
    }

}
