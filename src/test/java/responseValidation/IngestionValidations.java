package responseValidation;

import com.amazonaws.thirdparty.jackson.databind.JsonNode;
import commonUtils.Utils;
import constants.ConstantFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pojoClasses.*;
import io.restassured.response.Response;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.asserts.SoftAssert;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static constants.Endpoints.GET_USER;
import static serviceHelper.RestUtils.BASE_URL;
import static reporting.Setup.parentExtent;

public class IngestionValidations {
    SoftAssert softAssert=new SoftAssert();
    private static Logger logger = LoggerFactory.getLogger(IngestionValidations.class);
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

    }

    public void validateMetersSingleMeters(Response response, String scenario, String uuid, String pilotId, Map<String, String> executionVariables, MeterFilePOJO meterFilePOJO, int gws, int meters, String model){

        String path="/users/"+uuid+"/homes/1/gws/"+gws+"/meters/"+1;

        Map<String,Object> expectedValueMap = new HashMap<>();
        if(scenario.equals("AMI_E") || scenario.equals("AMI_E+AMI_E")){
            expectedValueMap.put(path+".model", "GreenButton");
        }else if(scenario.equals("AMR_E") || scenario.equals("AMR_E+AMR_G")){
            expectedValueMap.put(path+".model", "GB_MONTH");
        }
        if(gws==3 || gws==2){
            meterFilePOJO.setService_type("ELECTRIC");
        } else if (gws==4) {
            meterFilePOJO.setService_type("GAS");
        }
//            String dataStreamId=String.valueOf(Long.valueOf(executionVariables.get("dataStreamId")));
        expectedValueMap.put(path+".token", pilotId+":_"+executionVariables.get("partnerUserId")+"_"+executionVariables.get("premiseId")+"_"+executionVariables.get("dataStreamId")+"_"+meterFilePOJO.getMeter_type());
        expectedValueMap.put(path+".id", meterFilePOJO.getService_type()+(0));
        Utils.assertExpectedValuesWithJsonPath(response,expectedValueMap);
    }

    public void validateMetersMultiMeter(Response response, String scenario, String uuid, String pilotId, Map<String, String> executionVariables, MeterFilePOJO meterFilePOJO, int gws, int meters, String model){

        for(int i=1;i<=meters;i++){
            String path="/users/"+uuid+"/homes/1/gws/"+gws+"/meters/"+i;

            Map<String,Object> expectedValueMap = new HashMap<>();
            if(scenario.equals("AMI_E") || scenario.equals("AMI_E+AMI_E")){
                expectedValueMap.put(path+".model", "GreenButton");
            }else if(scenario.equals("AMR_E") || scenario.equals("AMR_E+AMR_G")){
                expectedValueMap.put(path+".model", "GB_MONTH");
            }

            if(gws==3 || gws==2){
                meterFilePOJO.setService_type("ELECTRIC");
            } else if (gws==4) {
                meterFilePOJO.setService_type("GAS");
            }
            String dataStreamId=String.valueOf(Long.valueOf(executionVariables.get("dataStreamId"))+(i-1));
            expectedValueMap.put(path+".token", pilotId+":_"+executionVariables.get("partnerUserId")+"_"+executionVariables.get("premiseId")+"_"+dataStreamId+"_"+meterFilePOJO.getMeter_type());
            expectedValueMap.put(path+".id", meterFilePOJO.getService_type()+(i-1));
            Utils.assertExpectedValuesWithJsonPath(response,expectedValueMap);
        }
    }

    public void validateMetersDuelFuel(Response response, String scenario, String uuid, String pilotId, Map<String, String> executionVariables, MeterFilePOJO meterFilePOJO, int gws, int meters, String model){

        for(int i=1;i<=meters;i++){
            String path="/users/"+uuid+"/homes/1/gws/"+gws+"/meters/"+i;

            Map<String,Object> expectedValueMap = new HashMap<>();
            if(scenario.equals("AMI_E") || scenario.equals("AMI_E+AMI_E")){
                expectedValueMap.put(path+".model", "GreenButton");
            }else if(scenario.equals("AMR_E") || scenario.equals("AMR_E+AMR_G")){
                expectedValueMap.put(path+".model", "GB_MONTH");
            }
            if(gws==3){
                meterFilePOJO.setService_type("ELECTRIC");
            } else if (gws==4) {
                meterFilePOJO.setService_type("GAS");
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
            Map<String,Object> expectedValueMap = new HashMap<>();
            long timeIncsv= ((Long.parseLong(csvset.getKey())+23400)-5400);
            for(int i=0;i<obj.length();i++){
                JSONObject jsonObject=obj.getJSONObject(i);
                long timeInResp= jsonObject.getLong("time");
                if(timeInResp==timeIncsv){
                    String path="["+(i)+"]"+".value";
                    expectedValueMap.put(path,Float.valueOf(csvset.getValue())*1000);
//                    Assert.assertEquals(String.valueOf(String.format("%.4f" ,jsonObject.getFloat("value")/1000)),String.valueOf(String.format("%.4f", Float.valueOf(csvset.getValue()))));
                    break;
                }
            }
            Utils.assertExpectedValuesWithJsonPath(response,expectedValueMap);
        }
    }

    public void validateUtilityData(Response response, Map<String, Map<String, Map<String,String>>> mapTimestampInvoiceData, int meterId) {

        JSONObject jsonObj = new JSONObject(response.asString());

        for (Map.Entry<String, Map<String, Map<String, String>>> key : mapTimestampInvoiceData.entrySet()) {
            String timeStamp = key.getKey();
            Map<String,Object> expectedValueMap = new HashMap<>();
            JSONObject consolidatedData = jsonObj.getJSONObject(timeStamp);
            Map<String, Map<String, String>> map1 = key.getValue();
            Map.Entry<String, Map<String, String>> firstEntry = map1.entrySet().iterator().next();

            String mapFirstKey= firstEntry.getKey();
            Map<String,String> firstMapBlock = map1.get(mapFirstKey);

            parentExtent.info("Validating the Invoice API data for timeStamp"+key);
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
            Utils.assertExpectedValuesWithJsonPath(response,expectedValueMap);

            JSONArray jsonArray1 = consolidatedData.getJSONArray("invoiceDataList");

            for(int i=0;i<jsonArray1.length();i++) {
                JSONObject jsonObject = jsonArray1.getJSONObject(i);
                String chargeT= getChecker(jsonObject);
                parentExtent.info("Validating  : " + chargeT);
                Map<String, String> mapTemp= map1.get(chargeT);
                Map<String,Object> expectedValMap = new HashMap<>();

                expectedValMap.put(timeStamp+".invoiceDataList["+i+"].cost", Float.valueOf(mapTemp.get("dollar_cost")));
                expectedValMap.put(timeStamp+".invoiceDataList["+i+"].consumption", Float.valueOf(mapTemp.get("kWh_Consumption")));
                expectedValMap.put(timeStamp+".invoiceDataList["+i+"].meterId", meterId);

                if(firstMapBlock.get("meter_type").equals("AMI_E")){
                    expectedValMap.put(timeStamp+".invoiceDataList["+i+"].measurementType", "Electricity");
                }else if(firstMapBlock.get("meter_type").equals("AMI_G")){
                    expectedValMap.put(timeStamp+".invoiceDataList["+i+"].measurementType", "GAS");
                }
                expectedValMap.put(timeStamp+".invoiceDataList["+i+"].chargeType", mapTemp.get("charge_type"));
                expectedValMap.put(timeStamp+".invoiceDataList["+i+"].estimationType", "ACTUAL");

                Utils.assertExpectedValuesWithJsonPath(response,expectedValMap);
            }

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
    public void validateFirehoseS3(Dataset<Row> joinedData, boolean solarFieldFromMeterFile)
    {
        //To verify the field getting in S3 and input file
        joinedData.foreach(row -> {

            if(row.getAs("billing_start_time")!=null &&
                    row.getAs("billing_start_time").toString().equals(row.getAs("billingStartDate").toString())) {
                SoftAssert softAssert = new SoftAssert();
                logger.info("the billing end time values in s3 and input file is " + row.getAs("billing_end_time").toString() + " " + row.getAs("billingEndDate").toString());
                softAssert.assertEquals(row.getAs("billing_end_time").toString(), row.getAs("billingEndDate").toString());

                //to verify the consumption values
                logger.info("the consumption values in s3 and input file is " + row.getAs("consumption").toString() + " " + row.getAs("consumption_value").toString());
                softAssert.assertEquals(row.getAs("consumption").toString(), row.getAs("consumption_value").toString(), "Consumption Not getting matched"
                        + row.getAs("consumption").toString() + " "     + row.getAs("consumption_value"));

                //to verify the cost values
                logger.info("the cost values in s3 and input file is " + row.getAs("cost").toString() + " " + row.getAs("currencyCost").toString());
                softAssert.assertEquals(row.getAs("cost").toString(), row.getAs("currencyCost").toString());

                //to Verify the USER type in s3
                if (row.getAs("dataStreamType").toString().equals("AMI")) {
                    logger.info("verifying the user type in S3 based on Meter type");
                    softAssert.assertEquals(row.getAs("user_type"), ConstantFile.UserTypeAMIMeter);
                } else if (row.getAs("dataStreamType").toString().equals("AMR")) {
                    logger.info("verifying the user type in S3 based on Meter type");
                    softAssert.assertEquals(row.getAs("user_type"), ConstantFile.UserTypeAMRMeter);
                }
                else
                {
                    logger.info("verifying the user type in S3 based on Meter type");
                    softAssert.assertEquals(row.getAs("user_type"), ConstantFile.UserTypeNSMMeter);
                }
                //to verify the Measurement type in s3
                if (row.getAs("fuelType").toString().equals("ELECTRIC")) {
                    logger.info("verifying the measurement type in S3 based on Meter type");
                    softAssert.assertEquals(row.getAs("measurement_type"), "Electricity");
                }
                //to verify the solar field
                logger.info("verifying the solar field in S3 based on Meter File" + row.getAs("solar").toString() + " " + solarFieldFromMeterFile);
                softAssert.assertEquals(Boolean.parseBoolean(row.getAs("solar").toString()), solarFieldFromMeterFile);

                softAssert.assertEquals(row.getAs("bidgely_generated_invoice").toString(), "false");

                //to verify the hid
                softAssert.assertEquals(row.getAs("hid").toString(), "1");

                //to verify the transition bill cycle
                softAssert.assertEquals(row.getAs("transition_bill_cycle").toString(), "false");
                softAssert.assertAll();
            }
            else
            {
                logger.info("Records not present in S3 bucket") ;
                logger.info(row.toString());
            }

        });
    }
    public static  void validateRedshiftData(Dataset<Row> rowDatasetInvoiceFileTotal, HashMap<String, JsonNode> hm, boolean solarFieldFromMeterFile) throws java.text.ParseException {
        for (Row row : rowDatasetInvoiceFileTotal.collectAsList()) {
            String column1Value = row.getAs("billingStartDate");

            if (hm.containsKey(column1Value)) {
                JsonNode jsonNode= hm.get(column1Value);
                logger.info(jsonNode.toString());
                String endTime = jsonNode.get("billing_end_time").asText();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                Date enDate = dateFormat.parse(endTime);
                String endFormatedDate = dateFormat.format(enDate);

                Assert.assertEquals(row.getAs("billingEndDate").toString(),endFormatedDate);


                //for consumption value verification
                Double value1 = Double.parseDouble(row.getAs("consumption").toString());
                Double value2 = Double.parseDouble(jsonNode.get("consumption_value").toString().replaceAll("\"",""));
                BigDecimal consumtionValueInput = BigDecimal.valueOf(value1).setScale(1, BigDecimal.ROUND_HALF_UP);
                BigDecimal consumtionValueRedshift = BigDecimal.valueOf(value2).setScale(1, BigDecimal.ROUND_HALF_UP);
                logger.info(consumtionValueInput +" "+consumtionValueRedshift);
                Assert.assertEquals(consumtionValueInput,consumtionValueRedshift);

                //for currency cost verification
                Double val1 = Double.parseDouble(row.getAs("currencyCost").toString());
                Double val2 = Double.parseDouble(jsonNode.get("cost").toString().replaceAll("\"",""));
                BigDecimal costValueInput = BigDecimal.valueOf(val1).setScale(2, BigDecimal.ROUND_DOWN);
                BigDecimal costValueRedshift = BigDecimal.valueOf(val2).setScale(2, BigDecimal.ROUND_DOWN);
                Assert.assertEquals(costValueInput,costValueRedshift);
                logger.info(costValueInput+"  "+costValueRedshift);

                //for user type verification in redshift
                //to Verify the USER type in s3
                if (row.getAs("dataStreamType").toString().equals("AMI")) {
                    logger.info("verifying the user type in S3 based on Meter type");
                    Assert.assertEquals(jsonNode.get("user_type").toString().replaceAll("\"",""), ConstantFile.UserTypeAMIMeter);
                } else if (row.getAs("dataStreamType").toString().equals("AMR")) {
                    logger.info("verifying the user type in S3 based on Meter type");
                    Assert.assertEquals(jsonNode.get("user_type").toString().replaceAll("\"",""), ConstantFile.UserTypeAMRMeter);
                }
                else
                {
                    logger.info("verifying the user type in S3 based on Meter type");
                    Assert.assertEquals(jsonNode.get("user_type").toString().replaceAll("\"",""), ConstantFile.UserTypeNSMMeter);
                }

                //to verify the Measurement type in s3
                if (row.getAs("fuelType").toString().equals("ELECTRIC")) {
                    logger.info("verifying the measurement type in S3 based on Meter type");
                    Assert.assertEquals(jsonNode.get("measurement_type").toString().replaceAll("\"",""), "Electricity");
                }

                //to verify the hid
                Assert.assertEquals(jsonNode.get("hid").toString().replaceAll("\"",""), "1");

                //to verify the transition bill cycle
                Assert.assertEquals(jsonNode.get("transition_bill_cycle").toString().replaceAll("\"",""), "false");

                //to verify the solar field

                //to validate data between s3 and input file
                boolean solarInRedshift = Boolean.parseBoolean(jsonNode.get("solar").toString().replaceAll("\"", ""));
                Assert.assertEquals( solarInRedshift,solarFieldFromMeterFile);


            } else {
                logger.info("Following Records not present is the Redshift");
                logger.info(row.toString());
            }
        }
    }


    public static void validateUserConfig(Response userConfigResponse) throws ParseException {
        JSONObject jsonObject= new JSONObject(userConfigResponse.asString());
        Map<String,Object> expectedValueMap = new HashMap<>();
        String ELECTRIC_OPT_IN=jsonObject.getString("event_subscriptions.SUMMER_ALERT.ELECTRIC.OPT_IN").replace("\\\\","");
        String ELECTRIC_OPT_OUT=jsonObject.getString("event_subscriptions.SUMMER_ALERT.ELECTRIC.OPT_OUT").replace("\\\\","");
        String USER_WELCOME_OPT_IN=jsonObject.getString("event_subscriptions.USER_WELCOME.OPT_IN").replace("\\\\","");
        String USER_WELCOME_OPT_OUT=jsonObject.getString("event_subscriptions.USER_WELCOME.OPT_OUT").replace("\\\\","");
        String BILL_PROJECTION_ELECTRIC_OPT_IN=jsonObject.getString("event_subscriptions.BILL_PROJECTION.ELECTRIC.OPT_IN").replace("\\\\","");
        String BILL_PROJECTION_ELECTRIC_OPT_OUT=jsonObject.getString("event_subscriptions.BILL_PROJECTION.ELECTRIC.OPT_OUT").replace("\\\\","");
        String BUDGET_ALERT_ELECTRIC_OPT_IN=jsonObject.getString("event_subscriptions.BUDGET_ALERT.ELECTRIC.OPT_IN").replace("\\\\","");
        String BUDGET_ALERT_ELECTRIC_OPT_OUT=jsonObject.getString("event_subscriptions.BUDGET_ALERT.ELECTRIC.OPT_OUT").replace("\\\\","");
        String WINTER_ALERT_ELECTRIC_OPT_IN=jsonObject.getString("event_subscriptions.WINTER_ALERT.ELECTRIC.OPT_IN").replace("\\\\","");
        String WINTER_ALERT_ELECTRIC_OPT_OUT=jsonObject.getString("event_subscriptions.WINTER_ALERT.ELECTRIC.OPT_OUT").replace("\\\\","");
        String AMI_WELCOME_OPT_IN=jsonObject.getString("event_subscriptions.AMI_WELCOME.OPT_IN").replace("\\\\","");
        String AMI_WELCOME_OPT_OUT=jsonObject.getString("event_subscriptions.AMI_WELCOME.OPT_OUT").replace("\\\\","");


        System.out.println(ELECTRIC_OPT_IN);
        JSONObject json = new JSONObject(ELECTRIC_OPT_IN);
        String userPref="";

        JSONArray jsonArray= json.getJSONArray("kvs");
        for(Object jsonObj:jsonArray){
            JSONObject obj=(JSONObject) jsonObj;
            userPref= obj.getString("val");
            }
        }

}