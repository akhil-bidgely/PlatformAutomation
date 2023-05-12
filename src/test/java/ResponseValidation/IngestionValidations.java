package ResponseValidation;

import CommonUtils.Utils;
import Constants.UserType;
import PojoClasses.MeterFilePOJO;
import PojoClasses.UserFilePOJO;
import com.amazonaws.thirdparty.jackson.databind.JsonNode;
import io.restassured.response.Response;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.asserts.SoftAssert;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static Constants.Endpoints.GET_USER;
import static ServiceHelper.RestUtils.BASE_URL;

public class IngestionValidations {
    SoftAssert softAssert=new SoftAssert();
    private static Logger logger = LoggerFactory.getLogger(IngestionValidations.class);
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



    public void validateFirehoseS3(Dataset<Row> joinedData, boolean solarFieldFromMeterFile)
    {
        //To verify the field getting in S3 and input file
        joinedData.foreach(row -> {

            //to verify the billing end time
            //   System.out.println(row.getAs("billing_start_time").toString() +"   "+row.getAs("billingStartDate").toString());

            if(row.getAs("billing_start_time")!=null &&
                    row.getAs("billing_start_time").toString().equals(row.getAs("billingStartDate").toString())) {
                SoftAssert softAssert = new SoftAssert();
                logger.info("the billing end time values in s3 and input file is " + row.getAs("billing_end_time").toString() + " " + row.getAs("billingEndDate").toString());
                softAssert.assertEquals(row.getAs("billing_end_time").toString(), row.getAs("billingEndDate").toString());

                //to verify the consumption values
                logger.info("the consumption values in s3 and input file is " + row.getAs("consumption").toString() + " " + row.getAs("consumption_value").toString());
                softAssert.assertEquals(row.getAs("consumption").toString(), row.getAs("consumption_value").toString(), "Consumption Not getting matched"
                        + row.getAs("consumption").toString() + " " + row.getAs("consumption_value"));

                //to verify the cost values
                logger.info("the cost values in s3 and input file is " + row.getAs("cost").toString() + " " + row.getAs("currencyCost").toString());
                softAssert.assertEquals(row.getAs("cost").toString(), row.getAs("currencyCost").toString());

                //to Verify the USER type in s3
                if (row.getAs("dataStreamType").toString().equals("AMI")) {
                    logger.info("verifying the user type in S3 based on Meter type");
                    softAssert.assertEquals(row.getAs("user_type"), UserType.UserTypeAMIMeter);
                } else if (row.getAs("dataStreamType").toString().equals("AMR")) {
                    logger.info("verifying the user type in S3 based on Meter type");
                    softAssert.assertEquals(row.getAs("user_type"), UserType.UserTypeAMRMeter);
                }
                else
                {
                    logger.info("verifying the user type in S3 based on Meter type");
                    softAssert.assertEquals(row.getAs("user_type"), UserType.UserTypeNSMMeter);
                }
                //TODO
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
                System.out.println("Records not present in S3 bucket");
                System.out.println(row.toString());
            }

        });
    }



        public static  void validateRedshiftData(Dataset<Row> rowDatasetInvoiceFileTotal, HashMap<String, JsonNode > hm, boolean solarFieldFromMeterFile) throws java.text.ParseException {
        for (Row row : rowDatasetInvoiceFileTotal.collectAsList()) {
            String column1Value = row.getAs("billingStartDate");

            if (hm.containsKey(column1Value)) {
                JsonNode jsonNode= hm.get(column1Value);
                System.out.println(jsonNode);
                String endTime = jsonNode.get("billing_end_time").asText();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                Date enDate = dateFormat.parse(endTime);
                String endFormatedDate = dateFormat.format(enDate);

                Assert.assertEquals(row.getAs("billingEndDate").toString(),endFormatedDate);


                //for consumption value verification
                Double value1 = Double.parseDouble(row.getAs("consumption").toString());
                Double value2 = Double.parseDouble(jsonNode.get("consumption_value").toString().replaceAll("\"",""));
                BigDecimal decimal1 = BigDecimal.valueOf(value1).setScale(1, BigDecimal.ROUND_HALF_UP);
                BigDecimal decimal2 = BigDecimal.valueOf(value2).setScale(1, BigDecimal.ROUND_HALF_UP);
                System.out.println(decimal1 +" "+decimal2);
                Assert.assertEquals(decimal1,decimal2);

                //for currency cost verification
                Double val1 = Double.parseDouble(row.getAs("currencyCost").toString());
                Double val2 = Double.parseDouble(jsonNode.get("cost").toString().replaceAll("\"",""));
                decimal1 = BigDecimal.valueOf(val1).setScale(2, BigDecimal.ROUND_DOWN);
                decimal2 = BigDecimal.valueOf(val2).setScale(2, BigDecimal.ROUND_DOWN);
                Assert.assertEquals(decimal1,decimal2);
                System.out.println(decimal1+"  "+decimal2);

                //for user type verification in redshift
                //to Verify the USER type in s3
                if (row.getAs("dataStreamType").toString().equals("AMI")) {
                    logger.info("verifying the user type in S3 based on Meter type");
                    Assert.assertEquals(jsonNode.get("user_type").toString().replaceAll("\"",""), UserType.UserTypeAMIMeter);
                } else if (row.getAs("dataStreamType").toString().equals("AMR")) {
                    logger.info("verifying the user type in S3 based on Meter type");
                    Assert.assertEquals(jsonNode.get("user_type").toString().replaceAll("\"",""), UserType.UserTypeAMRMeter);
                }
                else
                {
                    logger.info("verifying the user type in S3 based on Meter type");
                    Assert.assertEquals(jsonNode.get("user_type").toString().replaceAll("\"",""), UserType.UserTypeNSMMeter);
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
                System.out.println("Following Records not present is the Redshift");
                System.out.println(row.toString());
            }
        }
    }



}
