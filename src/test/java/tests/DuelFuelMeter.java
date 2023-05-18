package tests;

import commonUtils.JsonUtils;
import commonUtils.Utils;
import dataProviderFile.IngestionsDataProvider;
import io.restassured.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import pojoClasses.UserFilePOJO;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static commonUtils.JsonUtils.getAuthToken;
import static commonUtils.Utils.*;
import static constants.ConstantFile.AMEREN_PILOT_ID;

public class DuelFuelMeter extends BaseTest{

    String token= "";
    private static Logger logger = LoggerFactory.getLogger(SingleMeter.class);

    @BeforeMethod
    public void generateToken() {
        Response response= restUtils.generateToken();
        token=getAuthToken(response);
    }

    @Test(enabled = true, dataProvider = "duelFuelDP", dataProviderClass = IngestionsDataProvider.class)
    public void duelFuelIngestion (String scenario,String userFilePath, String meterFilePath, String rawFilePath_1, String rawFilePath_2, String invoiceFilePath_1,
                                   String model, int gws1, int gws2) throws IOException, java.text.ParseException {

        //Map of variable to be changed in csv files
        Map<String, String> executionVariables = JsonUtils.getExecutionVariables();
        String dataStreamId1=executionVariables.get("dataStreamId");
        String dataStreamId2= String.valueOf(Long.parseLong(executionVariables.get("dataStreamId"))+1);

        //USERENROLL file upload
        UserFilePOJO userFilePOJO= new UserFilePOJO();
        String userTempFilePath=processFile(userFilePath,executionVariables,userFilePOJO, meterFilePOJO,executionVariables.get("dataStreamId"));
        Utils.s3UploadFile(userTempFilePath);

        //METERENROLL file upload
        String meterTempFilePath=processFile(meterFilePath,executionVariables,null,meterFilePOJO,executionVariables.get("dataStreamId"));
        Utils.s3UploadFile(meterTempFilePath);

        //RAW file upload
        String rawTempFilePath1=processFile(rawFilePath_1,executionVariables,userFilePOJO,meterFilePOJO,dataStreamId1);
        Utils.s3UploadFile(rawTempFilePath1);

        //RAW file upload
        String rawTempFilePath2=processFile(rawFilePath_2,executionVariables,userFilePOJO,meterFilePOJO, dataStreamId2);
        Utils.s3UploadFile(rawTempFilePath2);

        //Invoice file upload
        String invoiceTempFilePath1=processFile(invoiceFilePath_1,executionVariables,userFilePOJO,meterFilePOJO,dataStreamId1);
        Utils.s3UploadFile(invoiceTempFilePath1);

        //TODO : Add awaitility wait instead of hard wait
//        Thread.sleep(5000);
        Response partnerUserIdResponse= restUtils.getPartnerUserId(token,executionVariables);
        userFilePOJO.setUuid(JsonUtils.getUuidFromPremiseId(partnerUserIdResponse));

        //Calling Pilot config API
        Response getPilotConfigResponse= restUtils.getPilotConfigs(token,AMEREN_PILOT_ID);
        String timeZone=JsonUtils.getTimeZone(getPilotConfigResponse);

        //Calling User Details API
        Response usersApiResponse= restUtils.getUsers(userFilePOJO.getUuid(),token);

        //Validating User Details API response
        ingestionValidations.validateUserDetails(usersApiResponse,userFilePOJO,timeZone,executionVariables,AMEREN_PILOT_ID);

        //Calling Meter API
        Response metersApiResponse= restUtils.getMetersApi(userFilePOJO.getUuid(),token,gws1);
        restUtils.printResponseLogInReport(metersApiResponse);
        ingestionValidations.validateMetersDuelFuel(metersApiResponse,scenario,userFilePOJO.getUuid(),AMEREN_PILOT_ID,executionVariables,meterFilePOJO,gws1,1,model);

        //Calling Meter API
        metersApiResponse= restUtils.getMetersApi(userFilePOJO.getUuid(),token,gws2);
        restUtils.printResponseLogInReport(metersApiResponse);
        ingestionValidations.validateMetersDuelFuel(metersApiResponse,scenario,userFilePOJO.getUuid(),AMEREN_PILOT_ID,executionVariables,meterFilePOJO,gws2,1,model);

        String t1 = String.valueOf(Instant.now().getEpochSecond());

        //Calling Label TimeStamp API
        Response label= restUtils.getLabelTimeStamp(userFilePOJO.getUuid(),token);
        ingestionValidations.validateLableTimeStamp(label.asString());

        //Calling gbJson API
        Response gbJsonApiResponse= restUtils.getGbJsonApi(userFilePOJO.getUuid(),token,t1,scenario,gws1);
        Map<String, String> mapTimestampConsumption = new HashMap<>();
        mapTimestampConsumption.putAll(getTimeStampsConsumption(rawTempFilePath1));
        mapTimestampConsumption.putAll(getTimeStampsConsumption(rawTempFilePath2));
        ingestionValidations.validateGbJsonConsumption(gbJsonApiResponse,mapTimestampConsumption);

        //Internal bucket Invoice file upload
        t1 = String.valueOf(Instant.now().getEpochSecond());

        //Calling INVOICE API
        Response utilityDataResponse= restUtils.getUtilityData(userFilePOJO.getUuid(),token,t1);
        Map<String, Map<String, Map<String,String>>> mapTimestampCostData = new HashMap<>();
        mapTimestampCostData.putAll(getTimeStampsInvoiceData(invoiceTempFilePath1));
        ingestionValidations.validateUtilityData(utilityDataResponse,mapTimestampCostData,1);
//        mapTimestampCostData.clear();
//        mapTimestampCostData.putAll(getTimeStampsInvoiceData(invoiceTempFilePath2));
//        ingestionValidations.validateUtilityData(utilityDataResponse,mapTimestampCostData);

    }


}
