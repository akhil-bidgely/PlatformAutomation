package tests;

import commonUtils.UserFileAmi;
import dataProviderFile.IngestionsDataProvider;
import pojoClasses.MeterFilePOJO;
import pojoClasses.UserFilePOJO;
import responseValidation.IngestionValidations;
import serviceHelper.RestUtils;

public class BaseTest {
    IngestionValidations ingestionValidations;
    UserFileAmi userFileAmi;
    UserFilePOJO userFilePOJO;
    MeterFilePOJO meterFilePOJO;
    RestUtils restUtils;
    IngestionsDataProvider ingestionsDataProvider;

    public BaseTest(){
         ingestionValidations = new IngestionValidations();
         userFileAmi = new UserFileAmi();
         userFilePOJO= new UserFilePOJO();
         meterFilePOJO= new MeterFilePOJO();
         restUtils = new RestUtils();
        ingestionsDataProvider = new IngestionsDataProvider();
    }
}