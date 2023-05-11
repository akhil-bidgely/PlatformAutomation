package tests;

import CommonUtils.UserFileAmi;
import DataProviderFile.IngestionsDataProvider;
import PojoClasses.MeterFilePOJO;
import PojoClasses.UserFilePOJO;
import ResponseValidation.IngestionValidations;
import ServiceHelper.RestUtils;

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