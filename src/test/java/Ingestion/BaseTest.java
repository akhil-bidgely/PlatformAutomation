package Ingestion;

import CommonUtils.UserFileAmi;
import PojoClasses.MeterFilePOJO;
import PojoClasses.UserFilePOJO;
import ResponseValidation.IngestionValidations;
import ServiceHelper.RestUtils;
import reporting.Setup;

public class BaseTest {
    IngestionValidations ingestionValidations;
    UserFileAmi userFileAmi;
    UserFilePOJO userFilePOJO;
    MeterFilePOJO meterFilePOJO;
    RestUtils restUtils;

    public BaseTest(){
         ingestionValidations = new IngestionValidations();
         userFileAmi = new UserFileAmi();
         userFilePOJO= new UserFilePOJO();
         meterFilePOJO= new MeterFilePOJO();
         restUtils = new RestUtils();
    }
}