package Ingestion;

import CommonUtils.UserFileAmi;
import PojoClasses.MeterFilePOJO;
import PojoClasses.UserFilePOJO;
import ResponseValidation.IngestionValidations;
import reporting.Setup;

public class BaseTest {

    IngestionValidations ingestionValidations = new IngestionValidations();
    UserFileAmi userFileAmi = new UserFileAmi();
    UserFilePOJO userFilePOJO= new UserFilePOJO();
    MeterFilePOJO meterFilePOJO= new MeterFilePOJO();
}