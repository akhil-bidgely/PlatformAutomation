package Ingestion;

import CommonUtils.UserFileAmi;
import PojoClasses.UserFilePOJO;
import ResponseValidation.IngestionValidations;

public class BaseTest {

    IngestionValidations ingestionValidations = new IngestionValidations();
    UserFileAmi userFileAmi = new UserFileAmi();
    UserFilePOJO userFilePOJO= new UserFilePOJO();
}