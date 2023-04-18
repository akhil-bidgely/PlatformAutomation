package Ingestion;

import utils.S3Upload;
import org.testng.annotations.*;

public class UserIngestion extends BaseTest{

    @Test
    void demo1(){
        S3Upload.s3UploadFile();
        //Internal bucket file upload


    }
}
