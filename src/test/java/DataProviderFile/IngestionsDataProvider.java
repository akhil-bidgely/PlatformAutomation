package DataProviderFile;

import org.testng.annotations.DataProvider;

import java.util.Iterator;

import static Constants.ConstantFile.AMI_E_GWS;
import static Constants.FilePaths.*;

public class IngestionsDataProvider {

    @DataProvider(name = "singleMeterDP")
    public Object[][] singleMeterDP(){
        return new Object[][]{
                {"AMI_E",USER_ENROLLMENT_AMI_E_PATH, METER_ENROLLMENT_AMI_E_PATH, RAW_AMI_E_PATH, INVOICE_AMI_E_PATH,"GreenButton",AMI_E_GWS},
//                {"AMR_E",USER_ENROLLMENT_AMR_E_PATH, METER_ENROLLMENT_AMR_E_PATH, RAW_AMR_E_PATH, INVOICE_AMR_E_PATH, "GB_MONTH"},
        };
    }

    @DataProvider(name = "multimeterDP")
    public Object[][] multimeterDP(){
        return new Object[][]{
                {"AMI_E+AMI_E",USER_ENROLLMENT_AMI_E_AMI_E_PATH, METER_ENROLLMENT_AMI_E_AMI_E_PATH, RAW1_AMI_E_AMI_E_PATH, RAW2_AMI_E_AMI_E_PATH, INVOICE1_AMI_E_AMI_E_PATH, INVOICE2_AMI_E_AMI_E_PATH,"GreenButton",AMI_E_GWS},
        };
    }

}

