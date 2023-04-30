package CommonUtils;

import PojoClasses.MeterFilePOJO;
import PojoClasses.UserFilePOJO;
import com.amazonaws.services.dynamodbv2.xspec.S;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class UserFileAmi {

    //Generic method created for processing all kind of files
    /*public UserFilePOJO processFile(UserFilePOJO userFilePOJO, Map<String,String> executionVariables) throws IOException {
        File userFile = File.createTempFile("USERENROLL_D_600401000", ".csv");
        System.out.println(userFile.getAbsolutePath());
        userFilePOJO.setCustomer_id(executionVariables.get("customerId"));
        userFilePOJO.setPartner_user_id(executionVariables.get("partnerUserId"));
        userFilePOJO.setPremise_id(executionVariables.get("premiseId"));
        userFilePOJO.setEmail("akhil+500401002@bidgely.com");
        userFilePOJO.setFirst_name("Akhil");
        userFilePOJO.setLast_name("Sharma");
        userFilePOJO.setAddress_1("221B Baker Street");
        userFilePOJO.setCity("Hackney");
        userFilePOJO.setState("England");
        userFilePOJO.setPostal_code("208014");

        try (FileWriter writer = new FileWriter(userFile)) {

            userFile.deleteOnExit();
            StringBuilder userFileContent = new StringBuilder();
            userFileContent.append  (executionVariables.get("customerId")).append("|")
                    .append(executionVariables.get("partnerUserId")).append("|")
                    .append(executionVariables.get("premiseId")).append("|")
                    .append("RES").append("|")
                    .append("akhil+500401002@bidgely.com").append("|")
                    .append("Akhil").append("|")
                    .append("Sharma").append("|")
                    .append("221B Baker Street").append("||||")
                    .append("Hackney").append("|")
                    .append("England").append("|")
                    .append("208014").append("|||||||||||||");

            System.out.println(userFileContent);
            writer.write(userFileContent.toString());
        }
        userFilePOJO.setFile_abs_path(userFile.getAbsolutePath());

        return userFilePOJO;
    }*/

    public MeterFilePOJO processFile(MeterFilePOJO meterFilePOJO,Map<String,String> executionVariables) throws IOException {
        File meterFile = File.createTempFile("METERENROLL_D_600401000", ".csv");
        System.out.println(meterFile.getAbsolutePath());
        meterFilePOJO.setCustomer_id(executionVariables.get("customerId"));
        meterFilePOJO.setPartner_user_id(executionVariables.get("partnerUserId"));
        meterFilePOJO.setPremise_id(executionVariables.get("premiseId"));
        meterFilePOJO.setData_stream_id(executionVariables.get("dataStreamId"));
        meterFilePOJO.setData_stream_type("AMI");
        meterFilePOJO.setService_type("ELECTRIC");

        try (FileWriter writer = new FileWriter(meterFile)) {

            meterFile.deleteOnExit();
            StringBuilder userFileContent = new StringBuilder();
            userFileContent.append(executionVariables.get("customerId")).append("|")
                    .append(executionVariables.get("partnerUserId")).append("|")
                    .append(executionVariables.get("premiseId")).append("|")
                    .append(executionVariables.get("dataStreamId")).append("|")
                    .append("ELECTRIC").append("|")
                    .append("2017-01-01").append("||")
                    .append("001").append("|")
                    .append("2020-07-06").append("|")
                    .append("4").append("|")
                    .append("2020-07-06").append("|")
                    .append("False").append("|")
                    .append("AMI").append("|");

            System.out.println(userFileContent);
            writer.write(userFileContent.toString());
        }
        meterFilePOJO.setFile_abs_path(meterFile.getAbsolutePath());

        return meterFilePOJO;
    }

}