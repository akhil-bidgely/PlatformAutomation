package CommonUtils;

import PojoClasses.UserFilePOJO;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import static CommonUtils.JsonUtils.getRandom10Digits;

public class UserFileAmi {
    UserFilePOJO userFilePOJO = new UserFilePOJO();

    public UserFilePOJO processFile(UserFilePOJO userFilePOJO,String premiseId,String account_id,String user_segment) throws IOException {
        File userFile = File.createTempFile("USERENROLL_D_600401000", ".csv");
        System.out.println(userFile.getAbsolutePath());
        userFilePOJO.setPremise_id(premiseId);
        userFilePOJO.setAccount_id(account_id);
        userFilePOJO.setUser_segment(user_segment);

        try (FileWriter writer = new FileWriter(userFile)) {

            userFile.deleteOnExit();
            StringBuilder userFileContent = new StringBuilder();
            userFileContent.append(account_id).append("|")
                    .append(premiseId).append("|")
                    .append(user_segment).append("|").
                    append("RES").append("|")
                    .append("moorthi+500401002@bidgely.com").append("|")
                    .append("DP1278-test-500401002").append("|")
                    .append("user").append("|")
                    .append("221B Baker Street").append("||||")
                    .append("Akhil CHARLES").append("|")
                    .append("MO").append("|")
                    .append("6883").append("|||||||||||||");

            System.out.println(userFileContent);
            writer.write(userFileContent.toString());
        }
        userFilePOJO.setFile_abs_path(userFile.getAbsolutePath());

        return userFilePOJO;
    }

    public UserFilePOJO processFile(String premiseId,String account_id,String user_segment) throws IOException {
        File userFile = File.createTempFile("METERENROLL_D_600401000", ".csv");
        System.out.println(userFile.getAbsolutePath());

        try (FileWriter writer = new FileWriter(userFile)) {

            userFile.deleteOnExit();
            StringBuilder userFileContent = new StringBuilder();
            userFileContent.append(account_id).append("|")
                    .append(premiseId).append("|")
                    .append(user_segment).append("|").
                    append("2862740041").append("|")
                    .append("ELECTRIC").append("|")
                    .append("2017-01-01").append("||")
                    .append("601").append("|")
                    .append("2020-07-06").append("|")
                    .append("SS_1").append("|")
                    .append("2020-07-06").append("|")
                    .append("False").append("|")
                    .append("AMI").append("|");

            System.out.println(userFileContent);
            writer.write(userFileContent.toString());
        }
        userFilePOJO.setFile_abs_path(userFile.getAbsolutePath());

        return userFilePOJO;
    }

}