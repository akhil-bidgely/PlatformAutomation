package PojoClasses;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import static CommonUtils.JsonUtils.getRandom10Digits;

public class UserFileAmi {
    UserFilePOJO userFilePOJO = new UserFilePOJO();

    public UserFilePOJO processFile(UserFilePOJO userFilePOJO) throws IOException {
        File userFile = File.createTempFile("USERENROLL_D_600401000", ".csv");
        System.out.println(userFile.getAbsolutePath());
        String premiseId=getRandom10Digits();
        String account_id=getRandom10Digits();
        String user_segment=getRandom10Digits();
        userFilePOJO.setPremise_id(premiseId);
        userFilePOJO.setAccount_id(account_id);
        userFilePOJO.setUser_segment(user_segment);

        try (FileWriter writer = new FileWriter(userFile)) {

            userFile.deleteOnExit();
            StringBuilder userFileContent = new StringBuilder();
            userFileContent.append(getRandom10Digits()).append("|")
                    .append(premiseId).append("|")
                    .append(getRandom10Digits()).append("|").
                    append("RES").append("|")
                    .append("moorthi+500401002@bidgely.com").append("|")
                    .append("DP1278-test-500401002").append("|")
                    .append("user").append("|")
                    .append("221B Baker Street").append("||||")
                    .append("Akhil CHARLES").append("|")
                    .append("MO").append("|")
                    .append("6883").append("|||||||||||||");

//            userFileContent.append("79101|1988664101|850015101|RES|akhil+01@bidgely.com|Sophia|Watson|221B Baker Street||||SAINT CHARLES|MO|6883|||||||||||||");
            System.out.println(userFileContent);
            writer.write(userFileContent.toString());
        }
        userFilePOJO.setUserfile_abs_path(userFile.getAbsolutePath());

        return userFilePOJO;
    }

}