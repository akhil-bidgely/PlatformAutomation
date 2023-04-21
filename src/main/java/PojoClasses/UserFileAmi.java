package PojoClasses;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static utils.JsonUtils.getRandom10Digits;

public class UserFileAmi {

    public String processFile() throws IOException {
        File userFile = File.createTempFile("USERENROLL_D_600401000", ".csv");
        System.out.println(userFile.getAbsolutePath());
        try (FileWriter writer = new FileWriter(userFile)) {

            userFile.deleteOnExit();
            StringBuilder userFileContent = new StringBuilder();
            userFileContent.append(getRandom10Digits()).append("|")
                    .append(getRandom10Digits()).append("|")
                    .append(getRandom10Digits()).append("|").
                    append("RES").append("|")
                    .append("moorthi+500401002@bidgely.com").append("|")
                    .append("DP1278-test-500401002").append("|")
                    .append("user").append("|")
                    .append("221B Baker Street").append("||||")
                    .append("Akhil CHARLES").append("|")
                    .append("MO").append("|")
                    .append("6883").append("||||||||||||||");
            System.out.println(userFileContent);
            writer.write(userFileContent.toString());
        }

        return userFile.getAbsolutePath();
    }

}