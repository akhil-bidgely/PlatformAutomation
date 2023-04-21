package utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Map;

public class JsonUtils {

    private static SecureRandom rnd = new SecureRandom();

    private static ObjectMapper objectMapper=new ObjectMapper();
    public static Map<String,String> getJsonDataAsMap(String jsonFileName){
        String env=System.getProperty("env")==null?"nonprodqa":System.getProperty("env");
        String completeJsonFilePath=System.getProperty("user.dir")+"/src/test/resources/"+env+"/"+jsonFileName;

        Map<String,String> data= null;
        try {
            data = objectMapper.readValue(new File(completeJsonFilePath),new TypeReference<>(){});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return data;
    }

    public static char getRandomDigit() {
        String AB = "0123456789";
        return AB.charAt(rnd.nextInt(AB.length()));
    }

    public static String getRandom10Digits() {
        long number = (long) Math.floor(Math.random() * 9_000_000_000L) +
                1_000_000_000L;
        return Long.toString(number);
    }
}
