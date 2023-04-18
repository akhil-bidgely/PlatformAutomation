package utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class JsonUtils {



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
}
