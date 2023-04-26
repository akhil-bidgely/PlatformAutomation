package CommonUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.response.Response;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.text.ParseException;
import java.util.Map;

public class JsonUtils {

    private static SecureRandom rnd = new SecureRandom();

    private static final ObjectMapper objectMapper=new ObjectMapper();
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
        long number = (long) Math.floor(Math.random() * 9_000_000_00L) +
                1_000_000_00L;
        return Long.toString(number);
    }

    public static String getUuidFromPremiseId(Response partnerUserIdResponse) {
        JSONObject obj= new JSONObject(partnerUserIdResponse.asString());
        JSONObject payloadObj=obj.getJSONObject("payload");
        JSONArray jsonArray= payloadObj.getJSONArray("data");
        JSONObject obj1=jsonArray.getJSONObject(0);
        return obj1.getString("uuid");
    }
    public static String getTimeZone(Response pilotConfigResponse) throws ParseException {
        JSONObject jsonObject= new JSONObject(pilotConfigResponse.asString());
        String timeZone="";
        String payload=jsonObject.getString("launchpad_ingestion_configs").replace("\\\\","");
        System.out.println(payload);
        JSONObject json = new JSONObject(payload);

        JSONArray jsonArray= json.getJSONArray("kvs");
        for(Object jsonObj:jsonArray){
            JSONObject obj=(JSONObject) jsonObj;
            if(obj.getString("key").equals("parser_time_zone")){
                timeZone= obj.getString("val");
                break;
            }
        }
        return timeZone;
    }
}
