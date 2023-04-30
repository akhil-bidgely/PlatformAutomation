package Constants;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConfigManagar {
    public static Properties prop= new Properties();
    public static Map<String,String> filemap= new HashMap<>();


    public static String getConfig(String key){
        try{
            FileInputStream input = new FileInputStream("src/env.properties");
            prop.load(input);
            filemap.put(key,prop.getProperty(key));

        }catch (Exception e){
            e.printStackTrace();
        }

        String config= filemap.get(key);

        return config;
    }
}
