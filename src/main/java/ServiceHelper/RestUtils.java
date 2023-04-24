package ServiceHelper;


import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

import java.util.Map;

public class RestUtils {




    public static Response getUsers(String endpoint, String uuid, String token)
    {

        String basePath="v2.0/users/"+uuid;
        RequestSpecification given = RestAssured.given();
        Response response = given.baseUri(endpoint).basePath(basePath)
                .header("Content-Type", "application/json").header("Authorization", "Bearer" + token).log().all()
                .get();
        System.out.println("Response :"+ response.then().log().body());

        return response;

    }

    public static Response getMetersApi(String endpoint, String uuid, String token)
    {

        String basePath="/meta/users/"+uuid+"/homes/1/gws/3/meters";
        RequestSpecification given = RestAssured.given();
        Response response = given.baseUri(endpoint).basePath(basePath)
                .header("Content-Type", "application/json").header("Authorization", "Bearer" + token).log().all()
                .get();
        System.out.println("Response :"+ response.then().log().body());

        return response;

    }

    public static Response getPartnerUserId(String endpoint,String token, String partnerUserId)
    {
        String basePath="/v2.0/users/";
        RequestSpecification given = RestAssured.given();
        Response response = given.baseUri(endpoint).basePath(basePath).queryParam("partnerUserId",partnerUserId)
                .header("Content-Type", "application/json").header("Authorization", "Bearer" + token).log().all()
                .get();
        System.out.println("Response :"+ response.then().log().body());
        return response;
    }

    public static Response generateToken(Map<String,String> envData)
    {

        String pathParam="/oauth/token?grant_type=client_credentials&scope=all";
        RequestSpecification given = RestAssured.given();
        Response response = given.baseUri(envData.get("baseUri"))
                .basePath(pathParam)
                .header("Content-Type", "application/json").auth().basic(envData.get("username"),envData.get("password"))
                .params("grant_type","client_credentials").params("scope","all")
                .get();

        return response;

    }
}
