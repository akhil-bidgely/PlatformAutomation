package restutils;


import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

import java.util.Properties;

public class RestUtils {




    public static Response getUsers(String endpoint, String uuid, String token)
    {

        String basePath="v2.0/users/";
        RequestSpecification given = RestAssured.given();
        Response response = given.baseUri(endpoint).basePath(basePath + uuid)
                .header("Content-Type", "application/json").header("Authorization", "Bearer" + token)
                .get();

        return response;

    }
    public static Response generateToken(String endpoint)
    {

        String pathParam="/oauth/token?grant_type=client_credentials&scope=all";
        RequestSpecification given = RestAssured.given();
        Response response = given.baseUri(endpoint)
                .basePath(pathParam)
                .header("Content-Type", "application/json").auth().basic("akhil@bidgely.com","ab0ufM@bK630")
                .params("grant_type","client_credentials").params("scope","all")
                .get();

        return response;

    }
}
