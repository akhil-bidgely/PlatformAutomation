package restutils;


import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

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
}
