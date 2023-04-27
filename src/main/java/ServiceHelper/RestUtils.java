package ServiceHelper;


import io.restassured.specification.QueryableRequestSpecification;
import io.restassured.specification.SpecificationQuerier;
import reporting.ExtentReportManager;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

import java.util.HashMap;
import java.util.Map;

public class RestUtils {

    public static void printRequestLogInReport(RequestSpecification requestSpecification){
        QueryableRequestSpecification queryableRequestSpecification= SpecificationQuerier.query(requestSpecification);
        ExtentReportManager.logInfoDetails("Endpoint is " + queryableRequestSpecification.getBaseUri()+queryableRequestSpecification.getBasePath());
        ExtentReportManager.logInfoDetails("Method : " + queryableRequestSpecification.getMethod());
        ExtentReportManager.logInfoDetails("Request Headers :  ");
        ExtentReportManager.logHeaders(queryableRequestSpecification.getHeaders().asList());
        ExtentReportManager.logInfoDetails("Request Body : ");
//        ExtentReportManager.logJson(queryableRequestSpecification.getBody());
    }

    public static void printResponseLogInReport(Response response){
        ExtentReportManager.logInfoDetails("Response Status is " + response.getStatusCode());
        ExtentReportManager.logInfoDetails("Response headers : ");
        ExtentReportManager.logHeaders(response.getHeaders().asList());
        ExtentReportManager.logInfoDetails("Response Body : ");
//        ExtentReportManager.logJson(response.getBody().prettyPrint());
    }

    public static Response getPilotConfigs(String endpoint,String token, String pilotId)
    {

        String basePath="/entities/pilot/"+pilotId+"/configs/launchpad_ingestion_configs";
        RequestSpecification given = RestAssured.given();
        Response response = given.baseUri(endpoint).basePath(basePath)
                .header("Content-Type", "application/json").header("Authorization", "Bearer" + token).log().all()
                .get();
        printRequestLogInReport(given);
        printResponseLogInReport(response);
        return response;

    }

    public static Response getUsers(String endpoint, String uuid, String token)
    {

        String basePath="v2.0/users/"+uuid;
        RequestSpecification given = RestAssured.given();
        Response response = given.baseUri(endpoint).basePath(basePath)
                .header("Content-Type", "application/json").header("Authorization", "Bearer" + token).log().all()
                .get();
        printRequestLogInReport(given);
        printResponseLogInReport(response);
        System.out.println("Response :"+ response.then().log().body());

        return response;

    }

    public static Response getMetersApi(String endpoint, String uuid, String token)
    {

        String basePath="/meta/users/"+uuid+"/homes/1/gws/2/meters";
        RequestSpecification given = RestAssured.given();
        Response response = given.baseUri(endpoint).basePath(basePath)
                .header("Content-Type", "application/json").header("Authorization", "Bearer" + token).log().all()
                .get();
        printRequestLogInReport(given);
        printResponseLogInReport(response);
        System.out.println("Response :"+ response.then().log().body());

        return response;

    }

    public static Response getGbJsonApi(String endpoint, String uuid, String token, String t1)
    {

        String basePath="/streams/users/"+uuid+"/homes/1/gws/2/meters/1/gb.csv?t0=1422776400&t1="+t1;
        RequestSpecification given = RestAssured.given();
        Response response = given.baseUri(endpoint).basePath(basePath)
                .header("Content-Type", "application/json").header("Authorization", "Bearer" + token).log().all()
                .get();
        printRequestLogInReport(given);
        printResponseLogInReport(response);
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
//        printRequestLogInReport(given);
//        printResponseLogInReport(response);
        return response;
    }

    public static Response generateToken(Map<String,String> envData)
    {

        String pathParam="/oauth/token?grant_type=client_credentials&scope=all";
        RequestSpecification given = RestAssured.given().baseUri(envData.get("baseUri"))
                .basePath(pathParam)
                .header("Content-Type", "application/json").auth().basic(envData.get("username"),envData.get("password"))
                .params("grant_type","client_credentials").params("scope","all");
        Response response = given.get();
        return response;

    }
}
