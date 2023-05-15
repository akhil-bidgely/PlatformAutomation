package serviceHelper;


import pojoClasses.MeterFilePOJO;
import io.restassured.http.ContentType;
import io.restassured.specification.QueryableRequestSpecification;
import io.restassured.specification.SpecificationQuerier;
import org.json.JSONObject;
import reporting.ExtentReportManager;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import constants.ConfigManagar;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static constants.Endpoints.*;

public class RestUtils {
    public static final String BASE_URL= ConfigManagar.getConfig("baseUri");
    public static final String REDSHIFT_URL=ConfigManagar.getConfig("redShiftUrl");
    public void printRequestLogInReport(RequestSpecification requestSpecification, String APIName){
        QueryableRequestSpecification queryableRequestSpecification= SpecificationQuerier.query(requestSpecification);
        ExtentReportManager.logHeadings("<b>Calling API : </b>" + APIName);
        ExtentReportManager.logInfoDetails("<b>Endpoint : </b>" + queryableRequestSpecification.getBaseUri()+queryableRequestSpecification.getBasePath());
        ExtentReportManager.logInfoDetails("<b>Method : </b>" + queryableRequestSpecification.getMethod());
        ExtentReportManager.logInfoDetails("<b>Request Headers : </b> ");
        ExtentReportManager.logHeaders(queryableRequestSpecification.getHeaders().asList());
        ExtentReportManager.logInfoDetails("<b>Request Body : </b>"+ queryableRequestSpecification.getBody());
//        ExtentReportManager.logJson(queryableRequestSpecification.getBody());
    }

    public void printResponseLogInReport(Response response){
        ExtentReportManager.logInfoDetails("Response Status : " + response.getStatusCode());
        ExtentReportManager.logInfoDetails("Response headers : ");
        ExtentReportManager.logHeaders(response.getHeaders().asList());
//        ExtentReportManager.logInfoDetails("Response Body : " + response.getBody().prettyPrint());
        ExtentReportManager.logInfoDetails("Response Body : ");
        ExtentReportManager.logJson(response.getBody().prettyPrint());
    }

    public Response getPilotConfigs(String token, String pilotId)
    {

        RequestSpecification given = RestAssured.given();
        Response response = given.auth().oauth2(token).pathParam("pilotId",pilotId)
                .baseUri(BASE_URL).basePath(GET_PILOT_CONFIGS)
                .contentType(ContentType.JSON)
                .log().all()
                .get();
        printRequestLogInReport(given, "Pilot Config API");
        printResponseLogInReport(response);
        return response;

    }

    public Response getUsers(String uuid, String token)
    {

        RequestSpecification given = RestAssured.given();
        Response response = given.auth().oauth2(token).baseUri(BASE_URL).pathParam("uuid",uuid).basePath(GET_USER)
                .contentType(ContentType.JSON).log().all()
                .get();
        printRequestLogInReport(given, "Pilot User Details API");
        printResponseLogInReport(response);

        return response;

    }

    public Response getMetersApi(String uuid, String token, int gws)
    {
        RequestSpecification given = RestAssured.given();
        Response response = given.auth().oauth2(token).baseUri(BASE_URL).basePath(GET_METERS).pathParam("uuid",uuid).pathParam("gws",gws)
                .contentType(ContentType.JSON).log().all()
                .get();

        printRequestLogInReport(given, "Get Meters API");

        return response;

    }

    public Response getGbJsonApi(String uuid, String token, String t1, String scenario, int gws)
    {
//        int gws;
//        if(scenario.equals("AMI_E")){
//            gws=2;
//        }else if(scenario.equals("AMR_E")){
//            gws=3;
//        }else {
//            gws=0;
//        }
        RequestSpecification given = RestAssured.given();
        Response response = given.auth().oauth2(token).baseUri(BASE_URL).basePath(GB_JSON).pathParam("uuid",uuid).pathParam("gws",gws)
                .queryParam("t0","1422776400")
                .queryParam("t1",t1)
                .contentType(ContentType.JSON)
                .get();
        printRequestLogInReport(given, "GB.json API");
        printResponseLogInReport(response);

        return response;

    }

    public Response getLabelTimeStamp(String uuid, String token)
    {
        RequestSpecification given = RestAssured.given();
        Response response = given.auth().oauth2(token).baseUri(BASE_URL).basePath(LABEL_TIMESTAMP).pathParam("uuid",uuid)
                .contentType(ContentType.JSON).log().all()
                .get();
        System.out.println("Response :");
        printRequestLogInReport(given, "Label Timestamp API");
        printResponseLogInReport(response);

        return response;

    }
///  was testing awaitility method here , will be deleted later
//    public Response getPartnerUserId(String token, Map<String, String> executionVariables) throws InterruptedException {
//
////        RequestSpecification given = RestAssured.given();
////        Response response = given.baseUri(BASE_URL).basePath(PARTNER_USERID).queryParam("partnerUserId",executionVariables.get("partnerUserId"))
////                .contentType(ContentType.JSON).header("Authorization", "Bearer" + token).log().all()
////                .get();
//
////        JSONObject obj= new JSONObject(response.asString());
////        System.out.println("Length of data : "+obj.getJSONObject("payload").getJSONArray("data").length());
//        int len=0;
//        Response response = null;
//
//        while(len < 1){
//            RequestSpecification given = RestAssured.given();
//            response = given.baseUri(BASE_URL).basePath(PARTNER_USERID).queryParam("partnerUserId",executionVariables.get("partnerUserId"))
//                    .contentType(ContentType.JSON).header("Authorization", "Bearer" + token).log().all()
//                    .get();
//            Thread.sleep(1000);
//            JSONObject obj= new JSONObject(response.asString());
//            len=obj.getJSONObject("payload").getJSONArray("data").length();
//            System.out.println("Length of data : "+obj.getJSONObject("payload").getJSONArray("data").length());
//            if(len ==1){
//                printRequestLogInReport(given, "Partner User Id API");
//                printResponseLogInReport(response);
//            }
//        }
//
//        return response;
//    }

    public Response getPartnerUserId(String token, Map<String, String> executionVariables)
    {
        RequestSpecification given = RestAssured.given();
        Response response = given.auth().oauth2(token).baseUri(BASE_URL).basePath(PARTNER_USERID).queryParam("partnerUserId",executionVariables.get("partnerUserId"))
                .contentType(ContentType.JSON).log().all()
                .get();
        printRequestLogInReport(given, "Partner User Id API");
        printResponseLogInReport(response);
        return response;
    }

    public Response getGbDisaggResp(String token, String partnerUserId)
    {
        RequestSpecification given = RestAssured.given();
        Response response = given.auth().oauth2(token).baseUri(BASE_URL).basePath(PARTNER_USERID).queryParam("partnerUserId",partnerUserId)
                .contentType(ContentType.JSON).log().all()
                .get();
        System.out.println("Response :"+ response.prettyPrint());
        printRequestLogInReport(given, "Partner User Id API");
        printResponseLogInReport(response);
        return response;
    }

    public Response getUtilityData(String uuid,String token, String t1)
    {
        RequestSpecification given = RestAssured.given();
        Response response = given.auth().oauth2(token).log().all().baseUri(BASE_URL).basePath(UTILITY_DATA)
                .pathParam("uuid",uuid)
                .queryParam("t0","1422776400").queryParam("t1",t1)
                .contentType(ContentType.JSON)
                .get();
        printRequestLogInReport(given, "Utility Data API");
        printResponseLogInReport(response);
        return response;
    }

    public Response generateToken()
    {

        RequestSpecification given = RestAssured.given().baseUri(BASE_URL)
                .basePath(GENERATE_TOKEN)
                .queryParam("grant_type","client_credentials")
                .queryParam("scope","all").contentType(ContentType.JSON)
                .auth().basic(ConfigManagar.getConfig("username"),ConfigManagar.getConfig("password"))
                .params("grant_type","client_credentials").params("scope","all").log().all();
        Response response = given.get();
        response.prettyPrint();
//        ExtentReportManager.logInfoDetails("<b>Endpoint : </b>" +given.get().body().prettyPrint());
        return response;

    }

    public Response postRedshiftQuery(String query)
    {
        RequestSpecification given = RestAssured.given();
        given.baseUri(REDSHIFT_URL);
        JSONObject requestParams = new JSONObject();
        requestParams.put("statement", query);
        given.header("Content-Type", "application/json");
        given.body(requestParams.toString());
        Response post = given.post();
        return post;
    }
}
