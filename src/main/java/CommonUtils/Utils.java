package CommonUtils;

import PojoClasses.AssertionKeys;
import PojoClasses.MeterFilePOJO;
import PojoClasses.UserFilePOJO;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.aventstack.extentreports.markuputils.MarkupHelper;
import io.restassured.response.Response;
import reporting.ExtentReportManager;
import reporting.Setup;
import org.apache.commons.io.FilenameUtils;


import java.io.*;
import java.util.*;

public class Utils {

    public static void s3UploadFile(String filePath){
        Regions clientRegion = Regions.DEFAULT_REGION;
        String bucketName = "bidgely-amerenres-nonprodqa";
        String stringObjKeyName = "";
        String[] fileNameTemp  = filePath.split("/");
        String fileObjKeyName = fileNameTemp[fileNameTemp.length-1];
        String fileName = filePath;

        try

        {
            //This code expects that you have AWS credentials set up per:
            // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .build();

            // Upload a text string as a new object.
            //s3Client.putObject(bucketName, stringObjKeyName, "Uploaded String Object");

            // Upload a file as a new object with ContentType and title specified.
            PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(fileName));
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.addUserMetadata("utility_file_name", fileObjKeyName);
            request.setMetadata(metadata);
            s3Client.putObject(request);
        } catch(
                AmazonServiceException e)

        {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch(
                SdkClientException e)

        {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
    }

    public static void assertExpectedValuesWithJsonPath(Response response, Map<String,Object> expectedValuesMap){
        List<AssertionKeys> actualValuesMap = new ArrayList<>();
        actualValuesMap.add(new AssertionKeys("JSON_PATH", "EXPECTED_VALUE","ACTUAL_VALUE","RESULT"));

        boolean allMatched = true;

        Set<String> jsonPaths = expectedValuesMap.keySet();
        for(String jsonPath:jsonPaths){
            Optional<Object> actualValue = Optional.ofNullable(response.jsonPath().get(jsonPath));
            if(actualValue.isPresent()){
                Object value = actualValue.get();
                if(value.equals(expectedValuesMap.get(jsonPath)))
                    actualValuesMap.add(new AssertionKeys(jsonPath, expectedValuesMap.get(jsonPath),value,"Matched"));
                else {
                    allMatched=false;
                    actualValuesMap.add(new AssertionKeys(jsonPath, expectedValuesMap.get(jsonPath), value, "Not Matched"));
                }
            }else {
                allMatched = false;
                actualValuesMap.add(new AssertionKeys(jsonPath, expectedValuesMap.get(jsonPath), "Value not found", "Not Matched"));
            }
        }
        if(allMatched)
            ExtentReportManager.logPassDetails("All assertion are passed. "+"😊😊😊😊😊😊");
        else
            ExtentReportManager.logFailureDetails("All assertions are not passed"+ "😔😔😔😔😔");

        String[][] finalAssertionsMap= actualValuesMap.stream().map(assertions -> new String[] {assertions.getJsonPath(),String.valueOf(assertions.getExpectedValue()), String.valueOf(assertions.getActualValue()), assertions.getResult()})
                .toArray(String[][] :: new);
        Setup.extentTest.get().info(MarkupHelper.createTable(finalAssertionsMap));
    }


    public static String processRawFile(String fileToUpdate, Map<String,String> executionVariables, UserFilePOJO userfileInfo, MeterFilePOJO meterFilePOJO) throws IOException {

        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(fileToUpdate)));
            String line = br.readLine();
            String[] fileParam = line.split("\\|", 5);

            String fileNameToUpdate=FilenameUtils.getBaseName(fileToUpdate);

            File updatedFile = File.createTempFile(fileNameToUpdate, ".csv");
            updatedFile.deleteOnExit();
            FileWriter writer = new FileWriter(updatedFile);
            System.out.println(updatedFile.getAbsolutePath());
            StringBuilder updatedContext = new StringBuilder();
            for (int i = 0; i < fileParam.length - 1; i++) {
                while (line != null) {
                    String updatedLine = "";
                    if(fileNameToUpdate.contains("USERENROLL")){
                        updatedLine = line.replace(fileParam[0], executionVariables.get("customerId")).replace(fileParam[1], executionVariables.get("partnerUserId")).replace(fileParam[2],executionVariables.get("premiseId"));
                        String[] updatedLineArr = updatedLine.split("\\|");

                        userfileInfo.setEmail(updatedLineArr[4]);
                        userfileInfo.setFirst_name(updatedLineArr[5]);
                        userfileInfo.setLast_name(updatedLineArr[6]);
                        userfileInfo.setAddress_1(updatedLineArr[7]);
                        userfileInfo.setCity(updatedLineArr[11]);
                        userfileInfo.setState(updatedLineArr[12]);
                        userfileInfo.setPostal_code(updatedLineArr[13]);

                    }else {
                        updatedLine = line.replace(fileParam[0], executionVariables.get("customerId")).replace(fileParam[1], executionVariables.get("partnerUserId")).replace(fileParam[2],executionVariables.get("premiseId"))
                                .replace(fileParam[3],executionVariables.get("dataStreamId"));

                        if(fileNameToUpdate.contains("METER")){
                            String[] updatedLineArr = updatedLine.split("\\|");
                            meterFilePOJO.setService_type(updatedLineArr[4]);
                            meterFilePOJO.setMeter_type(updatedLineArr[12]);
                        }
                    }
                    updatedContext.append(updatedLine + System.lineSeparator());
                    line = br.readLine();
                }
            }

            String contentToWrite = updatedContext.toString();
            contentToWrite = contentToWrite.substring(0, contentToWrite.lastIndexOf(System.lineSeparator()));

            writer.write(contentToWrite);
            br.close();
            writer.close();

            return updatedFile.getAbsolutePath();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static Map<String,String> getTimeStampsConsumption(String fileToUpdate) throws IOException {
        Map<String,String> timeStampConsumption = new HashMap<>();

        try {
            LineNumberReader lnr = new LineNumberReader(new FileReader(fileToUpdate));
            long lines=0;
            int[] linenos=new int[2];
            Random random= new Random();
            linenos[0]=random.nextInt(1000);
            linenos[1]=random.nextInt(1000);
            while (lnr.readLine() != null) {
                lines = lnr.getLineNumber();
                for(int line:linenos){
                    if(lines == line){
                        timeStampConsumption.putAll(getTS(lnr.readLine()));
                    }
                }
            }
            return timeStampConsumption;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static Map<String,String> getTS(String s){
        Map<String,String> map=new HashMap<>();
        String[] fileParam = s.split("\\|", 7);
        map.put(fileParam[4],fileParam[5]);

        return map;
    }
}
