package CommonUtils;

import PojoClasses.AssertionKeys;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.aventstack.extentreports.markuputils.MarkupHelper;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import io.restassured.response.Response;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.poi.ss.format.CellFormatType;
import org.apache.poi.xssf.usermodel.XSSFShape;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import reporting.ExtentReportManager;
import reporting.Setup;

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
            ExtentReportManager.logpassDetails("All assertion are passed. "+"😊😊😊😊😊😊");
        else
            ExtentReportManager.logFailureDetails("All assertions are not passed"+ "😔😔😔😔😔");

        String[][] finalAssertionsMap= actualValuesMap.stream().map(assertions -> new String[] {assertions.getJsonPath(),String.valueOf(assertions.getExpectedValue()), String.valueOf(assertions.getActualValue()), assertions.getResult()})
                .toArray(String[][] :: new);
        Setup.extentTest.get().info(MarkupHelper.createTable(finalAssertionsMap));
    }


    public static String processRawFile(String fileToUpdate,String customerId, String partnerUserId, String premiseId,String dataStreamId) throws IOException {

        String[] replacements = {customerId, partnerUserId, premiseId, dataStreamId};
        Map<String, String> substitutionMap = new HashMap<>();

        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(fileToUpdate)));
            String line = br.readLine();
            String[] fileParam = br.readLine().split("\\|", 5);

            File updatedFile = File.createTempFile("RAW_D_900_S_500400306_", ".csv");
            updatedFile.deleteOnExit();
            FileWriter writer = new FileWriter(updatedFile);
            System.out.println(updatedFile.getAbsolutePath());
            StringBuilder updatedContext = new StringBuilder();
            for (int i = 0; i < fileParam.length - 1; i++) {
                while (line != null) {
                    String updatedLine = "";
                    updatedLine = line.replace(fileParam[0], replacements[0]).replace(fileParam[1], replacements[1]).replace(fileParam[2], replacements[2])
                            .replace(fileParam[3], replacements[3]);

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

    }}
