package commonUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import pojoClasses.*;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.aventstack.extentreports.ExtentTest;
import com.aventstack.extentreports.markuputils.MarkupHelper;
import io.restassured.response.Response;
import reporting.ExtentReportManager;
import reporting.Setup;
import org.apache.commons.io.FilenameUtils;


import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static reporting.Setup.parentExtent;

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
                    actualValuesMap.add(new AssertionKeys("Validating "+jsonPath.substring(jsonPath.lastIndexOf('.') + 1), expectedValuesMap.get(jsonPath),value,"Matched"));
                else {
                    allMatched=false;
                    actualValuesMap.add(new AssertionKeys("Validating "+jsonPath.substring(jsonPath.lastIndexOf('.') + 1), expectedValuesMap.get(jsonPath), value, "Not Matched"));
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


    public static String processFile(String fileToUpdate, Map<String,String> executionVariables, UserFilePOJO userFilePOJO, MeterFilePOJO meterFilePOJO, String dataStreamId) throws IOException {

        try {
            BufferedReader br = new BufferedReader(new FileReader(fileToUpdate));
            String line = br.readLine();
            String[] fileParam = line.split("\\|", 5);

            String fileNameToUpdate=FilenameUtils.getBaseName(fileToUpdate);

            File updatedFile = File.createTempFile(fileNameToUpdate, ".csv");
            updatedFile.deleteOnExit();
            FileWriter writer = new FileWriter(updatedFile);
            System.out.println(updatedFile.getAbsolutePath());
            StringBuilder updatedContext = new StringBuilder();
            for (int i = 0; i < fileParam.length - 1; i++) {
                int k=0;
                while (line != null) {
                    long dataStreamIdTemp=Long.valueOf(executionVariables.get("dataStreamId"))+k;
                    String[] fileParamTemp = line.split("\\|", 5);
                    String updatedLine = "";
                    if(fileNameToUpdate.contains("USERENROLL")){
                        updatedLine = line.replace(fileParamTemp[0], executionVariables.get("customerId")).replace(fileParamTemp[1]
                                , executionVariables.get("partnerUserId")).replace(fileParamTemp[2],executionVariables.get("premiseId"));
                        String[] updatedLineArr = updatedLine.split("\\|");

                        userFilePOJO.setEmail(updatedLineArr[4]);
                        userFilePOJO.setFirst_name(updatedLineArr[5]);
                        userFilePOJO.setLast_name(updatedLineArr[6]);
                        userFilePOJO.setAddress_1(updatedLineArr[7]);
                        userFilePOJO.setCity(updatedLineArr[11]);
                        userFilePOJO.setState(updatedLineArr[12]);
                        userFilePOJO.setPostal_code(updatedLineArr[13]);

                    } else if (fileNameToUpdate.contains("METER")) {
                        updatedLine = line.replace(fileParamTemp[0], executionVariables.get("customerId")).replace(fileParamTemp[1], executionVariables.get("partnerUserId"))
                                .replace(fileParamTemp[2],executionVariables.get("premiseId")).replace(fileParamTemp[3],dataStreamId);

                        updatedLine = line.replace(fileParamTemp[0], executionVariables.get("customerId")).replace(fileParamTemp[1], executionVariables.get("partnerUserId"))
                                .replace(fileParamTemp[2],executionVariables.get("premiseId")).replace(fileParamTemp[3],String.valueOf(dataStreamIdTemp));

                        String[] updatedLineArr = updatedLine.split("\\|");
                        meterFilePOJO.setService_type(updatedLineArr[4]);
                        meterFilePOJO.setMeter_type(updatedLineArr[12]);
                    } else {
                        updatedLine = line.replace(fileParamTemp[0], executionVariables.get("customerId")).replace(fileParamTemp[1], executionVariables.get("partnerUserId"))
                                .replace(fileParamTemp[2],executionVariables.get("premiseId")).replace(fileParamTemp[3],dataStreamId);
                    }
                    updatedContext.append(updatedLine + System.lineSeparator());
                    line = br.readLine();
                    k++;
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
            long linesNo=0;
            int[] linenos=new int[2];
            Random random= new Random();
            linenos[0]=random.nextInt(1000);
            linenos[1]=random.nextInt(1000);
            while (lnr.readLine() != null) {
                linesNo = lnr.getLineNumber();
                for(int line:linenos){
                    if(linesNo == line){
                        timeStampConsumption.putAll(getTimeStamp(lnr.readLine()));
                    }
                }
            }
            return timeStampConsumption;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static Map<String,String> getTimeStamp(String s){
        Map<String,String> map=new HashMap<>();
        String[] fileParam = s.split("\\|", 7);
        map.put(fileParam[4],fileParam[5]);

        return map;
    }

    public static Map<String,Map<String,Map<String,String>>> getTimeStampsInvoiceData(String filePath) throws IOException {
        Map<String,Map<String,Map<String,String>>> timeStampInvoiceCost = new HashMap<>();

        long lines = countNoOfLines(filePath);
        try {
            LineNumberReader lnr = new LineNumberReader(new FileReader(filePath));
            long rowNo=0;
            Long[] randomRowNoArray=new Long[2];
            Random random= new Random();

            for(int i=0;i<2;i++){
                randomRowNoArray[i] = random.nextLong(0, lines/5) * 5;  //getting random rows in multiple of 5
            }

            while (lnr.readLine() != null) {
                rowNo = lnr.getLineNumber();

                for(long row:randomRowNoArray){
                    Map<String,Map<String,Map<String,String>>> parentMap = new HashMap<>();
                    if(rowNo == row){
                        Map<String,Map<String,String>> timeStampBasedMap = new HashMap<>();
                        for(int i=0;i<5;i++){
                            Map<String,String> chargeTypeBasedMap=getInvoiceDataForTimeStamp(lnr.readLine());
                            timeStampBasedMap.put(chargeTypeBasedMap.get("charge_type")+chargeTypeBasedMap.get("charge_name"),chargeTypeBasedMap);
                        }
                        Map<String,String> m=timeStampBasedMap.get("TOTAL");
                        parentMap.put(m.get("bc_start_date"),timeStampBasedMap);

                    }
                    timeStampInvoiceCost.putAll(parentMap);
                }

            }
            return timeStampInvoiceCost;

        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String,String> getInvoiceDataForTimeStamp(String line) throws ParseException {
        Map<String,String> cMap= new HashMap<>();
        String[] fileParam = line.split("\\|");
        cMap.put("bc_start_date", String.valueOf(getEpochTimeFromDate(fileParam[6])));
        parentExtent.info("bc_start_date in sheet :"+fileParam[6]);
        parentExtent.info("bc_start_date converted:"+String.valueOf(getEpochTimeFromDate(fileParam[6])));
        cMap.put("bc_end_date", String.valueOf(getEpochTimeFromDate(fileParam[7])));
        parentExtent.info("bc_end_date in sheet :"+fileParam[7]);
        parentExtent.info("bc_end_date converted :"+String.valueOf(getEpochTimeFromDate(fileParam[7])));
        cMap.put("charge_name", fileParam[9]);
        cMap.put("charge_type", fileParam[10]);
        cMap.put("kWh_Consumption", fileParam[11]);
        cMap.put("dollar_cost", fileParam[12]);
        cMap.put("meter_type", fileParam[13]);

        return cMap;
    }

    public static long getEpochTimeFromDate(String bc_date) throws ParseException {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("CST"));
        final Long millis = sdf.parse(bc_date).getTime() / 1000;
        return millis;
    }

    public static long countNoOfLines(String fileName) {
        File file = new File(fileName);
        long lines = 0;
        try (LineNumberReader lnr = new LineNumberReader(new FileReader(file))) {
            while (lnr.readLine() != null) ;
            lines = lnr.getLineNumber();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

    /**
     * this method will read the data from s3 using Spark session
     * @param bucketName
     * @param key
     * @return
     */
    public static Dataset<Row> getS3FirehoseData(SparkSession spark, String bucketName, String key)
    {

        String path="s3a://"+bucketName+key;
        Dataset<Row> df = spark.read().option("inferSchema",true).json(path);
        return df;
    }

    /**
     *
     * To get the today date in the Format yyyy/MM/dd format
     */
    public static  String getTodayDate()
    {
        LocalDate today = LocalDate.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
        return  today.format(formatter);
    }

    /**
     * To get the current UTC hour based on local time
     */
    public static String getUtcTime()
    {
        LocalDateTime utcTime = LocalDateTime.now(ZoneOffset.UTC);

        // Format the hour with leading zero if less than 9
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH");
        String formattedHour = utcTime.format(formatter);

        // Check if hour is less than 9, prepend a zero
        if (Integer.parseInt(formattedHour) < 9) {
            formattedHour = "0" + formattedHour;
        }
        return formattedHour;

    }

    public static Dataset<Row> readInvoiceInputFile(SparkSession spark, String filePath)
    {
        //To define the schema of the input invoice File
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("customerId", DataTypes.StringType, true),
                DataTypes.createStructField("partnerId", DataTypes.StringType, true),
                DataTypes.createStructField("premiseId", DataTypes.StringType, true),
                DataTypes.createStructField("dataStreamId", DataTypes.StringType, true),
                DataTypes.createStructField("fuelType", DataTypes.StringType, true),
                DataTypes.createStructField("meterCycleCode", DataTypes.StringType, true),
                DataTypes.createStructField("billingStartDate", DataTypes.StringType, true),
                DataTypes.createStructField("billingEndDate", DataTypes.StringType, true),
                DataTypes.createStructField("billingDuration", DataTypes.StringType, true),
                DataTypes.createStructField("chargeName", DataTypes.StringType, true),
                DataTypes.createStructField("chargeType", DataTypes.StringType, true),
                DataTypes.createStructField("consumption", DataTypes.StringType, true),
                DataTypes.createStructField("currencyCost", DataTypes.StringType, true),
                DataTypes.createStructField("dataStreamType", DataTypes.StringType, true)
        });


        Dataset<Row> df1=spark.read().format("csv").schema(schema) .option("sep","|").load(filePath);
        return df1;
    }

    public static Dataset<Row> readMeterInputFile(SparkSession spark,String filePath)
    {
        StructType schemaMeter = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("customerId", DataTypes.StringType, true),
                DataTypes.createStructField("partnerId", DataTypes.StringType, true),
                DataTypes.createStructField("premiseId", DataTypes.StringType, true),
                DataTypes.createStructField("dataStreamId", DataTypes.StringType, true),
                DataTypes.createStructField("fuelType", DataTypes.StringType, true),
                DataTypes.createStructField("serviceAgreementStDate", DataTypes.StringType, true),
                DataTypes.createStructField("serviceAgreementEnDate", DataTypes.StringType, true),
                DataTypes.createStructField("ratePlanId", DataTypes.StringType, true),
                DataTypes.createStructField("ratePlanEffectiveDate", DataTypes.StringType, true),
                DataTypes.createStructField("billingCycleCode", DataTypes.StringType, true),
                DataTypes.createStructField("billingCycleEffectiveDate", DataTypes.StringType, true),
                DataTypes.createStructField("solar", DataTypes.StringType, true),
                DataTypes.createStructField("dataStreamType", DataTypes.StringType, true),
                DataTypes.createStructField("label", DataTypes.StringType, true)
        });
        Dataset<Row> df2=spark.read().format("csv").schema(schemaMeter).option("sep","|").load(filePath);
        return df2;
    }
}
