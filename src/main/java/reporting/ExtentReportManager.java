package reporting;

import com.aventstack.extentreports.ExtentReports;
import com.aventstack.extentreports.Status;
import com.aventstack.extentreports.markuputils.CodeLanguage;
import com.aventstack.extentreports.markuputils.ExtentColor;
import com.aventstack.extentreports.markuputils.MarkupHelper;
import com.aventstack.extentreports.reporter.ExtentSparkReporter;
import com.aventstack.extentreports.reporter.configuration.Theme;
import io.restassured.http.Header;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static com.aventstack.extentreports.reporter.configuration.Protocol.HTTPS;

public class ExtentReportManager {
    public static ExtentReports extentReports;

    public static ExtentReports createInstance(String reportPath,String reportName, String documentTitle) throws IOException {
        ExtentSparkReporter extentSparkReporter = new ExtentSparkReporter(reportPath);
//        extentSparkReporter.loadJSONConfig(new File("./src/test/resources/nonprodqa/extent-report-config.json"));
        extentSparkReporter.config().setReportName(reportName);
        extentSparkReporter.config().setDocumentTitle(documentTitle);
        extentSparkReporter.config().setTheme(Theme.DARK);
        extentSparkReporter.config().setEncoding("utf-8");
        extentSparkReporter.config().setProtocol(HTTPS);

        extentReports = new ExtentReports();
        extentReports.attachReporter(extentSparkReporter);
        return extentReports;
    }

    public static String getReportNameWithTimeStamp(){
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss");
        LocalDateTime localDateTime = LocalDateTime.now();
        String formattedTime = dateTimeFormatter.format(localDateTime);
        String reportName= "TestReport" + formattedTime + ".html";
        return reportName;
    }

    public static void  logPassDetails(String log){
        Setup.extentTest.get().pass(MarkupHelper.createLabel(log, ExtentColor.GREEN));
    }
    public static void  logHeadings(String log){
        Setup.extentTest.get().info(MarkupHelper.createLabel(log, ExtentColor.CYAN));
    }
    public static void  logFailureDetails(String log){
        Setup.extentTest.get().fail(MarkupHelper.createLabel(log, ExtentColor.RED));
    }
    public static void  logExceptionDetails(String log){
        Setup.extentTest.get().fail(log);
    }
    public static void  logInfoDetails(String log){
        Setup.extentTest.get().info(MarkupHelper.createLabel(log, ExtentColor.GREY));
    }
    public static void  logWarningDetails(String log){
        Setup.extentTest.get().warning(MarkupHelper.createLabel(log, ExtentColor.YELLOW));
    }

    public static void  logJson(String json ){
        Setup.extentTest.get().info(MarkupHelper.createCodeBlock(json, CodeLanguage.JSON));
    }
    public static void  logHeaders(List<Header> headerList ){
        String[][] arrayHeaders=headerList.stream().map(header -> new String[] {header.getName(),header.getValue()})
                        .toArray(String[][] :: new);
        Setup.extentTest.get().info(MarkupHelper.createTable(arrayHeaders));
    }
}
