package reporting;

import com.aventstack.extentreports.ExtentReports;
import com.aventstack.extentreports.ExtentTest;
import com.aventstack.extentreports.Status;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

public class Setup implements ITestListener {
    private static ExtentReports extentReports;
    public static ExtentTest test;
    public static ThreadLocal<ExtentTest> extentTest = new ThreadLocal<>();

    @Override
    public void onTestStart(ITestResult result) {
        test= extentReports.createTest(result.getTestClass()+" :- "+result.getMethod().getMethodName());
        extentTest.set(test);
    }

    @Override
    public void onStart(ITestContext context) {
        String fileName = ExtentReportManager.getReportNameWithTimeStamp();
        String reportPath = System.getProperty("user.dir")+"\\reports\\"+fileName;
        try {
            extentReports = ExtentReportManager.createInstance(fileName,"Test Api Automation Report","Test Execution Report");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        extentTest.get().log(Status.PASS,"Success");
    }

    @Override
    public void onTestFailure(ITestResult result) {
        ExtentReportManager.logFailureDetails(result.getThrowable().getMessage());

        String stackTrace = Arrays.toString(result.getThrowable().getStackTrace());
        stackTrace = stackTrace.replaceAll(",","<br>");
        String formattedTrace = "<details>\n" +
                "  <summary>Click here to see exception</summary>\n" +
                "  <p>\n" +
                stackTrace+"\n"+
                "  </p>\n" +
                "</details>\n";
        ExtentReportManager.logExceptionDetails(formattedTrace);
    }

    @Override
    public void onFinish(ITestContext context) {
        if(extentReports !=null)
            extentReports.flush();
    }
}
