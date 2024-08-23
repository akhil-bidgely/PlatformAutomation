package dataProviderFile;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.testng.annotations.DataProvider;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import static constants.ConstantFile.*;
import static constants.FilePaths.*;

public class IngestionsDataProvider {

    @DataProvider(name = "singleMeterDP")
    public Object[][] singleMeterDP(){
        return new Object[][]{
                {"AMI_E",USER_ENROLLMENT_AMI_E_PATH, METER_ENROLLMENT_AMI_E_PATH, RAW_AMI_E_PATH, INVOICE_AMI_E_PATH,USER_PREFERENCE_AMI_E_PATH,"GreenButton",AMI_E_GWS},
//                {"AMI_E",OPWER_AMI_E_PATH, METER_ENROLLMENT_AMI_E_PATH, RAW_AMI_E_PATH, INVOICE_AMI_E_PATH,USER_PREFERENCE_AMI_E_PATH,"GreenButton",AMI_E_GWS},
//                {"AMR_E",USER_ENROLLMENT_AMR_E_PATH, METER_ENROLLMENT_AMR_E_PATH, RAW_AMR_E_PATH, INVOICE_AMR_E_PATH, "GB_MONTH", AMR_E_GWS},
        };
    }

    @DataProvider(name = "zipDP")
    public Object[][] getDataFromExcel() {
        String excelFilePath = "/Users/akhilsharma/IdeaProjects/AutomationNew/src/test/resources/Book2.xlsx";
        String sheetName = "Sheet1";

        Object[][] arrayObject = getExcelData(excelFilePath,sheetName);
        return arrayObject;
    }



    public String[][] getExcelData(String fileName, String sheetName) {
        String[][] arrayExcelData = null;
        try {
            FileInputStream fs = new FileInputStream(fileName);

            XSSFWorkbook xssfWorkbook= new XSSFWorkbook(fs);
            XSSFSheet sheet= xssfWorkbook.getSheet(sheetName);

            int totalNoOfRows = sheet.getPhysicalNumberOfRows();
            int totalNoOfCols = sheet.getRow(0).getPhysicalNumberOfCells();


            arrayExcelData = new String[totalNoOfRows-1][totalNoOfCols];
            for (int i = 1; i < totalNoOfRows; i++) {
                Row row = sheet.getRow(i);
                if (row != null) {
                    for (int j = 0; j < totalNoOfCols; j++) {
                        Cell cell = row.getCell(j);
                        if (cell != null) {
                            arrayExcelData[i - 1][j] = cell.toString();
                        } else {
                            arrayExcelData[i - 1][j] = ""; // Or handle null cell value as needed
                        }
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            e.printStackTrace();
        }
        return arrayExcelData;
    }

    @DataProvider(name = "multimeterDP")
    public Object[][] multimeterDP(){
        return new Object[][]{
                {"AMI_E+AMI_E",USER_ENROLLMENT_AMI_E_AMI_E_PATH, METER_ENROLLMENT_AMI_E_AMI_E_PATH, RAW1_AMI_E_AMI_E_PATH, RAW2_AMI_E_AMI_E_PATH, INVOICE1_AMI_E_AMI_E_PATH, INVOICE2_AMI_E_AMI_E_PATH,"GreenButton",AMI_E_GWS},
        };
    }

    @DataProvider(name = "duelFuelDP")
    public Object[][] duelFuelDP(){
        return new Object[][]{
                {"AMR_E+AMR_G",USER_ENROLLMENT_AMR_E_AMR_G_PATH, METER_ENROLLMENT_AMR_E_AMR_G_PATH, RAW1_AMR_E_AMR_G_PATH, RAW2_AMR_E_AMR_G_PATH, INVOICE1_AMR_E_AMR_G_PATH,"GB_MONTH",AMR_E_GWS,AMR_G_GWS},
        };
    }

}

