package com.ebubeokoli.datadeliverer.io.excel;

import org.apache.commons.csv.CSVFormat;
import org.dflib.DataFrame;
import org.dflib.excel.Excel;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExcelFileHandler {

    public static DataFrame readExcelToDataFrame(Path filePath) throws FileNotFoundException {
        return readExcelToDataFrame(filePath, 0);
    }

    public static DataFrame readExcelToDataFrame(Path filePath, int sheetNumber) throws FileNotFoundException {
        return Excel.loader().firstRowAsHeader().loadSheet(filePath, sheetNumber);
    }

    public static DataFrame readExcelToDataFrame(Path filePath, String sheetName) throws FileNotFoundException {
        return Excel.loader().firstRowAsHeader().loadSheet(filePath, sheetName);
    }

    public static Map<String, DataFrame> readExcelToDataFrame(Path filePath, String[] sheetNames) throws FileNotFoundException {
        Map<String, DataFrame> dfBySheetName = new HashMap<>();
        for (String sheet: sheetNames) {
            dfBySheetName.put(sheet, readExcelToDataFrame(filePath, sheet));
        }

        return dfBySheetName;
    }

    public static void writeDataFrameToExcel(Map<String, DataFrame> dfBySheetName, Path filePath) {

        Excel.saver().autoSizeColumns().save(dfBySheetName, filePath);
    }
}
