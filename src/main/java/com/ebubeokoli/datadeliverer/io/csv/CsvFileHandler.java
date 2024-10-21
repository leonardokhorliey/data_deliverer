package com.ebubeokoli.datadeliverer.io.csv;

import org.apache.commons.csv.CSVFormat;
import org.dflib.DataFrame;
import org.dflib.csv.Csv;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class CsvFileHandler {

    public static DataFrame readCsvToDataFrame(Path filePath) throws FileNotFoundException {

        String[] columns = Csv.loader().limit(0).load(filePath).getColumnsIndex().toArray();
        return Csv.loader().header(columns).load(filePath);
    }

    public static List<DataFrame> readCsvToDataFrame(Path filePath, int chunkSize) throws FileNotFoundException {
        List<DataFrame> outputDfs = new ArrayList<>();

        boolean reachedEnd = false;
        int currentBatch =  0;
        String[] columns = Csv.loader().limit(0).load(filePath).getColumnsIndex().toArray();
        while (!reachedEnd) {
            System.out.println("Running for file " + ++currentBatch);
            DataFrame df2 = Csv.loader().header(columns).offset((currentBatch - 1) * chunkSize + 1).limit(chunkSize).load(filePath);
            outputDfs.add(df2);
            if (df2.height() == 0 || df2.height() < chunkSize) {
                reachedEnd = true;
            }
        }
        return outputDfs;
    }

    public static void writeDataFrameToCsv(DataFrame df, Path filePath) {

        Csv.saver().format(CSVFormat.DEFAULT).save(df, filePath);
    }


}
