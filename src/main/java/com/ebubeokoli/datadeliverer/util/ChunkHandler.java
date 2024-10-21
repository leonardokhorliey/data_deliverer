package com.ebubeokoli.datadeliverer.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.ebubeokoli.datadeliverer.io.excel.ExcelFileHandler;
import org.dflib.DataFrame;
import org.dflib.Printers;
import org.dflib.Series;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class ChunkHandler implements Runnable {

    private static final int DEFAULT_CHUNK_SIZE = 1_000_000;
    static int chunkSize;
    private Object chunk;

    private OutputConfiguration outputConfig;

    private Path fullyDefinedPath;

    public ChunkHandler(Object chunk,
                        String outputDirectory,
                        String outputFilesPrefix,
                        int chunkBatchNumber,
                        OutputConfiguration outputConfig
    ) {
//        System.out.println("Param size: " + chunk.size());
        this.chunk = chunk;
        System.out.println("Assigned size: " + ((Map<String, List<Object>>) this.chunk).size());
//        System.out.println(chunk.getFirst());
//        System.out.println(chunk.size());
        this.outputConfig = outputConfig;
        fullyDefinedPath = Path.of(outputDirectory + "/" + outputFilesPrefix + "_file_" + chunkBatchNumber + "." + outputConfig.getOutputFileType().getLabel());
    }

    public Path getFullyDefinedPath() {
        return fullyDefinedPath;
    }

    public static void setChunkSize(int newChunkSize) {
        chunkSize = newChunkSize;
    }

    public static int getChunkSize() {
        return chunkSize == 0 ? DEFAULT_CHUNK_SIZE : chunkSize;
    }

    public static boolean processChunkToCsv(List<Map<String, Object>> clickHouseOutputChunk, String fileName) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = mapper.writeValueAsString(clickHouseOutputChunk);

        JsonNode jsonTree = new ObjectMapper().readTree(jsonString);

        CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder();
        JsonNode firstObject = jsonTree.elements().next();
        firstObject.fieldNames().forEachRemaining(csvSchemaBuilder::addColumn);
        CsvSchema csvSchema = csvSchemaBuilder
                .build()
                .withHeader();

        CsvMapper csvMapper = new CsvMapper();
        File outputFile = new File(fileName);
        try {
            csvMapper.writerFor(JsonNode.class)
                    .with(csvSchema)
                    .writeValue(outputFile, jsonTree);

            return true;

        } catch (IOException e) {
            return false;
        }

    }

//    public DataFrame processChunkToDataFrame() throws IOException {
//        ObjectMapper mapper = new ObjectMapper();
//        System.out.println(this.chunk.getFirst());
//        System.out.println("Got here " + this.chunk.size());
//        String jsonString = mapper.writeValueAsString(this.chunk);
//
//        JsonNode jsonTree = new ObjectMapper().readTree(jsonString);
//
//        CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder();
//        JsonNode firstObject = jsonTree.elements().next();
//        firstObject.fieldNames().forEachRemaining(csvSchemaBuilder::addColumn);
//        List<String> columns = csvSchemaBuilder
//                .build()
//                .withHeader().getColumnNames();
//
//        Map<String, List<Object>> colMaps = new LinkedHashMap<>();
//
//        for (int i = 0; i < this.chunk.size(); i++) {
//            Map<String, Object> record = this.chunk.get(i);
//            for (String col: columns) {
//                List<Object> colData = colMaps.getOrDefault(col, new ArrayList<>());
//                colData.add(record.get(col));
//                colMaps.put(col, colData);
//            }
//        }
//
//        return DataFrame.byColumn(columns.toArray(String[]::new)).of(colMaps.values().stream().map(Series::ofIterable).toArray(Series[]::new));
//    }

    public DataFrame processChunkToDataFrameC() throws IOException {
        Set<String> columns = ((Map<String, List<Object>>) this.chunk).keySet();

        return DataFrame.byColumn(columns.toArray(String[]::new)).of(((Map<String, List<Object>>) this.chunk).values().stream().map(Series::ofIterable).toArray(Series[]::new));
    }



    @Override
    public void run() {
        long startTime;
        switch (outputConfig.getOutputFileType()) {
            case CSV -> {
                startTime = new Date().getTime();
                try {
                    ChunkHandler.processChunkToCsv((List<Map<String, Object>>) chunk, fullyDefinedPath.toAbsolutePath().toString());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            case XLSX -> {
                DataFrame df = null;
                try {
                    df = processChunkToDataFrameC();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                System.out.println(Printers.tabular.toString(df));
                startTime = new Date().getTime();

                ExcelFileHandler.writeDataFrameToExcel(Map.of("Sheet1", df), fullyDefinedPath);
            }
            default -> startTime = new Date().getTime();
        }
        System.out.println("Writing to " + fullyDefinedPath + " took " + (new Date().getTime() - startTime) + " milliseconds");
    }
}
