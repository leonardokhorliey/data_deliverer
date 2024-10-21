package com.ebubeokoli.datadeliverer;

import com.clickhouse.client.ClickHouseException;
import com.ebubeokoli.datadeliverer.clickhouse.ClickHouse;
import com.ebubeokoli.datadeliverer.io.ZipFileHandler;
import com.ebubeokoli.datadeliverer.util.ChunkHandler;
import com.ebubeokoli.datadeliverer.util.OutputConfiguration;
import io.github.cdimascio.dotenv.Dotenv;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataDeliverer {

    static {
        loadEnvVars();
    }

    public static void loadEnvVars() {
        Dotenv dotenv = Dotenv.configure().load();
        dotenv.entries().forEach(e -> System.setProperty(e.getKey(), e.getValue()));
    }

    public static String[] runDelivery(
            final String query,
            final String outputType,
            final String outputConfigurationType,
            final int processingChunkSize,
            final String outputFilesPrefix,
            final String driveLocation,
            final int numberOfFilesPerOutputBatch
    ) throws ClickHouseException, IOException, GeneralSecurityException {

        final String dateFormatSuffix = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        final String outputDirectory = "data";
        OutputConfiguration outputConfig = new OutputConfiguration(outputConfigurationType, outputType, numberOfFilesPerOutputBatch);
        ExecutorService es = Executors.newCachedThreadPool();
        final List<Thread> threads = new ArrayList<>();
        String[] paths = ClickHouse.executeQueryAndWrite(query, processingChunkSize, (a, b) -> {
//                Path fullyDefinedPath = Path.of(outputDirectory + "/" + outputFilesPrefix + "_file_" + b + "." + outputConfig.getOutputFileType().getLabel());
//                DataFrame df = ChunkHandler.processChunkToDataFrame((List<Map<String, Object>>) a);
//                System.out.println(Printers.tabular.toString(df));
//                long startTime = new Date().getTime();
//
//                switch (outputConfig.getOutputFileType()) {
//                    case XLSX -> ExcelFileHandler.writeDataFrameToExcel(Map.of("Sheet1", df), fullyDefinedPath);
//                    case CSV -> CsvFileHandler.writeDataFrameToCsv(df, fullyDefinedPath);
//                }
//                System.out.println("Writing to " + fullyDefinedPath + " took " + (new Date().getTime() - startTime) + " milliseconds");
//                return fullyDefinedPath.toString();
//            System.out.println(((List<Map<String, Object>>) a).size());
            ChunkHandler processorObj = new ChunkHandler(a, outputDirectory, outputFilesPrefix, (int) b, outputConfig);
            Thread processingThread = new Thread(processorObj);
            processingThread.setName("Chunk Processor " + b + ".");
            processingThread.start();
//            es.execute(processingThread);
            threads.add(processingThread);

            return processorObj.getFullyDefinedPath().toString();
        }).toArray(String[]::new);

        for (Thread t: threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        System.out.println("Finished executor things");
        String[] result = switch (outputConfig.getOutputConfigurationType()) {
            case SINGLE_FILES -> paths;
            case ALL_FILES_IN_SINGLE_ZIP -> {
                String destinationPath = outputDirectory + "/" + outputFilesPrefix + "_batched_" + dateFormatSuffix + ".zip";
                ZipFileHandler.writeZip(paths, destinationPath);
                yield new String[]{destinationPath};
            }
            case PART_FILES_MULTIPLE_ZIP -> {
                int filesCount = paths.length;
                int i = 1;
                int filesPerZip = outputConfig.getFilesCount();
                int zipCount = (int) Math.ceil((double) filesCount/filesPerZip);
                String[] destinations = new String[zipCount];
                while (i <= zipCount) {
                    String destinationPath = outputDirectory + "/" + outputFilesPrefix + "_batched_file_" + i + "_" + dateFormatSuffix + ".zip";
                    ZipFileHandler.writeZip(Arrays.copyOfRange(paths, (i - 1) * filesPerZip, i * filesPerZip), destinationPath);
                    destinations[i-1] = destinationPath;
                    i++;
                }
                yield destinations;
            }
            case PART_FILES_ZIPPED_SINGLE_ZIP -> {
                int filesCount = paths.length;
                int i = 1;
                int filesPerZip = outputConfig.getFilesCount();
                int zipCount = (int) Math.ceil((double) filesCount/filesPerZip);
                String[] destinations = new String[zipCount];
                while (i <= zipCount) {
                    String destinationPath = outputDirectory + "/" + outputFilesPrefix + "_batched_file_" + i + dateFormatSuffix + ".zip";
                    ZipFileHandler.writeZip(Arrays.copyOfRange(paths, (i - 1) * filesPerZip, i * filesPerZip), destinationPath);
                    destinations[i-1] = destinationPath;
                    i++;
                }
                String finalDestination = outputDirectory + "/" + outputFilesPrefix + "_batched_whole_file_" + dateFormatSuffix + ".zip";
                ZipFileHandler.writeZip(destinations, finalDestination);
                yield new String[]{finalDestination};
            }
        };

        System.out.println(Arrays.toString(result));
        String[] driveIds = new String[result.length];

//        for (int i = 0; i < result.length; i++) {
//            driveIds[i] = GoogleDriveHandler.uploadFileToDrive(
//                    GoogleDriveDestination.of(driveLocation),
//                    new File(result[i])
//            );
//        }

        return driveIds;

    }

//    @Override
//    public void service(HttpRequest request, HttpResponse response)
//            throws IOException {
//        boolean isSuccess = false;
//        String message;
//        int responseCode;
//        Map<String, List<String>> params = request.getQueryParameters();
//        if (params.get("job_type").getFirst().equals("sql")) {
//            try {
//                String[] outputDriveIds = runDelivery(params.get("query").getFirst(),
//                        params.get("file_type").getFirst(),
//                        params.get("configuration_type").getFirst(),
//                        Integer.parseInt(params.get("output_chunk_size").getFirst()),
//                        params.getOrDefault("output_prefix", List.of("")).getFirst(),
//                        params.get("destination_drive_location").getFirst(),
//                        Integer.parseInt(params.getOrDefault("files_per_batch", List.of("1")).getFirst())
//                );
//
//                isSuccess = true;
//                responseCode = 200;
//                message = "All files processed and written with drive Ids " + Arrays.toString(outputDriveIds);
//            } catch (Exception e) {
//                responseCode = 500;
//                message = "Job failed with " + e.getClass() + " and message: " + e.getMessage() + " at " + Arrays.toString(e.getStackTrace());
//            }
//        } else {
//            responseCode = 400;
//            message = "Currently unsupported job type passed in query";
//        }
//        response.setStatusCode(responseCode);
//
//        BufferedWriter writer = response.getWriter();
//        writer.write(Map.of("success", isSuccess, "message", message).toString());
//    }

    public static void main(String[] args) throws ClickHouseException, IOException, GeneralSecurityException {

        String query = "";
        String outputType = "xlsx";
        String outputConfigurationType = "batch-multi-zip";
        String outputPrefix = "unreconciled_entries";
        int numberOfFiles = 2;
        int chunkSize = 1_000_000;

        System.out.println(Arrays.toString(runDelivery(query, outputType, outputConfigurationType, chunkSize, outputPrefix, "", numberOfFiles)));

    }
}
