package com.ebubeokoli.datadeliverer;

import com.clickhouse.client.ClickHouseException;
import com.ebubeokoli.datadeliverer.clickhouse.ClickHouse;
import com.ebubeokoli.datadeliverer.io.ZipFileHandler;
import com.ebubeokoli.datadeliverer.io.googledrive.GoogleDriveDestination;
import com.ebubeokoli.datadeliverer.io.googledrive.GoogleDriveHandler;
import com.ebubeokoli.datadeliverer.util.ChunkHandler;
import com.ebubeokoli.datadeliverer.util.OutputConfiguration;
import com.ebubeokoli.datadeliverer.util.RowProcessorConfig;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import io.github.cdimascio.dotenv.Dotenv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataDeliverer implements HttpFunction {
    private static List<Thread> threads = new ArrayList<>();

    static {
        loadEnvVars();
    }

    public static void loadEnvVars() {
        Dotenv dotenv = Dotenv.configure().load();
        dotenv.entries().forEach(e -> System.setProperty(e.getKey(), e.getValue()));
    }

    public static String[] buildRunnables(
            final String query,
            OutputConfiguration outputConfig,
            final String outputDirectory,
            final int processingChunkSize,
            final String outputFilesPrefix
    ) throws ClickHouseException {
        RowProcessorConfig rowProcessorConfig = RowProcessorConfig.BY_COLUMN;
        String[] paths = ClickHouse.executeQueryAndWrite(query, processingChunkSize, rowProcessorConfig, (a, b) -> {

            ChunkHandler processorObj = new ChunkHandler(a, outputDirectory, outputFilesPrefix, (int) b, outputConfig, rowProcessorConfig);
            Thread processingThread = new Thread(processorObj);
            processingThread.setName("Chunk Processor " + b + ".");
            threads.add(processingThread);

            return processorObj.getFullyDefinedPath().toString();
        }).toArray(String[]::new);

        return paths;
    }

    public static String[] runDelivery(
            String[] paths,
            final OutputConfiguration outputConfig,
            final String outputDirectory,
            final String outputFilesPrefix,
            final String driveLocation,
            final int concurrentCount
    ) throws IOException, GeneralSecurityException {

        Thread[] runningThreads = new Thread[concurrentCount];
        int lastIndex = 0;
        for (int i = 0; i < threads.size(); i++) {
            System.out.println("Goals");
            runningThreads[i - lastIndex] = threads.get(i);
            threads.get(i).start();
            if ((i + 1)%concurrentCount == 0 || i + 1 == threads.size()) {
                lastIndex = i + 1;
                for (Thread t: runningThreads) {
                    try {
                        t.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        System.out.println("Finished executor things");
        final String dateFormatSuffix = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
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

        for (int i = 0; i < result.length; i++) {
            driveIds[i] = GoogleDriveHandler.uploadFileToDrive(
                    GoogleDriveDestination.of(driveLocation),
                    new File(result[i])
            );
        }

        return driveIds;

    }

    @Override
    public void service(HttpRequest request, HttpResponse response)
            throws IOException {
        boolean isSuccess = false;
        String message;
        int responseCode;
        Map<String, List<String>> params = request.getQueryParameters();

        String jobType = params.get("job_type").getFirst();
        String fileType = params.get("file_type").getFirst();
        String outputConfigType = params.get("configuration_type").getFirst();
        int filesPerBatch = Integer.parseInt(params.getOrDefault("files_per_batch", List.of("1")).getFirst());
        OutputConfiguration outputConfig = new OutputConfiguration(outputConfigType, fileType, filesPerBatch);

        String[] intermediatePaths = new String[]{};
        if (jobType.equals("sql")) {
            try {
                intermediatePaths = buildRunnables(
                        params.get("query").getFirst(),
                        outputConfig,
                        "data",
                        Integer.parseInt(params.get("output_chunk_size").getFirst()),
                        params.getOrDefault("output_prefix", List.of("")).getFirst()
                );
                isSuccess = true;
                responseCode = 200;
                message = "Query run and file processing commenced";

            } catch (ClickHouseException e) {
                responseCode = 500;
                message = "Job failed with " + e.getClass() + " and message: " + e.getMessage() + " at " + Arrays.toString(e.getStackTrace());
            }
            response.setStatusCode(responseCode);

            BufferedWriter writer = response.getWriter();
            writer.write(Map.of("success", isSuccess, "message", message).toString());

            try {

                String[] outputDriveIds = runDelivery(intermediatePaths,
                        outputConfig,
                        "data",
                        params.getOrDefault("output_prefix", List.of("")).getFirst(),
                        params.get("destination_drive_location").getFirst(),
                        Integer.parseInt(params.getOrDefault("concurrent_level", List.of("1")).getFirst())
                );

                isSuccess = true;
                responseCode = 200;
                message = "All files processed and written with drive Ids " + Arrays.toString(outputDriveIds);
            } catch (Exception e) {
                responseCode = 500;
                message = "Job failed with " + e.getClass() + " and message: " + e.getMessage() + " at " + Arrays.toString(e.getStackTrace());
            }
        } else {
            responseCode = 400;
            message = "Currently unsupported job type passed in query";
        }
        response.setStatusCode(responseCode);

        BufferedWriter writer = response.getWriter();
        writer.write(Map.of("success", isSuccess, "message", message).toString());
    }

    public static void main(String[] args) throws ClickHouseException, IOException, GeneralSecurityException {

        String query = "";
        String outputType = "csv";
        String outputConfigurationType = "batch-multi-zip";
        String outputPrefix = "unreconciled_entries";
        int numberOfFiles = 2;
        int chunkSize = 1_000_000;
        final String outputDirectory = "data";

//        System.out.println(Arrays.toString(runDelivery(query, outputType, outputConfigurationType, chunkSize, outputPrefix, "", numberOfFiles, 2)));

    }
}
