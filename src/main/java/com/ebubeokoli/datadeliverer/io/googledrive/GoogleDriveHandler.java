package com.ebubeokoli.datadeliverer.io.googledrive;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;

import java.io.*;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GoogleDriveHandler {
    private static final String APPLICATION_NAME = "Daily Recon Pipeline Checker";
    /**
     * Global instance of the JSON factory.
     */
    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final String TOKENS_DIRECTORY_PATH = "tokens";

    private static Map<String, String> MIME_TYPE_MAPPING;

    /**
     * Global instance of the scopes required by this quickstart.
     * If modifying these scopes, delete your previously saved tokens/ folder.
     */
    private static final List<String> SCOPES =
            Collections.singletonList(DriveScopes.DRIVE_FILE);
    private static final String CREDENTIALS_FILE_PATH = "/credentials.json";

    static {
        MIME_TYPE_MAPPING = Map.of(
            "zip", "application/zip",
            "csv", "text/csv",
            "tsv", "text/plain",
            "txt", "text/plain",
            "gzip", "application/zip"
        );
    }

    /**
     * Creates an authorized Credential object.
     *
     * @param HTTP_TRANSPORT The network HTTP Transport.
     * @return An authorized Credential object.
     * @throws IOException If the credentials.json file cannot be found.
     */
    private static Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT)
            throws IOException {
        // Load client secrets.
        InputStream in = GoogleDriveHandler.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
        if (in == null) {
            throw new FileNotFoundException("Resource not found: " + CREDENTIALS_FILE_PATH);
        }
        GoogleClientSecrets clientSecrets =
                GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                .setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
                .setAccessType("offline")
                .build();
        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
        //returns an authorized Credential object.
        return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
    }

    public static Drive getDriveService() throws IOException, GeneralSecurityException {
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();

        return new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT))
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    public static String uploadFileToDrive(final GoogleDriveDestination destination, java.io.File fileToUpload) throws IOException, GeneralSecurityException {
        String fileName = fileToUpload.getName();

        String[] fileNameSplit = fileName.split("\\.");
        String fileType = fileNameSplit[fileNameSplit.length - 1];

        Drive drive = getDriveService();

        FileInputStream fileStream = new FileInputStream(fileToUpload);
        InputStreamContent fileStreamContent = new InputStreamContent(
                MIME_TYPE_MAPPING.getOrDefault(fileType, "text/plain"
                ), fileStream);

//        FileContent fileCont = new FileContent(MIME_TYPE_MAPPING.getOrDefault(fileType, "text/plain"
//        ), ExternalFileHandler.getFile(filePath, false));

        File file = new File().setName(fileName).setParents(Arrays.stream(destination.getDriveId().split("\\.")).toList());
        Drive.Files.Create fileUploadRequest = drive.files().create(file, fileStreamContent).setSupportsAllDrives(true);

        File createdFile = fileUploadRequest.setFields("id").execute();
        return createdFile.getId();
    }

    public static void main(String... args) throws IOException, GeneralSecurityException {
        // Print the names and IDs for up to 10 files.
        System.out.println(uploadFileToDrive(
                GoogleDriveDestination.of("1ppn1g9S-rMB5VYTYh29QTDWhKlkzp9-2"),
                new java.io.File("run_outputs/recon_entries_gap_2024-09-21.csv")
        ));
//
//
//        FileList result = getDriveService().files().list()
//                .setPageSize(10)
//                .setFields("nextPageToken, files(id, name)")
//                .execute();
//        List<File> files = result.getFiles();
//        if (files == null || files.isEmpty()) {
//            System.out.println("No files found.");
//        } else {
//            System.out.println("Files:");
//            for (File file : files) {
//                System.out.printf("%s (%s)\n", file.getName(), file.getId());
//            }
//        }
    }

}
