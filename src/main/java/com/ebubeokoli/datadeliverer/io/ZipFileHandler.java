package com.ebubeokoli.datadeliverer.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipFileHandler {

    public static void writeZip(String[] filePaths, String destinationPath) throws IOException {
        System.out.println(Arrays.toString(filePaths));
        if (!destinationPath.endsWith(".zip")) destinationPath += ".zip";

        File f = new File(destinationPath);
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));

        for (String file: filePaths) {
            if (file == null) continue;
            String[] filePathSplit = file.split("/");
            String zipFileName = filePathSplit[filePathSplit.length -1];
            ZipEntry e = new ZipEntry(zipFileName);
            out.putNextEntry(e);

            out.write(Files.readAllBytes(Path.of(file)));
            out.closeEntry();
        }

        out.close();
    }

    public static void main(String[] args) throws IOException {

        writeZip(new String[]{"data/data_file_1.csv", "data/data_file_2.csv"}, "data/zipd");
    }
}
