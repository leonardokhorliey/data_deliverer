package com.ebubeokoli.datadeliverer.io.googledrive;

public class GoogleDriveDestination {

    private String driveId;

    private GoogleDriveDestination(String driveId) {
        if (driveId == null) throw new NullPointerException("Invalid drive id passed as destination");
        this.driveId = driveId;
    }

    public static GoogleDriveDestination of(String urlOrFullPath) {
        if (urlOrFullPath.contains("/")) {
            String[] urlSplit = urlOrFullPath.split("/");
            return new GoogleDriveDestination(urlSplit[urlSplit.length - 1]);
        }
        return new GoogleDriveDestination(urlOrFullPath);
    }

    public String getDriveId() {
        return driveId;
    }

    public static void main(String[] args) {
        System.out.println(GoogleDriveDestination.of("https://drive.google.com/drive/folders/1VurU72AtaWCUI5IPfKPBZIdRsBOLU_d3").getDriveId());
    }
}
