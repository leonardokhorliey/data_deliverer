package com.ebubeokoli.datadeliverer.util;

public enum OutputFileType {
    CSV("csv"),
    XLSX("xlsx");

    private final String label;

    OutputFileType(String type) {
        this.label = type;
    }

    public static OutputFileType valueOfLabel(String label) {
        for (OutputFileType e : values()) {
            if (e.label.equals(label)) {
                return e;
            }
        }
        return null;
    }

    public String getLabel() {
        return this.label;
    }
}
