package com.ebubeokoli.datadeliverer.util;

public enum OutputConfigurationType {
    SINGLE_FILES("single"),
    ALL_FILES_IN_SINGLE_ZIP("zipped"),
    PART_FILES_MULTIPLE_ZIP("batch-multi-zip"),
    PART_FILES_ZIPPED_SINGLE_ZIP("batch-multi-zip-zipped");

    private final String label;

    OutputConfigurationType(String s) {
        this.label = s;
    }

    public static OutputConfigurationType valueOfLabel(String label) {
        for (OutputConfigurationType e : values()) {
            if (e.label.equals(label)) {
                return e;
            }
        }
        return null;
    }
}
