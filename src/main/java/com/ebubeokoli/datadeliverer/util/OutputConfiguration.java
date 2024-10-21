package com.ebubeokoli.datadeliverer.util;

import java.security.InvalidParameterException;

public class OutputConfiguration {
    private static final OutputFileType DEFAULT_OUTPUT_FILE_TYPE = OutputFileType.CSV;
    private static final Integer DEFAULT_FILE_COUNT = 1;

    private final OutputConfigurationType configurationType;
    private final OutputFileType outFileType;
    private final Integer filesCount;

    public OutputConfiguration(OutputConfigurationType configType, OutputFileType outFileType, Integer filesCount) {

        this.configurationType = configType;
        this.filesCount = filesCount;
        this.outFileType = outFileType;
    }

    public OutputConfiguration(String configType, String outputFileType, Integer filesCount) {
        OutputConfigurationType configTypeEnum = OutputConfigurationType.valueOfLabel(configType);
        OutputFileType fileTypeEnum = OutputFileType.valueOfLabel(outputFileType);
        if (configTypeEnum == null || fileTypeEnum == null) {
            throw new InvalidParameterException("Invalid output config type specified.");
        }
        this.configurationType = configTypeEnum;
        this.filesCount = filesCount;
        this.outFileType = fileTypeEnum;
    }

    public OutputConfiguration(OutputConfigurationType configType, OutputFileType outputFileType) {
        this(configType, outputFileType, DEFAULT_FILE_COUNT);
    }

    public OutputConfiguration(OutputConfigurationType configType) {
        this(configType, DEFAULT_OUTPUT_FILE_TYPE);
    }

    public OutputConfiguration(String configType, String outputFileType) {
        this(configType, outputFileType, DEFAULT_FILE_COUNT);
    }

    public OutputConfiguration(String configType) {
        this(configType, DEFAULT_OUTPUT_FILE_TYPE.getLabel());
    }

    public OutputConfigurationType getOutputConfigurationType() {
        return configurationType;
    }

    public OutputFileType getOutputFileType() {
        return outFileType;
    }

    public Integer getFilesCount() {
        return filesCount;
    }
}
