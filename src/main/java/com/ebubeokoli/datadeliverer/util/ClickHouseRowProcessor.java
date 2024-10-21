package com.ebubeokoli.datadeliverer.util;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.value.UnsignedByte;
import com.clickhouse.data.value.UnsignedInteger;
import com.clickhouse.data.value.UnsignedLong;
import com.clickhouse.data.value.UnsignedShort;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

public class ClickHouseRowProcessor {
    private RowProcessorConfig type;
    private Map<String, List<Object>> colMaps;
    private List<Map<String, Object>> records;
    private long recordsCount;

    public ClickHouseRowProcessor(RowProcessorConfig how) {
        switch (how) {
            case BY_COLUMN -> colMaps = new LinkedHashMap<>();
            case BY_ROW -> records = new ArrayList<>();
        }
        this.type = how;
    }

    public ClickHouseRowProcessor() {
        this(RowProcessorConfig.BY_ROW);
    }

    public void processRow(ClickHouseRecord row, List<ClickHouseColumn> columns) {
        Iterator<ClickHouseValue> rowRecords = row.iterator();
        Map<String, Object> rowMap = new LinkedHashMap<>();
        int columnCounter = 0;
        while (rowRecords.hasNext()) {
            ClickHouseColumn column = columns.get(columnCounter);
            ClickHouseValue dataAsClickHouseValue = rowRecords.next();
            Object dataAsObject = transformClickHouseData(column, dataAsClickHouseValue);
            String colName = column.getColumnName();
            switch (this.type) {
                case BY_COLUMN -> {
                    if (!colMaps.containsKey(colName)) {
                        colMaps.put(colName, new ArrayList<>());
                    }
                    colMaps.get(colName).add(dataAsObject);
                }
                case BY_ROW -> {
                    rowMap.put(colName, dataAsObject);
                    if(!rowRecords.hasNext()) records.add(rowMap);
                }
            }
            columnCounter++;
        }
        recordsCount++;
    }

    private static Object transformClickHouseData(ClickHouseColumn column, ClickHouseValue dataAsClickHouseValue) {
        Class<?> columnDataType = column.getDataType().getObjectClass();

        Object dataAsObject = null;
        if (columnDataType.equals(String.class)) {
            dataAsObject = dataAsClickHouseValue.asString();
        } else if (columnDataType.equals(Integer.class) || columnDataType.equals(UnsignedInteger.class)) {
            dataAsObject = dataAsClickHouseValue.asInteger();
        } else if (columnDataType.equals(LocalDateTime.class)) {
            dataAsObject = dataAsClickHouseValue.asString();
        } else if (columnDataType.equals(LocalDate.class)) {
            dataAsObject = dataAsClickHouseValue.asString();
        } else if (columnDataType.equals(Long.class) || columnDataType.equals(UnsignedLong.class)) {
            dataAsObject = dataAsClickHouseValue.asLong();
        } else if (columnDataType.equals(Byte.class) || columnDataType.equals(UnsignedByte.class)) {
            dataAsObject = dataAsClickHouseValue.asByte();
        } else if (columnDataType.equals(Short.class) || columnDataType.equals(UnsignedShort.class)) {
            dataAsObject = dataAsClickHouseValue.asShort();
        } else if (columnDataType.equals(Double.class) || columnDataType.equals(Float.class)) {
            dataAsObject = dataAsClickHouseValue.asDouble();
        } else if (columnDataType.equals(List.class)) {
            dataAsObject = dataAsClickHouseValue.asTuple();
        } else if (columnDataType.equals(Boolean.class)) {
            dataAsObject = dataAsClickHouseValue.asBoolean();
        } else if (columnDataType.equals(Map.class)) {
            dataAsObject = dataAsClickHouseValue.asMap();
        } else {
            dataAsObject = dataAsClickHouseValue.asObject();
        }

        return dataAsObject;
    }

    public long getRecordsCount() {
        return recordsCount;
    }

    public Object getData() {
        return switch (this.type) {
            case BY_COLUMN -> colMaps;
            case BY_ROW -> records;
        };
    }
}
