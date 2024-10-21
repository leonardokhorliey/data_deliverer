package com.ebubeokoli.datadeliverer.clickhouse;

import com.clickhouse.client.*;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.*;
import com.clickhouse.data.value.UnsignedByte;
import com.clickhouse.data.value.UnsignedInteger;
import com.clickhouse.data.value.UnsignedLong;
import com.clickhouse.data.value.UnsignedShort;
import com.ebubeokoli.datadeliverer.util.ClickHouseRowProcessor;
import com.ebubeokoli.datadeliverer.util.RowProcessorConfig;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

public class ClickHouse {

    private static final String DB_HOST =  System.getProperty("DB_HOST");
    private static final Integer DB_PORT =  Integer.valueOf(System.getProperty("DB_PORT"));
    private static final String DB_NAME =  System.getProperty("DB_NAME");
    private static final String CLICKHOUSE_USERNAME = System.getProperty("DB_USER");
    private static final String CLICKHOUSE_PASSWORD = System.getProperty("DB_PASSWORD");
    

    protected static ClickHouseNode generateServerConnection(
        final String host, 
        final int port, 
        final String dbName,
        final String userName,
        final String password
        ) {

        ClickHouseNode server = ClickHouseNode.builder()
                .host(host)
                .port(ClickHouseProtocol.HTTP, Integer.valueOf(port))
                .database(dbName).credentials(ClickHouseCredentials.fromUserAndPassword(
                        userName, password))
                .build();
        
        return server;
    }

    private static ClickHouseNode generateServerConnection() {

        return generateServerConnection(DB_HOST, DB_PORT, DB_NAME, CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD);
    }

    public static List<Map<String, Object>> executeQuery(String query) throws ClickHouseException {
        ClickHouseNode server = ClickHouse.generateServerConnection();
        Map<ClickHouseOption, Serializable> configOptions = new HashMap<>();

        configOptions.put(ClickHouseClientOption.COMPRESS_ALGORITHM, ClickHouseCompression.GZIP);
        configOptions.put(ClickHouseClientOption.SOCKET_TIMEOUT, 3000000);
        List<Map<String, Object>> records = new ArrayList<>();

        try (ClickHouseClient client = ClickHouseClient.builder()
                .config(new ClickHouseConfig(configOptions))
                .build();
                ClickHouseResponse response = client.read(server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query(query).execute().get()
            ) {
            
            List<ClickHouseColumn> columnNames = response.getColumns();
            int rowCounter = 0;
            for (ClickHouseRecord row: response.records()) {
                Iterator<ClickHouseValue> rowRecords = row.iterator();
                Map<String, Object> rowMap = new HashMap<>();
                int columnCounter = 0;
                while (rowRecords.hasNext()) {
                    ClickHouseColumn column = columnNames.get(columnCounter);
                    ClickHouseValue dataAsClickHouseValue = rowRecords.next();
                    
                    Object dataAsObject = transformClickHouseData(column, dataAsClickHouseValue);
                    rowMap.put(columnNames.get(columnCounter).getColumnName(), dataAsObject);
                    columnCounter++;
                }
                records.add(rowMap);
                rowCounter++;
            }
            System.out.println(records);
            return records;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ClickHouseException.forCancellation(e, server);
        } catch (ExecutionException e) {
            throw ClickHouseException.of(e, server);
        }
    }

    public static List<String> executeQueryAndWrite(String query, int chunkSize, RowProcessorConfig rowProcessorConfig, BiFunction processor) throws ClickHouseException {
        ClickHouseNode server = ClickHouse.generateServerConnection();
        Map<ClickHouseOption, Serializable> configOptions = new HashMap<>();

        configOptions.put(ClickHouseClientOption.COMPRESS_ALGORITHM, ClickHouseCompression.GZIP);
        configOptions.put(ClickHouseClientOption.SOCKET_TIMEOUT, 3000000);
        List<String> outputPaths = new ArrayList<>();

        try (ClickHouseClient client = ClickHouseClient.builder()
                .config(new ClickHouseConfig(configOptions))
                .build();
             ClickHouseResponse response = client.read(server)
                     .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                     .query(query).execute().get()
        ) {

            List<ClickHouseColumn> columnNames = response.getColumns();
            int rowCounter = 0;
            int batch = 1;
            ClickHouseRowProcessor rowProcessor = new ClickHouseRowProcessor(rowProcessorConfig);
            for (ClickHouseRecord row: response.records()) {
                rowProcessor.processRow(row, columnNames);
                if (++rowCounter == chunkSize) {
                    Object records = rowProcessor.getData();
                    String fileOutputPath = (String) processor.apply(records, batch);
                    outputPaths.add(fileOutputPath);
                    rowCounter = 0;
                    batch++;
                    rowProcessor = new ClickHouseRowProcessor(rowProcessorConfig);
                }
            }
            return outputPaths;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ClickHouseException.forCancellation(e, server);
        } catch (ExecutionException e) {
            throw ClickHouseException.of(e, server);
        }
    }


    public static Object transformClickHouseData(ClickHouseColumn column, ClickHouseValue dataAsClickHouseValue) {
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
}
