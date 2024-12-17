package com.southern.dataconsistencychecker.strategy.impl;

import com.alibaba.fastjson2.JSON;
import com.southern.dataconsistencychecker.entity.CompareConfig;
import com.southern.dataconsistencychecker.entity.CompareResult;
import com.southern.dataconsistencychecker.manager.DataSourceManager;
import com.southern.dataconsistencychecker.mapper.CompareResultMapper;
import com.southern.dataconsistencychecker.strategy.ConsistencyCheckStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;


@Component("memory")
public class MemoryBasedStrategy implements ConsistencyCheckStrategy {

    @Autowired
    private DataSourceManager dataSourceManager;

    @Autowired
    private CompareResultMapper compareResultMapper;

    @Override
    public void execute(CompareConfig config) {
        DataSource sourceDS = dataSourceManager.getDataSourceById(config.getSourceDataSourceId());
        DataSource targetDS = dataSourceManager.getDataSourceById(config.getTargetDataSourceId());

        String sourceTable = config.getSourceTable();
        String targetTable = config.getTargetTable();

        String[] sourceUniqueKeys = config.getSourceUniqueKeys().split(",");
        String[] targetUniqueKeys = config.getTargetUniqueKeys().split(",");

        String[] sourceCompareFields = config.getSourceCompareFields().split(",");
        String[] targetCompareFields = config.getTargetCompareFields().split(",");

        if (sourceUniqueKeys.length != targetUniqueKeys.length) {
            throw new IllegalArgumentException("Source and Target unique keys count mismatch.");
        }

        if (sourceCompareFields.length != targetCompareFields.length) {
            throw new IllegalArgumentException("Source and Target compare fields count mismatch.");
        }

        String sourceConditions = config.getSourceConditions();
        String targetConditions = config.getTargetConditions();

        String sourceQuery = buildSelectQuery(sourceTable, sourceUniqueKeys, sourceCompareFields, sourceConditions);
        String targetQuery = buildSelectQuery(targetTable, targetUniqueKeys, targetCompareFields, targetConditions);

        Connection sourceConn = null;
        Connection targetConn = null;
        PreparedStatement sourceStmt = null;
        PreparedStatement targetStmt = null;
        ResultSet sourceRs = null;
        ResultSet targetRs = null;

        StringBuilder descriptionBuilder = new StringBuilder();
        int discrepancies = 0;

        try {
            sourceConn = sourceDS.getConnection();
            targetConn = targetDS.getConnection();

            sourceStmt = sourceConn.prepareStatement(sourceQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            targetStmt = targetConn.prepareStatement(targetQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            // Optimize for large data sets
            if (sourceConn.getMetaData().getDriverName().toLowerCase().contains("oracle")) {
                sourceStmt.setFetchSize(1000);
            } else {
                sourceStmt.setFetchSize(Integer.MIN_VALUE); // MySQL streaming
            }

            if (targetConn.getMetaData().getDriverName().toLowerCase().contains("oracle")) {
                targetStmt.setFetchSize(1000);
            } else {
                targetStmt.setFetchSize(Integer.MIN_VALUE); // MySQL streaming
            }

            sourceRs = sourceStmt.executeQuery();
            targetRs = targetStmt.executeQuery();

            Map<String, Integer> sourceKeyMap = getColumnIndexMap(sourceRs, sourceUniqueKeys);
            Map<String, Integer> targetKeyMap = getColumnIndexMap(targetRs, targetUniqueKeys);

            Map<String, Integer> sourceFieldMap = getColumnIndexMap(sourceRs, sourceCompareFields);
            Map<String, Integer> targetFieldMap = getColumnIndexMap(targetRs, targetCompareFields);

            boolean sourceHasNext = sourceRs.next();
            boolean targetHasNext = targetRs.next();

            while (sourceHasNext || targetHasNext) {
                if (sourceHasNext && targetHasNext) {
                    String sourceKey = getCompositeKey(sourceRs, sourceUniqueKeys, sourceKeyMap);
                    String targetKey = getCompositeKey(targetRs, targetUniqueKeys, targetKeyMap);

                    int comparison = sourceKey.compareTo(targetKey);

                    if (comparison == 0) {
                        // Keys match, compare fields
                        List<String> fieldDifferences = new ArrayList<>();
                        for (int i = 0; i < sourceCompareFields.length; i++) {
                            String sourceValue = sourceRs.getString(sourceCompareFields[i].trim());
                            String targetValue = targetRs.getString(targetCompareFields[i].trim());
                            if (!Objects.equals(sourceValue, targetValue)) {
                                fieldDifferences.add(String.format("字段不一致，%s = %s, %s = %s",
                                        sourceCompareFields[i].trim(), sourceValue,
                                        targetCompareFields[i].trim(), targetValue));
                            }
                        }
                        if (!fieldDifferences.isEmpty()) {
                            discrepancies++;
                            descriptionBuilder.append(String.format("唯一索引为%s时，比对字段不一致，%s%n",
                                    sourceKey, String.join("; ", fieldDifferences)));
                        }
                        sourceHasNext = sourceRs.next();
                        targetHasNext = targetRs.next();
                    } else if (comparison < 0) {
                        // Source key missing in target
                        discrepancies++;
                        descriptionBuilder.append(String.format("唯一索引在source缺失，索引为%s%n", sourceKey));
                        sourceHasNext = sourceRs.next();
                    } else {
                        // Target key missing in source
                        discrepancies++;
                        descriptionBuilder.append(String.format("唯一索引在target缺失，索引为%s%n", targetKey));
                        targetHasNext = targetRs.next();
                    }
                } else if (sourceHasNext) {
                    // Remaining source records missing in target
                    String sourceKey = getCompositeKey(sourceRs, sourceUniqueKeys, sourceKeyMap);
                    discrepancies++;
                    descriptionBuilder.append(String.format("唯一索引在table1缺失，索引为%s%n", sourceKey));
                    sourceHasNext = sourceRs.next();
                } else {
                    // Remaining target records missing in source
                    String targetKey = getCompositeKey(targetRs, targetUniqueKeys, targetKeyMap);
                    discrepancies++;
                    descriptionBuilder.append(String.format("唯一索引在table2缺失，索引为%s%n", targetKey));
                    targetHasNext = targetRs.next();
                }

                // Optionally, batch descriptions to avoid large memory usage
                if (descriptionBuilder.length() > 10_000) { // example threshold
                    break; // or handle accordingly
                }
            }

            // Prepare CompareResult
            CompareResult compareResult = new CompareResult();
            compareResult.setCompareConfigId(config.getId());
            compareResult.setCompareTaskId(generateCompareTaskId()); // Implement this method as needed
            compareResult.setSourceDataDetails(JSON.toJSONString(getDataDetails(sourceDS)));
            compareResult.setTargetDataDetails(JSON.toJSONString(getDataDetails(targetDS)));
            compareResult.setCompareTime(LocalDateTime.now());
            compareResult.setCompareStatus("SUCCESS");
            compareResult.setDescription(descriptionBuilder.toString());
            compareResult.setEmailNotificationStatus("noNeed"); // Set based on config or logic
            compareResult.setSmsNotificationStatus("noNeed");   // Set based on config or logic
            compareResult.setIsConsistent(discrepancies == 0 ? true : false);
            compareResult.setCreateTime(LocalDateTime.now());
            compareResult.setUpdateTime(LocalDateTime.now());

            // Insert CompareResult
            compareResultMapper.insertCompareResult(compareResult);

        } catch (SQLException e) {
            // Handle exceptions, possibly log and set compare_result accordingly
            e.printStackTrace();
        } finally {
            // Close resources in reverse order of their opening
            closeQuietly(targetRs);
            closeQuietly(targetStmt);
            closeQuietly(targetConn);
            closeQuietly(sourceRs);
            closeQuietly(sourceStmt);
            closeQuietly(sourceConn);
        }
    }

    /**
     * Builds a SELECT query with specified fields, conditions, and ordering by unique keys.
     */
    private String buildSelectQuery(String table, String[] uniqueKeys, String[] compareFields, String conditions) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        List<String> allFields = new ArrayList<>();
        Collections.addAll(allFields, uniqueKeys);
        Collections.addAll(allFields, compareFields);
        sb.append(String.join(", ", allFields));
        sb.append(" FROM ").append(table);
        if (conditions != null && !conditions.trim().isEmpty()) {
            sb.append(" WHERE ").append(conditions);
        }
        sb.append(" ORDER BY ").append(String.join(", ", uniqueKeys));
        return sb.toString();
    }

    /**
     * Retrieves a map of column names to their indices in the ResultSet.
     * 获得 唯一键->对应列的映射
     */
//    private Map<String, Integer> getColumnIndexMap(ResultSet rs, String[] columns) throws SQLException {
//        Map<String, Integer> map = new HashMap<>();
//        ResultSetMetaData meta = rs.getMetaData();
//        for (String col : columns) {
//            map.put(col.trim(), meta.getColumnLabel(col.trim()).equalsIgnoreCase(col.trim()) ?
//                    meta.getColumnIndex(col.trim()) : rs.findColumn(col.trim()));
//        }
//        return map;
//    }
    private Map<String, Integer> getColumnIndexMap(ResultSet rs, String[] columns) throws SQLException {
        Map<String, Integer> map = new HashMap<>();
        for (String col : columns) {
            if (col != null && !col.trim().isEmpty()) {
                map.put(col.trim(), rs.findColumn(col.trim()));
            } else {
                throw new IllegalArgumentException("Column name cannot be null or empty");
            }
        }
        return map;
    }

    /**
     * Constructs a composite key from the ResultSet based on the unique keys.
     */
    private String getCompositeKey(ResultSet rs, String[] uniqueKeys, Map<String, Integer> keyMap) throws SQLException {
        StringBuilder keyBuilder = new StringBuilder();
        for (int i = 0; i < uniqueKeys.length; i++) {
            if (i > 0) keyBuilder.append("_");
            keyBuilder.append(rs.getString(keyMap.get(uniqueKeys[i].trim())));
        }
        return keyBuilder.toString();
    }

    /**
     * Retrieves data source details such as IP, port, and database name.
     * Implement this method based on your DataSource implementation.
     */
    private Map<String, Object> getDataDetails(DataSource ds) {
        Map<String, Object> details = new HashMap<>();
        try (Connection conn = ds.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            details.put("URL", meta.getURL());
            details.put("Username", meta.getUserName());
            // Add more details as needed
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return details;
    }

    /**
     * Generates a unique compare task ID.
     * Implement this method based on your application's requirements.
     */
    private long generateCompareTaskId() {
        // Example implementation using current time
        return System.currentTimeMillis();
    }

    /**
     * Closes a ResultSet quietly.
     */
    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ignored) {}
        }
    }

    /**
     * Closes a Statement quietly.
     */
    private void closeQuietly(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException ignored) {}
        }
    }

    /**
     * Closes a Connection quietly.
     */
    private void closeQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {}
        }
    }


}