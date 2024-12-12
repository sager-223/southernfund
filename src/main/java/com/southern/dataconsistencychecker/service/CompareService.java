package com.southern.dataconsistencychecker.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.southern.dataconsistencychecker.config.DynamicDataSource;
import com.southern.dataconsistencychecker.entity.CompareConfig;
import com.southern.dataconsistencychecker.entity.CompareResult;
import com.southern.dataconsistencychecker.entity.DataSourceConfigEntity;
import com.southern.dataconsistencychecker.mapper.CompareResultMapper;
import com.southern.dataconsistencychecker.mapper.DataSourceConfigMapper;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;


@Service
public class CompareService {

    private final DataSourceConfigMapper dataSourceConfigMapper;
    private final DynamicDataSource dynamicDataSource;
    private final CompareResultMapper compareResultMapper;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicLong taskIdGenerator = new AtomicLong(System.currentTimeMillis());

    public CompareService(DataSourceConfigMapper dataSourceConfigMapper,
                          DynamicDataSource dynamicDataSource,
                          CompareResultMapper compareResultMapper) {
        this.dataSourceConfigMapper = dataSourceConfigMapper;
        this.dynamicDataSource = dynamicDataSource;
        this.compareResultMapper = compareResultMapper;
    }

    public void executeCompare(CompareConfig config) {
        Long taskId = taskIdGenerator.incrementAndGet();
        CompareResult result = new CompareResult();
        result.setCompareConfigId(config.getId());
        result.setCompareTaskId(taskId);
        result.setCompareTime(new Date());

        try {
            DataSource sourceDS = dynamicDataSource.getDataSourceById(config.getSourceDataSourceId());
            DataSource targetDS = dynamicDataSource.getDataSourceById(config.getTargetDataSourceId());

            if (sourceDS == null || targetDS == null) {
                throw new RuntimeException("Data source not found for the given IDs.");
            }

            DataSourceConfigEntity sourceConfig = dataSourceConfigMapper.getDataSourceConfigById(config.getSourceDataSourceId());
            DataSourceConfigEntity targetConfig = dataSourceConfigMapper.getDataSourceConfigById(config.getTargetDataSourceId());

            // 设置数据详情为 JSON
            result.setSourceDataDetails(objectMapper.writeValueAsString(sourceConfig));
            result.setTargetDataDetails(objectMapper.writeValueAsString(targetConfig));

            // 获取数据
            List<Map<String, Object>> sourceData = fetchData(sourceDS, config.getSourceTable(),
                    config.getSourceConditions(), config.getSourceCompareFields(), config.getSourceUniqueKeys());
            List<Map<String, Object>> targetData = fetchData(targetDS, config.getTargetTable(),
                    config.getTargetConditions(), config.getTargetCompareFields(), config.getTargetUniqueKeys());

            // 比较数据并收集不一致信息
            List<String> inconsistencyDetails = new ArrayList<>();
            boolean isConsistent = compareData(
                    sourceData,
                    targetData,
                    config.getSourceUniqueKeys(),
                    config.getSourceCompareFields(),
                    config.getTargetUniqueKeys(),
                    config.getTargetCompareFields(),
                    inconsistencyDetails
            );

            result.setIsConsistent(isConsistent);
            if (isConsistent) {
                result.setCompareStatus("SUCCESS");
                result.setDescription("Data is consistent.");
            } else {
                result.setCompareStatus("SUCCESS");
                // 将所有不一致信息拼接成描述
                StringBuilder descriptionBuilder = new StringBuilder("Data inconsistencies found:\n");
                for (String detail : inconsistencyDetails) {
                    descriptionBuilder.append("- ").append(detail).append("\n");
                }
                result.setDescription(descriptionBuilder.toString());
            }

            // 根据需求处理通知状态
            result.setEmailNotificationStatus("noNeed");
            result.setSmsNotificationStatus("noNeed");

        } catch (Exception e) {
            result.setCompareStatus("FAIL");
            result.setDescription("Exception occurred: " + e.getMessage());
            result.setIsConsistent(false);
            result.setEmailNotificationStatus("false");
            result.setSmsNotificationStatus("false");
        }

        // 插入比较结果
        compareResultMapper.insertCompareResult(result);
    }



    private List<Map<String, Object>> fetchData(DataSource dataSource, String table, String conditions,
                                                String compareFields, String uniqueKeys) throws Exception {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(uniqueKeys).append(",").append(compareFields).append(" FROM ").append(table);
        if (conditions != null && !conditions.trim().isEmpty()) {
            sql.append(" WHERE ").append(conditions);
        }

        List<Map<String, Object>> dataList = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql.toString());
             ResultSet rs = pstmt.executeQuery()) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> rowMap = new HashMap<>();
                for(int i=1;i<=columnCount;i++) {
                    String columnName = metaData.getColumnLabel(i);
                    Object value = rs.getObject(i);
                    rowMap.put(columnName, value);
                }
                dataList.add(rowMap);
            }
        }

        return dataList;
    }



    private boolean compareData(
            List<Map<String, Object>> sourceData,
            List<Map<String, Object>> targetData,
            String sourceUniqueKeys,
            String sourceCompareFields,
            String targetUniqueKeys,
            String targetCompareFields,
            List<String> inconsistencyDetails
    ) {
        boolean isConsistent = true;

        // 检查源和目标数据的大小是否一致
        if (sourceData.size() != targetData.size()) {
            inconsistencyDetails.add(String.format("Record count mismatch: Source has %d records, Target has %d records.", sourceData.size(), targetData.size()));
            isConsistent = false;
        }

        // 分割源和目标的唯一键及比较字段
        String[] sourceUniqueKeyArr = sourceUniqueKeys.split(",");
        String[] targetUniqueKeyArr = targetUniqueKeys.split(",");
        String[] sourceCompareFieldsArr = sourceCompareFields.split(",");
        String[] targetCompareFieldsArr = targetCompareFields.split(",");

        // 验证源和目标的唯一键数量是否一致
        if (sourceUniqueKeyArr.length != targetUniqueKeyArr.length) {
            throw new IllegalArgumentException("Source and target unique keys must have the same number of fields");
        }

        // 验证源和目标的比较字段数量是否一致
        if (sourceCompareFieldsArr.length != targetCompareFieldsArr.length) {
            throw new IllegalArgumentException("Source and target compare fields must have the same number of fields");
        }

        // 构建源数据的映射（key -> 行数据）
        Map<String, Map<String, Object>> sourceMap = new HashMap<>();
        for (Map<String, Object> row : sourceData) {
            String key = buildKey(row, sourceUniqueKeyArr);
            sourceMap.put(key, row);
        }

        // 遍历目标数据并进行比较
        for (Map<String, Object> row : targetData) {
            String targetKey = buildKey(row, targetUniqueKeyArr);
            Map<String, Object> sourceRow = sourceMap.get(targetKey);
            if (sourceRow == null) {
                // 目标数据中存在源数据中不存在的记录
                inconsistencyDetails.add(String.format("Record with key [%s] exists in Target but not in Source.", targetKey));
                isConsistent = false;
                continue;
            }

            // 比较指定的字段
            for (int i = 0; i < sourceCompareFieldsArr.length; i++) {
                String sourceField = sourceCompareFieldsArr[i].trim();
                String targetField = targetCompareFieldsArr[i].trim();

                Object sourceValue = sourceRow.get(sourceField);
                Object targetValue = row.get(targetField);

                if (sourceValue == null && targetValue == null) {
                    continue;
                }
                if (sourceValue == null || targetValue == null) {
                    inconsistencyDetails.add(String.format("Field mismatch for key [%s]: Source.%s = %s, Target.%s = %s",
                            targetKey, sourceField, sourceValue, targetField, targetValue));
                    isConsistent = false;
                    continue;
                }
                if (!sourceValue.equals(targetValue)) {
                    inconsistencyDetails.add(String.format("Field mismatch for key [%s]: Source.%s = %s, Target.%s = %s",
                            targetKey, sourceField, sourceValue, targetField, targetValue));
                    isConsistent = false;
                }
            }

            // 移除已比较的源记录，方便后续检查源中多余的记录
            sourceMap.remove(targetKey);
        }

        // 检查源数据中存在但目标数据中缺失的记录
        for (String remainingKey : sourceMap.keySet()) {
            inconsistencyDetails.add(String.format("Record with key [%s] exists in Source but not in Target.", remainingKey));
            isConsistent = false;
        }

        return isConsistent;
    }



    private String buildKey(Map<String, Object> row, String[] uniqueKeys) {
        StringBuilder keyBuilder = new StringBuilder();
        for (String key : uniqueKeys) {
            Object value = row.get(key.trim());
            keyBuilder.append(value != null ? value.toString() : "null").append("_");
        }
        return keyBuilder.toString();
    }
}