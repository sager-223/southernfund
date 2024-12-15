package com.southern.dataconsistencychecker.strategy.impl;

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
import java.util.concurrent.ConcurrentHashMap;

@Component("memory")
public class MemoryBasedStrategy implements ConsistencyCheckStrategy {

    @Autowired
    private DataSourceManager dataSourceManager;

    @Autowired
    private CompareResultMapper compareResultMapper;

    @Override
    public void execute(CompareConfig config) {
        // 获取数据源
        DataSource sourceDS = dataSourceManager.getDataSourceById(config.getSourceDataSourceId());
        DataSource targetDS = dataSourceManager.getDataSourceById(config.getTargetDataSourceId());

        String sourceTable = config.getSourceTable();
        String targetTable = config.getTargetTable();

        String[] sourceUniqueKeys = config.getSourceUniqueKeys().split(",");
        String[] targetUniqueKeys = config.getTargetUniqueKeys().split(",");

        String[] sourceCompareFields = config.getSourceCompareFields().split(",");
        String[] targetCompareFields = config.getTargetCompareFields().split(",");

        try (Connection sourceConn = sourceDS.getConnection();
             Connection targetConn = targetDS.getConnection()) {

            Map<String, Map<String, Object>> sourceData = fetchData(sourceConn, sourceTable, sourceUniqueKeys, sourceCompareFields);
            Map<String, Map<String, Object>> targetData = fetchData(targetConn, targetTable, targetUniqueKeys, targetCompareFields);

            Set<String> allKeys = new HashSet<>();
            allKeys.addAll(sourceData.keySet());
            allKeys.addAll(targetData.keySet());

            List<String> discrepancies = new ArrayList<>();

            for (String key : allKeys) {
                Map<String, Object> sourceRow = sourceData.get(key);
                Map<String, Object> targetRow = targetData.get(key);

                if (sourceRow == null) {
                    discrepancies.add("Target存在但Source缺失的唯一键：" + key);
                } else if (targetRow == null) {
                    discrepancies.add("Source存在但Target缺失的唯一键：" + key);
                } else {
                    if (!sourceRow.equals(targetRow)) {
                        discrepancies.add("唯一键：" + key + " 的比对字段不一致");
                    }
                }
            }

            // 记录比对结果
            CompareResult result = new CompareResult();
            result.setCompareConfigId(config.getId());
            result.setCompareTaskId(Thread.currentThread().getId()); // 示例，实际应有任务ID
            result.setSourceDataDetails("{\"ip\":\"...\",\"port\":...}"); // 填充实际信息
            result.setTargetDataDetails("{\"ip\":\"...\",\"port\":...}"); // 填充实际信息
            result.setCompareTime(LocalDateTime.now());
            result.setCompareStatus(discrepancies.isEmpty() ? "SUCCESS" : "FAIL");
            result.setDescription(String.join("\n", discrepancies));
            result.setEmailNotificationStatus("noNeed"); // 根据需求设置
            result.setSmsNotificationStatus("noNeed"); // 根据需求设置
            result.setIsConsistent(discrepancies.isEmpty() ? true : false);

            compareResultMapper.insertCompareResult(result);

        } catch (SQLException e) {
            e.printStackTrace();
            // 记录异常情况
        }
    }

    private Map<String, Map<String, Object>> fetchData(Connection conn, String table, String[] uniqueKeys, String[] compareFields) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(",", uniqueKeys));
        sql.append(",");
        sql.append(String.join(",", compareFields));
        sql.append(" FROM ").append(table);

        Map<String, Map<String, Object>> dataMap = new ConcurrentHashMap<>();

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql.toString())) {

            while (rs.next()) {
                StringBuilder keyBuilder = new StringBuilder();
                for (String uk : uniqueKeys) {
                    keyBuilder.append(rs.getString(uk)).append("_");
                }
                String key = keyBuilder.toString();

                Map<String, Object> fields = new HashMap<>();
                for (int i = 0; i < compareFields.length; i++) {
                    fields.put(compareFields[i], rs.getObject(compareFields[i]));
                }
                dataMap.put(key, fields);
            }
        }

        return dataMap;
    }
}