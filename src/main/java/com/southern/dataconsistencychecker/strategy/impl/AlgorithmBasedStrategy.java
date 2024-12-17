//package com.southern.dataconsistencychecker.strategy.impl;
//
//import com.southern.dataconsistencychecker.entity.CompareConfig;
//import com.southern.dataconsistencychecker.manager.DataSourceManager;
//import com.southern.dataconsistencychecker.mapper.CompareConfigMapper;
//import com.southern.dataconsistencychecker.mapper.CompareResultMapper;
//import com.southern.dataconsistencychecker.strategy.ConsistencyCheckStrategy;
//import org.springframework.stereotype.Component;
//
//
//import org.springframework.beans.factory.annotation.Autowired;
//
//
//import javax.sql.DataSource;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.sql.*;
//import java.util.*;
//import java.util.concurrent.*;
//import java.util.stream.Collectors;
//
//@Component("algorithm")
//public class AlgorithmBasedStrategy implements ConsistencyCheckStrategy {
//
//    @Autowired
//    private DataSourceManager dataSourceManager;
//
//    @Autowired
//    private CompareResultMapper compareResultMapper;
//
//    @Autowired
//    private CompareDetailLogMapper compareDetailLogMapper;
//
//    @Autowired
//    private CompareConfigMapper compareConfigMapper;
//
//    private static final int BATCH_SIZE = 1000;
//    private static final int THREAD_POOL_SIZE = 10;
//
//    private ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
//
//    @Override
//    public void execute(CompareConfig config) {
//        // 记录开始时间
//        long startTime = System.currentTimeMillis();
//
//        // 获取数据源
//        DataSource sourceDS = dataSourceManager.getDataSourceById(config.getSourceDataSourceId());
//        DataSource targetDS = dataSourceManager.getDataSourceById(config.getTargetDataSourceId());
//
//        // 获取表信息
//        String sourceTable = config.getSourceTable();
//        String targetTable = config.getTargetTable();
//
//        String sourceUniqueKeys = config.getSourceUniqueKeys();
//        String targetUniqueKeys = config.getTargetUniqueKeys();
//
//        String sourceCompareFields = config.getSourceCompareFields();
//        String targetCompareFields = config.getTargetCompareFields();
//
//        String sourceConditions = config.getSourceConditions();
//        String targetConditions = config.getTargetConditions();
//
//        // 解析字段
//        List<String> sourceUniqueKeyList = Arrays.asList(sourceUniqueKeys.split(","));
//        List<String> targetUniqueKeyList = Arrays.asList(targetUniqueKeys.split(","));
//        List<String> sourceCompareFieldList = Arrays.asList(sourceCompareFields.split(","));
//        List<String> targetCompareFieldList = Arrays.asList(targetCompareFields.split(","));
//
//        // 创建CompareResult记录
//        CompareResult compareResult = new CompareResult();
//        compareResult.setCompareConfigId(config.getId());
//        compareResult.setCompareTaskId(generateTaskId());
//        compareResult.setCompareTime(new Timestamp(System.currentTimeMillis()));
//        compareResult.setCompareStatus("SUCCESS");
//        compareResult.setIsConsistent(1);
//        compareResult.setSourceDataDetails(JSON.toJSONString(dataSourceManager.getDataSourceDetails(config.getSourceDataSourceId())));
//        compareResult.setTargetDataDetails(JSON.toJSONString(dataSourceManager.getDataSourceDetails(config.getTargetDataSourceId())));
//        compareResultMapper.insert(compareResult);
//
//        // 使用CompletionService来管理多线程任务
//        CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);
//        int totalTasks = 0;
//
//        try (Connection sourceConn = sourceDS.getConnection();
//             Connection targetConn = targetDS.getConnection()) {
//
//            // 获取总记录数
//            int sourceCount = getCount(sourceConn, sourceTable, sourceConditions);
//            int targetCount = getCount(targetConn, targetTable, targetConditions);
//            int maxCount = Math.max(sourceCount, targetCount);
//
//            // 分批处理
//            for (int offset = 0; offset < maxCount; offset += BATCH_SIZE) {
//                int batchOffset = offset;
//                completionService.submit(() -> {
//                    // 读取源数据
//                    List<Map<String, Object>> sourceData = fetchData(sourceConn, sourceTable, sourceUniqueKeyList, sourceCompareFieldList, sourceConditions, batchOffset, BATCH_SIZE);
//                    // 读取目标数据
//                    List<Map<String, Object>> targetData = fetchData(targetConn, targetTable, targetUniqueKeyList, targetCompareFieldList, targetConditions, batchOffset, BATCH_SIZE);
//
//                    // 构建唯一键到记录的映射
//                    Map<String, Map<String, Object>> sourceMap = sourceData.stream()
//                            .collect(Collectors.toMap(
//                                    record -> generateUniqueKey(record, sourceUniqueKeyList),
//                                    record -> record
//                            ));
//
//                    Map<String, Map<String, Object>> targetMap = targetData.stream()
//                            .collect(Collectors.toMap(
//                                    record -> generateUniqueKey(record, targetUniqueKeyList),
//                                    record -> record
//                            ));
//
//                    // 比较键的集合
//                    Set<String> allKeys = new HashSet<>();
//                    allKeys.addAll(sourceMap.keySet());
//                    allKeys.addAll(targetMap.keySet());
//
//                    for (String key : allKeys) {
//                        Map<String, Object> sourceRecord = sourceMap.get(key);
//                        Map<String, Object> targetRecord = targetMap.get(key);
//
//                        if (sourceRecord == null) {
//                            // 唯一键在源表缺失
//                            logInconsistency(compareResult.getCompareTaskId(), config, 1, key, null, null, null);
//                            continue;
//                        }
//
//                        if (targetRecord == null) {
//                            // 唯一键在目标表缺失
//                            logInconsistency(compareResult.getCompareTaskId(), config, 2, null, key, null, null);
//                            continue;
//                        }
//
//                        // 比较字段
//                        boolean inconsistent = false;
//                        StringBuilder description = new StringBuilder();
//                        for (int i = 0; i < sourceCompareFieldList.size(); i++) {
//                            String sourceField = sourceCompareFieldList.get(i).trim();
//                            String targetField = targetCompareFieldList.get(i).trim();
//                            Object sourceValue = sourceRecord.get(sourceField);
//                            Object targetValue = targetRecord.get(targetField);
//                            if (!Objects.equals(sourceValue, targetValue)) {
//                                inconsistent = true;
//                                description.append(String.format("字段 %s 与 %s 不一致，source: %s, target: %s; ",
//                                        sourceField, targetField, sourceValue, targetValue));
//                            }
//                        }
//
//                        if (inconsistent) {
//                            compareResult.setCompareStatus("FAIL");
//                            compareResult.setIsConsistent(0);
//                            compareResultMapper.update(compareResult);
//
//                            // 记录详细日志
//                            logInconsistency(compareResult.getCompareTaskId(), config, 3, key, key, sourceRecord, targetRecord);
//                        }
//                    }
//
//                    return null;
//                });
//                totalTasks++;
//            }
//
//            // 等待所有任务完成
//            for (int i = 0; i < totalTasks; i++) {
//                Future<Void> future = completionService.take();
//                future.get();
//            }
//
//        } catch (Exception e) {
//            compareResult.setCompareStatus("FAIL");
//            compareResult.setDescription(e.getMessage());
//            compareResultMapper.update(compareResult);
//            e.printStackTrace();
//        } finally {
//            executorService.shutdown();
//        }
//
//        // 更新比较结果的耗时
//        long endTime = System.currentTimeMillis();
//        compareResult.setUpdateTime(new Timestamp(endTime));
//        compareResultMapper.update(compareResult);
//    }
//
//    private int getCount(Connection conn, String table, String conditions) throws SQLException {
//        String sql = String.format("SELECT COUNT(*) FROM %s WHERE %s", table, conditions);
//        try (Statement stmt = conn.createStatement();
//             ResultSet rs = stmt.executeQuery(sql)) {
//            if (rs.next()) {
//                return rs.getInt(1);
//            }
//            return 0;
//        }
//    }
//
//    private List<Map<String, Object>> fetchData(Connection conn, String table, List<String> uniqueKeys,
//                                                List<String> compareFields, String conditions, int offset, int limit) throws SQLException {
//        String fields = String.join(", ", uniqueKeys) + ", " + String.join(", ", compareFields);
//        String sql = String.format("SELECT %s FROM %s WHERE %s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY",
//                fields, table, conditions, offset, limit);
//        List<Map<String, Object>> data = new ArrayList<>();
//        try (Statement stmt = conn.createStatement();
//             ResultSet rs = stmt.executeQuery(sql)) {
//            ResultSetMetaData meta = rs.getMetaData();
//            int columnCount = meta.getColumnCount();
//            while (rs.next()) {
//                Map<String, Object> record = new HashMap<>();
//                for (int i = 1; i <= columnCount; i++) {
//                    record.put(meta.getColumnName(i), rs.getObject(i));
//                }
//                data.add(record);
//            }
//        }
//        return data;
//    }
//
//    private String generateUniqueKey(Map<String, Object> record, List<String> uniqueKeys) {
//        return uniqueKeys.stream()
//                .map(key -> Objects.toString(record.get(key), ""))
//                .collect(Collectors.joining("_"));
//    }
//
//    private void logInconsistency(Long compareTaskId, CompareConfig config, int type,
//                                  String sourceKey, String targetKey,
//                                  Map<String, Object> sourceRecord, Map<String, Object> targetRecord) {
//        CompareDetailLog log = new CompareDetailLog();
//        log.setCompareTaskId(compareTaskId);
//        log.setSourceDataSourceId(config.getSourceDataSourceId());
//        log.setTargetDataSourceId(config.getTargetDataSourceId());
//        log.setSourceTable(config.getSourceTable());
//        log.setTargetTable(config.getTargetTable());
//        log.setType(type);
//        log.setSourceUniqueKeys(sourceKey);
//        log.setTargetUniqueKeys(targetKey);
//
//        if (type == 3 && sourceRecord != null && targetRecord != null) {
//            // 记录字段不一致的详细信息
//            for (String field : config.getSourceCompareFields().split(",")) {
//                String sourceValue = Objects.toString(sourceRecord.get(field.trim()), "");
//                String targetField = config.getTargetCompareFields().split(",")[config.getSourceCompareFields().split(",").length];
//                String targetValue = Objects.toString(targetRecord.get(targetField.trim()), "");
//                CompareDetailLog detailLog = new CompareDetailLog();
//                detailLog.setCompareTaskId(compareTaskId);
//                detailLog.setSourceDataSourceId(config.getSourceDataSourceId());
//                detailLog.setTargetDataSourceId(config.getTargetDataSourceId());
//                detailLog.setSourceTable(config.getSourceTable());
//                detailLog.setTargetTable(config.getTargetTable());
//                detailLog.setType(type);
//                detailLog.setSourceUniqueKeys(sourceKey);
//                detailLog.setTargetUniqueKeys(targetKey);
//                detailLog.setSourceFieldKey(field.trim());
//                detailLog.setSourceFieldValue(sourceValue);
//                detailLog.setTargetFieldKey(config.getTargetCompareFields().split(",")[config.getSourceCompareFields().split(",").length].trim());
//                detailLog.setTargetFieldValue(targetValue);
//                compareDetailLogMapper.insert(detailLog);
//
//                // 生成修复SQL
//                generateRepairSQL(config, field.trim(), sourceValue, targetValue);
//            }
//        } else {
//            compareDetailLogMapper.insert(log);
//            // 根据类型生成修复SQL
//            generateRepairSQL(config, null, null, null);
//        }
//    }
//
//    private void generateRepairSQL(CompareConfig config, String field, String sourceValue, String targetValue) {
//        // 根据数据源类型生成修复语句
//        String sourceDSType = dataSourceManager.getDataSourceType(config.getSourceDataSourceId());
//        String targetDSType = dataSourceManager.getDataSourceType(config.getTargetDataSourceId());
//
//        String repairSourceSQL = "";
//        String repairTargetSQL = "";
//
//        if (field != null) {
//            // 字段不一致，生成UPDATE语句
//            String sourceUniqueKey = config.getSourceUniqueKeys();
//            String targetUniqueKey = config.getTargetUniqueKeys();
//            repairSourceSQL = String.format("UPDATE %s SET %s = '%s' WHERE %s = '%s';",
//                    config.getSourceTable(), field, sourceValue, sourceUniqueKey, sourceValue);
//            repairTargetSQL = String.format("UPDATE %s SET %s = '%s' WHERE %s = '%s';",
//                    config.getTargetTable(), field, targetValue, targetUniqueKey, targetValue);
//        } else if (sourceValue != null && config.getType() == 1) {
//            // 源表缺失，生成INSERT语句
//            repairTargetSQL = String.format("INSERT INTO %s (%s) VALUES (%s);",
//                    config.getSourceTable(), config.getSourceUniqueKeys(), sourceValue);
//        } else if (targetValue != null && config.getType() == 2) {
//            // 目标表缺失，生成INSERT语句
//            repairSourceSQL = String.format("INSERT INTO %s (%s) VALUES (%s);",
//                    config.getTargetTable(), config.getTargetUniqueKeys(), targetValue);
//        }
//
//        // 写入SQL文件
//        try {
//            if (!repairSourceSQL.isEmpty()) {
//                String fileName = String.format("repairSource_%s_%s.sql", config.getSourceTable(), UUID.randomUUID().toString().replace("-", ""));
//                try (FileWriter writer = new FileWriter("repair/" + fileName, true)) {
//                    writer.write(repairSourceSQL + "\n");
//                }
//            }
//            if (!repairTargetSQL.isEmpty()) {
//                String fileName = String.format("repairTarget_%s_%s.sql", config.getTargetTable(), UUID.randomUUID().toString().replace("-", ""));
//                try (FileWriter writer = new FileWriter("repair/" + fileName, true)) {
//                    writer.write(repairTargetSQL + "\n");
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private Long generateTaskId() {
//        return System.currentTimeMillis();
//    }
//}
