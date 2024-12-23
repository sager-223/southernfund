package com.southern.dataconsistencychecker.strategy.impl;

import com.alibaba.fastjson2.JSON;
import com.southern.dataconsistencychecker.manager.DataSourceManager;
import com.southern.dataconsistencychecker.mapper.CompareDetailLogMapper;
import com.southern.dataconsistencychecker.mapper.CompareResultMapper;
import com.southern.dataconsistencychecker.pojo.entity.CompareConfig;
import com.southern.dataconsistencychecker.pojo.entity.CompareDetailLog;
import com.southern.dataconsistencychecker.pojo.entity.CompareResult;
import com.southern.dataconsistencychecker.strategy.ConsistencyCheckStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于内存，分批引入内存
 * md5 hash分区，多线程
 * 结果写入更详细的CompareDetailLog
 * （新增）修复SQL
 */
@Component("memory5")
public class MemoryBasedStrategy5 implements ConsistencyCheckStrategy {
    @Autowired
    private DataSourceManager dataSourceManager;

    @Autowired
    private CompareResultMapper compareResultMapper;

    @Autowired // 新增注入 CompareDetailLogMapper
    private CompareDetailLogMapper compareDetailLogMapper;

    private static final int DEFAULT_SHARD_COUNT = 4; // 默认分片数，可根据需要调整或从配置中获取

    @Override
    public void execute(CompareConfig config) {
        // 生成唯一的 compare_task_id
        long compareTaskId = generateCompareTaskId();

        int shardCount = DEFAULT_SHARD_COUNT;
        ExecutorService executorService = Executors.newFixedThreadPool(shardCount);
        List<Future<ShardCompareResult>> futures = new ArrayList<>();

        for (int shard = 0; shard < shardCount; shard++) {
            ShardCompareTask task = new ShardCompareTask(config, shard, shardCount, compareTaskId);
            Future<ShardCompareResult> future = executorService.submit(task);
            futures.add(future);
        }

        // 初始化总的差异计数和修复SQL集合
        AtomicInteger totalDiscrepancies = new AtomicInteger(0);
        List<String> allRepairSources = new ArrayList<>();
        List<String> allRepairTargets = new ArrayList<>();

        // 汇总所有分片的结果
        for (Future<ShardCompareResult> future : futures) {
            try {
                ShardCompareResult result = future.get();
                totalDiscrepancies.addAndGet(result.getDiscrepancies());
                allRepairSources.addAll(result.getRepairSources());
                allRepairTargets.addAll(result.getRepairTargets());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                // 这里可以记录失败的分片信息
            }
        }

        executorService.shutdown();

        // 准备 CompareResult
        CompareResult compareResult = new CompareResult();
        compareResult.setCompareConfigId(config.getId());
        compareResult.setCompareTaskId(compareTaskId); // 使用生成的 compare_task_id
        compareResult.setSourceDataDetails(JSON.toJSONString(getDataDetails(
                dataSourceManager.getDataSourceById(config.getSourceDataSourceId()))));
        compareResult.setTargetDataDetails(JSON.toJSONString(getDataDetails(
                dataSourceManager.getDataSourceById(config.getTargetDataSourceId()))));
        compareResult.setCompareTime(LocalDateTime.now());
        compareResult.setCompareStatus("SUCCESS");
        compareResult.setDescription(String.format("发现不一致，一共%d处。", totalDiscrepancies.get())); // 设置为简单描述
        compareResult.setRepairSource(String.join("\n", allRepairSources)); // 汇总所有修复Source的SQL
        compareResult.setRepairTarget(String.join("\n", allRepairTargets)); // 汇总所有修复Target的SQL
        compareResult.setEmailNotificationStatus("noNeed"); // 根据需要设置
        compareResult.setSmsNotificationStatus("noNeed");   // 根据需要设置
        compareResult.setIsConsistent(totalDiscrepancies.get() == 0);
        compareResult.setCreateTime(LocalDateTime.now());
        compareResult.setUpdateTime(LocalDateTime.now());

        // 插入 CompareResult
        compareResultMapper.insertCompareResult(compareResult);
    }

    /**
     * 分片比较任务
     */
    private class ShardCompareTask implements Callable<ShardCompareResult> {
        private final CompareConfig config;
        private final int shardNumber;
        private final int shardCount;
        private final long compareTaskId; // 新增 compare_task_id

        public ShardCompareTask(CompareConfig config, int shardNumber, int shardCount, long compareTaskId) {
            this.config = config;
            this.shardNumber = shardNumber;
            this.shardCount = shardCount;
            this.compareTaskId = compareTaskId;
        }

        @Override
        public ShardCompareResult call() {
            int discrepancies = 0;
            List<String> repairSources = new ArrayList<>();
            List<String> repairTargets = new ArrayList<>();

            DataSource sourceDS = dataSourceManager.getDataSourceById(config.getSourceDataSourceId());
            DataSource targetDS = dataSourceManager.getDataSourceById(config.getTargetDataSourceId());

            String sourceTable = config.getSourceTable();
            String targetTable = config.getTargetTable();

            String[] sourceUniqueKeys = config.getSourceUniqueKeys().split(",");
            String[] targetUniqueKeys = config.getTargetUniqueKeys().split(",");

            String[] sourceCompareFields = config.getSourceCompareFields().split(",");
            String[] targetCompareFields = config.getTargetCompareFields().split(",");

            String sourceConditions = config.getSourceConditions();
            String targetConditions = config.getTargetConditions();

            // 构建字段映射：source字段 -> target字段
            Map<String, String> sourceToTargetMap = new HashMap<>();
            if (sourceCompareFields.length != targetCompareFields.length) {
                throw new IllegalArgumentException("源和目标的比较字段数量不匹配。");
            }
            //TODO  新增的唯一键映射
            for (int i = 0; i < sourceUniqueKeys.length; i++) {
                String sourceUniqueKey = sourceUniqueKeys[i].trim();
                String targetUniqueKey = targetUniqueKeys[i].trim();
                sourceToTargetMap.put(sourceUniqueKey, targetUniqueKey);
            }
            for (int i = 0; i < sourceCompareFields.length; i++) {
                String sourceField = sourceCompareFields[i].trim();
                String targetField = targetCompareFields[i].trim();
                sourceToTargetMap.put(sourceField, targetField);
            }



            // 构建字段映射：target字段 -> source字段
            Map<String, String> targetToSourceMap = new HashMap<>();

            //TODO  增加你的唯一键映射
            for (int i = 0; i < targetUniqueKeys.length; i++) {
                String targetUniqueKey = targetUniqueKeys[i].trim();
                String sourceUniqueKey = sourceUniqueKeys[i].trim();
                targetToSourceMap.put(targetUniqueKey, sourceUniqueKey);
            }

            for (int i = 0; i < targetCompareFields.length; i++) {
                String targetField = targetCompareFields[i].trim();
                String sourceField = sourceCompareFields[i].trim();
                targetToSourceMap.put(targetField, sourceField);
            }




            // 构建包含分片条件的 SELECT 查询
            String sourceQuery = buildShardSelectQuery(sourceDS, sourceTable, sourceUniqueKeys, sourceCompareFields, sourceConditions, shardNumber, shardCount);
            String targetQuery = buildShardSelectQuery(targetDS, targetTable, targetUniqueKeys, targetCompareFields, targetConditions, shardNumber, shardCount);

            Connection sourceConn = null;
            Connection targetConn = null;
            PreparedStatement sourceStmt = null;
            PreparedStatement targetStmt = null;
            ResultSet sourceRs = null;
            ResultSet targetRs = null;

            try {
                sourceConn = sourceDS.getConnection();
                targetConn = targetDS.getConnection();

                sourceStmt = sourceConn.prepareStatement(sourceQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                targetStmt = targetConn.prepareStatement(targetQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

                // 根据数据库类型设置 fetch size
                setFetchSize(sourceConn, sourceStmt);
                setFetchSize(targetConn, targetStmt);

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
                            // 键匹配，比较字段
                            List<String> sourceDifferingFields = new ArrayList<>();
                            List<String> targetDifferingFields = new ArrayList<>();
                            Map<String, String> sourceRow = getRowData(sourceRs, sourceUniqueKeys, sourceCompareFields);
                            Map<String, String> targetRow = getRowData(targetRs, targetUniqueKeys, targetCompareFields);

                            // 用于构建修复SQL的数据
                            Map<String, String> fieldsToUpdateSource = new HashMap<>();
                            Map<String, String> fieldsToUpdateTarget = new HashMap<>();

                            for (int i = 0; i < sourceCompareFields.length; i++) {
                                String sourceField = sourceCompareFields[i].trim();
                                String targetField = targetCompareFields[i].trim();
                                String sourceValue = sourceRow.get(sourceField);
                                String targetValue = targetRow.get(targetField);
                                if (!Objects.equals(sourceValue, targetValue)) {
                                    sourceDifferingFields.add(sourceField);
                                    targetDifferingFields.add(targetField);
                                    // 收集需要更新的字段
                                    fieldsToUpdateSource.put(sourceField, targetValue);
                                    fieldsToUpdateTarget.put(targetField, sourceValue);
                                }
                            }

                            if (!sourceDifferingFields.isEmpty()) {
                                discrepancies++;

                                // 生成单一的修复SQL，更新所有有差异的字段
                                String repairSourceSQL = generateUpdateSQL(sourceTable, fieldsToUpdateSource, sourceRow, sourceUniqueKeys);
                                String repairTargetSQL = generateUpdateSQL(targetTable, fieldsToUpdateTarget, targetRow, targetUniqueKeys);

                                // 创建 CompareDetailLog 记录
                                CompareDetailLog log = new CompareDetailLog();
                                log.setCompareTaskId(compareTaskId);
                                log.setSourceDataSourceId(config.getSourceDataSourceId());
                                log.setTargetDataSourceId(config.getTargetDataSourceId());
                                log.setSourceTable(sourceTable);
                                log.setTargetTable(targetTable);
                                log.setType(3); // 类型3：字段不一致
                                log.setSourceUniqueKeys(sourceKey);
                                log.setTargetUniqueKeys(targetKey);
                                log.setCreateTime(LocalDateTime.now());
                                log.setUpdateTime(LocalDateTime.now());

                                // **修正部分：正确记录源和目标字段的原始值**
                                log.setSourceFieldKey(String.join(", ", sourceDifferingFields));
                                log.setSourceFieldValue(String.join(", ", sourceDifferingFields.stream()
                                        .map(field -> sourceRow.get(field))
                                        .toArray(String[]::new))
                                );
                                log.setTargetFieldKey(String.join(", ", targetDifferingFields));
                                log.setTargetFieldValue(String.join(", ", targetDifferingFields.stream()
                                        .map(field -> targetRow.get(field))
                                        .toArray(String[]::new))
                                );

                                // 设置修复SQL
                                log.setRepairSource(repairSourceSQL);
                                log.setRepairTarget(repairTargetSQL);

                                // 插入 CompareDetailLog
                                compareDetailLogMapper.insertCompareDetailLog(log);

                                // 收集修复SQL
                                if (!repairSourceSQL.isEmpty()) {
                                    repairSources.add(repairSourceSQL);
                                }
                                if (!repairTargetSQL.isEmpty()) {
                                    repairTargets.add(repairTargetSQL);
                                }
                            }

                            sourceHasNext = sourceRs.next();
                            targetHasNext = targetRs.next();
                        } else if (comparison < 0) {
                            // Type2：目标表缺失唯一键（源表有，目标表缺失）
                            discrepancies++;
                            CompareDetailLog log = new CompareDetailLog();
                            log.setCompareTaskId(compareTaskId);
                            log.setSourceDataSourceId(config.getSourceDataSourceId());
                            log.setTargetDataSourceId(config.getTargetDataSourceId());
                            log.setSourceTable(sourceTable);
                            log.setTargetTable(targetTable);
                            log.setType(2); // 类型2：目标表缺失唯一键
                            log.setSourceUniqueKeys(getCompositeKey(sourceRs, sourceUniqueKeys, sourceKeyMap));
                            log.setTargetUniqueKeys(null);
                            log.setCreateTime(LocalDateTime.now());
                            log.setUpdateTime(LocalDateTime.now());

                            // 用于生成修复SQL
                            Map<String, String> sourceRow = getRowData(sourceRs, sourceUniqueKeys, sourceCompareFields);

                            // 修复目标表缺失，应该向目标表插入数据，并从源表删除数据
                            String insertTargetSQL = generateInsertSQL(targetTable, sourceRow, sourceToTargetMap);
                            String deleteSourceSQL = generateDeleteSQL(sourceTable, sourceRow, sourceUniqueKeys);

                            // 设置修复SQL
                            log.setRepairSource(deleteSourceSQL);
                            log.setRepairTarget(insertTargetSQL);

                            // 插入 CompareDetailLog
                            compareDetailLogMapper.insertCompareDetailLog(log);

                            // 收集修复SQL
                            if (!insertTargetSQL.isEmpty()) {
                                repairTargets.add(insertTargetSQL);
                            }
                            if (!deleteSourceSQL.isEmpty()) {
                                repairSources.add(deleteSourceSQL);
                            }

                            sourceHasNext = sourceRs.next();
                        } else {
                            // Type1：源表缺失唯一键（目标表有，源表缺失）
                            discrepancies++;
                            CompareDetailLog log = new CompareDetailLog();
                            log.setCompareTaskId(compareTaskId);
                            log.setSourceDataSourceId(config.getSourceDataSourceId());
                            log.setTargetDataSourceId(config.getTargetDataSourceId());
                            log.setSourceTable(sourceTable);
                            log.setTargetTable(targetTable);
                            log.setType(1); // 类型1：源表缺失唯一键
                            log.setSourceUniqueKeys(null);
                            log.setTargetUniqueKeys(getCompositeKey(targetRs, targetUniqueKeys, targetKeyMap));
                            log.setCreateTime(LocalDateTime.now());
                            log.setUpdateTime(LocalDateTime.now());

                            // 用于生成修复SQL
                            Map<String, String> targetRow = getRowData(targetRs, targetUniqueKeys, targetCompareFields);

                            // 修复源表缺失，应该向源表插入数据，并从目标表删除数据
                            String insertSourceSQL = generateInsertSQL(sourceTable, targetRow, targetToSourceMap);
                            String deleteTargetSQL = generateDeleteSQL(targetTable, targetRow, targetUniqueKeys);

                            // 设置修复SQL
                            log.setRepairSource(insertSourceSQL);
                            log.setRepairTarget(deleteTargetSQL);

                            // 插入 CompareDetailLog
                            compareDetailLogMapper.insertCompareDetailLog(log);

                            // 收集修复SQL
                            if (!insertSourceSQL.isEmpty()) {
                                repairSources.add(insertSourceSQL);
                            }
                            if (!deleteTargetSQL.isEmpty()) {
                                repairTargets.add(deleteTargetSQL);
                            }

                            targetHasNext = targetRs.next();
                        }
                    } else if (sourceHasNext) {
                        // Type2：目标表缺失唯一键（源表有，目标表缺失）
                        discrepancies++;
                        CompareDetailLog log = new CompareDetailLog();
                        log.setCompareTaskId(compareTaskId);
                        log.setSourceDataSourceId(config.getSourceDataSourceId());
                        log.setTargetDataSourceId(config.getTargetDataSourceId());
                        log.setSourceTable(sourceTable);
                        log.setTargetTable(targetTable);
                        log.setType(2); // 类型2：目标表缺失唯一键
                        log.setSourceUniqueKeys(getCompositeKey(sourceRs, sourceUniqueKeys, sourceKeyMap));
                        log.setTargetUniqueKeys(null);
                        log.setCreateTime(LocalDateTime.now());
                        log.setUpdateTime(LocalDateTime.now());

                        // 用于生成修复SQL
                        Map<String, String> sourceRow = getRowData(sourceRs, sourceUniqueKeys, sourceCompareFields);

                        // 修复目标表缺失，应该向目标表插入数据，并从源表删除数据
                        String insertTargetSQL = generateInsertSQL(targetTable, sourceRow, sourceToTargetMap);
                        String deleteSourceSQL = generateDeleteSQL(sourceTable, sourceRow, sourceUniqueKeys);

                        // 设置修复SQL
                        log.setRepairSource(deleteSourceSQL);
                        log.setRepairTarget(insertTargetSQL);

                        // 插入 CompareDetailLog
                        compareDetailLogMapper.insertCompareDetailLog(log);

                        // 收集修复SQL
                        if (!insertTargetSQL.isEmpty()) {
                            repairTargets.add(insertTargetSQL);
                        }
                        if (!deleteSourceSQL.isEmpty()) {
                            repairSources.add(deleteSourceSQL);
                        }

                        sourceHasNext = sourceRs.next();
                    } else {
                        // Type1：源表缺失唯一键（目标表有，源表缺失）
                        discrepancies++;
                        CompareDetailLog log = new CompareDetailLog();
                        log.setCompareTaskId(compareTaskId);
                        log.setSourceDataSourceId(config.getSourceDataSourceId());
                        log.setTargetDataSourceId(config.getTargetDataSourceId());
                        log.setSourceTable(sourceTable);
                        log.setTargetTable(targetTable);
                        log.setType(1); // 类型1：源表缺失唯一键
                        log.setSourceUniqueKeys(null);
                        log.setTargetUniqueKeys(getCompositeKey(targetRs, targetUniqueKeys, targetKeyMap));
                        log.setCreateTime(LocalDateTime.now());
                        log.setUpdateTime(LocalDateTime.now());

                        // 用于生成修复SQL
                        Map<String, String> targetRow = getRowData(targetRs, targetUniqueKeys, targetCompareFields);

                        // 修复源表缺失，应该向源表插入数据，并从目标表删除数据
                        String insertSourceSQL = generateInsertSQL(sourceTable, targetRow, targetToSourceMap);
                        String deleteTargetSQL = generateDeleteSQL(targetTable, targetRow, targetUniqueKeys);

                        // 设置修复SQL
                        log.setRepairSource(insertSourceSQL);
                        log.setRepairTarget(deleteTargetSQL);

                        // 插入 CompareDetailLog
                        compareDetailLogMapper.insertCompareDetailLog(log);

                        // 收集修复SQL
                        if (!insertSourceSQL.isEmpty()) {
                            repairSources.add(insertSourceSQL);
                        }
                        if (!deleteTargetSQL.isEmpty()) {
                            repairTargets.add(deleteTargetSQL);
                        }

                        targetHasNext = targetRs.next();
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
                // 在异常情况下，记录一处不一致
                CompareDetailLog log = new CompareDetailLog();
                log.setCompareTaskId(compareTaskId);
                log.setSourceDataSourceId(config.getSourceDataSourceId());
                log.setTargetDataSourceId(config.getTargetDataSourceId());
                log.setSourceTable(config.getSourceTable());
                log.setTargetTable(config.getTargetTable());
                log.setType(3); // 类型3：字段不一致（异常类型也归为字段不一致）
                log.setSourceUniqueKeys(null);
                log.setTargetUniqueKeys(null);
                log.setSourceFieldKey("Exception");
                log.setSourceFieldValue(e.getMessage());
                log.setTargetFieldKey("Exception");
                log.setTargetFieldValue(e.getMessage());
                log.setCreateTime(LocalDateTime.now());
                log.setUpdateTime(LocalDateTime.now());

                // 生成修复SQL（此处可能不适用，根据具体需求调整）
                log.setRepairSource(null);
                log.setRepairTarget(null);

                compareDetailLogMapper.insertCompareDetailLog(log);
                discrepancies++;
            } finally {
                // 关闭资源
                closeQuietly(targetRs);
                closeQuietly(targetStmt);
                closeQuietly(targetConn);
                closeQuietly(sourceRs);
                closeQuietly(sourceStmt);
                closeQuietly(sourceConn);
            }

            return new ShardCompareResult(discrepancies, repairSources, repairTargets);
        }

        /**
         * 根据数据库类型设置 fetch size 优化大数据集查询
         */
        private void setFetchSize(Connection conn, PreparedStatement stmt) throws SQLException {
            String driverName = conn.getMetaData().getDriverName().toLowerCase();
            if (driverName.contains("oracle")) {
                stmt.setFetchSize(1000);
            } else if (driverName.contains("mysql")) {
                stmt.setFetchSize(Integer.MIN_VALUE); // MySQL 流式查询
            } else {
                stmt.setFetchSize(1000); // 默认值
            }
        }

        /**
         * 构建带有分片条件的 SELECT 查询
         */
        private String buildShardSelectQuery(DataSource ds, String table, String[] uniqueKeys, String[] compareFields,
                                             String conditions, int shardNum, int shardCount) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ");
            List<String> allFields = new ArrayList<>();
            Collections.addAll(allFields, uniqueKeys);
            Collections.addAll(allFields, compareFields);
            sb.append(String.join(", ", allFields));
            sb.append(" FROM ").append(table);
            List<String> whereClauses = new ArrayList<>();

            if (conditions != null && !conditions.trim().isEmpty()) {
                whereClauses.add(conditions);
            }

            // 添加分片条件
            String hashExpression = buildHashExpression(ds, uniqueKeys);
            if (hashExpression != null) {
                whereClauses.add(String.format("MOD(%s, %d) = %d", hashExpression, shardCount, shardNum));
            } else {
                throw new UnsupportedOperationException("Unsupported database type for sharding.");
            }

            if (!whereClauses.isEmpty()) {
                sb.append(" WHERE ").append(String.join(" AND ", whereClauses));
            }
            sb.append(" ORDER BY ").append(String.join(", ", uniqueKeys));
            return sb.toString();
        }

        /**
         * 构建 HASH 表达式，根据不同数据库使用不同的函数
         */
        private String buildHashExpression(DataSource ds, String[] uniqueKeys) {
            try (Connection conn = ds.getConnection()) {
                DatabaseMetaData meta = conn.getMetaData();
                String dbProduct = meta.getDatabaseProductName().toLowerCase();
                String concatenatedFields = buildConcatenatedFields(uniqueKeys, dbProduct);

                if (dbProduct.contains("oracle")) {
                    // 使用 Oracle 的标准哈希函数，如 STANDARD_HASH
                    return String.format("ABS(TO_NUMBER(SUBSTR(STANDARD_HASH(%s, 'MD5'), 1, 8), 'XXXXXXXXXXXXXXXX'))", concatenatedFields);
                } else if (dbProduct.contains("mysql")) {
                    // 使用 MySQL 的 MD5 函数，并转换为数值
                    return String.format("ABS(CONV(SUBSTR(MD5(CONCAT(%s)), 1, 8), 16, 10))", buildMySQLConcat(uniqueKeys));
                } else {
                    // 可扩展其他数据库类型
                    return null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        }

        /**
         * 构建字符串连接表达式，适用于不同数据库
         */
        private String buildConcatenatedFields(String[] uniqueKeys, String dbProduct) {
            if (dbProduct.contains("oracle") || dbProduct.contains("postgresql")) {
                return String.join(" || ", uniqueKeys);
            } else if (dbProduct.contains("mysql") || dbProduct.contains("sql server")) {
                return String.join(", ", uniqueKeys);
            } else {
                // 默认使用逗号分隔
                return String.join(", ", uniqueKeys);
            }
        }

        /**
         * 构建 MySQL 的 CONCAT 表达式
         */
        private String buildMySQLConcat(String[] uniqueKeys) {
            StringBuilder sb = new StringBuilder();
            sb.append("CONCAT(");
            sb.append(String.join(", ", uniqueKeys));
            sb.append(")");
            return sb.toString();
        }

        /**
         * 获取列名到列索引的映射
         */
        private Map<String, Integer> getColumnIndexMap(ResultSet rs, String[] columns) throws SQLException {
            Map<String, Integer> map = new HashMap<>();
            ResultSetMetaData meta = rs.getMetaData();
            for (String col : columns) {
                if (col != null && !col.trim().isEmpty()) {
                    try {
                        int index = rs.findColumn(col.trim());
                        map.put(col.trim(), index);
                    } catch (SQLException e) {
                        throw new IllegalArgumentException("列名不存在于结果集中: " + col.trim());
                    }
                } else {
                    throw new IllegalArgumentException("列名不能为空或null");
                }
            }
            return map;
        }

        /**
         * 根据 ResultSet 和唯一键构建复合键
         */
        private String getCompositeKey(ResultSet rs, String[] uniqueKeys, Map<String, Integer> keyMap) throws SQLException {
            StringBuilder keyBuilder = new StringBuilder();
            for (int i = 0; i < uniqueKeys.length; i++) {
                if (i > 0) keyBuilder.append("_");
                String value = rs.getString(keyMap.get(uniqueKeys[i].trim()));
                keyBuilder.append(value != null ? value : "NULL");
            }
            return keyBuilder.toString();
        }

        /**
         * 获取当前行的所有字段数据
         */
        private Map<String, String> getRowData(ResultSet rs, String[] uniqueKeys, String[] compareFields) throws SQLException {
            Map<String, String> rowData = new HashMap<>();
            for (String key : uniqueKeys) {
                rowData.put(key.trim(), rs.getString(key.trim()));
            }
            for (String field : compareFields) {
                rowData.put(field.trim(), rs.getString(field.trim()));
            }
            return rowData;
        }

        /**
         * 生成 INSERT SQL 语句
         *
         * @param tableName    目标表名
         * @param rowData      源数据行，key为源字段名，value为字段值
         * @param fieldMapping 源字段名到目标字段名的映射
         * @return 生成的 INSERT SQL 语句
         */
        private String generateInsertSQL(String tableName, Map<String, String> rowData, Map<String, String> fieldMapping) {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ").append(tableName).append(" (");
            List<String> targetFields = new ArrayList<>();
            List<String> values = new ArrayList<>();

            for (Map.Entry<String, String> entry : fieldMapping.entrySet()) {
                String sourceField = entry.getKey();
                String targetField = entry.getValue();
                if (rowData.containsKey(sourceField)) {
                    targetFields.add(targetField);
                    String value = rowData.get(sourceField);
                    if (value == null) {
                        values.add("NULL");
                    } else {
                        values.add("'" + value.replace("'", "''") + "'");
                    }
                }
            }

            sb.append(String.join(", ", targetFields));
            sb.append(") VALUES (");
            sb.append(String.join(", ", values));
            sb.append(");");
            return sb.toString();
        }

        /**
         * 生成 DELETE SQL 语句
         */
        private String generateDeleteSQL(String tableName, Map<String, String> rowData, String[] uniqueKeys) {
            StringBuilder sb = new StringBuilder();
            sb.append("DELETE FROM ").append(tableName).append(" WHERE ");
            List<String> conditions = new ArrayList<>();
            boolean allKeysNull = true;
            for (String key : uniqueKeys) {
                String value = rowData.get(key.trim());
                if (value != null) {
                    allKeysNull = false;
                    conditions.add(key + "='" + value.replace("'", "''") + "'");
                } else {
                    conditions.add(key + " IS NULL");
                }
            }
            // 如果所有关键字都是 NULL，则跳过生成 DELETE 语句，避免执行无效的 DELETE
            if (allKeysNull) {
                // 可以记录日志或选择性处理
                return ""; // 返回空字符串，不生成 DELETE 语句
            }
            sb.append(String.join(" AND ", conditions));
            sb.append(";");
            return sb.toString();
        }

        /**
         * 生成 UPDATE SQL 语句
         * 根据目标或源行数据更新表
         */
        private String generateUpdateSQL(String tableName, Map<String, String> fieldsToUpdate, Map<String, String> originalRowData, String[] uniqueKeys) {
            if (fieldsToUpdate.isEmpty()) {
                return "";
            }
            StringBuilder sb = new StringBuilder();
            sb.append("UPDATE ").append(tableName).append(" SET ");
            List<String> setClauses = new ArrayList<>();
            for (Map.Entry<String, String> entry : fieldsToUpdate.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (value == null) {
                    setClauses.add(key + "=NULL");
                } else {
                    setClauses.add(key + "='" + value.replace("'", "''") + "'");
                }
            }
            sb.append(String.join(", ", setClauses));
            sb.append(" WHERE ");
            List<String> conditions = new ArrayList<>();
            boolean allKeysNull = true;
            for (String key : uniqueKeys) {
                String value = originalRowData.get(key.trim());
                if (value != null) {
                    allKeysNull = false;
                    conditions.add(key + "='" + value.replace("'", "''") + "'");
                } else {
                    conditions.add(key + " IS NULL");
                }
            }
            // 如果所有关键字都是 NULL，则跳过生成 UPDATE 语句，避免执行无效的 UPDATE
            if (allKeysNull) {
                // 可以记录日志或选择性处理
                return ""; // 返回空字符串，不生成 UPDATE 语句
            }
            sb.append(String.join(" AND ", conditions));
            sb.append(";");
            return sb.toString();
        }

        /**
         * 分割多个SQL语句为单个语句
         */
        private List<String> splitSqlStatements(String sql) {
            if (StringUtils.isEmpty(sql)) return Collections.emptyList();
            // 简单按分号分割，注意处理分号在字符串中的情况需要更复杂的逻辑
            String[] parts = sql.split(";");
            List<String> statements = new ArrayList<>();
            for (String part : parts) {
                part = part.trim();
                if (!part.isEmpty()) {
                    statements.add(part + ";");
                }
            }
            return statements;
        }
    }

    /**
     * 分片比较结果
     */
    private static class ShardCompareResult {
        private final int discrepancies;
        private final List<String> repairSources;
        private final List<String> repairTargets;

        public ShardCompareResult(int discrepancies, List<String> repairSources, List<String> repairTargets) {
            this.discrepancies = discrepancies;
            this.repairSources = repairSources;
            this.repairTargets = repairTargets;
        }

        public int getDiscrepancies() {
            return discrepancies;
        }

        public List<String> getRepairSources() {
            return repairSources;
        }

        public List<String> getRepairTargets() {
            return repairTargets;
        }
    }

    /**
     * 生成唯一的比较任务ID
     */
    private long generateCompareTaskId() {
        // 例如，使用当前时间戳和随机数生成唯一ID
        return System.currentTimeMillis() + new Random().nextInt(1000);
    }

    /**
     * 获取数据源的详细信息
     */
    private Map<String, Object> getDataDetails(DataSource ds) {
        Map<String, Object> details = new HashMap<>();
        try (Connection conn = ds.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            details.put("URL", meta.getURL());
            details.put("Username", meta.getUserName());
            details.put("DatabaseProductName", meta.getDatabaseProductName());
            // 根据需要添加更多细节
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return details;
    }

    /**
     * 安静地关闭 ResultSet
     */
    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ignored) {}
        }
    }

    /**
     * 安静地关闭 Statement
     */
    private void closeQuietly(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException ignored) {}
        }
    }

    /**
     * 安静地关闭 Connection
     */
    private void closeQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {}
        }
    }
}