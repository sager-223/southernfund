package com.southern.dataconsistencychecker.strategy.impl;

import com.alibaba.fastjson2.JSON;
import com.southern.dataconsistencychecker.entity.CompareConfig;
import com.southern.dataconsistencychecker.entity.CompareResult;
import com.southern.dataconsistencychecker.entity.CompareDetailLog; // 新增导入
import com.southern.dataconsistencychecker.manager.DataSourceManager;
import com.southern.dataconsistencychecker.mapper.CompareDetailLogMapper; // 新增导入
import com.southern.dataconsistencychecker.mapper.CompareResultMapper;
import com.southern.dataconsistencychecker.strategy.ConsistencyCheckStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
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
 */
@Component("memory3")
public class MemoryBasedStrategy3 implements ConsistencyCheckStrategy {
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

        // 初始化总的差异计数
        AtomicInteger totalDiscrepancies = new AtomicInteger(0);

        // 汇总所有分片的结果
        for (Future<ShardCompareResult> future : futures) {
            try {
                ShardCompareResult result = future.get();
                totalDiscrepancies.addAndGet(result.getDiscrepancies());
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
        compareResult.setEmailNotificationStatus("noNeed"); // 根据需要设置
        compareResult.setSmsNotificationStatus("noNeed");   // 根据需要设置
        compareResult.setIsConsistent(totalDiscrepancies.get() == 0 ? true : false);
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
                            boolean hasFieldDifference = false;
                            for (int i = 0; i < sourceCompareFields.length; i++) {
                                String sourceValue = sourceRs.getString(sourceCompareFields[i].trim());
                                String targetValue = targetRs.getString(targetCompareFields[i].trim());
                                if (!Objects.equals(sourceValue, targetValue)) {
                                    hasFieldDifference = true;
                                    // 创建并插入 CompareDetailLog 记录
                                    CompareDetailLog log = new CompareDetailLog();
                                    log.setCompareTaskId(compareTaskId);
                                    log.setSourceDataSourceId(config.getSourceDataSourceId());
                                    log.setTargetDataSourceId(config.getTargetDataSourceId());
                                    log.setSourceTable(sourceTable);
                                    log.setTargetTable(targetTable);
                                    log.setType(3); // 类型3：字段不一致
                                    log.setSourceUniqueKeys(sourceKey);
                                    log.setTargetUniqueKeys(targetKey);
                                    log.setSourceFieldKey(sourceCompareFields[i].trim());
                                    log.setSourceFieldValue(sourceValue);
                                    log.setTargetFieldKey(targetCompareFields[i].trim());
                                    log.setTargetFieldValue(targetValue);
                                    log.setCreateTime(LocalDateTime.now());
                                    log.setUpdateTime(LocalDateTime.now());

                                    compareDetailLogMapper.insertCompareDetailLog(log);
                                    discrepancies++;
                                }
                            }
                            sourceHasNext = sourceRs.next();
                            targetHasNext = targetRs.next();
                        } else if (comparison < 0) {
                            // Source 键在 Target 中缺失
                            CompareDetailLog log = new CompareDetailLog();
                            log.setCompareTaskId(compareTaskId);
                            log.setSourceDataSourceId(config.getSourceDataSourceId());
                            log.setTargetDataSourceId(config.getTargetDataSourceId());
                            log.setSourceTable(sourceTable);
                            log.setTargetTable(targetTable);
                            log.setType(2); // 类型2：target表唯一键缺失
                            log.setSourceUniqueKeys(sourceKey);
                            log.setTargetUniqueKeys(null);
                            log.setSourceFieldKey(null);
                            log.setSourceFieldValue(null);
                            log.setTargetFieldKey(null);
                            log.setTargetFieldValue(null);
                            log.setCreateTime(LocalDateTime.now());
                            log.setUpdateTime(LocalDateTime.now());

                            compareDetailLogMapper.insertCompareDetailLog(log);
                            discrepancies++;
                            sourceHasNext = sourceRs.next();
                        } else {
                            // Target 键在 Source 中缺失
                            CompareDetailLog log = new CompareDetailLog();
                            log.setCompareTaskId(compareTaskId);
                            log.setSourceDataSourceId(config.getSourceDataSourceId());
                            log.setTargetDataSourceId(config.getTargetDataSourceId());
                            log.setSourceTable(sourceTable);
                            log.setTargetTable(targetTable);
                            log.setType(1); // 类型1：source表唯一键缺失
                            log.setSourceUniqueKeys(null);
                            log.setTargetUniqueKeys(targetKey);
                            log.setSourceFieldKey(null);
                            log.setSourceFieldValue(null);
                            log.setTargetFieldKey(null);
                            log.setTargetFieldValue(null);
                            log.setCreateTime(LocalDateTime.now());
                            log.setUpdateTime(LocalDateTime.now());

                            compareDetailLogMapper.insertCompareDetailLog(log);
                            discrepancies++;
                            targetHasNext = targetRs.next();
                        }
                    } else if (sourceHasNext) {
                        // Source 还有剩余记录，Target 中缺失
                        String sourceKey = getCompositeKey(sourceRs, sourceUniqueKeys, sourceKeyMap);
                        CompareDetailLog log = new CompareDetailLog();
                        log.setCompareTaskId(compareTaskId);
                        log.setSourceDataSourceId(config.getSourceDataSourceId());
                        log.setTargetDataSourceId(config.getTargetDataSourceId());
                        log.setSourceTable(sourceTable);
                        log.setTargetTable(targetTable);
                        log.setType(2); // 类型2：target表唯一键缺失
                        log.setSourceUniqueKeys(sourceKey);
                        log.setTargetUniqueKeys(null);
                        log.setSourceFieldKey(null);
                        log.setSourceFieldValue(null);
                        log.setTargetFieldKey(null);
                        log.setTargetFieldValue(null);
                        log.setCreateTime(LocalDateTime.now());
                        log.setUpdateTime(LocalDateTime.now());

                        compareDetailLogMapper.insertCompareDetailLog(log);
                        discrepancies++;
                        sourceHasNext = sourceRs.next();
                    } else {
                        // Target 还有剩余记录，Source 中缺失
                        String targetKey = getCompositeKey(targetRs, targetUniqueKeys, targetKeyMap);
                        CompareDetailLog log = new CompareDetailLog();
                        log.setCompareTaskId(compareTaskId);
                        log.setSourceDataSourceId(config.getSourceDataSourceId());
                        log.setTargetDataSourceId(config.getTargetDataSourceId());
                        log.setSourceTable(sourceTable);
                        log.setTargetTable(targetTable);
                        log.setType(1); // 类型1：source表唯一键缺失
                        log.setSourceUniqueKeys(null);
                        log.setTargetUniqueKeys(targetKey);
                        log.setSourceFieldKey(null);
                        log.setSourceFieldValue(null);
                        log.setTargetFieldKey(null);
                        log.setTargetFieldValue(null);
                        log.setCreateTime(LocalDateTime.now());
                        log.setUpdateTime(LocalDateTime.now());

                        compareDetailLogMapper.insertCompareDetailLog(log);
                        discrepancies++;
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
                log.setSourceTable(sourceTable);
                log.setTargetTable(targetTable);
                log.setType(3); // 类型3：字段不一致（异常类型也归为字段不一致）
                log.setSourceUniqueKeys(null);
                log.setTargetUniqueKeys(null);
                log.setSourceFieldKey("Exception");
                log.setSourceFieldValue(e.getMessage());
                log.setTargetFieldKey("Exception");
                log.setTargetFieldValue(e.getMessage());
                log.setCreateTime(LocalDateTime.now());
                log.setUpdateTime(LocalDateTime.now());

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

            return new ShardCompareResult(discrepancies);
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
                String concatenatedFields = String.join("||", uniqueKeys); // Oracle 使用 || 作为字符串连接符，MySQL 使用 CONCAT

                if (dbProduct.contains("oracle")) {
                    // 使用 Oracle 的标准哈希函数，如 STANDARD_HASH
                    return String.format("ABS(TO_NUMBER(SUBSTR(STANDARD_HASH(%s, 'MD5'), 1, 8), 'XXXXXXXXXXXXXXXX'))", concatenatedFields);
                } else if (dbProduct.contains("mysql")) {
                    // 使用 MySQL 的 MD5 函数，并转换为数值
                    return String.format("ABS(CONV(SUBSTR(MD5(CONCAT(%s)), 1, 8), 16, 10))", String.join(", ", uniqueKeys));
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
         * 获取列名到列索引的映射
         */
        private Map<String, Integer> getColumnIndexMap(ResultSet rs, String[] columns) throws SQLException {
            Map<String, Integer> map = new HashMap<>();
            ResultSetMetaData meta = rs.getMetaData();
            for (String col : columns) {
                if (col != null && !col.trim().isEmpty()) {
                    // 注意：列名在 ResultSet 中是从 1 开始的
                    map.put(col.trim(), rs.findColumn(col.trim()));
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
                keyBuilder.append(rs.getString(keyMap.get(uniqueKeys[i].trim())));
            }
            return keyBuilder.toString();
        }
    }

    /**
     * 分片比较结果
     */
    private static class ShardCompareResult {
        private final int discrepancies;

        public ShardCompareResult(int discrepancies) {
            this.discrepancies = discrepancies;
        }

        public int getDiscrepancies() {
            return discrepancies;
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