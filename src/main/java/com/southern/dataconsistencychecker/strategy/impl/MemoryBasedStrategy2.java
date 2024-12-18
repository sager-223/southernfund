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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于内存比对
 * md5分片  多线程
 * 不一致信息全部一次性拼接在description中
 */
@Component("memory2")
public class MemoryBasedStrategy2 implements ConsistencyCheckStrategy {
    @Autowired
    private DataSourceManager dataSourceManager;

    @Autowired
    private CompareResultMapper compareResultMapper;

    private static final int DEFAULT_SHARD_COUNT = 4; // 默认分片数，可根据需要调整或从配置中获取

    @Override
    public void execute(CompareConfig config) {
//        int shardCount = config.getShardCount() != null ? config.getShardCount() : DEFAULT_SHARD_COUNT;
        int shardCount =  DEFAULT_SHARD_COUNT;
        ExecutorService executorService = Executors.newFixedThreadPool(shardCount);
        List<Future<ShardCompareResult>> futures = new ArrayList<>();

        for (int shard = 0; shard < shardCount; shard++) {
            ShardCompareTask task = new ShardCompareTask(config, shard, shardCount);
            Future<ShardCompareResult> future = executorService.submit(task);
            futures.add(future);
        }

        // 初始化总的差异计数和描述
        AtomicInteger totalDiscrepancies = new AtomicInteger(0);
        StringBuilder totalDescription = new StringBuilder();

        // 汇总所有分片的结果
        for (Future<ShardCompareResult> future : futures) {
            try {
                ShardCompareResult result = future.get();
                totalDiscrepancies.addAndGet(result.getDiscrepancies());
                totalDescription.append(result.getDescription());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                // 这里可以记录失败的分片信息
            }
        }

        executorService.shutdown();

        // 准备 CompareResult
        CompareResult compareResult = new CompareResult();
        compareResult.setCompareConfigId(config.getId());
        compareResult.setCompareTaskId(generateCompareTaskId()); // 实现此方法以生成唯一的任务ID
        compareResult.setSourceDataDetails(JSON.toJSONString(getDataDetails(
                dataSourceManager.getDataSourceById(config.getSourceDataSourceId()))));
        compareResult.setTargetDataDetails(JSON.toJSONString(getDataDetails(
                dataSourceManager.getDataSourceById(config.getTargetDataSourceId()))));
        compareResult.setCompareTime(LocalDateTime.now());
        compareResult.setCompareStatus("SUCCESS");
        compareResult.setDescription(totalDescription.toString());
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

        public ShardCompareTask(CompareConfig config, int shardNumber, int shardCount) {
            this.config = config;
            this.shardNumber = shardNumber;
            this.shardCount = shardCount;
        }

        @Override
        public ShardCompareResult call() {
            int discrepancies = 0;
            StringBuilder descriptionBuilder = new StringBuilder();

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
                                descriptionBuilder.append(String.format("分片%d，唯一索引为%s时，比对字段不一致，%s%n",
                                        shardNumber, sourceKey, String.join("; ", fieldDifferences)));
                            }
                            sourceHasNext = sourceRs.next();
                            targetHasNext = targetRs.next();
                        } else if (comparison < 0) {
                            // Source 键在 Target 中缺失
                            discrepancies++;
                            descriptionBuilder.append(String.format("分片%d，唯一索引在source缺失，索引为%s%n", shardNumber, sourceKey));
                            sourceHasNext = sourceRs.next();
                        } else {
                            // Target 键在 Source 中缺失
                            discrepancies++;
                            descriptionBuilder.append(String.format("分片%d，唯一索引在target缺失，索引为%s%n", shardNumber, targetKey));
                            targetHasNext = targetRs.next();
                        }
                    } else if (sourceHasNext) {
                        // Source 还有剩余记录，Target 中缺失
                        String sourceKey = getCompositeKey(sourceRs, sourceUniqueKeys, sourceKeyMap);
                        discrepancies++;
                        descriptionBuilder.append(String.format("分片%d，唯一索引在source缺失，索引为%s%n", shardNumber, sourceKey));
                        sourceHasNext = sourceRs.next();
                    } else {
                        // Target 还有剩余记录，Source 中缺失
                        String targetKey = getCompositeKey(targetRs, targetUniqueKeys, targetKeyMap);
                        discrepancies++;
                        descriptionBuilder.append(String.format("分片%d，唯一索引在target缺失，索引为%s%n", shardNumber, targetKey));
                        targetHasNext = targetRs.next();
                    }

                    // 可选：限制描述长度避免内存过大
                    if (descriptionBuilder.length() > 10000) { // 阈值示例
                        break; // 或者采取其他处理方式
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
                descriptionBuilder.append(String.format("分片%d，执行比较时发生异常：%s%n", shardNumber, e.getMessage()));
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

            return new ShardCompareResult(discrepancies, descriptionBuilder.toString());
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
        private final String description;

        public ShardCompareResult(int discrepancies, String description) {
            this.discrepancies = discrepancies;
            this.description = description;
        }

        public int getDiscrepancies() {
            return discrepancies;
        }

        public String getDescription() {
            return description;
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