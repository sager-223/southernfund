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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class CompareService {

    private final DataSourceConfigMapper dataSourceConfigMapper;
    private final DynamicDataSource dynamicDataSource;
    private final CompareResultMapper compareResultMapper;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicLong taskIdGenerator = new AtomicLong(System.currentTimeMillis());

    // 定义分页大小和线程池大小
    private static final int PAGE_SIZE = 1000;
    private static final int THREAD_POOL_SIZE = 10;

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

        // 使用线程安全的集合来存储不一致信息
        ConcurrentLinkedQueue<String> inconsistencyDetails = new ConcurrentLinkedQueue<>();
        AtomicBoolean isConsistent = new AtomicBoolean(true);

        try {
            DataSource sourceDS = dynamicDataSource.getDataSourceById(config.getSourceDataSourceId());
            DataSource targetDS = dynamicDataSource.getDataSourceById(config.getTargetDataSourceId());

            if (sourceDS == null || targetDS == null) {
                throw new RuntimeException("数据源未找到，请检查配置的源和目标数据源 ID。");
            }

            DataSourceConfigEntity sourceConfig = dataSourceConfigMapper.getDataSourceConfigById(config.getSourceDataSourceId());
            DataSourceConfigEntity targetConfig = dataSourceConfigMapper.getDataSourceConfigById(config.getTargetDataSourceId());

            // 设置数据详情为 JSON
            result.setSourceDataDetails(objectMapper.writeValueAsString(sourceConfig));
            result.setTargetDataDetails(objectMapper.writeValueAsString(targetConfig));

            // 获取比较配置
            String targetTable = config.getTargetTable();
            String targetConditions = config.getTargetConditions();
            String targetUniqueKeys = config.getTargetUniqueKeys();
            String targetCompareFields = config.getTargetCompareFields();

            String sourceTable = config.getSourceTable();
            String sourceUniqueKeys = config.getSourceUniqueKeys();
            String sourceCompareFields = config.getSourceCompareFields();
            String sourceConditions = config.getSourceConditions();

            // 准备分页查询
            long totalRecords = getTotalRecordCount(targetDS, targetTable, targetConditions);
            int totalPages = (int) Math.ceil((double) totalRecords / PAGE_SIZE);

            ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
            List<Future<?>> futures = new ArrayList<>();

            for (int page = 0; page < totalPages; page++) {
                final int currentPage = page;
                futures.add(executor.submit(() -> {
                    try {
                        List<Map<String, Object>> targetPageData = fetchDataPage(targetDS, targetTable, targetConditions,
                                targetCompareFields, targetUniqueKeys, currentPage, PAGE_SIZE);

                        for (Map<String, Object> targetRow : targetPageData) {
                            String targetKey = buildKey(targetRow, targetUniqueKeys);
                            // 构建源数据查询条件
                            String sourceQueryCondition = buildCondition(sourceUniqueKeys, targetUniqueKeys, targetRow);

                            // 查询源数据
                            Map<String, Object> sourceRow = fetchSingleRow(sourceDS, sourceTable, sourceConditions, sourceCompareFields,
                                    sourceUniqueKeys, sourceQueryCondition);

                            if (sourceRow == null) {
                                // 情况1: 目标数据在源数据中不存在
                                inconsistencyDetails.add(String.format("唯一键: [%s]的数据行在源表中不存在", targetKey));
                                isConsistent.set(false);
                            } else {
                                // 情况2: 比较字段不一致
                                String[] sourceCompareFieldsArr = sourceCompareFields.split(",");
                                String[] targetCompareFieldsArr = targetCompareFields.split(",");

                                for (int i = 0; i < sourceCompareFieldsArr.length; i++) {
                                    String sourceField = sourceCompareFieldsArr[i].trim().toUpperCase();;
                                    String targetField = targetCompareFieldsArr[i].trim().toUpperCase();;

                                    Object sourceValue = sourceRow.get(sourceField);
                                    Object targetValue = targetRow.get(targetField);

                                    if (!Objects.equals(sourceValue, targetValue)) {
                                        inconsistencyDetails.add(String.format("字段不一致 - 唯一键[%s]: 源表.%s = %s, 目标表.%s = %s",
                                                targetKey, sourceField, sourceValue, targetField, targetValue));
                                        isConsistent.set(false);
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        // 记录异常信息
                        inconsistencyDetails.add(String.format("页面 %d 处理异常: %s", currentPage, e.getMessage()));
                        isConsistent.set(false);
                    }
                }));
            }

            // 等待所有任务完成
            for (Future<?> future : futures) {
                future.get();
            }

            executor.shutdown();

            // 设置比较结果
            result.setIsConsistent(isConsistent.get());
            if (isConsistent.get()) {
                result.setCompareStatus("SUCCESS");
                result.setDescription("数据一致。");
            } else {
                result.setCompareStatus("SUCCESS");
                StringBuilder descriptionBuilder = new StringBuilder("发现数据不一致:\n");
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
            result.setDescription("比较过程中发生异常: " + e.getMessage());
            result.setIsConsistent(false);
            result.setEmailNotificationStatus("false");
            result.setSmsNotificationStatus("false");
        }

        // 插入比较结果
        compareResultMapper.insertCompareResult(result);
    }

    /**
     * 获取目标表的总记录数
     */
    private long getTotalRecordCount(DataSource dataSource, String table, String conditions) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ").append(table);
        if (conditions != null && !conditions.trim().isEmpty()) {
            sql.append(" WHERE ").append(conditions);
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql.toString());
             ResultSet rs = pstmt.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }

        return 0;
    }

    /**
     * 分页获取目标表的数据，支持 Oracle 和 MySQL
     * 其中Map的key  都是大写
     * value改成了字符串形式
     */
    private List<Map<String, Object>> fetchDataPage(DataSource dataSource, String table, String conditions,
                                                    String compareFields, String uniqueKeys, int page, int pageSize) throws SQLException {
        // Step 1: 构建基础 SQL 查询语句
        StringBuilder baseSql = new StringBuilder("SELECT ");
        baseSql.append(uniqueKeys).append(",").append(compareFields).append(" FROM ").append(table);

        if (conditions != null && !conditions.trim().isEmpty()) {
            baseSql.append(" WHERE ").append(conditions);
        }

        // 检查是否包含 ORDER BY
        String lowerConditions = (conditions != null) ? conditions.toLowerCase() : "";
        boolean hasOrderBy = lowerConditions.contains("order by");

        // 如果没有 ORDER BY，则默认按 uniqueKeys 排序
        String orderByClause = "";
        if (hasOrderBy) {
            // 提取已有的 ORDER BY 子句
            int orderByIndex = lowerConditions.lastIndexOf("order by");
            if (orderByIndex != -1) {
                orderByClause = conditions.substring(orderByIndex, conditions.length());
                // 将前面的条件去掉
                baseSql = new StringBuilder("SELECT ");
                baseSql.append(uniqueKeys).append(",").append(compareFields).append(" FROM ").append(table);
                if (conditions.length() > orderByIndex) {
                    String newConditions = conditions.substring(0, orderByIndex).trim();
                    if (!newConditions.isEmpty()) {
                        baseSql.append(" WHERE ").append(newConditions);
                    }
                }
            }
        } else {
            orderByClause = " ORDER BY " + uniqueKeys;
        }

        // Step 2: 获取数据库类型
        String dbProductName;
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            dbProductName = metaData.getDatabaseProductName().toLowerCase();
        }

        // Step 3: 根据数据库类型构建分页 SQL
        String pagedSql;
        boolean isOracle = dbProductName.contains("oracle");
        boolean isMySQL = dbProductName.contains("mysql");

        if (isMySQL) {
            // MySQL 分页使用 LIMIT ? OFFSET ?
            pagedSql = baseSql.toString() + orderByClause + " LIMIT ? OFFSET ?";
        } else if (isOracle) {
            // Oracle 使用 ROW_NUMBER() 进行分页
            pagedSql = "SELECT * FROM ( " +
                    "SELECT a.*, ROW_NUMBER() OVER (" + orderByClause + ") AS rnum " +
                    "FROM (" + baseSql.toString() + orderByClause + ") a " +
                    ") WHERE rnum > ? AND rnum <= ?";
        } else {
            throw new UnsupportedOperationException("目前仅支持 Oracle 和 MySQL 数据库");
        }

        List<Map<String, Object>> dataList = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(pagedSql)) {

            if (isMySQL) {
                // 对于 MySQL，绑定 LIMIT 和 OFFSET 参数
                pstmt.setInt(1, pageSize);
                pstmt.setInt(2, page * pageSize);
            } else if (isOracle) {
                // 对于 Oracle，绑定 startRow 和 endRow 参数
                int startRow = page * pageSize;
                int endRow = (page + 1) * pageSize;
                pstmt.setInt(1, startRow);
                pstmt.setInt(2, endRow);
            }


            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (rs.next()) {

                    // TODO 如果 dataList 的类型是 List<Map<String, String>>，建议修改为如下：
                    // Map<String, String> rowMap = new HashMap<>();
                    // 如果 dataList 的类型保持为 List<Map<String, Object>>，也可以这样做
                    Map<String, Object> rowMap = new HashMap<>();

                    for (int i = 1; i <= columnCount; i++) {
                        // 获取列名并转换为大写
                        String columnName = metaData.getColumnLabel(i).toUpperCase();

                        // 获取列值
                        Object value = rs.getObject(i);

                        // 将值转换为字符串，如果值为 null，则存储字符串 "null"
                        String strValue = (value != null) ? value.toString() : "null";

                        // 将字符串值存入 rowMap
                        rowMap.put(columnName, strValue);

                        // 如果使用 Map<String, Object>，也可以直接存入转换后的字符串
                        // rowMap.put(columnName, strValue);
                    }

                    // 将转换后的 rowMap 添加到 dataList
                    dataList.add(rowMap);
                }
            }
        }

        return dataList;
    }



    /**
     * 构建源数据查询条件，支持大小写
     * @param sourceUniqueKeys 源唯一键字段，以逗号分隔
     * @param targetUniqueKeys 目标唯一键字段，以逗号分隔
     * @param targetRow 当前目标记录
     * @return 查询条件字符串
     */
    private String buildCondition(String sourceUniqueKeys, String targetUniqueKeys, Map<String, Object> targetRow) {
        // 参数验证
        if (sourceUniqueKeys == null || targetUniqueKeys == null ||
                sourceUniqueKeys.trim().isEmpty() || targetUniqueKeys.trim().isEmpty()) {
            throw new IllegalArgumentException("源和目标唯一键不能为空。");
        }

        // 拆分唯一键字符串
        String[] sourceKeys = sourceUniqueKeys.split(",");
        String[] targetKeys = targetUniqueKeys.split(",");

        // 校验源和目标唯一键数量是否一致
        if (sourceKeys.length != targetKeys.length) {
            throw new IllegalArgumentException("源和目标唯一键字段数目不匹配。");
        }

        StringBuilder condition = new StringBuilder();

        for (int i = 0; i < sourceKeys.length; i++) {
            // 处理源键：去除首尾空格
            String sourceKey = sourceKeys[i].trim();

            // 处理目标键：去除首尾空格并转换为大写
            String targetKey = targetKeys[i].trim().toUpperCase();

            // 从 targetRow 中获取值
            Object value = targetRow.get(targetKey);

            // 构建条件字符串
            condition.append(sourceKey).append(" = ");
            if (value instanceof Number) {
                condition.append(value);
            } else {
                condition.append("'").append(value).append("'");
            }

            // 如果不是最后一个条件，追加 "AND"
            if (i < sourceKeys.length - 1) {
                condition.append(" AND ");
            }
        }

        return condition.toString();
    }


    /**
     * 查询源表的单条记录，支持oracle & mysql
     * value改为了字符串形式
     */
    public Map<String, Object> fetchSingleRow(DataSource dataSource, String table, String conditions,
                                              String compareFields, String uniqueKeys, String queryCondition) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(uniqueKeys).append(", ").append(compareFields).append(" FROM ").append(table);

        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            String dbProductName = metaData.getDatabaseProductName().toLowerCase();

            boolean isOracle = dbProductName.contains("oracle");
            boolean isMySQL = dbProductName.contains("mysql");

            // 构建 WHERE 子句
            StringBuilder whereClause = new StringBuilder();
            if (conditions != null && !conditions.trim().isEmpty()) {
                whereClause.append(conditions).append(" AND ");
            }
            whereClause.append(queryCondition);

            // 根据数据库类型添加限制行数的语句
            if (isOracle) {
                sql.append(" WHERE ").append(whereClause).append(" AND ROWNUM <= 1");
            } else if (isMySQL) {
                sql.append(" WHERE ").append(whereClause).append(" LIMIT 1");
            } else {
                // 对于其他数据库，可以根据需要添加支持
                throw new SQLException("Unsupported database type: " + dbProductName);
            }

            String finalSql = sql.toString();
            try (PreparedStatement pstmt = conn.prepareStatement(finalSql);
                 ResultSet rs = pstmt.executeQuery()) {

                if (rs.next()) {
                    Map<String, Object> rowMap = new HashMap<>();
                    ResultSetMetaData rsMeta = rs.getMetaData();
                    int columnCount = rsMeta.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = rsMeta.getColumnLabel(i);
                        Object value = rs.getObject(i);
                        // 将值转换为字符串，如果值为 null，则存储字符串 "null"
                        String strValue = (value != null) ? value.toString() : "null";
                        rowMap.put(columnName, strValue);
                    }
                    return rowMap;
                }
            }
        }

        return null;
    }

    /**
     * 根据唯一键构建唯一标识
     * 支持大小写
     */
    private String buildKey(Map<String, Object> row, String uniqueKeys) {
        if (uniqueKeys == null || uniqueKeys.trim().isEmpty()) {
            return "";
        }

        String[] keys = uniqueKeys.split(",");
        StringBuilder keyBuilder = new StringBuilder();

        for (String key : keys) {
            // 去除首尾空格并转换为大写
            String upperKey = key.trim().toUpperCase();
            Object value = row.get(upperKey);
            keyBuilder.append(value != null ? value.toString() : "null").append("_");
        }

        // 移除最后一个下划线
        if (keyBuilder.length() > 0) {
            keyBuilder.setLength(keyBuilder.length() - 1);
        }

        return keyBuilder.toString();
    }
}