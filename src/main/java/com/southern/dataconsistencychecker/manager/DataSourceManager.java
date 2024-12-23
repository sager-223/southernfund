package com.southern.dataconsistencychecker.manager;

import com.southern.dataconsistencychecker.factory.DataSourceFactory;
import com.southern.dataconsistencychecker.strategy.DataSourceStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DataSourceManager {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceManager.class);

    private ConcurrentHashMap<Long, DataSource> dataSourceMap = new ConcurrentHashMap<>();
    private final DataSourceFactory dataSourceFactory;

    DataSourceManager(DataSourceFactory dataSourceFactory){
        this.dataSourceFactory = dataSourceFactory;
    }

    @PostConstruct
    public void init() {
        // 初始化，如果需要加载已有数据源
    }

    public void addDataSource(Long id, String type, String host, Integer port, String databaseName, String username, String password, String connectionType) {
        DataSourceStrategy strategy = dataSourceFactory.getStrategy(type);
        DataSource dataSource = strategy.createDataSource(host, port, databaseName, username, password, connectionType);
        dataSourceMap.put(id, dataSource);
    }


    /**
     * 根据ID获取数据源
     *
     * @param id 数据源的唯一标识
     * @return 对应的 DataSource 实例，若不存在则返回 null
     */
    public DataSource getDataSourceById(Long id) {
        return dataSourceMap.get(id);
    }

    /**
     * 根据ID关闭并移除数据源
     *
     * @param id 数据源的唯一标识
     */
    public void closeDataSourceById(Long id) {
        DataSource dataSource = dataSourceMap.remove(id);
        if (dataSource != null) {
            if (dataSource instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) dataSource).close();
                    logger.info("DataSource with id {} has been closed and removed.", id);
                } catch (Exception e) {
                    logger.error("Error closing DataSource with id {}", id, e);
                }
            } else {
                logger.warn("DataSource with id {} does not implement AutoCloseable and cannot be closed.", id);
            }
        } else {
            logger.warn("No DataSource found with id {} to close.", id);
        }
    }

    /**
     * 关闭所有数据源并清空管理器
     */
    public void closeAllDataSources() {
        dataSourceMap.forEach((id, dataSource) -> {
            closeDataSourceById(id);
        });
        logger.info("All DataSources have been closed and removed.");
    }


}