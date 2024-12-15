package com.southern.dataconsistencychecker.manager;

import com.southern.dataconsistencychecker.factory.DataSourceFactory;
import com.southern.dataconsistencychecker.strategy.DataSourceStrategy;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DataSourceManager {

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

    public DataSource getDataSourceById(Long id) {
        return dataSourceMap.get(id);
    }
}