package com.southern.dataconsistencychecker.config;

import com.southern.dataconsistencychecker.entity.DataSourceConfigEntity;
import com.southern.dataconsistencychecker.mapper.DataSourceConfigMapper;
import com.southern.dataconsistencychecker.util.DBTypeEnum;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.stereotype.Component;

import java.util.*;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;

@Component
public class DynamicDataSource {

    private final Map<Long, DataSource> dataSourceMap = new HashMap<>();

    // In a real-world application, you might want to listen to changes in data_source_config table
    // and dynamically add/remove data sources. For simplicity, we initialize them at startup.

    private final DataSourceConfigMapper dataSourceConfigMapper;

    public DynamicDataSource(DataSourceConfigMapper dataSourceConfigMapper) {
        this.dataSourceConfigMapper = dataSourceConfigMapper;
    }

    @PostConstruct
    public void init() {
        List<DataSourceConfigEntity> configs = dataSourceConfigMapper.getAllDataSourceConfigs();
        for (DataSourceConfigEntity config : configs) {
            DataSource ds = createDataSource(config);
            dataSourceMap.put(config.getId(), ds);
        }
    }

    private DataSource createDataSource(DataSourceConfigEntity config) {
        HikariConfig hikariConfig = new HikariConfig();
        String jdbcUrl = "";
        if (DBTypeEnum.ORACLE.name().equalsIgnoreCase(config.getType())) {
            jdbcUrl = String.format("jdbc:oracle:thin:@//%s:%d/%s",
                    config.getHost(),
                    config.getPort(),
                    config.getDatabaseName());
            hikariConfig.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        } else if (DBTypeEnum.MYSQL.name().equalsIgnoreCase(config.getType())) {
            jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC",
                    config.getHost(),
                    config.getPort(),
                    config.getDatabaseName());
            hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
        } else {
            throw new IllegalArgumentException("Unsupported DB type: " + config.getType());
        }

        if (config.getAdditionalParams() != null && !config.getAdditionalParams().isEmpty()) {
            jdbcUrl += "&" + config.getAdditionalParams();
        }

        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(config.getUsername());
        hikariConfig.setPassword(config.getPassword());
        hikariConfig.setMaximumPoolSize(10);
        return new HikariDataSource(hikariConfig);
    }

    public DataSource getDataSourceById(Long id) {
        return dataSourceMap.get(id);
    }
}