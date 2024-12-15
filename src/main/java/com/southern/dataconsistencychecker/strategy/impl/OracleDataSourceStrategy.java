package com.southern.dataconsistencychecker.strategy.impl;

import com.southern.dataconsistencychecker.strategy.DataSourceStrategy;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.stereotype.Component;


import javax.sql.DataSource;


@Component("oracle")
public class OracleDataSourceStrategy implements DataSourceStrategy {

    @Override
    public DataSource createDataSource(String host, Integer port, String databaseName,
                                       String username, String password, String connectionType) {
        HikariConfig config = new HikariConfig();

        // 设置驱动类名
        config.setDriverClassName("oracle.jdbc.driver.OracleDriver");

        // 根据连接类型构建JDBC URL
        String url;
        if ("SID".equalsIgnoreCase(connectionType)) {
            url = String.format("jdbc:oracle:thin:@%s:%d:%s", host, port, databaseName);
        } else { // ServiceName
            url = String.format("jdbc:oracle:thin:@//%s:%d/%s", host, port, databaseName);
        }
        config.setJdbcUrl(url);

        // 设置数据库用户名和密码
        config.setUsername(username);
        config.setPassword(password);

        // 可选：设置连接池的其他参数
        config.setMaximumPoolSize(10); // 最大连接数
        config.setMinimumIdle(2);       // 最小空闲连接数
        config.setPoolName("OracleHikariCP");

        // 创建 HikariDataSource 实例
        HikariDataSource dataSource = new HikariDataSource(config);

        return dataSource;
    }
}
