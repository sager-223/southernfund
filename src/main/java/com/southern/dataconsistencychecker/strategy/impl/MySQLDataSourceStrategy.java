package com.southern.dataconsistencychecker.strategy.impl;

import com.southern.dataconsistencychecker.strategy.DataSourceStrategy;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.stereotype.Component;


import javax.sql.DataSource;

@Component("mysql")
public class MySQLDataSourceStrategy implements DataSourceStrategy {

    @Override
    public DataSource createDataSource(String host, Integer port, String databaseName,
                                       String username, String password, String connectionType) {
        HikariConfig config = new HikariConfig();

        // 设置驱动类名
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");

        // 构建JDBC URL
        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC",
                host, port, databaseName);
        config.setJdbcUrl(url);

        // 设置数据库用户名和密码
        config.setUsername(username);
        config.setPassword(password);

        // 可选：设置连接池的其他参数
        config.setMaximumPoolSize(10); // 最大连接数
        config.setMinimumIdle(2);       // 最小空闲连接数
        config.setPoolName("MySQLHikariCP");

        // 创建 HikariDataSource 实例
        HikariDataSource dataSource = new HikariDataSource(config);

        return dataSource;
    }
}
