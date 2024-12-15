package com.southern.dataconsistencychecker.strategy;

import javax.sql.DataSource;

public interface DataSourceStrategy {
    DataSource createDataSource(String host, Integer port, String databaseName, String username, String password, String connectionType);
}
