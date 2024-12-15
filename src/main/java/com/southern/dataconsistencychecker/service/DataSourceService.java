package com.southern.dataconsistencychecker.service;

import com.southern.dataconsistencychecker.entity.DataSourceConfig;
import com.southern.dataconsistencychecker.mapper.DataSourceConfigMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DataSourceService {

    @Autowired
    private DataSourceConfigMapper dataSourceConfigMapper;

    public void createDataSource(DataSourceConfig config) {
        dataSourceConfigMapper.insert(config);
    }

    public List<DataSourceConfig> getAllDataSources() {
        return dataSourceConfigMapper.findAllNonSensitive();
    }

    public DataSourceConfig getDataSourceById(Long id) {
        return dataSourceConfigMapper.findById(id);
    }
}
