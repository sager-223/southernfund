package com.southern.dataconsistencychecker.controller;

import com.southern.dataconsistencychecker.common.exception.BusinessException;
import com.southern.dataconsistencychecker.common.result.Result;
import com.southern.dataconsistencychecker.entity.DataSourceConfig;
import com.southern.dataconsistencychecker.manager.DataSourceManager;
import com.southern.dataconsistencychecker.service.DataSourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/datasource")
public class DataSourceController {

    @Autowired
    private DataSourceService service;

    @Autowired
    private DataSourceManager manager;

    // 1. 创建数据源
    @PostMapping("/create")
    public Result createDataSource(@RequestBody DataSourceConfig config) {
        service.createDataSource(config);
        return Result.success();
    }

    // 2. 查看数据源信息
    @GetMapping("/list")
    public Result<List<DataSourceConfig>> listDataSources() {
        List<DataSourceConfig> dataSourceConfiglist = service.getAllDataSources();
        return Result.success(dataSourceConfiglist);
    }

    // 3. 连接数据源
    @PostMapping("/connect")
    public Result connectDataSource(@RequestParam Long id, @RequestParam String password) {
        DataSourceConfig config = service.getDataSourceById(id);
        if (config == null) {
            throw new BusinessException("DataSource not found");
        }

        // 使用用户输入的密码覆盖存储的密码
        config.setPassword(password);

        try {
            manager.addDataSource(config.getId(), config.getType(), config.getHost(), config.getPort(),
                    config.getDatabaseName(), config.getUsername(), config.getPassword(), config.getConnectionType());
            return Result.success();
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("Failed to connect DataSource");
        }

    }
}
