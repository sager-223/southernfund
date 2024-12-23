package com.southern.dataconsistencychecker.controller;

import com.southern.dataconsistencychecker.common.exception.BusinessException;
import com.southern.dataconsistencychecker.common.result.Result;
import com.southern.dataconsistencychecker.pojo.dto.ConnectDataSourceRequest;
import com.southern.dataconsistencychecker.pojo.dto.UpdateDataSourceRequest;
import com.southern.dataconsistencychecker.pojo.entity.DataSourceConfig;
import com.southern.dataconsistencychecker.manager.DataSourceManager;
import com.southern.dataconsistencychecker.pojo.vo.DataSourceConfigVO;
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
    public Result<List<DataSourceConfigVO>> listDataSources() {
        List<DataSourceConfigVO> list = service.getAllDataSources();
        return Result.success(list);
    }

    // 3. 连接数据源
    @PostMapping("/connect")
    public Result connectDataSource(@RequestBody ConnectDataSourceRequest request) {

        Long id = request.getId();
        String password = request.getPassword();
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

    // 查询数据源连接状态
    @GetMapping("/isConnected/{id}")
    public Result<Boolean> isConnected(@PathVariable Long id) {
        boolean connected = service.isConnected(id);
        return Result.success(connected);
    }

    // 关闭指定数据源连接
    @PostMapping("/close/{id}")
    public Result closeConnection(@PathVariable Long id) {
        service.closeConnection(id);
        return Result.success();
    }

    // 关闭所有数据源连接
    @PostMapping("/closeAll")
    public Result closeAllConnections() {
        service.closeAllConnections();
        return Result.success();
    }

    // 删除指定数据源
    @DeleteMapping("/delete/{id}")
    public Result deleteDataSource(@PathVariable Long id) {
        service.deleteDataSource(id);
        return Result.success();
    }

    // 更新数据源配置
    @PutMapping("/update")
    public Result updateDataSource(@RequestBody UpdateDataSourceRequest config) {
        service.updateDataSource(config);
        return Result.success();
    }
}
