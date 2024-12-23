package com.southern.dataconsistencychecker.service;

import com.southern.dataconsistencychecker.common.exception.BusinessException;
import com.southern.dataconsistencychecker.pojo.dto.UpdateDataSourceRequest;
import com.southern.dataconsistencychecker.pojo.entity.DataSourceConfig;
import com.southern.dataconsistencychecker.manager.DataSourceManager;
import com.southern.dataconsistencychecker.mapper.DataSourceConfigMapper;
import com.southern.dataconsistencychecker.pojo.vo.DataSourceConfigVO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class DataSourceService {

    @Autowired
    private DataSourceConfigMapper dataSourceConfigMapper;

    @Autowired
    private DataSourceManager dataSourceManager;


    public void createDataSource(DataSourceConfig config) {
        dataSourceConfigMapper.insert(config);
    }

    public List<DataSourceConfigVO> getAllDataSources() {
        List<DataSourceConfig> dataSourceConfigs = dataSourceConfigMapper.findAllNonSensitive();

        return dataSourceConfigs.stream().map(config -> DataSourceConfigVO.builder()
                .id(config.getId())
                .name(config.getName())
                .type(config.getType())
                .host(config.getHost())
                .port(config.getPort())
                .databaseName(config.getDatabaseName())
                .username(config.getUsername())
                .additionalParams(config.getAdditionalParams())
                .connectionType(config.getConnectionType())
                .isConnected(isConnected(config.getId()))
                .build()
        ).collect(Collectors.toList());
    }

    public DataSourceConfig getDataSourceById(Long id) {
        return dataSourceConfigMapper.findById(id);
    }

    public boolean isConnected(Long id) {
        DataSource dataSource = dataSourceManager.getDataSourceById(id);
        return dataSource != null;
    }

    /**
     * 2. 关闭指定数据源连接
     *
     * @param id 数据源的唯一标识
     */
    public void closeConnection(Long id) {
        dataSourceManager.closeDataSourceById(id);
    }

    /**
     * 3. 关闭所有数据源连接
     */
    public void closeAllConnections() {
        dataSourceManager.closeAllDataSources();
    }

    /**
     * 4. 删除指定数据源
     * TODO 注意先断开连接
     *
     * @param id 数据源的唯一标识
     */
    public void deleteDataSource(Long id) {
        // 关闭连接（如果存在）
        dataSourceManager.closeDataSourceById(id);
        // 从数据库中删除配置
        dataSourceConfigMapper.deleteById(id);
    }

    /**
     * 5. 更新数据源配置
     *
     * @param config 数据源配置
     */
    public void updateDataSource(UpdateDataSourceRequest config) {
        if (config.getId() == null) {
            throw new BusinessException("DataSource ID不能为空");
        }

        DataSourceConfig existingConfig = dataSourceConfigMapper.findById(config.getId());
        if (existingConfig == null) {
            throw new BusinessException("DataSource not found");
        }

        // 更新非空字段
        if (config.getName() != null) {
            existingConfig.setName(config.getName());
        }
        if (config.getType() != null) {
            existingConfig.setType(config.getType());
        }
        if (config.getHost() != null) {
            existingConfig.setHost(config.getHost());
        }
        if (config.getPort() != null) {
            existingConfig.setPort(config.getPort());
        }
        if (config.getDatabaseName() != null) {
            existingConfig.setDatabaseName(config.getDatabaseName());
        }
        if (config.getUsername() != null) {
            existingConfig.setUsername(config.getUsername());
        }
        if (config.getPassword() != null) {
            existingConfig.setPassword(config.getPassword());
        }
        if (config.getAdditionalParams() != null) {
            existingConfig.setAdditionalParams(config.getAdditionalParams());
        }
        if (config.getConnectionType() != null) {
            existingConfig.setConnectionType(config.getConnectionType());
        }
        existingConfig.setUpdateTime(LocalDateTime.now());

        // 更新数据库
        dataSourceConfigMapper.update(existingConfig);

        // TODO 如果数据源已连接，重新加载数据源
        if (isConnected(config.getId())) {
            // 关闭现有连接
            dataSourceManager.closeDataSourceById(config.getId());
            // 重新添加数据源
            try {
                dataSourceManager.addDataSource(
                        existingConfig.getId(),
                        existingConfig.getType(),
                        existingConfig.getHost(),
                        existingConfig.getPort(),
                        existingConfig.getDatabaseName(),
                        existingConfig.getUsername(),
                        existingConfig.getPassword(),
                        existingConfig.getConnectionType()
                );
            } catch (Exception e) {
                throw new BusinessException("更新数据源连接失败");
            }
        }
    }
}
