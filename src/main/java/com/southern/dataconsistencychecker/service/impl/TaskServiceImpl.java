package com.southern.dataconsistencychecker.service.impl;

import com.southern.dataconsistencychecker.common.exception.BusinessException;
import com.southern.dataconsistencychecker.pojo.dto.UpdateCompareConfigRequest;
import com.southern.dataconsistencychecker.pojo.entity.CompareConfig;
import com.southern.dataconsistencychecker.mapper.CompareConfigMapper;
import com.southern.dataconsistencychecker.pojo.vo.CompareConfigVO;
import com.southern.dataconsistencychecker.scheduler.TaskScheduler;
import com.southern.dataconsistencychecker.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class TaskServiceImpl implements TaskService {

    @Autowired
    private CompareConfigMapper compareConfigMapper;

    @Autowired
    private TaskScheduler taskScheduler;

    @Override
    public Long createTask(CompareConfig config) {
        compareConfigMapper.insertCompareConfig(config);
        return config.getId();
    }

    @Override
    public List<CompareConfigVO> getAllTasks() {
        List<CompareConfig> compareConfigs = compareConfigMapper.getAllCompareConfigs();

        List<CompareConfigVO> compareConfigVOList = compareConfigs.stream()
                .map(config -> CompareConfigVO.builder()
                        .id(config.getId())
                        .sourceDataSourceId(config.getSourceDataSourceId())
                        .targetDataSourceId(config.getTargetDataSourceId())
                        .sourceTable(config.getSourceTable())
                        .targetTable(config.getTargetTable())
                        .sourceConditions(config.getSourceConditions())
                        .targetConditions(config.getTargetConditions())
                        .sourceCompareFields(config.getSourceCompareFields())
                        .targetCompareFields(config.getTargetCompareFields())
                        .sourceUniqueKeys(config.getSourceUniqueKeys())
                        .targetUniqueKeys(config.getTargetUniqueKeys())
                        .cronExpression(config.getCronExpression())
                        .notificationEmail(config.getNotificationEmail())
                        .notificationPhone(config.getNotificationPhone())
                        .isRunning(isTaskRunning(config.getId()))
                        .build())
                .collect(Collectors.toList());

        return compareConfigVOList;
    }

    @Override
    public void startTask(Long id) {
        CompareConfig config = compareConfigMapper.getCompareConfigById(id);
        if (config != null) {
            taskScheduler.scheduleTask(config);
        }
    }

    @Override
    public void stopTask(Long id) {
        taskScheduler.cancelTask(id);
    }

    @Override
    public boolean isTaskRunning(Long id) {
        return taskScheduler.isTaskRunning(id);
    }

    @Override
    public void deleteCompareConfig(Long id) {
        // TODO 先停止任务（如果正在运行）
        if (isTaskRunning(id)) {
            stopTask(id);
        }
        // 删除配置
        int rows = compareConfigMapper.deleteCompareConfigById(id);
        if (rows == 0) {
            throw new BusinessException("CompareConfig with id " + id + " not found.");
        }
    }

    @Override
    public void updateCompareConfig(UpdateCompareConfigRequest configRequest) {
        if (configRequest.getId() == null) {
            throw new BusinessException("ID cannot be null for update.");
        }

        // 获取现有配置
        CompareConfig existingConfig = compareConfigMapper.getCompareConfigById(configRequest.getId());
        if (existingConfig == null) {
            throw new BusinessException("CompareConfig with id " + configRequest.getId() + " not found.");
        }

        // 更新字段（仅更新非 null 字段）
        boolean isUpdated = false;

        if (configRequest.getSourceDataSourceId() != null) {
            existingConfig.setSourceDataSourceId(configRequest.getSourceDataSourceId());
            isUpdated = true;
        }
        if (configRequest.getTargetDataSourceId() != null) {
            existingConfig.setTargetDataSourceId(configRequest.getTargetDataSourceId());
            isUpdated = true;
        }
        if (configRequest.getSourceTable() != null) {
            existingConfig.setSourceTable(configRequest.getSourceTable());
            isUpdated = true;
        }
        if (configRequest.getTargetTable() != null) {
            existingConfig.setTargetTable(configRequest.getTargetTable());
            isUpdated = true;
        }
        if (configRequest.getSourceConditions() != null) {
            existingConfig.setSourceConditions(configRequest.getSourceConditions());
            isUpdated = true;
        }
        if (configRequest.getTargetConditions() != null) {
            existingConfig.setTargetConditions(configRequest.getTargetConditions());
            isUpdated = true;
        }
        if (configRequest.getSourceCompareFields() != null) {
            existingConfig.setSourceCompareFields(configRequest.getSourceCompareFields());
            isUpdated = true;
        }
        if (configRequest.getTargetCompareFields() != null) {
            existingConfig.setTargetCompareFields(configRequest.getTargetCompareFields());
            isUpdated = true;
        }
        if (configRequest.getSourceUniqueKeys() != null) {
            existingConfig.setSourceUniqueKeys(configRequest.getSourceUniqueKeys());
            isUpdated = true;
        }
        if (configRequest.getTargetUniqueKeys() != null) {
            existingConfig.setTargetUniqueKeys(configRequest.getTargetUniqueKeys());
            isUpdated = true;
        }
        if (configRequest.getCronExpression() != null) {
            existingConfig.setCronExpression(configRequest.getCronExpression());
            isUpdated = true;
        }
        if (configRequest.getNotificationEmail() != null) {
            existingConfig.setNotificationEmail(configRequest.getNotificationEmail());
            isUpdated = true;
        }
        if (configRequest.getNotificationPhone() != null) {
            existingConfig.setNotificationPhone(configRequest.getNotificationPhone());
            isUpdated = true;
        }

        if (isUpdated) {
            existingConfig.setUpdateTime(LocalDateTime.now());
            int rows = compareConfigMapper.updateCompareConfig(existingConfig);
            if (rows == 0) {
                throw new BusinessException("Failed to update CompareConfig with id " + configRequest.getId());
            }
        }
    }


}
