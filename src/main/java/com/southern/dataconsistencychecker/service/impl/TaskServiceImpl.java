package com.southern.dataconsistencychecker.service.impl;

import com.southern.dataconsistencychecker.entity.CompareConfig;
import com.southern.dataconsistencychecker.mapper.CompareConfigMapper;
import com.southern.dataconsistencychecker.scheduler.TaskScheduler;
import com.southern.dataconsistencychecker.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Service;

import java.util.List;

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
    public List<CompareConfig> getAllTasks() {
        return compareConfigMapper.getAllCompareConfigs();
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
}
