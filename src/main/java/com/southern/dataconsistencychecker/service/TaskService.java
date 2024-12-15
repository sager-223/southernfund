package com.southern.dataconsistencychecker.service;

import com.southern.dataconsistencychecker.entity.CompareConfig;

import java.util.List;

public interface TaskService {
    Long createTask(CompareConfig config);
    List<CompareConfig> getAllTasks();
    void startTask(Long id);
    void stopTask(Long id);
}
