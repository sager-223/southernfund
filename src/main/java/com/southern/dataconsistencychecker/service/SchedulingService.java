package com.southern.dataconsistencychecker.service;

import com.southern.dataconsistencychecker.entity.CompareConfig;
import com.southern.dataconsistencychecker.mapper.CompareConfigMapper;

import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.HashMap;
import java.util.Map;

@Service
public class SchedulingService {

    private final CompareConfigMapper compareConfigMapper;
    private final CompareService compareService;
    private final TaskScheduler taskScheduler;
    private final Map<Long, ScheduledFuture<?>> scheduledTasks = new HashMap<>();

    public SchedulingService(CompareConfigMapper compareConfigMapper, CompareService compareService) {
        this.compareConfigMapper = compareConfigMapper;
        this.compareService = compareService;
        this.taskScheduler = new ConcurrentTaskScheduler();
    }

    @PostConstruct
    public void scheduleAll() {
        List<CompareConfig> configs = compareConfigMapper.getAllCompareConfigs();
        for (CompareConfig config : configs) {
            scheduleTask(config);
        }
    }

    private void scheduleTask(CompareConfig config) {
        Runnable task = () -> compareService.executeCompare(config);
        ScheduledFuture<?> future = taskScheduler.schedule(task, new CronTrigger(config.getCronExpression()));
        scheduledTasks.put(config.getId(), future);
    }
}
