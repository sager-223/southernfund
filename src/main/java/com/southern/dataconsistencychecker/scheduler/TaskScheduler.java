package com.southern.dataconsistencychecker.scheduler;

import com.southern.dataconsistencychecker.entity.CompareConfig;
import com.southern.dataconsistencychecker.factory.ConsistencyCheckStrategyFactory;
import com.southern.dataconsistencychecker.strategy.ConsistencyCheckStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

@Component
public class TaskScheduler {

    @Autowired
    private ThreadPoolTaskScheduler threadPoolTaskScheduler;

    @Autowired
    private ConsistencyCheckStrategyFactory strategyFactory;

    private ConcurrentHashMap<Long, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        threadPoolTaskScheduler.initialize();
    }

    public void scheduleTask(CompareConfig config) {
        String strategyType = "database"; // 根据config决定使用哪种策略，这里假设使用内存策略
        ConsistencyCheckStrategy strategy = strategyFactory.getStrategy(strategyType);

        // 使用 CronTrigger 处理 CRON 表达式
        CronTrigger cronTrigger = new CronTrigger(config.getCronExpression());

        ScheduledFuture<?> future = threadPoolTaskScheduler.schedule(() -> {
            strategy.execute(config);
        }, cronTrigger);

        scheduledTasks.put(config.getId(), future);
    }

    public void cancelTask(Long id) {
        ScheduledFuture<?> future = scheduledTasks.get(id);
        if (future != null) {
            future.cancel(true);
            scheduledTasks.remove(id);
        }
    }

    // 移除或重构 parseCronExpression 方法
}