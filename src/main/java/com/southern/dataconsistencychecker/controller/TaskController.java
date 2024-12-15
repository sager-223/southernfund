package com.southern.dataconsistencychecker.controller;

import com.southern.dataconsistencychecker.common.result.Result;
import com.southern.dataconsistencychecker.entity.CompareConfig;
import com.southern.dataconsistencychecker.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    @Autowired
    private TaskService taskService;

    /**
     * 创建任务
     */
    @PostMapping("/create")
    public Result createTask(@RequestBody CompareConfig config) {
        taskService.createTask(config);
        return Result.success();
    }

    /**
     * 查看全部任务
     */
    @GetMapping("/all")
    public Result<List<CompareConfig>> getAllTasks() {
        List<CompareConfig> list = taskService.getAllTasks();
        return Result.success(list);
    }

    /**
     * 开启任务
     */
    @PostMapping("/start/{id}")
    public Result startTask(@PathVariable Long id) {
        taskService.startTask(id);
        return Result.success();
    }

    /**
     * 关闭任务
     */
    @PostMapping("/stop/{id}")
    public Result stopTask(@PathVariable Long id) {
        taskService.stopTask(id);
        return Result.success();
    }
}
