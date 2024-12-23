package com.southern.dataconsistencychecker.controller;

import com.southern.dataconsistencychecker.common.result.Result;
import com.southern.dataconsistencychecker.pojo.dto.UpdateCompareConfigRequest;
import com.southern.dataconsistencychecker.pojo.entity.CompareConfig;
import com.southern.dataconsistencychecker.pojo.vo.CompareConfigVO;
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
    public Result<List<CompareConfigVO>> getAllTasks() {
        List<CompareConfigVO> list = taskService.getAllTasks();
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

    /**
     * 查询任务状态
     */
    @GetMapping("/status/{id}")
    public Result<Boolean> getTaskStatus(@PathVariable Long id) {
        boolean isRunning = taskService.isTaskRunning(id);
        return Result.success(isRunning);
    }

    // 删除 CompareConfig
    @DeleteMapping("/delete/{id}")
    public Result deleteCompareConfig(@PathVariable Long id) {
        taskService.deleteCompareConfig(id);
        return Result.success();
    }

    // 更新 CompareConfig
    @PutMapping("/update")
    public Result updateCompareConfig(@RequestBody UpdateCompareConfigRequest config) {
        taskService.updateCompareConfig(config);
        return Result.success();
    }
}
