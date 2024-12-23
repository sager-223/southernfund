package com.southern.dataconsistencychecker.service;

import com.southern.dataconsistencychecker.pojo.dto.UpdateCompareConfigRequest;
import com.southern.dataconsistencychecker.pojo.entity.CompareConfig;
import com.southern.dataconsistencychecker.pojo.vo.CompareConfigVO;

import java.util.List;

public interface TaskService {
    Long createTask(CompareConfig config);
    List<CompareConfigVO> getAllTasks();
    void startTask(Long id);
    void stopTask(Long id);
    /**
     * 根据任务ID查询任务状态
     * @param id 任务ID
     * @return 如果任务正在运行，返回true；否则返回false
     */
    boolean isTaskRunning(Long id);

    void deleteCompareConfig(Long id);
    void updateCompareConfig(UpdateCompareConfigRequest config);
}
