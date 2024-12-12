package com.southern.dataconsistencychecker.mapper;

import com.southern.dataconsistencychecker.entity.CompareTaskProgress;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface CompareTaskProgressMapper {
    /**
     * 根据 taskId 和 partitionId 获取任务进度
     *
     * @param taskId      任务 ID
     * @param partitionId 分区 ID
     * @return CompareTaskProgress 实体
     */
    CompareTaskProgress getProgress(@Param("taskId") Long taskId, @Param("partitionId") Integer partitionId);

    /**
     * 插入新的任务进度记录
     *
     * @param progress CompareTaskProgress 实体
     */
    void insertProgress(CompareTaskProgress progress);

    /**
     * 更新任务进度记录
     *
     * @param progress CompareTaskProgress 实体
     */
    void updateProgress(CompareTaskProgress progress);
}
