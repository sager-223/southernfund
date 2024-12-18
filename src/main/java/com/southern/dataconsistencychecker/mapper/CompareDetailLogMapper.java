package com.southern.dataconsistencychecker.mapper;

import com.southern.dataconsistencychecker.entity.CompareDetailLog;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface CompareDetailLogMapper {

    /**
     * 批量插入 CompareDetailLog 记录
     *
     * @param detailLogs 要插入的 CompareDetailLog 列表
     */
    void batchInsert(@Param("detailLogs") List<CompareDetailLog> detailLogs);
}
