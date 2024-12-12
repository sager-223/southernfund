package com.southern.dataconsistencychecker.mapper;

import com.southern.dataconsistencychecker.entity.CompareResult;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface CompareResultMapper {
    /**
     * 插入比较结果
     *
     * @param result CompareResult 实体
     */
    void insertCompareResult(CompareResult result);

    /**
     * 更新比较结果
     *
     * @param result CompareResult 实体
     */
    void updateCompareResult(CompareResult result);
}
