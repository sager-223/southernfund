package com.southern.dataconsistencychecker.service;

import com.github.pagehelper.PageInfo;
import com.southern.dataconsistencychecker.pojo.dto.CompareResultQueryDTO;
import com.southern.dataconsistencychecker.pojo.vo.CompareResultVO;

import java.util.List;

public interface CompareResultService {
    /**
     * 查询所有比对结果
     */
    List<CompareResultVO> getAllCompareResults();

    /**
     * 根据筛选条件查询比对结果
     */
    List<CompareResultVO> getCompareResultsByFilters(CompareResultQueryDTO queryDTO);


    /**
     * 根据 ID 和 repairType 生成修复 SQL
     *
     * @param id          记录的 ID
     * @param repairType  修复类型（1 - repairSource，2 - repairTarget）
     * @return 生成的修复 SQL
     */
    String generateRepairSql(Long id, Integer repairType);

}
