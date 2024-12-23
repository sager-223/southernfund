package com.southern.dataconsistencychecker.service;

import com.github.pagehelper.IPage;
import com.github.pagehelper.PageInfo;
import com.southern.dataconsistencychecker.pojo.dto.CompareDetailLogFilterDTO;
import com.southern.dataconsistencychecker.pojo.vo.CompareDetailLogVO;

import java.util.List;

public interface CompareDetailLogService {

    /**
     * 获取所有比对细节日志
     *
     * @return 日志列表
     */
    List<CompareDetailLogVO> getAllCompareDetailLogs();

    /**
     * 根据类型获取比对细节日志
     *
     * @param type 不一致类型
     * @return 日志列表
     */
    List<CompareDetailLogVO> getCompareDetailLogsByType(Integer type);

    /**
     * 根据筛选条件和分页查询比对细节日志
     */
    PageInfo<CompareDetailLogVO> getFilteredCompareDetailLogs(CompareDetailLogFilterDTO filterDTO);

    /**
     * 根据 ID 和 repairType 生成修复 SQL
     *
     * @param id          记录的 ID
     * @param repairType  修复类型（1 - repairSource，2 - repairTarget）
     * @return 生成的修复 SQL
     */
    String generateRepairSql(Long id, Integer repairType);
}
