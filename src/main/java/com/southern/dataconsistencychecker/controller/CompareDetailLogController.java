package com.southern.dataconsistencychecker.controller;

import com.github.pagehelper.IPage;
import com.github.pagehelper.PageInfo;
import com.southern.dataconsistencychecker.common.exception.BusinessException;
import com.southern.dataconsistencychecker.common.result.Result;
import com.southern.dataconsistencychecker.pojo.dto.CompareDetailLogFilterDTO;
import com.southern.dataconsistencychecker.pojo.vo.CompareDetailLogVO;
import com.southern.dataconsistencychecker.service.CompareDetailLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/compare-detail-log")
public class CompareDetailLogController {

    @Autowired
    private CompareDetailLogService compareDetailLogService;

    /**
     * 1. 查询所有比对细节日志
     */
    @GetMapping("/list")
    public Result<List<CompareDetailLogVO>> listAllCompareDetailLogs() {
        List<CompareDetailLogVO> list = compareDetailLogService.getAllCompareDetailLogs();
        return Result.success(list);
    }

    /**
     * 2. 根据type查询比对细节日志
     */
    @GetMapping("/listByType/{type}")
    public Result<List<CompareDetailLogVO>> listCompareDetailLogsByType(@PathVariable Integer type) {
        if (type == null || type < 1 || type > 3) {
            throw new BusinessException("Invalid type parameter");
        }
        List<CompareDetailLogVO> list = compareDetailLogService.getCompareDetailLogsByType(type);
        return Result.success(list);
    }

    /**
     * 3. 根据筛选条件和分页查询比对细节日志
     */
    @GetMapping("/listFiltered")
    public Result<PageInfo<CompareDetailLogVO>> listFilteredCompareDetailLogs(CompareDetailLogFilterDTO filterDTO) {
        if (filterDTO.getType() != null && (filterDTO.getType() < 1 || filterDTO.getType() > 3)) {
            throw new BusinessException("Invalid type parameter");
        }
        PageInfo<CompareDetailLogVO> pageInfo = compareDetailLogService.getFilteredCompareDetailLogs(filterDTO);
        return Result.success(pageInfo);
    }

    /**
     * 4. 生成修复 SQL
     */
    @GetMapping("/generateRepairSql")
    public Result<String> generateRepairSql(@RequestParam Long id, @RequestParam Integer repairType) {
        if (repairType == null || (repairType != 1 && repairType != 2)) {
            throw new BusinessException("Invalid repairType parameter");
        }
        String repairSql = compareDetailLogService.generateRepairSql(id, repairType);
        return Result.success(repairSql);
    }
}
