package com.southern.dataconsistencychecker.controller;

import com.github.pagehelper.PageInfo;
import com.southern.dataconsistencychecker.common.exception.BusinessException;
import com.southern.dataconsistencychecker.common.result.Result;
import com.southern.dataconsistencychecker.pojo.dto.CompareResultQueryDTO;
import com.southern.dataconsistencychecker.pojo.vo.CompareResultVO;
import com.southern.dataconsistencychecker.service.CompareResultService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/compare-result")
public class CompareResultController {

    @Autowired
    private CompareResultService compareResultService;

    /**
     * 1. 查询所有比对结果
     */
    @GetMapping("/list")
    public Result<List<CompareResultVO>> listAllCompareResults() {
        List<CompareResultVO> list = compareResultService.getAllCompareResults();
        return Result.success(list);
    }

    /**
     * 2. 根据条件联合筛选比对结果
     */
    @GetMapping("/filter")
    public Result<List<CompareResultVO>> filterCompareResults(
            @RequestParam(required = false) Long compareConfigId,
            @RequestParam(required = false) String compareStatus,
            @RequestParam(required = false) Boolean isConsistent) {

        CompareResultQueryDTO queryDTO = CompareResultQueryDTO.builder()
                .compareConfigId(compareConfigId)
                .compareStatus(compareStatus)
                .isConsistent(isConsistent)
                .build();

        List<CompareResultVO> filteredList = compareResultService.getCompareResultsByFilters(queryDTO);
        return Result.success(filteredList);
    }


    /**
     * 3. 生成修复 SQL
     */
    @GetMapping("/generateRepairSql")
    public Result<String> generateRepairSql(@RequestParam Long id, @RequestParam Integer repairType) {
        if (repairType == null || (repairType != 1 && repairType != 2)) {
            throw new BusinessException("Invalid repairType parameter");
        }
        String repairSql = compareResultService.generateRepairSql(id, repairType);
        return Result.success(repairSql);
    }




}
