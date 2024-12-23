package com.southern.dataconsistencychecker.service.impl;

import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.southern.dataconsistencychecker.common.exception.BusinessException;
import com.southern.dataconsistencychecker.mapper.CompareResultMapper;
import com.southern.dataconsistencychecker.pojo.dto.CompareResultQueryDTO;
import com.southern.dataconsistencychecker.pojo.entity.CompareResult;
import com.southern.dataconsistencychecker.pojo.vo.CompareResultVO;
import com.southern.dataconsistencychecker.service.CompareResultService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CompareResultServiceImpl implements CompareResultService {

    @Autowired
    private CompareResultMapper compareResultMapper;

    @Override
    public List<CompareResultVO> getAllCompareResults() {
        return compareResultMapper.findAllCompareResults();
    }

    @Override
    public List<CompareResultVO> getCompareResultsByFilters(CompareResultQueryDTO queryDTO) {
        return compareResultMapper.findCompareResultsByFilters(queryDTO);
    }

    @Override
    public String generateRepairSql(Long id, Integer repairType) {
        CompareResult result = compareResultMapper.selectById(id);
        if (result == null) {
            throw new BusinessException("Record not found for id: " + id);
        }

        String repairField;
        if (repairType == 1) {
            repairField = result.getRepairSource();
        } else if (repairType == 2) {
            repairField = result.getRepairTarget();
        } else {
            throw new BusinessException("Invalid repairType: " + repairType);
        }

        if (repairField == null || repairField.isEmpty()) {
            throw new BusinessException("No repair SQL available for the given type");
        }

        return repairField;
    }






}
