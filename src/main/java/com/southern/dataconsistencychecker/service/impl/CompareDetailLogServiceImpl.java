package com.southern.dataconsistencychecker.service.impl;

import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.southern.dataconsistencychecker.common.exception.BusinessException;
import com.southern.dataconsistencychecker.mapper.CompareDetailLogMapper;
import com.southern.dataconsistencychecker.pojo.dto.CompareDetailLogFilterDTO;
import com.southern.dataconsistencychecker.pojo.entity.CompareDetailLog;
import com.southern.dataconsistencychecker.pojo.vo.CompareDetailLogVO;
import com.southern.dataconsistencychecker.service.CompareDetailLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.time.format.DateTimeFormatter;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class CompareDetailLogServiceImpl implements CompareDetailLogService {

    @Autowired
    private CompareDetailLogMapper compareDetailLogMapper;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public List<CompareDetailLogVO> getAllCompareDetailLogs() {
        return compareDetailLogMapper.selectAllCompareDetailLogs();
    }

    @Override
    public List<CompareDetailLogVO> getCompareDetailLogsByType(Integer type) {
        return compareDetailLogMapper.selectCompareDetailLogsByType(type);
    }

//    @Override
//    public PageInfo<CompareDetailLogVO> getFilteredCompareDetailLogs(CompareDetailLogFilterDTO filterDTO) {
//        // 开启分页
//        PageHelper.startPage(filterDTO.getPage(), filterDTO.getSize());
//
//        List<CompareDetailLogVO> logs = compareDetailLogMapper.selectFilteredCompareDetailLogVO(filterDTO);
//
//        return new PageInfo<>(logs);
//    }


    @Override
    public PageInfo<CompareDetailLogVO> getFilteredCompareDetailLogs(CompareDetailLogFilterDTO filterDTO) {
        // 转换字符串为 LocalDateTime
        if (filterDTO.getCreateTimeStart() != null && filterDTO.getCreateTimeEnd() != null) {
            try {
                LocalDateTime start = LocalDateTime.parse(filterDTO.getCreateTimeStart(), FORMATTER);
                LocalDateTime end = LocalDateTime.parse(filterDTO.getCreateTimeEnd(), FORMATTER);
                // 使用转换后的日期
                filterDTO.setCreateTimeStart(start.toString());
                filterDTO.setCreateTimeEnd(end.toString());
            } catch (Exception e) {
                throw new RuntimeException("日期格式有误，请使用 'yyyy-MM-dd HH:mm:ss' 格式");
            }
        }

        // 开启分页，PageHelper 会自动拦截下方的查询并进行分页
        PageHelper.startPage(filterDTO.getPage(), filterDTO.getSize());

        List<CompareDetailLogVO> logs = compareDetailLogMapper.selectFilteredCompareDetailLogVO(filterDTO);

        return new PageInfo<>(logs);
    }

    @Override
    public String generateRepairSql(Long id, Integer repairType) {
        CompareDetailLog log = compareDetailLogMapper.selectById(id);
        if (log == null) {
            throw new BusinessException("Record not found for id: " + id);
        }

        String repairField;
        if (repairType == 1) {
            repairField = log.getRepairSource();
        } else if (repairType == 2) {
            repairField = log.getRepairTarget();
        } else {
            throw new BusinessException("Invalid repairType: " + repairType);
        }

        if (repairField == null || repairField.isEmpty()) {
            throw new BusinessException("No repair SQL available for the given type");
        }

        return repairField;
    }






}
