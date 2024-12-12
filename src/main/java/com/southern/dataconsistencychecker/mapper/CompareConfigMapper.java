package com.southern.dataconsistencychecker.mapper;

import com.southern.dataconsistencychecker.entity.CompareConfig;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface CompareConfigMapper {
    @Select("SELECT * FROM compare_config")
    List<CompareConfig> getAllCompareConfigs();

    @Select("SELECT * FROM compare_config WHERE id = #{id}")
    CompareConfig getCompareConfigById(Long id);
}
