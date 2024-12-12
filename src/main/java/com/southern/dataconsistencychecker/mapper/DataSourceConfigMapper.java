package com.southern.dataconsistencychecker.mapper;

import com.southern.dataconsistencychecker.entity.DataSourceConfigEntity;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface DataSourceConfigMapper {
    @Select("SELECT * FROM data_source_config")
    List<DataSourceConfigEntity> getAllDataSourceConfigs();

    @Select("SELECT * FROM data_source_config WHERE id = #{id}")
    DataSourceConfigEntity getDataSourceConfigById(Long id);
}
