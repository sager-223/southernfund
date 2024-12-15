package com.southern.dataconsistencychecker.mapper;

import com.southern.dataconsistencychecker.entity.DataSourceConfig;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface DataSourceConfigMapper {

    @Insert("INSERT INTO DATA_SOURCE_CONFIG (NAME, TYPE, HOST, PORT, DATABASE_NAME, USERNAME, PASSWORD, ADDITIONAL_PARAMS, CONNECTION_TYPE) " +
            "VALUES (#{name}, #{type}, #{host}, #{port}, #{databaseName}, #{username}, #{password}, #{additionalParams}, #{connectionType})")
    //@Options(useGeneratedKeys = true, keyProperty = "id")
    void insert(DataSourceConfig dataSourceConfig);

    @Select("SELECT ID, NAME, TYPE, HOST, PORT, DATABASE_NAME, USERNAME, ADDITIONAL_PARAMS, CONNECTION_TYPE, CREATE_TIME, UPDATE_TIME " +
            "FROM DATA_SOURCE_CONFIG")
    List<DataSourceConfig> findAllNonSensitive();

    @Select("SELECT * FROM DATA_SOURCE_CONFIG WHERE ID = #{id}")
    DataSourceConfig findById(Long id);
}
