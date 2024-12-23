package com.southern.dataconsistencychecker.mapper;

import com.southern.dataconsistencychecker.pojo.entity.DataSourceConfig;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface DataSourceConfigMapper {

    @Insert("INSERT INTO DATA_SOURCE_CONFIG (NAME, TYPE, HOST, PORT, DATABASE_NAME, USERNAME, PASSWORD, ADDITIONAL_PARAMS, CONNECTION_TYPE) " +
            "VALUES (#{name}, #{type}, #{host}, #{port}, #{databaseName}, #{username}, #{password}, #{additionalParams}, #{connectionType})")
    //@Options(useGeneratedKeys = true, keyProperty = "id")
    void insert(DataSourceConfig dataSourceConfig);

    @Select("SELECT ID, NAME, TYPE, HOST, PORT, CONNECTION_TYPE, DATABASE_NAME, USERNAME, ADDITIONAL_PARAMS, CONNECTION_TYPE, CREATE_TIME, UPDATE_TIME " +
            "FROM DATA_SOURCE_CONFIG")
    List<DataSourceConfig> findAllNonSensitive();

    @Select("SELECT * FROM DATA_SOURCE_CONFIG WHERE ID = #{id}")
    DataSourceConfig findById(Long id);

    /**
     * 4. 根据ID删除数据源配置
     *
     * @param id 数据源的唯一标识
     */
    @Delete("DELETE FROM DATA_SOURCE_CONFIG WHERE ID = #{id}")
    void deleteById(Long id);

    /**
     * 5. 更新数据源配置，仅更新非null字段
     *
     * @param dataSourceConfig 数据源配置
     */
    @Update("<script>" +
            "UPDATE DATA_SOURCE_CONFIG " +
            "<set>" +
            "<if test='name != null'>NAME = #{name},</if>" +
            "<if test='type != null'>TYPE = #{type},</if>" +
            "<if test='host != null'>HOST = #{host},</if>" +
            "<if test='port != null'>PORT = #{port},</if>" +
            "<if test='databaseName != null'>DATABASE_NAME = #{databaseName},</if>" +
            "<if test='username != null'>USERNAME = #{username},</if>" +
            "<if test='password != null'>PASSWORD = #{password},</if>" +
            "<if test='additionalParams != null'>ADDITIONAL_PARAMS = #{additionalParams},</if>" +
            "<if test='connectionType != null'>CONNECTION_TYPE = #{connectionType},</if>" +
            "UPDATE_TIME = CURRENT_TIMESTAMP " +
            "</set> " +
            "WHERE ID = #{id}" +
            "</script>")
    void update(DataSourceConfig dataSourceConfig);
}
