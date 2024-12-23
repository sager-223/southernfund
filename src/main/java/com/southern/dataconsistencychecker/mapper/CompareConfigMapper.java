package com.southern.dataconsistencychecker.mapper;

import com.southern.dataconsistencychecker.pojo.entity.CompareConfig;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface CompareConfigMapper {

    @Insert("INSERT INTO COMPARE_CONFIG (SOURCE_DATA_SOURCE_ID, TARGET_DATA_SOURCE_ID, SOURCE_TABLE, TARGET_TABLE, SOURCE_CONDITIONS, TARGET_CONDITIONS, SOURCE_COMPARE_FIELDS, TARGET_COMPARE_FIELDS, SOURCE_UNIQUE_KEYS, TARGET_UNIQUE_KEYS, CRON_EXPRESSION, NOTIFICATION_EMAIL, NOTIFICATION_PHONE) " +
            "VALUES (#{sourceDataSourceId}, #{targetDataSourceId}, #{sourceTable}, #{targetTable}, #{sourceConditions}, #{targetConditions}, #{sourceCompareFields}, #{targetCompareFields}, #{sourceUniqueKeys}, #{targetUniqueKeys}, #{cronExpression}, #{notificationEmail}, #{notificationPhone})")
    //@Options(useGeneratedKeys = true, keyProperty = "id")
    void insertCompareConfig(CompareConfig config);

    @Select("SELECT * FROM COMPARE_CONFIG")
    List<CompareConfig> getAllCompareConfigs();

    @Select("SELECT * FROM COMPARE_CONFIG WHERE ID = #{id}")
    CompareConfig getCompareConfigById(@Param("id") Long id);

    @Update("UPDATE COMPARE_CONFIG SET CRON_EXPRESSION = #{cronExpression} WHERE ID = #{id}")
    void updateCronExpression(@Param("id") Long id, @Param("cronExpression") String cronExpression);

    /**
     * 根据 ID 删除 CompareConfig
     *
     * @param id CompareConfig 的 ID
     * @return 受影响的行数
     */
    int deleteCompareConfigById(@Param("id") Long id);

    /**
     * 更新 CompareConfig（部分更新）
     *
     * @param config 要更新的 CompareConfig
     * @return 受影响的行数
     */
    int updateCompareConfig(CompareConfig config);
}
