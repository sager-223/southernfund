package com.southern.dataconsistencychecker.mapper;

import com.southern.dataconsistencychecker.entity.CompareDetailLog;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface CompareDetailLogMapper {

    @Insert("INSERT INTO COMPARE_DETAIL_LOG " +
            "(COMPARE_TASK_ID, SOURCE_DATA_SOURCE_ID, TARGET_DATA_SOURCE_ID, SOURCE_TABLE, TARGET_TABLE, TYPE, " +
            "SOURCE_UNIQUE_KEYS, TARGET_UNIQUE_KEYS, SOURCE_FIELD_KEY, SOURCE_FIELD_VALUE, TARGET_FIELD_KEY, TARGET_FIELD_VALUE, " +
            "CREATE_TIME, UPDATE_TIME) " +
            "VALUES " +
            "(#{compareTaskId}, #{sourceDataSourceId}, #{targetDataSourceId}, #{sourceTable}, #{targetTable}, #{type}, " +
            "#{sourceUniqueKeys}, #{targetUniqueKeys}, #{sourceFieldKey}, #{sourceFieldValue}, #{targetFieldKey}, #{targetFieldValue}, " +
            "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
    void insertCompareDetailLog(CompareDetailLog compareDetailLog);
}