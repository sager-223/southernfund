package com.southern.dataconsistencychecker.mapper;

import com.southern.dataconsistencychecker.pojo.dto.CompareDetailLogFilterDTO;
import com.southern.dataconsistencychecker.pojo.entity.CompareDetailLog;
import com.southern.dataconsistencychecker.pojo.vo.CompareDetailLogVO;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface CompareDetailLogMapper {

//    @Insert("INSERT INTO COMPARE_DETAIL_LOG " +
//            "(COMPARE_TASK_ID, SOURCE_DATA_SOURCE_ID, TARGET_DATA_SOURCE_ID, SOURCE_TABLE, TARGET_TABLE, TYPE, " +
//            "SOURCE_UNIQUE_KEYS, TARGET_UNIQUE_KEYS, SOURCE_FIELD_KEY, SOURCE_FIELD_VALUE, TARGET_FIELD_KEY, TARGET_FIELD_VALUE, " +
//            "CREATE_TIME, UPDATE_TIME) " +
//            "VALUES " +
//            "(#{compareTaskId}, #{sourceDataSourceId}, #{targetDataSourceId}, #{sourceTable}, #{targetTable}, #{type}, " +
//            "#{sourceUniqueKeys}, #{targetUniqueKeys}, #{sourceFieldKey}, #{sourceFieldValue}, #{targetFieldKey}, #{targetFieldValue}, " +
//            "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
//    void insertCompareDetailLog(CompareDetailLog compareDetailLog);

    @Insert("INSERT INTO COMPARE_DETAIL_LOG " +
            "(COMPARE_TASK_ID, SOURCE_DATA_SOURCE_ID, TARGET_DATA_SOURCE_ID, SOURCE_TABLE, TARGET_TABLE, TYPE, " +
            "SOURCE_UNIQUE_KEYS, TARGET_UNIQUE_KEYS, SOURCE_FIELD_KEY, SOURCE_FIELD_VALUE, TARGET_FIELD_KEY, TARGET_FIELD_VALUE, " +
            "REPAIR_SOURCE, REPAIR_TARGET, CREATE_TIME, UPDATE_TIME) " +
            "VALUES " +
            "(#{compareTaskId}, #{sourceDataSourceId}, #{targetDataSourceId}, #{sourceTable}, #{targetTable}, #{type}, " +
            "#{sourceUniqueKeys}, #{targetUniqueKeys}, #{sourceFieldKey}, #{sourceFieldValue}, #{targetFieldKey}, #{targetFieldValue}, " +
            "#{repairSource}, #{repairTarget}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
    void insertCompareDetailLog(CompareDetailLog compareDetailLog);

    @Update("UPDATE COMPARE_DETAIL_LOG SET REPAIR_SOURCE = #{repairSource}, REPAIR_TARGET = #{repairTarget} WHERE ID = #{id}")
    void updateRepairSQL(CompareDetailLog compareDetailLog);


    /**
     * 查询所有比对细节日志
     *
     * @return 日志列表
     */
    @Select("SELECT id, compare_task_id, source_data_source_id, target_data_source_id, " +
            "source_table, target_table, type, source_unique_keys, target_unique_keys, " +
            "source_field_key, source_field_value, target_field_key, target_field_value, " +
            "create_time " +
            "FROM compare_detail_log")
    List<CompareDetailLogVO> selectAllCompareDetailLogs();

    /**
     * 根据类型查询比对细节日志
     *
     * @param type 不一致类型
     * @return 日志列表
     */
    @Select("SELECT id, compare_task_id, source_data_source_id, target_data_source_id, " +
            "source_table, target_table, type, source_unique_keys, target_unique_keys, " +
            "source_field_key, source_field_value, target_field_key, target_field_value, " +
            "create_time " +
            "FROM compare_detail_log WHERE type = #{type}")
    List<CompareDetailLogVO> selectCompareDetailLogsByType(Integer type);

    /**
     * 根据筛选条件和分页查询比对细节日志
     */
    List<CompareDetailLogVO> selectFilteredCompareDetailLogVO(@Param("filter") CompareDetailLogFilterDTO filterDTO);



    /**
     * 根据 ID 查询 CompareDetailLog 记录
     *
     * @param id 记录的 ID
     * @return 对应的 CompareDetailLog 对象
     */
    CompareDetailLog selectById(@Param("id") Long id);

}