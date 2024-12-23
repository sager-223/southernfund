package com.southern.dataconsistencychecker.mapper;

import com.southern.dataconsistencychecker.pojo.dto.CompareResultQueryDTO;
import com.southern.dataconsistencychecker.pojo.entity.CompareResult;
import com.southern.dataconsistencychecker.pojo.vo.CompareResultVO;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface CompareResultMapper {

//    @Insert("INSERT INTO COMPARE_RESULT (COMPARE_CONFIG_ID, COMPARE_TASK_ID, SOURCE_DATA_DETAILS, TARGET_DATA_DETAILS, COMPARE_TIME, COMPARE_STATUS, DESCRIPTION, EMAIL_NOTIFICATION_STATUS, SMS_NOTIFICATION_STATUS, IS_CONSISTENT) " +
//            "VALUES (#{compareConfigId}, #{compareTaskId}, #{sourceDataDetails}, #{targetDataDetails}, #{compareTime}, #{compareStatus}, #{description}, #{emailNotificationStatus}, #{smsNotificationStatus}, #{isConsistent})")
//    void insertCompareResult(CompareResult result);
@Insert("INSERT INTO COMPARE_RESULT (COMPARE_CONFIG_ID, COMPARE_TASK_ID, SOURCE_DATA_DETAILS, TARGET_DATA_DETAILS, COMPARE_TIME, COMPARE_STATUS, DESCRIPTION, REPAIR_SOURCE, REPAIR_TARGET, EMAIL_NOTIFICATION_STATUS, SMS_NOTIFICATION_STATUS, IS_CONSISTENT, CREATE_TIME, UPDATE_TIME) " +
        "VALUES (#{compareConfigId}, #{compareTaskId}, #{sourceDataDetails}, #{targetDataDetails}, #{compareTime}, #{compareStatus}, #{description}, #{repairSource}, #{repairTarget}, #{emailNotificationStatus}, #{smsNotificationStatus}, #{isConsistent}, #{createTime}, #{updateTime})")
void insertCompareResult(CompareResult result);




    /**
     * 查询所有比对结果
     */
    @Select("SELECT id, compare_config_id AS compareConfigId, compare_task_id AS compareTaskId, " +
            "compare_time AS compareTime, compare_status AS compareStatus, description, " +
            "email_notification_status AS emailNotificationStatus, " +
            "sms_notification_status AS smsNotificationStatus, is_consistent AS isConsistent " +
            "FROM compare_result")
    List<CompareResultVO> findAllCompareResults();

    /**
     * 根据筛选条件查询比对结果
     */
    List<CompareResultVO> findCompareResultsByFilters(@Param("query") CompareResultQueryDTO queryDTO);

    /**
     * 根据 ID 查询 CompareResult 记录
     *
     * @param id 记录的 ID
     * @return 对应的 CompareResult 对象
     */
    CompareResult selectById(@Param("id") Long id);


}
