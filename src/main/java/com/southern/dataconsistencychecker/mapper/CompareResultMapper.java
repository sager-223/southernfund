package com.southern.dataconsistencychecker.mapper;

import com.southern.dataconsistencychecker.entity.CompareResult;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface CompareResultMapper {

    @Insert("INSERT INTO COMPARE_RESULT (COMPARE_CONFIG_ID, COMPARE_TASK_ID, SOURCE_DATA_DETAILS, TARGET_DATA_DETAILS, COMPARE_TIME, COMPARE_STATUS, DESCRIPTION, EMAIL_NOTIFICATION_STATUS, SMS_NOTIFICATION_STATUS, IS_CONSISTENT, CREATE_TIME, UPDATE_TIME) " +
            "VALUES (#{compareConfigId}, #{compareTaskId}, #{sourceDataDetails}, #{targetDataDetails}, #{compareTime}, #{compareStatus}, #{description}, #{emailNotificationStatus}, #{smsNotificationStatus}, #{isConsistent}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
    void insertCompareResult(CompareResult result);
}
