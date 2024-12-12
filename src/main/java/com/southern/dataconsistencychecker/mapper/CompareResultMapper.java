package com.southern.dataconsistencychecker.mapper;

import com.southern.dataconsistencychecker.entity.CompareResult;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface CompareResultMapper {
    @Insert("<script>" +
            "INSERT INTO compare_result " +
            "(compare_config_id, compare_task_id, source_data_details, target_data_details, compare_time, " +
            "compare_status, description, email_notification_status, sms_notification_status, is_consistent, create_time, update_time) " +
            "VALUES " +
            "(#{compareConfigId}, #{compareTaskId}, #{sourceDataDetails}, #{targetDataDetails}, #{compareTime}, " +
            "#{compareStatus}, #{description}, #{emailNotificationStatus}, #{smsNotificationStatus}, #{isConsistent}, sysdate, sysdate)" +
            "</script>")
    void insertCompareResult(CompareResult compareResult);
}
