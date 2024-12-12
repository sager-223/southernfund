package com.southern.dataconsistencychecker.entity;

import lombok.Builder;
import lombok.Data;
import java.util.Date;

@Data
@Builder
public class CompareResult {
    private Long id;
    private Long compareConfigId;
    private Long compareTaskId;
    private String sourceDataDetails; // JSON format
    private String targetDataDetails; // JSON format
    private Date compareTime;
    private String compareStatus; // SUCCESS, FAIL
    private String description;
    private String emailNotificationStatus; // noNeed, true, false
    private String smsNotificationStatus; // noNeed, true, false
    private Boolean isConsistent;
    private Date createTime;
    private Date updateTime;
}
