package com.southern.dataconsistencychecker.entity;

import lombok.Data;
import java.util.Date;

@Data
public class CompareConfig {
    private Long id;
    private Long sourceDataSourceId;
    private Long targetDataSourceId;
    private String sourceTable;
    private String targetTable;
    private String sourceConditions;
    private String targetConditions;
    private String sourceCompareFields;
    private String targetCompareFields;
    private String sourceUniqueKeys;
    private String targetUniqueKeys;
    private String cronExpression;
    private String notificationEmail;
    private String notificationPhone;
    private Date createTime;
    private Date updateTime;
}
