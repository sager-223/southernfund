package com.southern.dataconsistencychecker.entity;


import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
public class CompareConfig implements Serializable {
    private static final long serialVersionUID = 1L;
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
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
