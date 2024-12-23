package com.southern.dataconsistencychecker.pojo.entity;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
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
