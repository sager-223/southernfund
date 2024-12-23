package com.southern.dataconsistencychecker.pojo.dto;


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
public class UpdateCompareConfigRequest implements Serializable {
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

}
