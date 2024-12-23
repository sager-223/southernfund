package com.southern.dataconsistencychecker.pojo.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CompareResultQueryDTO {
    private Long compareConfigId;
    private String compareStatus;
    private Boolean isConsistent;
    private String startTime; // 新增字段，ISO 格式字符串
    private String endTime;   // 新增字段，ISO 格式字符串
}
