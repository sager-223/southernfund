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
public class CompareResult implements Serializable {
    private static final long serialVersionUID = 1L;
    private Long id;
    private Long compareConfigId;
    private Long compareTaskId;
    private String sourceDataDetails;
    private String targetDataDetails;
    private LocalDateTime compareTime;
    private String compareStatus;
    private String description;
    private String repairSource;
    private String repairTarget;
    private String emailNotificationStatus;
    private String smsNotificationStatus;
    private Boolean isConsistent;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
