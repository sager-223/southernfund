package com.southern.dataconsistencychecker.entity;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
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
    private String emailNotificationStatus;
    private String smsNotificationStatus;
    private Boolean isConsistent;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
