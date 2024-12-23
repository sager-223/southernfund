package com.southern.dataconsistencychecker.pojo.vo;

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
public class CompareResultVO implements Serializable {
    private static final long serialVersionUID = 1L;
    private Long id;
    private Long compareConfigId;
    private Long compareTaskId;
    private LocalDateTime compareTime;
    private String compareStatus;
    private String description;
    private String emailNotificationStatus;
    private String smsNotificationStatus;
    private Boolean isConsistent;
}
