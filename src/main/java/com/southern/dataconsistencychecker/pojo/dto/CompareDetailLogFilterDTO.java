package com.southern.dataconsistencychecker.pojo.dto;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CompareDetailLogFilterDTO {
    private static final long serialVersionUID = 1L;



    /**
     * 比对任务ID
     */
    private Long compareTaskId;

    /**
     * 不一致类型，1,2,3
     */
    private Integer type;

    /**
     * 记录开始时间
     */
    private String createTimeStart;

    /**
     * 记录结束时间
     */
    private String createTimeEnd;

    /**
     * 当前页码，从1开始
     */
    private Integer page = 1;

    /**
     * 每页大小
     */
    private Integer size = 8;


}
