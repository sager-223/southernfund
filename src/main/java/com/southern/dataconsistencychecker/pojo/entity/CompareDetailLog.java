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
public class CompareDetailLog implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 主键，唯一标识日志记录
     */
    private Long id;

    /**
     * 关联的比对任务ID
     */
    private Long compareTaskId;

    /**
     * 来源数据源ID
     */
    private Long sourceDataSourceId;

    /**
     * 目标数据源ID
     */
    private Long targetDataSourceId;

    /**
     * 来源数据源表名
     */
    private String sourceTable;

    /**
     * 目标数据源表名
     */
    private String targetTable;

    /**
     * 不一致类型
     * 1 - 来源表唯一键缺失
     * 2 - 目标表唯一键缺失
     * 3 - 唯一键匹配但字段不一致
     */
    private Integer type;

    /**
     * 来源表的唯一键（可为空）
     */
    private String sourceUniqueKeys;

    /**
     * 目标表的唯一键（可为空）
     */
    private String targetUniqueKeys;

    /**
     * 来源表中不一致的字段名或比对键（可为空）
     */
    private String sourceFieldKey;

    /**
     * 来源表中不一致的字段值或比对键对应的值（可为空）
     */
    private String sourceFieldValue;

    /**
     * 目标表中不一致的字段名或比对键（可为空）
     */
    private String targetFieldKey;

    /**
     * 目标表中不一致的字段值或比对键对应的值（可为空）
     */
    private String targetFieldValue;

    /**
     * 记录创建时间
     */
    private LocalDateTime createTime;

    /**
     * 记录更新时间
     */
    private LocalDateTime updateTime;
    private String repairSource;
    private String repairTarget;
}
