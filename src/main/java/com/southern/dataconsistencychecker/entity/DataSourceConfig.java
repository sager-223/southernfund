package com.southern.dataconsistencychecker.entity;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
public class DataSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long id;
    private String name;
    private String type;
    private String host;
    private Integer port;
    private String databaseName;
    private String username;
    private String password;
    private String additionalParams;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private String connectionType;

}
