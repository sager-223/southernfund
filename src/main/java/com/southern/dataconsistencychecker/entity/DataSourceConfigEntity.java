package com.southern.dataconsistencychecker.entity;

import lombok.Builder;
import lombok.Data;
import java.util.Date;

@Data
@Builder
public class DataSourceConfigEntity {
    private Long id;
    private String name;
    private String type; // oracle, mysql
    private String host;
    private Integer port;
    private String databaseName;
    private String username;
    private String password;
    private String additionalParams;
    private Date createTime;
    private Date updateTime;
}
