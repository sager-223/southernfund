package com.southern.dataconsistencychecker.entity;

import lombok.Data;
import java.util.Date;

@Data
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
