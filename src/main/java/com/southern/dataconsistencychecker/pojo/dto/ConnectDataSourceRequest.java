package com.southern.dataconsistencychecker.pojo.dto;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ConnectDataSourceRequest implements Serializable {
    private static final long serialVersionUID = 1L;
    private Long id;
    private String password;
}
