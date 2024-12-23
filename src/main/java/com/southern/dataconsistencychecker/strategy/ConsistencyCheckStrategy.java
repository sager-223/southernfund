package com.southern.dataconsistencychecker.strategy;

import com.southern.dataconsistencychecker.pojo.entity.CompareConfig;

public interface ConsistencyCheckStrategy {
    void execute(CompareConfig config);
}
