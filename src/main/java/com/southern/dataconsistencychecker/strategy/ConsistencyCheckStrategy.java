package com.southern.dataconsistencychecker.strategy;

import com.southern.dataconsistencychecker.entity.CompareConfig;

public interface ConsistencyCheckStrategy {
    void execute(CompareConfig config);
}
