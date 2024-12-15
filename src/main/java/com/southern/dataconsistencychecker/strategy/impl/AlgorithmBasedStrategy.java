package com.southern.dataconsistencychecker.strategy.impl;

import com.southern.dataconsistencychecker.entity.CompareConfig;
import com.southern.dataconsistencychecker.strategy.ConsistencyCheckStrategy;
import org.springframework.stereotype.Component;

@Component("algorithm")
public class AlgorithmBasedStrategy implements ConsistencyCheckStrategy {
    @Override
    public void execute(CompareConfig config) {
        // 基于算法的实现
    }
}
