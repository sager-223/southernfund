package com.southern.dataconsistencychecker.factory;

import com.southern.dataconsistencychecker.common.exception.BusinessException;
import com.southern.dataconsistencychecker.strategy.ConsistencyCheckStrategy;
import org.springframework.stereotype.Service;

import java.util.Map;


@Service
public class ConsistencyCheckStrategyFactory {
    // 策略 策略名称String -> 策略实体


    private final Map<String, ConsistencyCheckStrategy> strategyMap;

    public ConsistencyCheckStrategyFactory(Map<String, ConsistencyCheckStrategy> strategyMap) {
        this.strategyMap = strategyMap;
    }




    public ConsistencyCheckStrategy getStrategy(String type) {
        ConsistencyCheckStrategy strategy = strategyMap.get(type);
        if(strategy==null) throw new BusinessException("未知的策略类型");
        return strategy;
    }
}
