package com.southern.dataconsistencychecker.factory;

import com.southern.dataconsistencychecker.common.exception.BusinessException;
import com.southern.dataconsistencychecker.strategy.ConsistencyCheckStrategy;
import com.southern.dataconsistencychecker.strategy.DataSourceStrategy;
import com.southern.dataconsistencychecker.strategy.impl.MySQLDataSourceStrategy;
import com.southern.dataconsistencychecker.strategy.impl.OracleDataSourceStrategy;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DataSourceFactory {
    // 策略 策略名称String -> 策略实体
    private final Map<String, DataSourceStrategy> strategyMap;

    public DataSourceFactory(Map<String, DataSourceStrategy> strategyMap) {
        this.strategyMap = strategyMap;
    }



    public DataSourceStrategy getStrategy(String type) {
        DataSourceStrategy strategy = strategyMap.get(type);
        if(strategy==null) throw new BusinessException("未知的数据库类型");
        return strategy;
    }


}
