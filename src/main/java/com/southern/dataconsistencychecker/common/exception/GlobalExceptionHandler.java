package com.southern.dataconsistencychecker.common.exception;



import com.southern.dataconsistencychecker.common.result.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 全局异常处理器，处理项目中抛出的业务异常
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    /**
     * 捕获业务异常
     * @param ex
     * @return
     */
    @ExceptionHandler
    public Result exceptionHandler(BusinessException ex){
        log.error("业务异常信息：{}", ex.getMessage());
        return Result.error(ex.getMessage());
    }

    /**
     * 处理运行时异常
     * @param ex
     * @return
     */
    @ExceptionHandler
    public Result handleRuntimeException(RuntimeException ex){
        log.error("Runtime异常信息：{}", ex.getMessage());
        return Result.error("系统错误，请稍后再试");
    }
}
