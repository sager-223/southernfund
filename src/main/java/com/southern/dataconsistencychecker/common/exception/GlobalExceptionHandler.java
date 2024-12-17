package com.southern.dataconsistencychecker.common.exception;



import com.southern.dataconsistencychecker.common.result.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.io.IOException;
import java.sql.SQLException;

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
        return Result.error("RuntimeException，请稍后再试");
    }

    /**
     * 捕获受检异常 - SQLException
     * @param ex
     * @return
     */
    @ExceptionHandler
    public Result handleIOException(SQLException ex){
        log.error("SQL异常信息：{}", ex.getMessage());
        return Result.error("数据库处理错误");
    }

    /**
     * 捕获受检异常 - IOException
     * @param ex
     * @return
     */
    @ExceptionHandler
    public Result handleIOException(IOException ex){
        log.error("IO异常信息：{}", ex.getMessage());
        return Result.error("文件处理错误");
    }

    /**
     * 捕获所有其他异常
     * @param ex
     * @return
     */
    @ExceptionHandler
    public Result handleException(Exception ex){
        log.error("异常信息：{}", ex.getMessage());
        return Result.error("未知错误");
    }


}
