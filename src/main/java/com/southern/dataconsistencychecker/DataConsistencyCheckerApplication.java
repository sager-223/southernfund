package com.southern.dataconsistencychecker;


import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication
@EnableScheduling
@MapperScan("com.southern.dataconsistencychecker.mapper")
public class DataConsistencyCheckerApplication {

	public static void main(String[] args) {
		SpringApplication.run(DataConsistencyCheckerApplication.class, args);
	}

}
