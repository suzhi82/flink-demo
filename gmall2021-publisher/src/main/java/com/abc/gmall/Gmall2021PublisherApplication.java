package com.abc.gmall;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.abc.gmall.mapper")
public class Gmall2021PublisherApplication {

	public static void main(String[] args) {
		SpringApplication.run(Gmall2021PublisherApplication.class, args);
	}

}
