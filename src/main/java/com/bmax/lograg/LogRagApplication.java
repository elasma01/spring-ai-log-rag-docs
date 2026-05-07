package com.bmax.lograg;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class LogRagApplication {

    public static void main(String[] args) {
        SpringApplication.run(LogRagApplication.class, args);
    }
}
