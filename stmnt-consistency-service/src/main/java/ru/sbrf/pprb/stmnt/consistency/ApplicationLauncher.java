package ru.sbrf.pprb.stmnt.consistency;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ApplicationLauncher {
    public static void main(String[] args) {
        SpringApplication.run(ApplicationLauncher.class, args);
    }
}
