package com.jsg.controller; /**
 * Springboot 启动类
 */

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class Application {
    //启动文件
    public static void main(String[] args){
        SpringApplication.run(Application.class, args);
    }

}
