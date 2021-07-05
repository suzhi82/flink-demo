package com.abc.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Author: Cliff
 * Desc: SpringBoot 接口，接收消息转成json 发到Kafka
 */
// 标识为controller 组件，交给Sprint 容器管理，并接收处理请求
// 如果返回String，会当作网页进行跳转
//@Controller
//@RestController = @Controller + @ResponseBody 会将返回结果转换为json 进行响应
@RestController
@Slf4j
public class LoggerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/applog")
    public String getLogger(@RequestParam("param") String jsonStr) {
        // 将数据落盘
        log.info(jsonStr);
        // 将数据发送至 Kafka ODS  主题
        kafkaTemplate.send("ods_base_log", jsonStr);
        return "success";
    }
}