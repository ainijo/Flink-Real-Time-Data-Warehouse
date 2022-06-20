package org.example.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
@Slf4j
public class LoggerController {

    // kafkaTemplate 可以作为 Bean
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("applog")
    public String getLog(@RequestParam("param") String jsonStr){
        // 打印数据
        // System.out.println(jsonStr);

        // 落盘数据
        // 打印全部信息
        log.info(jsonStr);

        // 数据写入kafka
        // 主题为 ods_base_log
        kafkaTemplate.send("ods_base_log", jsonStr);
        return "success";
    }

}
