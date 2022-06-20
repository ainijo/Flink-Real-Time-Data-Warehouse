package com.example.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.example.gmallpublisher.service.SugarService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.HashMap;

@RestController
public class SugarController {

    @Autowired
    private SugarService sugarService;

    // date要有默认值——当天的时间
    @RequestMapping("api/sugar/gmv")
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") int date) {

        // 通过对默认值进行判断来决定是否赋值为当前的日期
        if (date == 0) {
            date = getToday();
        }

        HashMap<String, Object> result = new HashMap<>();
        result.put("status", 0);
        result.put("msg", "");
        result.put("data", sugarService.getGMV(date));

        /*
            return "{  " +
                    "  \"status\": 0,  " +
                    "  \"msg\": \"\",  " +
                    "  \"data\": " + sugarService.getGMV(date) + " " +
                    "}";
         */
        return JSON.toJSONString(result);
    }

    private int getToday() {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String dateTime = sdf.format(System.currentTimeMillis());

        return Integer.parseInt(dateTime);
    }
}
