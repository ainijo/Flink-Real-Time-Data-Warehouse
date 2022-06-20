package com.example.gmallpublisher.service.impl;

import com.example.gmallpublisher.mapper.ProductStatsMapper;
import com.example.gmallpublisher.service.SugarService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class SugarServiceImpl implements SugarService {

    @Autowired
    private ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.selectGMV(date);
    }
}
