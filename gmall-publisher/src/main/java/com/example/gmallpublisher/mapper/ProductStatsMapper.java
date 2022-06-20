package com.example.gmallpublisher.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

public interface ProductStatsMapper {

    // 实现一个方法查询 ClickHouse 的数据
    // select sum(order_amount) from product_stats where toYYYYMMDD(stt)=20220618;

    @Select("select sum(order_amount) from product_stats where toYYYYMMDD(stt)= #{date}")
    BigDecimal selectGMV(int date);

}
