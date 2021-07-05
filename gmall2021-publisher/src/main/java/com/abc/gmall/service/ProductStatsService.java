package com.abc.gmall.service;

import com.abc.gmall.bean.ProductStats;

import java.math.BigDecimal;
import java.util.List;

/**
 * Author: Cliff
 * Desc: 商品统计service 接口
 */
public interface ProductStatsService {
    //获取某一天交易总额
    BigDecimal getGMV(int date);

    //获取某一天不同品牌的交易额
    List<ProductStats> getProductStatsByTrademark(int date, int limit);

    //获取某一天不同品类的交易额
    List<ProductStats> getProductStatsByCategory3(int date,int limit);

    //获取某一天不同SPU的交易额
    List<ProductStats> getProductStatsBySPU(int date,int limit);
}
