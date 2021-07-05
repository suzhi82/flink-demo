package com.abc.gmall.service.impl;

import com.abc.gmall.bean.ProductStats;
import com.abc.gmall.mapper.ProductStatsMapper;
import com.abc.gmall.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * Author: Cliff
 * Desc: 商品统计Service 接口实现类
 */
@Service  // 标识是Spring的Sevice层组件，将对象的创建交给Spring的IOC管理
public class ProductStatsServiceImpl implements ProductStatsService {

    // 自动注入   在容器中，寻找ProductStatsMapper类型的对象，赋值给当前属性
    @Autowired
    ProductStatsMapper productStatsMapper ;

    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.getGMV(date);
    }

    @Override
    public List<ProductStats> getProductStatsByTrademark(int date, int limit) {
        return productStatsMapper.getProductStatsByTrademark(date,limit);
    }

    @Override
    public List<ProductStats> getProductStatsByCategory3(int date, int limit) {
        return productStatsMapper.getProductStatsByCategory3(date,limit);
    }

    @Override
    public List<ProductStats> getProductStatsBySPU(int date, int limit) {
        return productStatsMapper.getProductStatsBySPU(date,limit);
    }
}
