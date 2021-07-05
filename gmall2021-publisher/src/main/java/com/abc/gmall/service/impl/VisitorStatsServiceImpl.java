package com.abc.gmall.service.impl;

import com.abc.gmall.bean.VisitorStats;
import com.abc.gmall.mapper.VisitorStatsMapper;
import com.abc.gmall.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Author: Cliff
 * Desc: 访客统计接口的实现类
 */
@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {

    @Autowired
    VisitorStatsMapper visitorStatsMapper;
    @Override
    public List<VisitorStats> getVisitorStatsByNewFlag(int date) {
        return visitorStatsMapper.selectVisitorStatsByNewFlag(date);
    }

    @Override
    public List<VisitorStats> getVisitorStatsByHr(int date) {
        return visitorStatsMapper.selectVisitorStatsByHr(date);
    }
}
