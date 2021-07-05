package com.abc.gmall.service;

import com.abc.gmall.bean.VisitorStats;

import java.util.List;

/**
 * Author: Cliff
 * Desc: 访客统计业务层接口
 */
public interface VisitorStatsService {

    List<VisitorStats> getVisitorStatsByNewFlag(int date);

    List<VisitorStats> getVisitorStatsByHr(int date);

}
