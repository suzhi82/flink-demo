package com.abc.gmall.service;

import com.abc.gmall.bean.ProvinceStats;

import java.util.List;

/**
 * Author: Cliff
 * Desc:  按照地区统计的业务接口
 */
public interface ProvinceStatsService {
    List<ProvinceStats> getProvinceStats(int date);
}
