package com.abc.gmall.service;

import com.abc.gmall.bean.KeywordStats;

import java.util.List;

/**
 * Author: Cliff
 * Desc: 关键词统计接口
 */
public interface KeywordStatsService {
    public List<KeywordStats> getKeywordStats(int date, int limit);
}

