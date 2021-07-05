package com.abc.gmall.realtime.common;

/**
 * Author: Cliff
 * Desc:  项目配置 常量类
 */
public class GmallConfig {
    // HBase 的命名空间
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";

    // Phoenix 的服务器地址
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop114:2182";

    // ClickHouse 的服务器地址
    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hadoop112:8123/default";
}