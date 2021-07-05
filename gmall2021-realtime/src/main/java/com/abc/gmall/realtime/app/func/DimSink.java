package com.abc.gmall.realtime.app.func;

import com.abc.gmall.realtime.common.GmallConfig;
import com.abc.gmall.realtime.utils.DimUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Set;

/**
 * Author: Cliff
 * Desc: 通过Phoenix 向Hbase 表中写数据
 */
public class DimSink extends RichSinkFunction<JSONObject> {
    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /**
     * 生成语句提交 hbase
     */
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        String tableName = jsonObject.getString("sink_table");
        JSONObject dataJsonObj = jsonObject.getJSONObject("data");
        if (dataJsonObj != null && dataJsonObj.size() > 0) {
            String upsertSql = genUpsertSql(tableName.toUpperCase(),
                    jsonObject.getJSONObject("data"));
            try {
                System.out.println(upsertSql);
                PreparedStatement ps = connection.prepareStatement(upsertSql);
                ps.executeUpdate();
                // 注意！执行完Phoenix 插入操作之后，默认需要手动提交事务
                connection.commit();
                ps.close();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("执行 sql 失败！");
            }
        }
        // 如果维度数据发生变化，那么清空当前数据在Redis 中的缓存
        if (jsonObject.getString("type").equals("update")
                || jsonObject.getString("type").equals("delete")) {
            DimUtil.deleteCached(tableName, dataJsonObj.getString("id"));
        }
    }

    // 拼接插入Phoenix 的SQL
    private String genUpsertSql(String tableName, JSONObject jsonObject) {
        Set<String> fields = jsonObject.keySet();
        String upsertSql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(" +
                StringUtils.join(fields, ",") + ")";
        String valuesSql = " values ('" + StringUtils.join(jsonObject.values(), "','") + "')";
        return upsertSql + valuesSql;
    }
}