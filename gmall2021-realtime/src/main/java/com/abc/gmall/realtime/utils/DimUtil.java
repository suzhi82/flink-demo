package com.abc.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Author: Cliff
 * Desc:  查询维度的工具类
 */
public class DimUtil {
    // 直接从 Phoenix 查询，没有缓存
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... colNameAndValue) {
        // 组合查询条件
        String wheresql = " where ";
        for (int i = 0; i < colNameAndValue.length; i++) {
            // 获取查询列名以及对应的值
            Tuple2<String, String> nameValueTuple = colNameAndValue[i];
            String fieldName = nameValueTuple.f0;
            String fieldValue = nameValueTuple.f1;
            if (i > 0) {
                wheresql += " and ";
            }
            wheresql += fieldName + "='" + fieldValue + "'";
        }
        // 组合查询SQL
        String sql = "select * from " + tableName + wheresql;
        System.out.println(" 查询维度 SQL:" + sql);
        JSONObject dimInfoJsonObj = null;
        List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
        if (dimList != null && dimList.size() > 0) {
            // 因为关联维度，肯定都是根据 key 关联得到一条记录
            dimInfoJsonObj = dimList.get(0);
        } else {
            System.out.println(" 维度数据未找到:" + sql);
        }
        return dimInfoJsonObj;
    }

    // 先从Redis 中查，如果缓存中没有再通过Phoenix 查询，固定id 进行关联
    public static JSONObject getDimInfo(String tableName, String id) {
        Tuple2<String, String> kv = Tuple2.of("id", id);
        return getDimInfo(tableName, kv);
    }

    // 先从Redis 中查，如果缓存中没有再通过Phoenix 查询，可以使用其它字段灵活关联
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... colNameAndValue) {
        // 组合查询条件
        String wheresql = " where ";
        String redisKey = "";
        for (int i = 0; i < colNameAndValue.length; i++) {
            Tuple2<String, String> nameValueTuple = colNameAndValue[i];
            String fieldName = nameValueTuple.f0;
            String fieldValue = nameValueTuple.f1;
            if (i > 0) {
                wheresql += " and ";
                // 根据查询条件组合 redis key
                redisKey += "_";
            }
            wheresql += fieldName + "='" + fieldValue + "'";
            redisKey += fieldValue;
        }
        Jedis jedis = null;
        String dimJson = null;
        JSONObject dimInfo = null;
        String key = "dim:" + tableName.toLowerCase() + ":" + redisKey;
        try {
            // 从连接池获得连接
            jedis = RedisUtil.getJedis();
            // 通过key 查询缓存
            dimJson = jedis.get(key);
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }
        if (dimJson != null) {
            dimInfo = JSON.parseObject(dimJson);
        } else {  // 如未能从Redis 中找到，则从Phoenix 中查找，并同步到Redis 中
            String sql = "select * from " + tableName + wheresql;
            System.out.println("查询维度 sql:" + sql);
            List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
            if (dimList.size() > 0) {
                dimInfo = dimList.get(0);
                if (jedis != null) {
                    // 把从数据库中查询的数据同步到缓存
                    jedis.setex(key, 3600 * 24, dimInfo.toJSONString());
                }
            } else {
                System.out.println("维度数据未找到：" + sql);
            }
        }
        if (jedis != null) {
            jedis.close();  // 实际上是归还连接给Redis 连接池
            System.out.println("关闭Redis 缓存连接！");
        }
        return dimInfo;
    }

    // 根据key 让Redis 中的缓存失效
    public static void deleteCached( String tableName, String id){
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            // 通过key 清除缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }
    }

    // 测试Main 方法
    public static void main(String[] args) {
        JSONObject dimInfoNoCache = DimUtil.getDimInfoNoCache("dim_base_trademark", Tuple2.of("id", "13"));
        System.out.println(dimInfoNoCache);
    }
}