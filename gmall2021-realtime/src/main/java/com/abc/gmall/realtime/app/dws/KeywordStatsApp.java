package com.abc.gmall.realtime.app.dws;

import com.abc.gmall.realtime.app.udf.KeywordUDTF;
import com.abc.gmall.realtime.bean.KeywordStats;
import com.abc.gmall.realtime.common.GmallConstant;
import com.abc.gmall.realtime.utils.ClickHouseUtil;
import com.abc.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang.SystemUtils;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: Cliff
 * Desc:  搜索 关键字计算
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        // TODO 0. Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度，与Kafka 分区数相同
        env.setParallelism(4);
        // 设置Checkpoint 相关参数
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 判断操作系统，非Windows 即Linux
        if (SystemUtils.IS_OS_WINDOWS) {
            env.setStateBackend(new FsStateBackend("file:///D:/ckp"));
        } else {
            env.setStateBackend(new FsStateBackend("hdfs://hadoop112:9820/gmall/flink/checkpoint/KeywordStatsApp"));
        }
        System.setProperty("HADOOP_USER_NAME", "abc");  // 设置HDFS 访问权限
        // 定义Table 流环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        // TODO 1. Source
        // 1.1 注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        // 1.2 将数据源定义为动态表
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        tableEnv.executeSql("CREATE TABLE page_view " +
                "(common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>, ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                "WITH (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")");


        // TODO 2. Transformation
        // 2.1 过滤数据
        Table fullwordView = tableEnv.sqlQuery(
                "select page['item'] fullword ,rowtime from page_view " +
                        "where page['page_id']='good_list' and page['item'] IS NOT NULL ");

        // 2.2 利用udtf 将数据拆分
        Table keywordView = tableEnv.sqlQuery(
                "select keyword,rowtime from " + fullwordView + " ," +
                        " LATERAL TABLE(ik_analyze(fullword)) as T(keyword)");

        // 2.3 根据各个关键词出现次数进行Count
        Table keywordStatsSearch = tableEnv.sqlQuery("select keyword,count(*) ct," +
                " '" + GmallConstant.KEYWORD_SEARCH + "' source ," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts from " + keywordView
                + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword");

        // 2.4 转换为数据流
        DataStream<KeywordStats> keywordStatsSearchDataStream =
                tableEnv.<KeywordStats>toAppendStream(keywordStatsSearch, KeywordStats.class);
        // 打印测试
        keywordStatsSearchDataStream.print();


        // TODO 3. Sink
        // 写入ClickHouse
        keywordStatsSearchDataStream.addSink(
                ClickHouseUtil.<KeywordStats>getJdbcSink(
                        "insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)")
        );


        // TODO 4. Execute
        env.execute();
    }
}