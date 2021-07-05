package com.abc.gmall.realtime.app.dws;

import com.abc.gmall.realtime.app.udf.KeywordProductC2RUDTF;
import com.abc.gmall.realtime.app.udf.KeywordUDTF;
import com.abc.gmall.realtime.bean.KeywordStats;
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
 * Desc:  商品行为关键字主题宽表计算
 */
public class KeywordStats4ProductApp {
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
            env.setStateBackend(new FsStateBackend("hdfs://hadoop112:9820/gmall/flink/checkpoint/KeywordStats4ProductApp"));
        }
        System.setProperty("HADOOP_USER_NAME", "abc");  // 设置HDFS 访问权限
        // 定义Table 流环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        // TODO 1. Source
        // 1.1 注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        tableEnv.createTemporarySystemFunction("keywordProductC2R", KeywordProductC2RUDTF.class);
        // 1.2 将数据源定义为动态表
        String groupId = "keyword_stats_app";
        String productStatsSourceTopic = "dws_product_stats";
        tableEnv.executeSql("CREATE TABLE product_stats (spu_name STRING, " +
                "click_ct BIGINT," +
                "cart_ct BIGINT," +
                "order_ct BIGINT ," +
                "stt STRING,edt STRING ) " +
                " WITH (" + MyKafkaUtil.getKafkaDDL(productStatsSourceTopic, groupId) + ")");


        // TODO 2. Transformation
        // 2.1 聚合计数
        Table keywordStatsProduct = tableEnv.sqlQuery("select keyword,ct,source, " +
                "DATE_FORMAT(stt,'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(edt,'yyyy-MM-dd HH:mm:ss') as edt, " +
                "UNIX_TIMESTAMP()*1000 ts from product_stats , " +
                "LATERAL TABLE(ik_analyze(spu_name)) as T(keyword) ," +
                "LATERAL TABLE(keywordProductC2R( click_ct ,cart_ct,order_ct)) as T2(ct,source)");
        // 2.2 转换为数据流
        DataStream<KeywordStats> keywordStatsProductDataStream =
                tableEnv.<KeywordStats>toAppendStream(keywordStatsProduct, KeywordStats.class);
        // 打印测试
        keywordStatsProductDataStream.print();


        // TODO 3. Sink
        // 写入ClickHouse
        keywordStatsProductDataStream.addSink(
                ClickHouseUtil.<KeywordStats>getJdbcSink(
                        "insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)")
        );


        // TODO 4. Execute
        env.execute();
    }
}