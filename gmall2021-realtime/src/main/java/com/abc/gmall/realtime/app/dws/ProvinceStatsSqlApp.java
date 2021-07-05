package com.abc.gmall.realtime.app.dws;

import com.abc.gmall.realtime.bean.ProvinceStats;
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
 * Desc: FlinkSQL 实现地区主题宽表计算
 */
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 0. Env
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
            env.setStateBackend(new FsStateBackend("hdfs://hadoop112:9820/gmall/flink/checkpoint/ProvinceStatsSqlApp"));
        }
        System.setProperty("HADOOP_USER_NAME", "abc");  // 设置HDFS 访问权限
        // 定义Table 流环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        // TODO 1. Source
        // 把数据源定义为动态表
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";
        tableEnv.executeSql(new StringBuffer()
                .append("CREATE TABLE ORDER_WIDE (")
                .append("  province_id BIGINT,")
                .append("  province_name STRING,")
                .append("  province_area_code STRING,")
                .append("  province_iso_code STRING,")
                .append("  province_3166_2_code STRING,")
                .append("  order_id STRING,")
                .append("  split_total_amount DOUBLE,")
                .append("  create_time STRING,")
                .append("  rowtime AS TO_TIMESTAMP(create_time),")
                .append("WATERMARK FOR rowtime AS rowtime) ")
                .append("WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")").toString());


        // TODO 2. Transformation
        // 2.1 聚合计算
        Table provinceStateTable = tableEnv.sqlQuery(new StringBuilder()
                .append("SELECT ")
                .append("  DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, ")
                .append("  DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt, ")
                .append("  province_id, province_name, province_area_code area_code, ")
                .append("  province_iso_code iso_code, province_3166_2_code iso_3166_2, ")
                .append("  COUNT( DISTINCT order_id) order_count, sum(split_total_amount) order_amount, ")
                .append("  UNIX_TIMESTAMP()*1000 ts ")
                .append("FROM ")
                .append("  ORDER_WIDE ")
                .append("GROUP BY ")
                .append("  TUMBLE(rowtime, INTERVAL '10' SECOND),")
                .append("  province_id, province_name, province_area_code, province_iso_code, province_3166_2_code").toString());

        // 2.2 转换为数据流
        DataStream<ProvinceStats> provinceStatsDataStream =
                tableEnv.toAppendStream(provinceStateTable, ProvinceStats.class);
        // 打印测试
        provinceStatsDataStream.print("ProvinceStats:");


        // TODO 3. Sink
        // 写入ClickHouse
        provinceStatsDataStream.addSink(
                ClickHouseUtil.<ProvinceStats>getJdbcSink(
                        "insert into province_stats_2021 values( ?, ?, ?,?,?,?,?,?,?,?)")
        );


        // TODO 4. Execute
        env.execute();
    }
}