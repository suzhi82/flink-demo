package com.abc.gmall.realtime.app.dws;

import com.abc.gmall.realtime.bean.VisitorStats;
import com.abc.gmall.realtime.utils.ClickHouseUtil;
import com.abc.gmall.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.SystemUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * Author: Cliff
 * Desc:  访客主题宽表计算
 * <p>
 * ? 要不要把多个明细的同样的维度统计在一起 ?
 * 因为单位时间内 mid 的操作数据非常有限不能明显的压缩数据量（如果是数据量够大，或者单位时间够长可以）
 * 所以用常用统计的四个维度进行聚合 渠道、新老用户、 app 版本、省市区域
 * 度量值包括 启动、日活（当日首次启动）、访问页面数、新增用户数、跳出数、平均页面停留时长、总访问时长
 * 聚合窗口： 10 秒
 * <p>
 * 各个数据在维度聚合前不具备关联性 ，所以 先进行维度聚合
 * 进行关联，这是一个 fulljoin，可以考虑使用flinksql 完成
 */
public class VisitorStatsApp {

    //@SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        // TODO 0. Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度，与Kafka 分区数一致
        env.setParallelism(4);
        // 检查点Checkpoint 相关设置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 判断操作系统，非Windows 即Linux
        if (SystemUtils.IS_OS_WINDOWS) {
            env.setStateBackend(new FsStateBackend("file:///D:/ckp"));
        } else {
            env.setStateBackend(new FsStateBackend("hdfs://hadoop112:9820/gmall/flink/checkpoint/VisitorStatsApp"));
        }
        System.setProperty("HADOOP_USER_NAME", "abc");  // 设置HDFS 访问权限


        //TODO 1. Source
        // 从Kafka 的pv、uv、跳转明细主题中获取数据
        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uniqueVisitSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> userJumpSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> uniqueVisitDStream = env.addSource(uniqueVisitSource);
        DataStreamSource<String> userJumpDStream = env.addSource(userJumpSource);
        // 打印测试
        //pageViewDStream.print("pv------>");
        //uniqueVisitDStream.print("uv------>");
        //userJumpDStream.print("uj------>");


        // TODO 2. Transformation
        // 2.1 转换pv 流
        SingleOutputStreamOperator<VisitorStats> pageViewStatsDstream = pageViewDStream.map(
                json -> {
                    // System.out.println("pv:"+json);
                    JSONObject jsonObj = JSON.parseObject(json);
                    return new VisitorStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            0L, 1L, 0L, 0L, jsonObj.getJSONObject("page").getLong("during_time"),
                            jsonObj.getLong("ts"));
                });

        // 2.2 转换uv 流
        SingleOutputStreamOperator<VisitorStats> uniqueVisitStatsDstream = uniqueVisitDStream.map(json -> {
            JSONObject jsonObj = JSON.parseObject(json);
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    1L, 0L, 0L, 0L, 0L, jsonObj.getLong("ts"));
        });

        // 2.3 转换sv 流
        SingleOutputStreamOperator<VisitorStats> sessionVisitDstream = pageViewDStream.process(
                new ProcessFunction<String, VisitorStats>() {
                    @Override
                    public void processElement(String json, Context ctx, Collector<VisitorStats> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(json);
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            // System.out.println("sc:"+json);
                            VisitorStats visitorStats = new VisitorStats("", "",
                                    jsonObj.getJSONObject("common").getString("vc"),
                                    jsonObj.getJSONObject("common").getString("ch"),
                                    jsonObj.getJSONObject("common").getString("ar"),
                                    jsonObj.getJSONObject("common").getString("is_new"),
                                    0L, 0L, 1L, 0L, 0L, jsonObj.getLong("ts"));
                            out.collect(visitorStats);
                        }
                    }
                });

        // 2.4 转换跳转流
        SingleOutputStreamOperator<VisitorStats> userJumpStatDstream = userJumpDStream.map(json -> {
            JSONObject jsonObj = JSON.parseObject(json);
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L, 0L, 0L, 1L, 0L, jsonObj.getLong("ts"));
        });

        // 2.5 将四条流合并起来
        DataStream<VisitorStats> unionDetailDstream = uniqueVisitStatsDstream.union(
                pageViewStatsDstream,
                sessionVisitDstream,
                userJumpStatDstream
        );

        // 2.6 设置水位线
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkDstream =
                unionDetailDstream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(1)).
                                withTimestampAssigner((visitorStats, ts) -> visitorStats.getTs())
                );
        // 打印测试
        //visitorStatsWithWatermarkDstream.print("after union:::");

        // 2.7 分组 选取四个维度作为key, 使用Tuple4 组合
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> visitorStatsTuple4KeyedStream =
                visitorStatsWithWatermarkDstream.keyBy(
                        new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                            @Override
                            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                                return new Tuple4<>(visitorStats.getVc(), visitorStats.getCh(), visitorStats.getAr(), visitorStats.getIs_new());
                            }
                        }
                );

        // 2.8 开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowStream =
                visitorStatsTuple4KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // 2.9 Reduce 聚合统计
        SingleOutputStreamOperator<VisitorStats> visitorStatsDstream = windowStream.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                        // 把度量数据两两相加
                        stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                        stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                        stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                        stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                        stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                        return stats1;
                    }
                },
                new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> tuple4, Context context,
                                        Iterable<VisitorStats> visitorStatsIn, Collector<VisitorStats> visitorStatsOut) throws Exception {
                        // 补时间字段
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (VisitorStats visitorStats : visitorStatsIn) {
                            String startDate = simpleDateFormat.format(new
                                    Date(context.window().getStart()));
                            String endDate = simpleDateFormat.format(new Date(context.window().getEnd()));
                            visitorStats.setStt(startDate);
                            visitorStats.setEdt(endDate);
                            visitorStatsOut.collect(visitorStats);
                        }
                    }
                });
        // 打印测试
        visitorStatsDstream.print("reduce:");


        // TODO 3. Sink
        // 向ClickHouse 中写入数据
        visitorStatsDstream.addSink(
                ClickHouseUtil.getJdbcSink("insert into visitor_stats_2021 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        );


        // TODO 4. Execute
        env.execute();
    }
}