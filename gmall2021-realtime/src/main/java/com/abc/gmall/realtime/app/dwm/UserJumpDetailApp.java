package com.abc.gmall.realtime.app.dwm;

import com.abc.gmall.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.SystemUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * Author: Cliff
 * Desc:  访客跳出情况判断
 */
public class UserJumpDetailApp {

    public static void main(String[] args) throws Exception {
        // TODO 0. Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);  // 设置并行数，与Kafka 分区数相同
        // Checkpoint 设置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 判断操作系统，非Windows 即Linux
        if (SystemUtils.IS_OS_WINDOWS) {
            env.setStateBackend(new FsStateBackend("file:///D:/ckp"));
        } else {
            env.setStateBackend(new FsStateBackend("hdfs://hadoop112:9820/gmall/flink/checkpoint/UserJumpDetailApp"));
        }
        System.setProperty("HADOOP_USER_NAME", "abc");  // 设置HDFS 访问权限

        // TODO 1. Source
        // 从kafka 的dwd_page_log 主题中读取页面日志
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";
        // 从kafka 中读取数据
        DataStreamSource<String> dataStream = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));

        // 测试数据，代替上面的数据源
        //DataStream<String> dataStream = env
        //        .fromElements(
        //                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
        //                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
        //                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":\"home\"},\"ts\":15000} ",
        //                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":\"detail\"},\"ts\":30000} "
        //        );

        dataStream.print("in json:");


        // TODO 2. Transformation
        // 对数据进行结构的转换
        DataStream<JSONObject> jsonObjStream = dataStream.map(JSON::parseObject);
        // 打印测试
        //jsonObjStream.print("json:");

        // TODO 2.1 指定事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithEtDstream =
                jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().
                        withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long recordTimestamp) {
                                return jsonObject.getLong("ts");
                            }
                        }));

        // TODO 2.2 根据日志数据的mid 进行分组
        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = jsonObjWithEtDstream.keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );

        // TODO 2.3 配置CEP 表达式
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("GoIn").where(
                new SimpleCondition<JSONObject>() {
                    @Override  // 条件1：进入的第一个页面，last_page_id 为空
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        System.out.println("first in :" + lastPageId);
                        if (lastPageId == null || lastPageId.length() == 0) {
                            return true;
                        }
                        return false;
                    }
                }
        ).next("next").where(
                new SimpleCondition<JSONObject>() {
                    @Override  // 条件2：在10 秒时间范围内必须有第二个页面
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        System.out.println("next:" + pageId);
                        if (pageId != null && pageId.length() > 0) {
                            return true;
                        }
                        return false;
                    }
                }
        ).within(Time.seconds(10));

        // TODO 2.4 根据表达式筛选流
        PatternStream<JSONObject> patternedStream = CEP.pattern(jsonObjectStringKeyedStream, pattern);

        // TODO 2.5 提取命中的数据
        final OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
        };
        SingleOutputStreamOperator<String> filteredStream = patternedStream.flatSelect(
                timeoutTag,
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        List<JSONObject> objectList = pattern.get("GoIn");
                        // 这里进入out 的数据都被timeoutTag 标记
                        for (JSONObject jsonObject : objectList) {
                            out.collect(jsonObject.toJSONString());
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {
                        // 因为不超时的事件不提取，所以这里不写代码
                    }
                });
        // 通过SideOutput 侧输出流输出超时数据
        DataStream<String> jumpDstream = filteredStream.getSideOutput(timeoutTag);
        jumpDstream.print("jump::");


        // TODO 3. Sink
        // 将跳出数据写回到kafka 的DWM 层
        jumpDstream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));


        // TODO 4. Execute
        env.execute();
    }
}