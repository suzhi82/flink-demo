package com.abc.gmall.realtime.app.dwm;

import com.abc.gmall.realtime.app.func.DimAsyncFunction;
import com.abc.gmall.realtime.bean.OrderDetail;
import com.abc.gmall.realtime.bean.OrderInfo;
import com.abc.gmall.realtime.bean.OrderWide;
import com.abc.gmall.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Author: Cliff
 * Desc:  处理订单和订单明细数据形成订单宽表
 */
public class OrderWideApp {

    public static void main(String[] args) throws Exception {
        // TODO 0. Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度读取kafka 分区数据
        env.setParallelism(4);  // 并行度和Kafka 分区数一致

        // 设置Checkpoint 相关配置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        if (SystemUtils.IS_OS_WINDOWS) {
            env.setStateBackend(new FsStateBackend("file:///D:/ckp"));
        } else {
            env.setStateBackend(new FsStateBackend("hdfs://hadoop112:9820/gmall/flink/checkpoint/OrderWideApp"));
        }
        System.setProperty("HADOOP_USER_NAME", "abc");


        // TODO 1. Source
        // 从Kafka 的dwd 层接收订单和订单明细数据
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";
        // 从Kafka 中读取数据
        FlinkKafkaConsumer<String> sourceOrderInfo = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> sourceOrderDetail = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        DataStream<String> orderInfojsonDStream = env.addSource(sourceOrderInfo);
        DataStream<String> orderDetailJsonDStream = env.addSource(sourceOrderDetail);


        // TODO 2. Transformation
        // 2.1 获取数据流
        // 订单信息流
        DataStream<OrderInfo> orderInfoDStream = orderInfojsonDStream.map(
                new RichMapFunction<String, OrderInfo>() {
                    SimpleDateFormat simpleDateFormat = null;

                    // open 方法用于初始化
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    // map 方法用于处理每一条数据
                    @Override
                    public OrderInfo map(String jsonString) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(jsonString, OrderInfo.class);
                        orderInfo.setCreate_ts(simpleDateFormat.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                }
        );
        // 订单明细流
        DataStream<OrderDetail> orderDetailDStream = orderDetailJsonDStream.map(
                new RichMapFunction<String, OrderDetail>() {
                    SimpleDateFormat simpleDateFormat = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public OrderDetail map(String jsonString) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(jsonString, OrderDetail.class);
                        orderDetail.setCreate_ts
                                (simpleDateFormat.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                });
        // 测试打印
        //orderInfoDStream.print("orderInfo::::");
        //orderDetailDStream.print("orderDetail::::");

        // 2.2 设定事件时间水位
        SingleOutputStreamOperator<OrderInfo> orderInfoWithEventTimeDstream =
                orderInfoDStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                                        return orderInfo.getCreate_ts();
                                    }
                                })
                );
        SingleOutputStreamOperator<OrderDetail> orderDetailWithEventTimeDstream =
                orderDetailDStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderDetail>() {
                                    @Override
                                    public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                                        return orderDetail.getCreate_ts();
                                    }
                                }));

        // 2.3 设定关联的 key
        KeyedStream<OrderInfo, Long> orderInfoKeyedDstream = orderInfoWithEventTimeDstream.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedStream = orderDetailWithEventTimeDstream.keyBy(OrderDetail::getOrder_id);

        // 2.4 订单和订单明细关联 intervalJoin
        SingleOutputStreamOperator<OrderWide> orderWideDstream = orderInfoKeyedDstream.intervalJoin(orderDetailKeyedStream)
                .between(Time.seconds(-5), Time.seconds(5))  // 与前后5 秒的数据进行join
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out)
                            throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
        // 打印测试
        //orderWideDstream.print("joined::::");

        // 2.5 关联用户维度
        // 使用异步的无序等待unorderedWait，返回一个处理一个，不看顺序，性能上快一些
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDstream = AsyncDataStream.unorderedWait(
                orderWideDstream,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        SimpleDateFormat formattor = new SimpleDateFormat("yyyy-MM-dd");
                        String birthday = jsonObject.getString("BIRTHDAY");
                        Date date = formattor.parse(birthday);
                        Long curTs = System.currentTimeMillis();
                        Long betweenMs = curTs - date.getTime();
                        Long ageLong = betweenMs / 1000L / 60L / 60L / 24L / 365L;
                        Integer age = ageLong.intValue();
                        orderWide.setUser_age(age);
                        orderWide.setUser_gender(jsonObject.getString("GENDER"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getUser_id());
                    }
                }, 60, TimeUnit.SECONDS);
        // 测试打印
        //orderWideWithUserDstream.print("dim join user:");

        // 2.6 关联省市维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDstream = AsyncDataStream.unorderedWait(
                orderWideWithUserDstream,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setProvince_name(jsonObject.getString("NAME"));
                        orderWide.setProvince_3166_2_code(jsonObject.getString("ISO_3166_2"));
                        orderWide.setProvince_iso_code(jsonObject.getString("ISO_CODE"));
                        orderWide.setProvince_area_code(jsonObject.getString("AREA_CODE"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getProvince_id());
                    }
                }, 60, TimeUnit.SECONDS);

        // 2.7 关联SKU 维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDstream = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDstream,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        // 2.8 关联SPU 商品维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDstream = AsyncDataStream.unorderedWait(
                orderWideWithSkuDstream,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        // 2.9 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3Dstream = AsyncDataStream.unorderedWait(
                orderWideWithSpuDstream,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        // 2.10 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDstream = AsyncDataStream.unorderedWait(
                orderWideWithCategory3Dstream,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);
        // 打印测试
        orderWideWithTmDstream.print("OrderWide All");


        // TODO 3. Sink
        // 将订单和订单明细Join 之后以及维度关联的宽表写到Kafka 的dwm 层
        orderWideWithTmDstream.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));


        // TODO 4. Execute
        env.execute();
    }

}