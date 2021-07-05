package com.abc.gmall.realtime.app.dwd;

import com.abc.gmall.realtime.app.func.DimSink;
import com.abc.gmall.realtime.app.func.TableProcessFunction;
import com.abc.gmall.realtime.bean.TableProcess;
import com.abc.gmall.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.SystemUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * Author: Cliff
 * Desc:  从Kafka 中读取ods 层业务数据并进行处理发送到DWD 层
 */
public class BaseDBApp {

    public static void main(String[] args) throws Exception {
        // TODO 0. Env
        // Flink 流式处理环境
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
            env.setStateBackend(new FsStateBackend("hdfs://hadoop112:9820/gmall/flink/checkpoint/BaseDBApp"));
        }
        System.setProperty("HADOOP_USER_NAME", "abc");  // 设置HDFS 访问权限


        // TODO 1. Source
        // 接收Kafka 数据，过滤空值数据
        // 定义消费者组以及指定消费主题
        String topic = "ods_base_db_m";
        String groupId = "ods_base_group";
        // 从Kafka 主题中读取数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStream<String> jsonDstream = env.addSource(kafkaSource);
        // 打印测试
        //jsonDstream.print("data json::::::");


        // TODO 2. Transformation
        // 2.1 对数据进行结构的转换 String -> JSONObject
        DataStream<JSONObject> jsonStream = jsonDstream.map(JSON::parseObject);
        // 过滤为空或者长度不足的数据
        SingleOutputStreamOperator<JSONObject> filteredDstream = jsonStream.filter(
                jsonObject -> {
                    boolean flag = jsonObject.getString("table") != null
                            && jsonObject.getJSONObject("data") != null
                            && jsonObject.getString("data").length() > 3;
                    return flag;
                });
        // 打印测试
        //filteredDstream.print("json::::::");

        // 2.2 动态分流 事实表放入主流，作为DWD 层；维度表放入侧输出流
        // 定义输出到HBase 的侧输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };
        // 使用自定义ProcessFunction 进行分流处理
        SingleOutputStreamOperator<JSONObject> kafkaDStream = filteredDstream.process(new TableProcessFunction(hbaseTag));
        // 获取侧输出流，即将通过Phoenix 写到Hbase 的数据
        DataStream<JSONObject> hbaseDStream = kafkaDStream.getSideOutput(hbaseTag);


        // TODO 3. Sink
        // 3.1 将侧输出流数据写入 HBase(Phoenix)
        hbaseDStream.print("hbase::::::");
        hbaseDStream.addSink(new DimSink());

        // 3.2 将主流数据写入 Kafka
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSinkBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public void open(SerializationSchema.InitializationContext context) throws Exception {
                        System.out.println("启动 Kafka Sink");
                    }

                    // 从每条数据得到该条数据应送往的主题名
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                        String topic = jsonObject.getString("sink_table");
                        JSONObject dataJsonObj = jsonObject.getJSONObject("data");
                        return new ProducerRecord<>(topic, dataJsonObj.toJSONString().getBytes());
                    }
                });
        kafkaDStream.print("kafka::::::");
        kafkaDStream.addSink(kafkaSink);


        // TODO 4. Execute
        env.execute();
    }
}