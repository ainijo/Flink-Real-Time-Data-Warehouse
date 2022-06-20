package org.example.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.app.function.CustomerDeserialization;
import org.example.app.function.DimSinkFunction;
import org.example.app.function.TableProcessFunction;
import org.example.bean.TableProcess;
import org.example.utils.MyKafkaUtil;

import javax.annotation.Nullable;

// 数据流： web/app -> nginx -> SpringBoot -> MySQL -> FinkApp -> Kafka(ods) -> FlinkApp -> Kafka/Phoenix(dwd/dim)
// 程序：            mockDB               -> MySQL -> FlinkCDC -> Kafka(ZK) -> BaseDBApp -> Kafka/Phoenix(Hbase,HDFS,ZK)
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        // 1.获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 练习使用不需要开ckp，注意生产环境要开就行
        // 1.1 开启checkpoint并指定状态后端为 fs
        // env.setStateBackend(new FsStateBackend("hdfs://node01:8020/gmall-flink/ck"));
        // env.enableCheckpointing(5000L);
        // env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        // 老版本需要设置任务重启次数
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart());

        // 2.消费kafka ods_base_db 主题数据创建流
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // 3.将每行数据转换为Json对象并过滤（delete）  主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .filter(
                        new FilterFunction<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject value) throws Exception {
                                // 取出数据的类型，这里要根据反序列化的格式来
                                String type = value.getString("type");
                                return !"delete".equals(type);
                            }
                        });

        // 4.使用 Flink CDC 消费配置表，并处理成广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("node02")
                .port(3306)
                .username("root")
                .password("123456")
                // 监控新的数据库
                .databaseList("gmall-realtime")
                // table_process 表示表的更新
                // 字段：表名、操作类型（区分新增和变化）、数据保存的类型（kafka或habse）、建表语句（hbase要先建表）
                .tableList("gmall-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();
        DataStreamSource<String> tableProcessStrDS = env.addSource(sourceFunction);
        // 表的主键是Key，Value是对应的数据
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);

        // 5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        // 6.分流 处理数据 处理广播流数据，并根据广播流处理数据
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {
        };
        // 为了保证侧输出流的Tag和状态描述器的内外一致，选择把外面定义好的Tag和状态描述器作为侧输出流传入
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        // 7.提取 Kafka流和 Hbase流
        // Hbase流直接使用OutputTag提取
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);

        // 8.将Kafka流写入对应主题，Hbase流写入Phoenix表
        kafka.print("kafka>>>>>>>>");
        hbase.print("Hbase>>>>>>>>");

        // 不能用jdbcSink，因为不清楚参数的数量，所以要自定义Sink函数
        hbase.addSink(new DimSinkFunction());
        kafka.addSink(MyKafkaUtil.getKafkaProducer(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                        return new ProducerRecord<byte[], byte[]>(
                                element.getString("sinkTable"),
                                element.getString("after").getBytes()
                        );
                    }
                }));

        // 9.启动任务
        env.execute("BaseDBApp");
    }
}
