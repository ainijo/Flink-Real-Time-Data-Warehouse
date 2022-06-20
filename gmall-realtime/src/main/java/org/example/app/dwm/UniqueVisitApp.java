package org.example.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.utils.MyKafkaUtil;

import java.text.SimpleDateFormat;

/*
  数据流：web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> Kafka(dwm)
  程序： mockLog -> Nginx -> logger.sh -> Kafka(Zk) -> BaseLogApp -> Kafka -> UniqueVisitApp -> Kafka
 */

public class UniqueVisitApp {
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

        // 2.读取 kafka dwd_page_log 主题的数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // 3.每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        // 4.过滤数据 状态编程 只保留每个mid每天第一次登录的数据
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(
                new RichFilterFunction<JSONObject>() {

                    // 定义一个状态 这里采用时间和状态信息做 ValueState
                    private ValueState<String> dateState;
                    private SimpleDateFormat simpleDateFormat;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("date-state", String.class);

                        // 设置TTL，即超时时间，一般设置24小时
                        // 注意要设置TTL的更新时间，这里选择的是重新写入就更新TTL
                        StateTtlConfig stateTtlConfig = new StateTtlConfig
                                .Builder(Time.hours(24))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();

                        valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                        dateState = getRuntimeContext().getState(valueStateDescriptor);
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        // 取出上一跳页面信息
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");

                        // 判断页面信息是否为Null 为Null则返回False
                        if (lastPageId == null || lastPageId.length() <= 0) {

                            // 取出状态 如果状态为 Null 则返回False
                            String lastDate = dateState.value();

                            // 否则基于ts字段取出当天的日期，判断是否相同
                            String curDate = simpleDateFormat.format(value.getLong("ts"));
                            // 为了防止空指针异常，当前时间要在前
                            if (!curDate.equals(lastDate)) {
                                // 如果不相等，说明这是今天的第一次访问，返回 true 并更新状态
                                dateState.update(curDate);
                                return true;
                            }
                        }

                        // 其余情况全都返回 False
                        return false;
                    }
                });

        // 5.将数据写入kafka
        uvDS.print();
        uvDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        // 启动任务
        env.execute("UniqueVisitApp");
    }
}
