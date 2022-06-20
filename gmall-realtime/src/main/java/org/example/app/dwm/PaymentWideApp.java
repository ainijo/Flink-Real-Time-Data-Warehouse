package org.example.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.example.bean.OrderWide;
import org.example.bean.PaymentInfo;
import org.example.bean.PaymentWide;
import org.example.utils.MyKafkaUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;

// 数据流： web/app -> nginx -> SpringBoot -> MySQL -> FinkApp -> Kafka(ods) -> FlinkApp -> Kafka/Phoenix(dwd/dim) -> FlinkApp(redis) -> Kafka(dwm) -> FlinkApp -> Kafka(dwm)
// 程序：            mockDB               -> MySQL -> FlinkCDC -> Kafka(ZK) -> BaseDBApp -> Kafka/Phoenix(Hbase,HDFS,ZK) -> OrderWideApp(redis) -> Kafka(dwm) -> PaymentWideApp -> Kafka(dwm)

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {

        // 1.准备环境
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

        // 2.读取Kafka主题的数据创建流并转换为JavaBean对象 提取时间戳转换为Watermark
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        SingleOutputStreamOperator<OrderWide> orderWideDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderWide>() {
                                    @Override
                                    public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {
                                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                        try {
                                            return sdf.parse(orderWide.getCreate_time()).getTime();
                                        } catch (ParseException e) {
                                            e.printStackTrace();
                                            // 异常则返回recordTimestamp
                                            return recordTimestamp;
                                        }
                                    }
                                }));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                        try {
                                            return sdf.parse(paymentInfo.getCreate_time()).getTime();
                                        } catch (ParseException e) {
                                            e.printStackTrace();
                                            // 异常则返回recordTimestamp
                                            return recordTimestamp;
                                        }
                                    }
                                }));

        // 3.双流Join
        // 由于是支付表去关联订单表，因此是 -15 而不是正15
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.minutes(5))
                .process(
                        new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<PaymentWide> out) throws Exception {
                                out.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        });

        // 4.将数据写入Kafka
        paymentWideDS.print("paymentWideDS>>>>>>>>");
        paymentWideDS.map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(paymentWideSinkTopic));

        // 5.启动并等待结束
        env.execute("PaymentWideApp");
    }
}
