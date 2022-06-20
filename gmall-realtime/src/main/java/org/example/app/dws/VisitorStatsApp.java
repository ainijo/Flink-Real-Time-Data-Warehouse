package org.example.app.dws;


/*

 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.bean.VisitorStats;
import org.example.utils.ClickHouseUtil;
import org.example.utils.DateTimeUtil;
import org.example.utils.MyKafkaUtil;

import java.time.Duration;
import java.util.Date;

/*
  数据流：web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> Kafka(dwm) -> FlinkApp -> ClickHouse
  程序： mockLog -> Nginx -> logger.sh -> Kafka(Zk) -> BaseLogApp -> Kafka -> VisitorStatsApp -> ClickHouse
 */

public class VisitorStatsApp {
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

        // 2.读取Kafka数据，创建三个流
        String groupId = "visitor_stats_app";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String pageViewSourceTopic = "dwd_page_log";

        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));

        // 3.每个流处理成相同的数据类型
        // 3.1 处理UV数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDS = uvDS.map(line -> {

            // 解析成 JSON 对象
            JSONObject jsonObject = JSON.parseObject(line);

            // 提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats(
                    "", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));
        });
        // 3.2 处理UJ数据
        // keypoint 测试发现 uj 数据基本都被丢弃 原因是 uj 数据处理完了相比于 ts 已经过了 10s 了 这时窗口已经被关闭了
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDS = ujDS.map(line -> {
            // 解析成 JSON 对象
            JSONObject jsonObject = JSON.parseObject(line);

            // 提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats(
                    "", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts"));
        });

        // 3.3 处理PV数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvDS = pvDS.map(line -> {
            // 解析成 JSON 对象
            JSONObject jsonObject = JSON.parseObject(line);

            // 提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");

            // 提取page字段
            JSONObject page = jsonObject.getJSONObject("page");
            // 获取页面信息的上一跳的页面id
            String lase_page_id = page.getString("lase_page_id");
            Long sv = 0L;
            if (lase_page_id == null || lase_page_id.length() <= 0) {
                sv = 1L;
            }

            return new VisitorStats(
                    "", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L, page.getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        // 4.Union流
        DataStream<VisitorStats> unionDS = visitorStatsWithUvDS.union(visitorStatsWithUjDS, visitorStatsWithPvDS);

        // 5.提取事件时间生成WaterMark
        // keypoint 由于uj数据容易被丢弃是因为uj流需要10s进行处理，所以通过增加窗口的时间进行处理
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats visitorStats, long l) {
                                return visitorStats.getTs();
                            }
                        }
                )
        );

        // 6.按照维度信息进行分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWMDS.keyBy(
                new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                        return new Tuple4<String, String, String, String>(
                                value.getAr(),
                                value.getCh(),
                                value.getIs_new(),
                                value.getVc());
                    }
                });

        // 7.开窗聚合，使用10s的滚动窗口
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        // reduce function + window function
        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                        /*
                            return new VisitorStats(
                                    value1.getStt(),
                                    value1.getEdt(),
                                    value1.getCh(),
                                    value1.getAr(),
                                    value1.getIs_new(),
                                    value1.getUv_ct() + value2.getUv_ct()
                                    );
                         */
                        // keypoint 只有滚动窗口可以这样做 滑动窗口会覆盖数据
                        value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                        value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                        value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                        value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                        value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                        return value1;
                    }
                },
                new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                        // 提取窗口信息
                        long start = window.getStart();
                        long end = window.getEnd();

                        VisitorStats visitorStats = input.iterator().next();

                        // 补充窗口信息
                        visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                        visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));

                        out.collect(visitorStats);
                    }
                });

        // 8.数据写入ClickHouse
        result.print("visitorStatsResult>>>>>>>>");
        result.addSink(ClickHouseUtil.getSink("insert into visitor_stats values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"));

        // 9.启动任务
        env.execute("VisitorStatsApp");
    }
}
