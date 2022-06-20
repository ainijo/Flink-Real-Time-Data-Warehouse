package org.example.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.utils.MyKafkaUtil;

/*
  数据流：web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd)
  程序： mockLog -> Nginx -> logger.sh -> Kafka(Zk) -> BaseLogApp -> Kafka
 */

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
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

        // 2.消费ods_log_base主题数据创建流
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // 3.每行数据转换为JSON对象
        // 定义一个outputTag收集脏数据
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        // 使用 process 算子处理 Json 数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> output) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            output.collect(jsonObject);
                        } catch (Exception e) {
                            // 异常数据送入脏数据侧输出流
                            ctx.output(dirtyTag, value);
                        }
                    }
                });

        // 使用侧输出流打印脏数据
        jsonObjDS.getSideOutput(dirtyTag).print("Dirty>>>>>>>>");

        // 4.新老用户效验 通过状态编程实现
        // 采用 mid 校验，这里要先用keyBy提取唯一的 mid
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                // 状态编程部分
                .map(
                        new RichMapFunction<JSONObject, JSONObject>() {
                            // 创建一个变量保存值的状态
                            private ValueState<String> valueState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
                            }

                            @Override
                            public JSONObject map(JSONObject value) throws Exception {
                                // 获取数据中的"is_new"标记
                                String isNew = value.getJSONObject("common").getString("is_new");
                                // 判断 isNew 是否为1
                                if (isNew.equals("1")) {
                                    // 获取状态数据
                                    String state = valueState.value();

                                    if (state != null) {
                                        // 有状态，说明不是新用户，则修改"is_new"标记
                                        value.getJSONObject("common").put("is_new", "0");
                                    } else {
                                        // 给状态赋值，使下一次状态值不为空
                                        valueState.update("1");
                                    }
                                }
                                return value;
                            }
                        });


        // 5.分流     页面：主流    启动：侧输出流    曝光：侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        // 涉及侧输出流 只能使用 process 函数
        SingleOutputStreamOperator<String> pageDS = jsonWithNewFlagDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<String> output) throws Exception {
                        // 获取启动日志字段
                        String start = value.getString("start");
                        if (start != null && start.length() > 0) {
                            // 数据写入启动日志侧输出流
                            ctx.output(startTag, start);
                        } else {
                            // 将数据写入业面日志主流
                            output.collect(value.toJSONString());

                            // 取出数据的曝光数据，注意是 displays
                            JSONArray displays = value.getJSONArray("displays");

                            // 压平写入侧输出流
                            if (displays != null && displays.size() > 0) {
                                // 获取页面ID
                                String pageId = value.getJSONObject("page").getString("page_id");
                                // 遍历曝光数据，写入曝光测输出流
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject display = displays.getJSONObject(i);

                                    // 添加页面id
                                    display.put("page_id", pageId);
                                    // 将输出写入曝光侧输出流
                                    ctx.output(displayTag, display.toJSONString());
                                }
                            }
                        }
                    }
                });

        // 6.提取测输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        // 7.打印到对应的kafka主题中
        startDS.print("Start>>>>>>>>");
        pageDS.print("Page>>>>>>>>");
        displayDS.print("Display>>>>>>>>");

        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        // 8.启动任务
        env.execute("BaseLogApp");
    }
}
