package org.example.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.app.function.CustomerDeserialization;
import org.example.utils.MyKafkaUtil;

public class FlinkCDC {
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

        // 2.通过FlinkCDC构建SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("node02")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-flink")
                .deserializer(new CustomerDeserialization())
                // 初始化
                // .startupOptions(StartupOptions.initial())
                // 这里只捕获最新的变化(但是第一次启动要用初始化)
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        // 3.打印数据并写入kafka
        streamSource.print();
        String sinkTopic = "ods_base_db";
        streamSource.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        // 4.启动任务
        env.execute("FlinkCDC");
    }
}
