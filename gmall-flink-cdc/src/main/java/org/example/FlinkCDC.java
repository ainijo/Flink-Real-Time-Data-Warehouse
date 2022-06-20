package org.example;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启checkpoint并指定状态后端为 fs
        env.setStateBackend(new FsStateBackend("hdfs://node01:8020/gmall-flink/ck"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        // 老版本需要设置任务重启次数
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart());

        // 2.通过FlinkCDC构建SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("node02")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-flink")
                // 如果不加这个参数，则表示捕捉所有的变化
                // 为了防止不同库中的同名表的情况，则要通过 库名.表名 指定名称
                .tableList("gmall-flink.base_trademark")
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        // 3.打印数据
        streamSource.print();

        // 4.启动任务
        env.execute("FlinkCDC");
    }
}
