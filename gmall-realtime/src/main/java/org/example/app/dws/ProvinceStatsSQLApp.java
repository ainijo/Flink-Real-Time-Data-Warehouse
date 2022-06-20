package org.example.app.dws;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.bean.ProvinceStats;
import org.example.utils.ClickHouseUtil;
import org.example.utils.MyKafkaUtil;

// 数据流： web/app -> nginx -> SpringBoot -> MySQL -> FinkApp -> Kafka(ods) -> FlinkApp -> Kafka/Phoenix(dwd/dim) -> FlinkApp(redis) -> Kafka(dwm) -> FlinkApp -> ClickHouse
// 程序：            mockDB               -> MySQL -> FlinkCDC -> Kafka(ZK) -> BaseDBApp -> Kafka/Phoenix(Hbase,HDFS,ZK) -> OrderWideApp(redis) -> Kafka -> ProvinceStatsSQLApp -> ClickHouse
// keypoint 测试时间隔10s以上 造两次数据
public class ProvinceStatsSQLApp {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境并定义表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        // 2.使用DDL创建表 提取时间戳生成WaterMark
        /*
            CREATE TABLE order_wide (
              `province_name` STRING,
              `province_area_code` STRING,
              `province_iso_code` STRING,
              `province_3166_2_code` STRING,
              `order_id` BIGINT,
              `split_total_amount` DECIMAL,
              `rt` as TO_TIMESTAMP(create_time),
              WATERMARK FOR rt AS rt - INTERVAL '1' SECOND
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'dwm_order_wide',
              'properties.bootstrap.servers' = 'node01:9092',
              'properties.group.id' = 'province_stats_sql_app',
              'scan.startup.mode' = 'earliest-offset',
              'format' = 'json'
            )

            需要将 yyyy-MM-dd HH:mm:ss 转换为 TIMESTAMP(3)
            通过查 API 文档得知使用函数 TO_TIMESTAMP(string1[, string2]) 实现
         */
        String sourceTopic = "dwm_order_wide";
        String groupId = "province_stats_sql_app";
        // keypoint 和 JavaBean 一一对应
        tableEnv.executeSql("CREATE TABLE order_wide ( " +
                        " `province_id` BIGINT, " +
                        " `province_name` STRING, " +
                        " `province_area_code` STRING, " +
                        " `province_iso_code` STRING, " +
                        " `province_3166_2_code` STRING, " +
                        " `order_id` BIGINT, " +
                        " `split_total_amount` DECIMAL, " +
                        " `create_time` STRING, " +
                        " `rt` as TO_TIMESTAMP(create_time), " +
                        " WATERMARK FOR rt AS rt - INTERVAL '1' SECOND " +
                        ") WITH ( " +
                            MyKafkaUtil.getKafkaDDL(sourceTopic, groupId) +
                        ")");

        // 3.查询数据 分组、开窗和聚合
        /*
            不含开窗的语句：
            select
                province_name,
                province_area_code,
                province_iso_code,
                province_3166_2_code,
                count(distinct order_id) order_ct,
                sum(split_total_amount) total_amount
            from
                order_wide
            group by
                province_name,
                province_area_code,
                province_iso_code,
                province_3166_2_code

            引入开窗的语句(查找自文档的 Group Window Function 部分)：
            select
                DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt,
                DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND) , 'yyyy-MM-dd HH:mm:ss') edt,
                province_name,
                province_area_code,
                province_iso_code,
                province_3166_2_code,
                count(distinct order_id) order_ct,
                sum(split_total_amount) total_amount
            from
                order_wide
            group by
                province_name,
                province_area_code,
                province_iso_code,
                province_3166_2_code,
                TUMBLE(rt, INTERVAL '10' SECOND);
         */
        // 注意这里要和 JavaBean 一致
        Table table = tableEnv.sqlQuery(
                "select " +
                    "   DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt, " +
                    "   DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt, " +
                    "   province_id, " +
                    "   province_name, " +
                    "   province_area_code, " +
                    "   province_iso_code, " +
                    "   province_3166_2_code, " +
                    "   count(distinct order_id) order_count, " +
                    "   sum(split_total_amount) order_amount, " +
                    "   UNIX_TIMESTAMP()*1000 ts " +
                    "from " +
                    "   order_wide " +
                    "group by " +
                    "   province_id, " +
                    "   province_name, " +
                    "   province_area_code, " +
                    "   province_iso_code, " +
                    "   province_3166_2_code, " +
                    "   TUMBLE(rt, INTERVAL '10' SECOND)");

        // 4.将动态表转换为流
        // 因为做了开窗聚合 所以可以用AppendStream()
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(table, ProvinceStats.class);

        // 5.打印数据并写入 ClickHouse
        provinceStatsDataStream.print("provinceStatsDataStream>>>>>>>> ");
        provinceStatsDataStream.addSink(ClickHouseUtil.getSink("insert into province_stats values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"));

        // 6.启动任务
        env.execute("ProvinceStatsSQLApp");
    }
}
