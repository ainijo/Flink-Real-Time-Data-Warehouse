package org.example.app.dws;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.app.function.SplitFunction;
import org.example.bean.KeywordStats;
import org.example.utils.ClickHouseUtil;
import org.example.utils.MyKafkaUtil;

/*
  数据流：web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> ClickHouse
  程序： mockLog -> Nginx -> logger.sh -> Kafka(Zk) -> BaseLogApp -> Kafka -> KeywordStatsApp -> ClickHouse
 */

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境并创建表环境
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

        // 2.使用DDL方式读取Kafka数据创建表
        /*
            create table page_view(
                `common` Map<STRING, STRING>,
                `page` Map<STRING, STRING>,
                `ts` BIGINT,
                `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),
                 WATERMARK FOR rt AS rt - INTERVAL '1' SECOND
            )
         */
        String sourceTopic = "dwd_page_log";
        String groupId= "keyword_stats_app";
        tableEnv.executeSql(
                "create table page_view(  " +
                "   `common` Map<STRING, STRING>,  " +
                "   `page` Map<STRING, STRING>,  " +
                "   `ts` BIGINT,  " +
                "   `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "   WATERMARK FOR rt AS rt - INTERVAL '1' SECOND  " +
                ") WITH ( " +
                        MyKafkaUtil.getKafkaDDL(sourceTopic, groupId) +
                ")"
        );
        
        // 3.过滤数据
        // keypoint 根据上一跳页面为搜索页面和搜索词不为空来过滤
        /*
            select
                page['item'] full_word,
                rt
            from
                page_view
            where
                page['lase_page_id'] = 'search' and page['item'] is not null
         */
        Table fullWordTable = tableEnv.sqlQuery(
                "select " +
                        "   page['item'] full_word, " +
                        "   rt " +
                        "from " +
                        "   page_view " +
                        "where " +
                        "   page['last_page_id'] = 'search' and page['item'] is not null"
        );

        // 4.注册UDTF函数，进行分词处理
        tableEnv.createTemporaryFunction("split_words", SplitFunction.class);
        /*
            select
                word,
                rt
            from
                fullWordTable, LATERAL TABLE(split_words(full_word))
         */
        Table wordTable = tableEnv.sqlQuery(
                "select " +
                        "   word, " +
                        "   rt " +
                        "from " +
                        "   " + fullWordTable + ", LATERAL TABLE(split_words(full_word))"
        );

        // 5.分组、开窗和聚合
        /*
            select
                DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt,
                DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND) , 'yyyy-MM-dd HH:mm:ss') edt,
                word,
                count(*),
                UNIX_TIMESTAMP() * 1000 ts
            from
                wordTable
            group by
                word,
                TUMBLE(rt, INTERVAL '10' SECOND);
         */
        Table resultTable = tableEnv.sqlQuery(
                "select " +
                        "   'search' source, " +
                        "   DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt, " +
                        "   DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt, " +
                        "   word keyword, " +
                        "   count(*) ct, " +
                        "   UNIX_TIMESTAMP()*1000 ts " +
                        "from " +
                        "   " + wordTable + " " +
                        "group by " +
                        "   word, " +
                        "   TUMBLE(rt, INTERVAL '10' SECOND)");

        // 6.将动态表转化为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);

        // 7.打印并写入ClickHouse
        keywordStatsDataStream.print("keywordStatsDataStream>>>>>>>>");
        // keywordStatsDataStream.addSink(ClickHouseUtil.getSink("insert into keyword_stats values(?, ?, ?, ?, ?, ?)"));
        // keypoint 按照JavaBean即数据流的字段顺序指定字段 但是字段名称要按表来
        keywordStatsDataStream.addSink(ClickHouseUtil.getSink("insert into keyword_stats(keyword, ct, source, stt, edt, ts) values(?, ?, ?, ?, ?, ?)"));

        // 8.启动任务
        env.execute("KeywordStatsApp");
    }
}
