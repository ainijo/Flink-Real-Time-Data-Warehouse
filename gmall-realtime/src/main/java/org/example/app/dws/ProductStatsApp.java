package org.example.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.app.function.DimAsyncFunction;
import org.example.bean.OrderWide;
import org.example.bean.PaymentWide;
import org.example.bean.ProductStats;
import org.example.common.GmallConstant;
import org.example.utils.ClickHouseUtil;
import org.example.utils.DateTimeUtil;
import org.example.utils.MyKafkaUtil;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/*
    数据流：
        app/web -> nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> ClickHouse
        app/web -> nginx -> SpringBoot -> MySQL -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka/Phoenix -> FlinkApp -> Kafka(dwm) -> FlinkApp -> ClickHouse
    程序：
        mock -> nginx -> logger.sh -> Kafka(ZK)/Phoenix(HDFS/HBase) -> Redis -> ClickHouse
 */

public class ProductStatsApp {
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

        // 2.读取 Kafka 数据创建流（7个主题）
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> favorDS = env.addSource(MyKafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId));
        DataStreamSource<String> cartDS = env.addSource(MyKafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId));
        DataStreamSource<String> orderDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> payDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId));
        DataStreamSource<String> refundDS = env.addSource(MyKafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentDS = env.addSource(MyKafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId));

        // 3.将7个流统一数据格式
        // 3.1 曝光
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pvDS.flatMap(
                new FlatMapFunction<String, ProductStats>() {
                    @Override
                    public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                        // 将数据转换为JSON格式
                        JSONObject jsonObject = JSON.parseObject(value);

                        // 取出 page 信息
                        JSONObject page = jsonObject.getJSONObject("page");
                        String pageId = page.getString("page_id");

                        // keypoint 注意这里的 ts 是从 jsonObject 中提取的
                        Long ts = jsonObject.getLong("ts");

                        // 判断是否为商品详情页
                        // keypoint 为了保证访问的不是广告页面 再增加一个 item_type 的判断
                        if ("good_detail".equals(pageId) && "sku_id".equals(page.getString("item_type"))) {
                            // 收集点击行为数据
                            out.collect(ProductStats.builder()
                                    .sku_id(page.getLong("item"))
                                    .click_ct(1L)
                                    .ts(ts)
                                    .build());
                        }

                        // 尝试取出曝光数据
                        JSONArray displays = jsonObject.getJSONArray("displays");
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                // 获取单条曝光数据
                                JSONObject display = displays.getJSONObject(i);

                                // 判断是否曝光的是商品
                                if ("sku_id".equals(display.getString("item_type"))) {
                                    out.collect(ProductStats.builder()
                                            .sku_id(display.getLong("item"))
                                            .click_ct(1L)
                                            .ts(ts)
                                            .build());
                                }
                            }
                        }
                    }
                });

        // 3.2 收藏
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorDS.map(line -> {
            // 解析 JSON 对象
            JSONObject jsonObject = JSON.parseObject(line);

            // 提取对应的字段
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 3.3 加入购物车
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartDS.map(line -> {
            // 解析 JSON 对象
            JSONObject jsonObject = JSON.parseObject(line);

            // 提取对应的字段
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 3.4 下单
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderDS.map(line -> {

            // 解析 JSON 对象
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);

            // 去重
            HashSet<Object> orderIds = new HashSet<>();
            orderIds.add(orderWide.getOrder_id());

            // 提取对应的字段
            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    // 这里不能使用总金额
                    .order_amount(orderWide.getOrder_price())
                    .orderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        // 3.5 支付
        SingleOutputStreamOperator<ProductStats> productStatsWithPayDS = payDS.map(line -> {
            // 解析JSON对象
            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);

            // 去重
            HashSet<Object> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());

            // 提取对应的字段
            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getOrder_price())
                    .paidOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });

        // 3.6 退款
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(line -> {
            // 解析 JSON 对象
            JSONObject jsonObject = JSON.parseObject(line);

            // 去重
            HashSet<Object> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));

            // 提取对应的字段
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 3.7 评价
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentDS.map(line -> {
            // 解析 JSON 对象
            JSONObject jsonObject = JSON.parseObject(line);

            // 判断该评价是否为好评
            String appraise = jsonObject.getString("appraise");
            long gootCt = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                gootCt = 1L;
            }

            // 提取对应的字段
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(gootCt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 4.union7个流
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(
                productStatsWithFavorDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPayDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS
        );

        // 5.提取时间戳生成WaterMark
        SingleOutputStreamOperator<ProductStats> productStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(
                new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats productStats, long l) {
                        return productStats.getTs();
                    }
                }));

        // 6.分组、开窗、聚合
        //   按照 sku_id 分组，滚动窗口设置为10s，结合增量聚合（累加值）和全量聚合（提取窗口信息）
        SingleOutputStreamOperator<ProductStats> reduceDS = productStatsWithWMDS.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<ProductStats>() {
                            @Override
                            public ProductStats reduce(ProductStats value1, ProductStats value2) throws Exception {

                                value1.setDisplay_ct(value1.getDisplay_ct() + value2.getDisplay_ct());
                                value1.setClick_ct(value1.getClick_ct() + value2.getClick_ct());

                                value1.setCart_ct(value1.getCart_ct() + value2.getCart_ct());

                                value1.setFavor_ct(value1.getFavor_ct() + value2.getFavor_ct());

                                value1.setOrder_sku_num(value1.getOrder_sku_num() + value2.getOrder_sku_num());
                                value1.setOrder_amount(value1.getOrder_amount().add(value2.getOrder_amount()));
                                // keypoint HashSet也要合并，但是数量的计算放在窗口函数中做
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());

                                value1.setPayment_amount(value1.getPayment_amount().add(value2.getPayment_amount()));
                                value1.getPaidOrderIdSet().addAll(value2.getPaidOrderIdSet());

                                value1.setRefund_amount(value1.getRefund_amount().add(value2.getRefund_amount()));
                                value1.getRefundOrderIdSet().addAll(value2.getRefundOrderIdSet());

                                value1.setComment_ct(value1.getComment_ct() + value2.getComment_ct());
                                value1.setGood_comment_ct(value1.getGood_comment_ct() + value2.getComment_ct());

                                return value1;
                            }
                        },
                        new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                            @Override
                            public void apply(Long aLong, TimeWindow timeWindow, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                                // 取出数据
                                ProductStats productStats = input.iterator().next();

                                // 补充字段
                                // 设置窗口时间
                                productStats.setStt(DateTimeUtil.toYMDhms(new Date(timeWindow.getStart())));
                                productStats.setEdt(DateTimeUtil.toYMDhms(new Date(timeWindow.getEnd())));

                                // keypoint 设置订单、支付和退款数量
                                productStats.setOrder_ct(productStats.getOrderIdSet().size() + 0L);
                                productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                                productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

                                // 将数据写出
                                out.collect(productStats);
                            }
                        });

        // 7.关联维度
        // 7.1 关联 SKU 维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {

                    @Override
                    public String getKey(ProductStats input) {
                        return input.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {

                        input.setSku_name(dimInfo.getString("SKU_NAME"));
                        input.setSku_price(dimInfo.getBigDecimal("PRICE"));
                        input.setSpu_id(dimInfo.getLong("SPU_ID"));
                        input.setTm_id(dimInfo.getLong("TM_ID"));
                        input.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));

                    }
                }, 60, TimeUnit.SECONDS);

        // 7.2 关联 SPU 维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats input) {
                        return String.valueOf(input.getSpu_id());
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);


        // 7.3 关联 Category 维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS = AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats input) {
                        return input.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setCategory3_name(dimInfo.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        // 7.4 关联 TM 维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTMDS = AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(ProductStats input) {
                        return String.valueOf(input.getSpu_id());
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setTm_name(dimInfo.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        // 8.写入ClickHouse
        productStatsWithTMDS.print("productStatsWithTMDS>>>>>>>>");
        // 注意有25个字段
        productStatsWithTMDS.addSink(ClickHouseUtil.getSink("insert into table product_stats values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"));

        // 9.启动并等待结束
        env.execute("ProductStatsApp");
    }
}
