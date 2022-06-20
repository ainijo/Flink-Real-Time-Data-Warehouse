package org.example.app.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.example.common.GmallConfig;
import org.example.utils.DimUtil;
import org.example.utils.ThreadPoolUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

// 采用泛型的方式设置异步I\O
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimAsyncJoinFunction<T> {

    private Connection connection = null;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化 phoenix 连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        // 初始化线程池
        threadPoolExecutor = ThreadPoolUtil.getThreadPool();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // 查询维度信息
                            // keypoint 获取表名和id
                            // 表名 参数
                            // id 通过 input 获得
                            // 获取查询的主键
                            String id = getKey(input);
                            JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);

                            // 补充维度信息
                            if (dimInfo != null) {
                                join(input, dimInfo);
                            }

                            // 输出补充好的数据
                            resultFuture.complete(Collections.singletonList(input));

                        } catch (Exception e) {
                            e.printStackTrace();;
                        }
                    }
                });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut: " + input);
    }
}
