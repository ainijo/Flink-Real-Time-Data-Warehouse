package org.example.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtil {

    // 通用的方法，用泛型定义
    public static <T> List<T> queryList(Connection connection, String querySQL, Class<T> clz, boolean underScoreToCamel) throws Exception {
        // 创建集合用于存放结果数据
        ArrayList<T> resultList = new ArrayList<>();

        // 预编译SQL
        // 工具类的异常直接往外抛
        PreparedStatement preparedStatement = connection.prepareStatement(querySQL);

        // 执行
        ResultSet resultSet = preparedStatement.executeQuery();

        // 解析resultSet
        // 获取resultSet的元数据信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        // 从元数据信息中获取列的数量
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {

            // 创建泛型对象
            T t = clz.newInstance();

            // 给泛型对象赋值
            // 注意JDBC的resultSet中所有的值都是从1开始遍历的
            for (int i = 1; i < columnCount + 1; i++) {
                // 获取列名
                String columnName = metaData.getColumnName(i);

                // 判断是否需要驼峰命名
                if (underScoreToCamel) {
                    // 先统一转换成下划线，再转换成驼峰命名
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }

                // 获取列值
                Object value = resultSet.getObject(i);

                // 通过工具类向泛型对象赋值
                BeanUtils.setProperty(t, columnName, value);
            }
            // 将该对象添加至结果集合
            resultList.add(t);
        }

        // 关闭结果集合
        preparedStatement.close();
        resultSet.close();

        // 返回结果集合
        return resultList;
    }
}
