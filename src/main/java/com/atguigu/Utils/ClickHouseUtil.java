package com.atguigu.Utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.Gmallconfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.IntSummaryStatistics;

/**
 * @PackageName:com.atguigu.Utils
 * @ClassNmae:ClickHouseUtil
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/26 2:04
 *
 * 首先明确使用jdbcsink
 * jdbcsink.sink（），返回值sinkfunction<T>
 *
 */


public class ClickHouseUtil {
    public static <T> SinkFunction<T> getJdbcSink(String sql){

            return JdbcSink.<T>sink(sql,
                    new JdbcStatementBuilder<T>() {
                        @Override
                        public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                            try {
                            //获取Bean对象所有属性字段
                            Field[] fields = t.getClass().getDeclaredFields();
                            //设置变量offset(占位符赋值错位)
                                int offset=0;

                            //循环填补占位符
                            for (int i = 0; i < fields.length; i++) {
                                //获取字段
                                Field field = fields[i];

                                //设置私有属性可访问
                                field.setAccessible(true);

                                //获取属性上的注解
                                TransientSink annotation = field.getAnnotation(TransientSink.class);
                                if (annotation !=null){
                                    offset++;
                                    continue;
                                }

                                //获取字段值
                                Object fieldvalue = null;
                                fieldvalue = field.get(t);

                                    //填补占位符
                                preparedStatement.setObject(i+1-offset,fieldvalue);
                                }

                            } catch (IllegalAccessException e) {
                                throw new RuntimeException(e);
                            }


                        }


                    },
                    new JdbcExecutionOptions.Builder()
                            .withBatchIntervalMs(200)
                            .withMaxRetries(5)
                            .withBatchSize(1000)
                            .build(),
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withDriverName(Gmallconfig.CLICKHOUSE_DRIVER)
                            .withUrl(Gmallconfig.CLICKHOUSE_URL)
                            .build()


            );
    }

}
