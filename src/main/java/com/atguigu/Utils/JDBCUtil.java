package com.atguigu.Utils;



import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @PackageName:com.atguigu.Utils
 * @ClassNmae:PhenixUtil
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/30 21:25
 *
 *
 */


public class JDBCUtil {

    public static <T> List<T> QueryList(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) throws Exception {

        PreparedStatement preparedStatement=null;
        ResultSet resultSet=null;
        ArrayList<T> resultList=null;
        try {

            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            resultList = new ArrayList<>();
            //获取结果集元数据信息拿到列数
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();


            //处理结果集
            while (resultSet.next()){
                //创建泛型对象
                T t = clz.newInstance();

                for (int i = 1; i < columnCount+1; i++) {
                    //获取列名和列值
                    String columnName = metaData.getColumnName(i);
                    //判断是否需要对列名转换写法
                    if(underScoreToCamel){
                        CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                    }

                    Object value = resultSet.getObject(i);
                    //给T对象赋值
                    BeanUtils.setProperty(t,columnName,value);

                }
                resultList.add(t);

            }


        }finally {
            if (resultSet!=null){
                resultSet.close();
            }
            if (preparedStatement!=null){
             preparedStatement.close();
            }

        }

        //返回结果集
        return resultList;

    }

}
