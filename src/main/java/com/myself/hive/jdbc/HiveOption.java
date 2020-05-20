package com.myself.hive.jdbc;

import java.sql.*;

/**
 * @author zxq
 * 2020/5/14
 */
public class HiveOption {

    public static void main(String[] args) {
        String driver="org.apache.hive.jdbc.HiveDriver";
        //和集群配置url相同，同为主机名或同为ip地址
        String url="jdbc:hive2://hadoop129:10000/default";
        String user="root";

        HiveOption hiveOption = new HiveOption();
        try {
            //获取连接
            Connection connection = hiveOption.getConnection(driver, url, user, null);
            //获取句柄或者
            Statement statement = connection.createStatement();
            PreparedStatement preparedStatement = connection.prepareStatement("");
            //执行sql
            statement.execute("create table student(id int,name string)");
            //关闭资源
            hiveOption.doClose(statement,connection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //获取连接
    private Connection getConnection(String driver,String url,String user,String password) throws ClassNotFoundException, SQLException {
        //加载驱动
        Class.forName(driver);
        return DriverManager.getConnection(url,user,password);
    }


    //关闭资源
    private void doClose(Statement statement,Connection connection) throws SQLException {
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
