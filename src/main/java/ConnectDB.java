import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectDB {
    private static Connection connection;

//    public static Connection getConnection() throws ClassNotFoundException, SQLException {
//        Class.forName("com.mysql.jdbc.Driver");
//        connection = DriverManager.getConnection("jdbc:mysql://192.168.133.140:3306/flink?characterEncoding=UTF-8", "root", "123456");
////        System.out.println("数据库连接成功");
//        return connection;
//    }

    private static DruidDataSource dataSource;
    public static Connection getConnection() throws Exception {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://192.168.133.140:3306/flink");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        //设置初始化连接数，最大连接数，最小闲置数
        dataSource.setInitialSize(10);
        dataSource.setMaxActive(50);
        dataSource.setMinIdle(5);
        //返回连接
        return  dataSource.getConnection();
    }

}
