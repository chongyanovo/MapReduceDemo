//import java.sql.*;
//
//public class JDBCDemo {
//    public static void main(String[] args) throws ClassNotFoundException, SQLException {
//        //1.加载驱动
//        Class.forName("com.mysql.jdbc.Driver");
//        //2.用户信息和URL
//        String URL = "jdbc:mysql://localhost:3306/jdbcstudy?characterEncoding=utf8&useSSL=false";
//        String username = "root";
//        String password = "root";
//        //3.连接成功数据库对象
//        Connection connection = DriverManager.getConnection(URL, username, password);
//        //4.执行SQL对象
//        Statement statement = connection.createStatement();
//        //5.执行SQL对象去执行SQL语句，可能存在结果，查看返回结果
//        String sql = "SELECT * FROM users;";
//        ResultSet resultSet = statement.executeQuery(sql);
//        while (resultSet.next()) {
//            System.out.println("id = " + resultSet.getObject("id"));
//            System.out.println("name = " + resultSet.getObject("NAME"));
//            System.out.println("pwd = " + resultSet.getObject("PASSWORD"));
//            System.out.println("email = " + resultSet.getObject("email"));
//            System.out.println("birth = " + resultSet.getObject("birthday"));
//            System.out.println("=================");
//        }
//        //6.释放连接
//        resultSet.close();
//        statement.close();
//        connection.close();
//    }
//}
