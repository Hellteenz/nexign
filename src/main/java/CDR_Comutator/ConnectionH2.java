package CDR_Comutator;

import java.sql.*;

/** Сервис, устанавливающий соединение с локальной базой данных
 *  @author Зезина Кристина
 *  @version 1.0
 *  */
public class ConnectionH2 {
    public static final String DB_URL = "jdbc:h2:~/nexign/database";
    public static final String USERNAME = "sa";
    public static final String PASSWORD = "sa";
    public static final String DB_Driver = "org.h2.Driver";
    public static Connection getConnection() {
        try {
            Class.forName(DB_Driver);
            return DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
