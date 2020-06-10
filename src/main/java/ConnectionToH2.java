import java.sql.Connection;
import java.sql.DriverManager;

public class ConnectionToH2 {
    protected static Connection getConnection() {
        java.sql.Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:h2:~/TrainsAndStations", "sa", "");
        }catch (Exception e) {
            System.out.println("Ошибка соединения с БД.");
            e.printStackTrace();
        }
        return connection;
    }
}
