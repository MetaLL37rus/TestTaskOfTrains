import au.com.bytecode.opencsv.CSVReader;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Stations {
    private Connection connection = ConnectionToH2.getConnection();
    private List<Integer> distance = new LinkedList();
    private Map<String, Integer> stations = new LinkedHashMap<>();

    public Stations() {
        createStationDB();
    }

    private void createStationDB() {
        addStationInMap();
        checkStations();
        try {
            Class.forName("org.h2.Driver").newInstance();
            Statement st = connection.createStatement();
            st.execute("drop table if exists Stations");
            st.execute("create table Stations (City varchar primary key not null , Distance int not null)");

            CSVReader reader = new CSVReader(new FileReader("CSV-Files\\Stations\\Stations.csv"));
            PreparedStatement stmt = connection.prepareStatement("insert into Stations (City, Distance) VALUES (?, ?)");
            String[] records;
            while ((records = reader.readNext()) != null) {
                stmt.setString(1, records[0]);
                stmt.setInt(2, Integer.parseInt(records[1]));
                stmt.executeUpdate();
            }
            System.out.println("База данных станций создана.");
        } catch (Exception e) {
            System.err.println("Ошибка создания базы данных станций");
            e.printStackTrace();
        }
    }

    //Проверка корректности расстояний
    private void checkStations() {
        try {
            CSVReader reader = new CSVReader(new FileReader("CSV-Files\\Stations\\Stations.csv"));
            String[] records;
            while ((records = reader.readNext()) != null) {
                distance.add(Integer.parseInt(records[1]));
            }
        } catch (Exception e) {
            System.err.println("Ошибка проверки станций");
            e.printStackTrace();
        }

        for (int i = 1; i < distance.size(); i++) {
            if (distance.get(i) <= distance.get(i - 1) || distance.get(i) < 0) {
                throw new IllegalArgumentException("Неккоректное расстояние в списке станций.");
            }
        }

    }

    protected void addStationInMap() {
        try {
            CSVReader reader = new CSVReader(new FileReader("CSV-Files\\Stations\\Stations.csv"));
            String[] records;
            while ((records = reader.readNext()) != null) {
                stations.put(records[0], Integer.parseInt(records[1]));
            }
        } catch (Exception e) {
            System.err.println("Ошибка добавления станций в мапу");
        }
    }

    public Map<String, Integer> getStations() {
        return stations;
    }

}

