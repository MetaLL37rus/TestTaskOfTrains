import au.com.bytecode.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

public class Main {
    private static Connection connection = ConnectionToH2.getConnection();


    public static void main(String[] args) {
        createScheduleDB();

        if (args.length == 0) {
            System.out.println("Доступные команды:");
            System.out.println("-station 'Путь до файла'  = Загрузить список станций.");
            System.out.println("-train 'Путь до файла'  =  Добавить поезд в расписание.");
            System.out.println("-? 'номер поезда'  =  Посмотреть расписание конкретного поезда.");
            System.out.println("-schedule 'Путь до файла'  =  Выгрузить расписание в .csv-файл.");
        } else if (args[0].equalsIgnoreCase("-stations")) {

            if (!args[1].endsWith(".csv")) throw new IllegalArgumentException("Файл не формата .csv");
            try {
                Path originalPath = Paths.get(args[1]);
                Path copied = Paths.get("CSV-Files\\Stations\\Stations.csv");
                Files.copy(originalPath, copied, StandardCopyOption.REPLACE_EXISTING);

            } catch (Exception e) {
                System.err.println("Файл не найден.");
                e.printStackTrace();
            }
        } else if (args[0].equalsIgnoreCase("-train")) {
            if (!args[1].endsWith(".csv")) throw new IllegalArgumentException("Фал не формата .csv");
            try {
                Train train = new Train(new File(args[1]));
            } catch (Exception e) {
                System.err.println("Файл не найден.");
                e.printStackTrace();
            }
        } else if (args[0].equalsIgnoreCase("-?")) {
            String number = args[1];
            try {
                Statement st = connection.createStatement();
                ResultSet rs = st.executeQuery("select * from schedule where train = " + number);
                while (rs.next()) {
                    System.out.println(rs.getString(2) + "\t" + rs.getTime(3) + "\t" + rs.getTime(4));
                }
            } catch (Exception e) {
                System.err.println("Такого поезда нет в расписании.");
            }
        } else if (args[0].equalsIgnoreCase("-schedule")) {
            String output = args[1];
            List<String[]> list = new LinkedList<>();
            try {
                Statement st = connection.createStatement();
                ResultSet rs = st.executeQuery("select * from schedule");

                while (rs.next()) {
                    list.add(new String[]{rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)});
                }

                CSVWriter csvWriter = new CSVWriter(new FileWriter(output), ',', ' ');

                csvWriter.writeAll(list);
                csvWriter.close();

            } catch (Exception e) {
                System.err.println("Ошибка выгрузки расписания в файл .csv");
                e.printStackTrace();
            }
        }
    }

    private static void createScheduleDB() {
        try {
            Statement st = connection.createStatement();
            st.execute("create table if not exists schedule (Train varchar, City varchar, Arrival time, Departure time)");

        } catch (Exception e) {
            System.err.println("Ошибка создания базы данных расписания поездов.");
            e.printStackTrace();
        }
    }

}
