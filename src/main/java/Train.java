import au.com.bytecode.opencsv.CSVReader;

import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalTime;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;

public class Train {
    private Connection connection = ConnectionToH2.getConnection();
    private Stations stations = new Stations();
    private File file;
    private int countErrors;
    private String number;
    private StringBuilder name;


    public Train(File file) {
        this.file = file;
        addTrainToSchedule();
    }

    //Добавление поезда в расписание
    private void addTrainToSchedule() {
        List<String> cityList = new LinkedList<>();
        number = file.getName().substring(0, file.getName().length() - 4);
        name = new StringBuilder("Train" + number);

        if (!file.getName().endsWith(".csv")) throw new IllegalArgumentException(file.getName());

        checkCurrentTime();
        checkSpeed();

        try {
            Statement st = connection.createStatement();
            st.execute("drop table if exists " + name);
            st.execute("create table " + name + " (Train varchar default " + number + ", City varchar not null, Arrival time, Departure time)");

            CSVReader reader = new CSVReader(new FileReader(file.getAbsoluteFile()), ',', ' ', 1);

            PreparedStatement stmt = connection.prepareStatement("insert into " + name + " (City, Arrival, Departure) VALUES (?, ?, ?)");
            String[] records;
            while ((records = reader.readNext()) != null) {
                cityList.add(records[0]);
                stmt.setString(1, records[0]);

                if (!records[1].equals("")) {
                    stmt.setTime(2, Time.valueOf(LocalTime.parse(records[1])));
                } else {
                    stmt.setString(2, null);
                }
                if (records.length == 3) {
                    stmt.setTime(3, Time.valueOf(LocalTime.parse(records[2])));
                } else {
                    stmt.setString(3, null);
                }
                stmt.executeUpdate();
            }

            ResultSet rs = st.executeQuery("select * from  " + name + ", schedule where " + name + ".City = schedule.City " +
                    "and datediff(minute, schedule.Departure, " + name + ".Departure) <= 2 and datediff(minute, schedule.Departure, " + name + ".Departure) >= -2");

            while (rs.next()) {
                System.err.println("Неккоректное время отправления: ");
                System.err.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(4));
                countErrors++;
            }

            if (!isScheduleEmpty()) {
                checkOvertaking();
            }

            if (countErrors == 0) {
                st.execute("insert into schedule (Train, City, Arrival, Departure) select * from " + name);
            } else
                System.err.println("Поезд " + number + " не добавлен в расписание. Исправьте указанные выше ошибки. ");

            st.execute("drop table " + name);
        } catch (Exception e) {
            System.err.println("Ошибка добавления поезда в базу данных.");
            e.printStackTrace();
        }
    }


    //Проверка корректной последовательности прибытия и отправки
    private void checkCurrentTime() {
        List<LocalTime> timeList = new LinkedList<>();
        try {
            CSVReader reader = new CSVReader(new FileReader(file), ',', ' ', 1);
            String[] records;
            while ((records = reader.readNext()) != null) {
                if (!(records[1].equals(""))) {
                    timeList.add(LocalTime.parse(records[1]));
                }
                if (records.length == 3) {
                    timeList.add(LocalTime.parse(records[2]));
                }
            }
        } catch (Exception e) {
            System.err.println("Ошибка проверки время в файле " + file.getName());
            e.printStackTrace();
        }

        for (int i = 1; i < timeList.size(); i++) {
            if (Duration.between(timeList.get(i - 1), timeList.get(i)).toMinutes() < 0) {
                throw new IllegalArgumentException("Неккоректное время в " + file.getName());
            }
        }
    }

 //Проверка превышения скорости
    private void checkSpeed() {
        try {
            CSVReader reader = new CSVReader(new FileReader(file), ',', ' ', 1);
            List<String[]> records = reader.readAll();
            int distance;
            double speed;
            for (int i = 1; i < records.size(); i++) {
                if (records.get(i)[1].equals("")) {
                    distance = stations.getStations().get(records.get(i)[0]) - stations.getStations().get(records.get(i - 1)[0]);

                    speed = formulaSpeed(LocalTime.parse(records.get(i - 1)[2]), LocalTime.parse(records.get(i)[2]), distance);
                    if (speed > 100) {
                        System.err.println("Превышение скорости между " + records.get(i - 1)[0] + " и " + records.get(i)[0] + ": " + speed);
                        countErrors++;

                    }
                } else {
                    distance = stations.getStations().get(records.get(i)[0]) - stations.getStations().get(records.get(i - 1)[0]);
                    speed = formulaSpeed(LocalTime.parse(records.get(i - 1)[2]), LocalTime.parse(records.get(i)[1]), distance);
                    if (speed > 100) {
                        System.err.println("Превышение скорости Поезда №" + number + " между " + records.get(i - 1)[0] + " и " + records.get(i)[0] + ": " + speed);
                        countErrors++;

                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Ошибка проверки скорости в " + file.getName());
            e.printStackTrace();
        }
    }

    private double formulaSpeed(LocalTime start, LocalTime finish, int distance) {
        double speed = 0;
        try {
            speed = distance / (Duration.between(start, finish).toMinutes() / (60 * 1.0));
            if (speed < 0) throw new IllegalArgumentException("Отрицательное значение скорости");
        } catch (Exception e) {
            System.err.println("Ошибка расчета скорости");
            e.printStackTrace();
        }
        return speed;
    }
    //Проверка расписания
    private boolean isScheduleEmpty() {
        boolean isEmpty = true;
        try {
            Statement st = connection.createStatement();

            ResultSet rs = st.executeQuery("select * from schedule");

            if (rs.next()) isEmpty = false;

        } catch (Exception e) {
            System.err.println("Ошибка проверки размера расписания.");
        }
        return isEmpty;
    }

    //Логика проверки обгонов
    private void checkOvertaking() {
        long firstVar = 0;
        long secondVar = 0;
        try {
            CSVReader reader = new CSVReader(new FileReader(file), ',', ' ', 1);
            List<String[]> trainData = reader.readAll();
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("select * from schedule");
            List<String[]> schedule = new LinkedList<>();
            while (rs.next()) {
                if (rs.getString(1).equals(String.valueOf(number)))
                    throw new IllegalArgumentException("Поезд №" + number + " уже есть в расписании.");
                schedule.add(new String[]{rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)});
            }

            for (int i = 0; i < trainData.size() - 1; i++) {
                for (int j = 0; j < schedule.size() - 1; j++) {
                    if (trainData.get(i)[0].equals(schedule.get(j)[1])) {

                            if (trainData.get(i + 1)[1].equals("") && schedule.get(j + 1)[2] == null) {
                                firstVar = Duration.between(LocalTime.parse(trainData.get(i)[2]), LocalTime.parse(schedule.get(j)[3])).toMinutes();
                                secondVar = Duration.between(LocalTime.parse(trainData.get(i + 1)[2]), LocalTime.parse(schedule.get(j + 1)[3])).toMinutes();
                            } else if (trainData.get(i + 1)[1].equals("")) {
                                firstVar = Duration.between(LocalTime.parse(trainData.get(i)[2]), LocalTime.parse(schedule.get(j)[3])).toMinutes();
                                secondVar = Duration.between(LocalTime.parse(trainData.get(i + 1)[2]), LocalTime.parse(schedule.get(j + 1)[2])).toMinutes();
                            } else if (schedule.get(j + 1)[2] == null) {
                                firstVar = Duration.between(LocalTime.parse(trainData.get(i)[2]), LocalTime.parse(schedule.get(j)[3])).toMinutes();
                                secondVar = Duration.between(LocalTime.parse(trainData.get(i + 1)[1]), LocalTime.parse(schedule.get(j + 1)[3])).toMinutes();
                            } else {
                                firstVar = Duration.between(LocalTime.parse(trainData.get(i)[2]), LocalTime.parse(schedule.get(j)[3])).toMinutes();
                                secondVar = Duration.between(LocalTime.parse(trainData.get(i + 1)[1]), LocalTime.parse(schedule.get(j + 1)[2])).toMinutes();
                            }


                        if (firstVar < 0 && secondVar > 0 && schedule.get(j + 1)[2] == null) {
                            System.err.print("Недопустимый обгон: ");
                            System.err.println("Добавляемый Поезд №" + number + " обогнал поезд №" + schedule.get(j)[0] +
                                    " между станциями " + trainData.get(i)[0] + " и " + trainData.get(i + 1)[0]);
                            countErrors++;
                        }

                        if (firstVar > 0 && secondVar < 0 && trainData.get(i + 1)[1].equals("")) {
                            System.err.print("Недопустимый обгон: ");
                            System.err.println("Ранее добавленный в расписании Поезд №" + schedule.get(j)[0] + " обогнал добавляемый поезд №" + number +
                                    " между станциями " + trainData.get(i)[0] + " и " + trainData.get(i + 1)[0]);
                            countErrors++;
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Ошибка проверки обгонов");
            e.printStackTrace();
        }
    }
}
