package CDR_Comutator;


import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Сервис, генерирующий Call Data Record (CDR) файлы.
 * Все записи из файлов дублируются в таблицу локальной базы данных.
 * Один файл - месяц записей, генерируется 12 файлов (год записей).
 * @author Зезина Кристина Максимовна
 * @version 1.0
 */
public class CDR_Comutator {
    /** Поле хранящее количество секунд в месяце*/
    private final long SEC_IN_MONTH = 30*24*60*60;
    /** После с максимальным количеством записей, которое будет генерироваться в одном файле */
    private static final int PACK_SIZE = 100;
    /** Поле с нижней границей начала тарификации, Wed Jan 01 2014 00:00:00 GMT+0000 */
    private static final int MIN_TIME_TARIFFICATION = 1388520000;
    /** Поле с верхней границей начала тарификации, Sun Mar 17 2024 00:00:00 GMT+0000 */
    private static final int MAX_TIME_OF_TARIFFICATION = 1710622800;
    /** Поле с минимальной длительностью возможного звонка в секундах*/
    private static final int MIN_CALLING_TIME = 60;
    /** Поле с максимальной длительностью возможного звонка в секундах */
    private static final int MAX_CALLING_TIME = 1800;

    /** Процедура подключения к локальной базе данных.
     * @see Connection
     * */
    private Connection connection = null;
    public CDR_Comutator() {
        this.connection = ConnectionH2.getConnection();
    }

    /** Процедура генерации CDR файла и заполнения таблицы в базе данных.
     * Таблица очищается перед заполнением, чтобы избежать повторения уникальных идентификаторов.
     * @see CDR_Comutator#resetCDRTable()
     * @see CDR_Comutator#generateSetOfCDRs()
     * @see CDR_Comutator#createDBofCDRs()
     * */
    public void generate() {
        Random rand = new Random();
        for (int i = 1; i <= 12; i++) {
            long startTime = rand.nextLong(MIN_CALLING_TIME, MAX_CALLING_TIME);
            resetCDRTable();
            generateCDR(startTime, i);
            createDBofCDRs(i);
        }
    }

    /** Процедура очистки базы данных.
     * @see Connection
     * @see ConnectionH2
     * */
    public void resetCDRTable() {
        String query_delete = "DROP TABLE IF EXISTS CALLS;";
        String query_create = "CREATE TABLE CALLS(ID INT PRIMARY KEY, TYPE VARCHAR(2), MSISDN VARCHAR(11), " +
                "STARTCALLIN VARCHAR(255), ENDCALLIN VARCHAR(255));";
        Connection connection = ConnectionH2.getConnection();
        try {
            connection.createStatement().execute(query_delete);
            connection.createStatement().execute(query_create);
            connection.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /** Процедура заполнения таблицы в базе данных.
     * Таблица содержит все те же записи, что и файлы.
     * @see CDR_Comutator#insertFile(int, Statement, AtomicInteger)
     * @see Connection
     * */
    private void createDBofCDRs(int num) {
        try (Statement statement = connection.createStatement()){
            AtomicInteger currRow = new AtomicInteger(0);
            try {
                insertFile(num, statement, currRow);
                int[] result = statement.executeBatch();
                connection.commit();
            } catch (BatchUpdateException e) {
                connection.rollback();
                System.out.println(e.getMessage());
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /** Процедура обработки CDR файла для добавления его записей в таблицу.
     *  Файл обрабатывается построчно процедурой insertFragment.
     * @param numOfFile номер файла для обработки
     * @param statement sql оператор
     * @param currRow номер строки, на которой остановилась запись
     * @see CDR_Comutator#insertFragment(String, Statement, AtomicInteger)
     * @see FileCreator#getPath(int)
     * @see Statement
     *  */
    private void insertFile(int numOfFile, Statement statement, AtomicInteger currRow) {
        String path = FileCreator.getPath(numOfFile);
        FileReader fileReader = null;

        try {
            fileReader = new FileReader(path);
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }

        try (BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            for(String line; (line = bufferedReader.readLine()) != null; ) {
                currRow.addAndGet(1);
                insertFragment(line, statement, currRow);
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /** Процедура обработки строки CDR файла.
     *  На основе данных из строки формируется SQL запрос, который добавляется в пакет для обработки.
     *  @param line обрабатываемая строка из файла
     *  @param statement sql оператор
     *  @param currRow номер строки, на которой остановилась запись
     *  @see Connection
     *  @see Statement
     *  @see CDR_Comutator#addToBatch(Statement, String)
     *  */
    private void insertFragment(@NotNull String line, @NotNull Statement statement, @NotNull AtomicInteger currRow) {
        String[] data = line.replace("\n", "").split(",");
        String query = "INSERT INTO CALLS(ID, TYPE, MSISDN, STARTCALLIN, ENDCALLIN) VALUES\n" +
                "(" +
                currRow.get() + "," +
                data[0] + "," +
                data[1] + "," +
                data[2] + "," +
                data[3] +
                ");";
        addToBatch(statement, query);
    }

    /** Процедура добавления запроса в пакет для обрадотки.
     * @param statement sql оператор
     * @param query sql запрос
     * @see Statement
     * */
    private void addToBatch(@NotNull Statement statement, @NotNull String query) {
        try {
            statement.addBatch(query);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /** Процедура создания одного CDR файла.
     *  Количество записей определяется случайным образом
     * @param startTime начало периода времени для генерации
     * @param numOfFile конец периода времени для генерации
     * @see CDR_Comutator#getRandomMSISDNs(int)
     * @see FileCreator#getPath(int)
     * @see FileCreator#createFile(int)
     * @see CDR_Comutator#generateFragment(Long, Long, String)
     *  */
    private void generateCDR(Long startTime, int numOfFile) throws  RuntimeException{
        Random rand = new Random();
        int amountToGenerate = rand.nextInt(50, PACK_SIZE);
        List<String> msisdNs = getRandomMSISDNs(amountToGenerate);
        String path = FileCreator.getPath(numOfFile);
        FileCreator.createFile(numOfFile);

        try (Writer writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8)
        )) {
            for (int i = 0; i < amountToGenerate; i++) {
                writer.write(generateFragment(startTime, startTime + SEC_IN_MONTH, msisdNs.get(i)));
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Функция генерации одной строки файла.
     * @param timeOfBeginning время начала звонка
     * @param timeOfEnding время конца звонка
     * @param msisdn номер мобильного абонента
     * @return Строка файла
     * */
    private @NotNull String generateFragment(Long timeOfBeginning, Long timeOfEnding, String msisdn) {
        String type = generateRandomType();

        Random rand = new Random();
        long startOfCall = rand.nextLong(timeOfBeginning, timeOfEnding);
        long endOfCall = startOfCall + rand.nextLong(MIN_CALLING_TIME, MAX_CALLING_TIME);

        return type + "," +
                msisdn + "," +
                startOfCall + "," +
                endOfCall + "\n";
    }

    /** Функция получения списка случайных номеров телефонов из локальной базы данных.
     * @param amount необходиммое количество номеров
     * @see CDR_Comutator#getOneMSISDN(Connection, List)
     * @return Список заданной размерности случайных номеров
     * */
    private @NotNull List<String> getRandomMSISDNs(int amount) throws RuntimeException{
        List<String> numbers = new ArrayList<>(amount);
        try {
            for (int i = 1; i <= amount; i++) {
                getOneMSISDN(connection, numbers);
            }
        }  catch (SQLException e) {
            System.out.println("Cannot generate random numbers");
        }
        return numbers;
    }

    /** Процедура получения одного случайного номера телефона из локальной базы данных.
     * @param connection соединение с базой данных
     * @param numbers список номеров
     * */
    private void getOneMSISDN(@NotNull Connection connection, List<String> numbers) throws SQLException {
        Random rand = new Random();
        int ranNum = rand.nextInt(1, 21);
        String query = String.format("SELECT MSISDN FROM MSISDNS WHERE id=%2d;", ranNum);
        ResultSet result = connection.createStatement().executeQuery(query);
        if (result.next()) {
            numbers.add(result.getString(1));
        }
    }
    /** Функция генерации случайного типа звонка.
     * @return Тип звонка
     * */
    private @NotNull String generateRandomType() {
        Random rand = new Random();
        int randInt = rand.nextInt(1, 3);
        return "0" + randInt;
    }
}
