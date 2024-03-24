package CDR_Comutator;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;

/** Класс, преднозначенный для работы с файловой системой для CDRGenerationService
 * @author Зезина Кристина
 * @version 1.0
 * */
class FileCreator {
    /** Процедура создания нового файла для записи данных.
     * @param numOfFile порядковый номер файла
     * */
    public static void createFile(int numOfFile) throws RuntimeException {
        File file = new File(getPath(numOfFile));
        file.getParentFile().mkdirs();
        try {
            file.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    /** Функция получения пути до файла.
     * @param numOfFile порядковый номер файла
     * @return Путь к CDR-файлу с заданным порядковым номмером
     * */
    public static @NotNull String getPath(int numOfFile){
        String filename = String.format("%2d_CDR.txt", numOfFile);
        return "src" + File.separator + "main" + File.separator
                + "resources" + File.separator + "CDRs" + File.separator + filename;
    }

}