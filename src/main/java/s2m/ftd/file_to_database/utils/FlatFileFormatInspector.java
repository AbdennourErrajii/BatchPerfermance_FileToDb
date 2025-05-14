package s2m.ftd.file_to_database.utils;
import java.io.File;
import java.io.IOException;

public class FlatFileFormatInspector {
    public static String determineFileFormatWithTika(String filePath) throws IOException {
        File file = new File(filePath);

        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        // Create the file if it doesn't exist
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new IOException("Failed to create file: " + filePath, e);
            }
        }

        // Determine file format based on extension
        String fileName = file.getName().toLowerCase();
        if (fileName.endsWith(".csv")) {
            return "CSV";
        } else if (fileName.endsWith(".txt")) {
            return "TEXT";
        } else {
            return "UNKNOWN";
        }
    }
}