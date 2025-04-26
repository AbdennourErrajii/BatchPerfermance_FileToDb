package s2m.ftd.file_to_database.partition;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class FileSizePartitioner implements Partitioner {
    private final Resource resource;

    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> partitions = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
            // Lire le header et compter les lignes de données
            reader.readLine(); // Skip header
            long totalDataLines = reader.lines().count(); // Nombre de lignes de données (sans header)

            long linesPerPartition = totalDataLines / gridSize;
            long remainder = totalDataLines % gridSize;

            long start = 1; // Première ligne de données (après header)
            for (int i = 0; i < gridSize; i++) {
                ExecutionContext context = new ExecutionContext();
                long end = start + linesPerPartition - 1;
                if (i < remainder) end++;

                // Ajuster end pour ne pas dépasser les lignes de données
                if (end > totalDataLines) end = totalDataLines;

                // Les lignes dans le fichier commencent à 1 (après header)
                context.putLong("startLine", start);
                context.putLong("endLine", end);
                context.putString("filePath", resource.getFile().getAbsolutePath());
                partitions.put("partition_" + i, context);

                start = end + 1;
            }
        } catch (IOException e) {
            log.error("Erreur lors du partitionnement", e);
        }
        log.info("******"+partitions);
        return partitions;
    }
}