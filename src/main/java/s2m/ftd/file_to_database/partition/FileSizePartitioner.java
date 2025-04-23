package s2m.ftd.file_to_database.partition;


import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class FileSizePartitioner implements Partitioner {
    private Resource resource;

    public void setResource(Resource resource) {
        this.resource = resource;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> partitions = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
            // Ignorer l'en-tête si nécessaire
            reader.readLine();

            long totalLines = reader.lines().count();
            long linesPerPartition = (long) Math.ceil((double) totalLines / gridSize);

            for (int i = 0; i < gridSize; i++) {
                ExecutionContext context = new ExecutionContext();
                long start = i * linesPerPartition + 1;
                long end = Math.min((i + 1) * linesPerPartition, totalLines);

                context.putLong("minLine", start);
                context.putLong("maxLine", end);
                context.putString("filePath", resource.getFile().getAbsolutePath());

                partitions.put("partition_" + i, context);
                log.info("Partition {}: lignes {} à {}", i, start, end);
            }
        } catch (IOException e) {
            throw new RuntimeException("Erreur de partitionnement du fichier", e);
        }
        return partitions;
    }
}