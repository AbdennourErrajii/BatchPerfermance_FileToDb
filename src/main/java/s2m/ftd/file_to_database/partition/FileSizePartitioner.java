package s2m.ftd.file_to_database.partition;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;


@Slf4j
@Component
@RequiredArgsConstructor
public class FileSizePartitioner implements Partitioner {
    private final Resource resource;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> partitions = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
            reader.readLine(); // Skip header
            long totalLines = reader.lines().count();
            if (totalLines == 0) {
                log.warn("Input file is empty after skipping header: {}", resource.getFilename());
                return partitions;
            }

            long linesPerPartition = (long) Math.ceil((double) totalLines / gridSize);
            for (int i = 0; i < gridSize; i++) {
                ExecutionContext context = new ExecutionContext();
                long start = i * linesPerPartition + 1;
                long end = Math.min((i + 1) * linesPerPartition, totalLines);
                context.putLong("startLine", start);
                context.putLong("endLine", end);
                context.putString("filePath", resource.getFile().getAbsolutePath());
                partitions.put("partition_" + i, context);
                log.debug("Created partition {}: lines {} to {}", i, start, end);
            }
        } catch (IOException e) {
            log.error("Failed to partition file: {}", resource.getFilename(), e);
            throw new IllegalStateException("Unable to partition file: " + resource.getFilename(), e);
        }
        return partitions;
    }
}