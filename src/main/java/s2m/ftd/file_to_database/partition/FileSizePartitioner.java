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

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> partitions = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
            reader.readLine();  // Skip the header

            long totalLines = reader.lines().count();
            long linesPerPartition = (long) Math.ceil((double) totalLines / gridSize);

            log.info("Total lines: {}. Lines per partition: {}", totalLines, linesPerPartition);

            for (int i = 0; i < gridSize; i++) {
                ExecutionContext context = new ExecutionContext();
                long start = i * linesPerPartition + 1;
                long end = Math.min((i + 1) * linesPerPartition, totalLines);

                context.putLong("startLine", start);
                context.putLong("endLine", end);
                partitions.put("partition_" + i, context);
                context.putString("partitionName", "partition_" + i);

                log.info("Partition {}: start line = {} , end line = {}", i, start, end);
            }
        } catch (IOException e) {
            log.error("Error reading the file for partitioning", e);
            throw new RuntimeException("Error during partitioning", e);
        }
        return partitions;
    }
}