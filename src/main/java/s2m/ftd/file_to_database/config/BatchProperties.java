package s2m.ftd.file_to_database.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Configuration properties for Spring Batch job, prefixed with "batch".
 * Defines settings for chunk size, retries, partitioning, and input file path.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString
@Validated
@ConfigurationProperties(prefix = "batch")
public class BatchProperties {

    /** Size of chunks for batch processing (default: 5000). */
    @Min(1)
    @Max(10_000)
    private int chunkSize = 5000;

    /** Maximum number of items to skip on failure (default: 10). */
    @Min(1)
    private int skipLimit = 10;

    /** Maximum number of retry attempts for failed operations (default: 3). */
    @Min(1)
    @Max(10)
    private int maxRetries = 3;

    /**
     * Time duration to wait before the first retry attempt after a failure (default: 3s).
     */
    @NotNull
    private Duration backoffInitialDelay = Duration.ofSeconds(3);

    /** Factor by which the delay between consecutive retries is multiplied (default: 2). */
    @Min(1)
    @Max(5)
    private int backoffMultiplier = 2;


    /** Threshold to trigger partitioning (default: 20). */
    @Min(1)
    private int triggerPartitioningThreshold = 20;

    /** Name of the task executor for parallel processing (optional). */
    private String taskExecutor;

    /** Path to the input CSV file (default: classpath resource). */
    private String inputFile = "./data/transactions.csv";

    /** Number of threads for parallel processing (default: 4). */
    @Min(1)
    @Max(16)
    private int threadCount = 5;

    /** Number of rows in the CSV file (default: 1000). */
    @Min(1)
    private int rowsCsv = 1000;

    /** Number of partitions for parallel processing (default: 3). */
    @Min(1)
    @Max(128)
    private int partitionCount = 20;

}
