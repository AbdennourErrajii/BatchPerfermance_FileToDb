package s2m.ftd.file_to_database.partitioning;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;
import s2m.ftd.file_to_database.config.BatchProperties;
import s2m.ftd.file_to_database.config.TaskExecutorConfig;
import s2m.ftd.file_to_database.model.Transaction;
import s2m.ftd.file_to_database.services.TransactionDbWriter;
import s2m.ftd.file_to_database.services.TransactionItemProcessor;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class CsvToDbConfigPartitioning {

    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final BatchProperties batchProperties;
    private final TaskExecutorConfig taskExecutorConfig;

    /**
     * Configures the ItemReader to read Transaction objects from a CSV file.
     */

    @Bean("partitioningTransactionCsvReader")
    @StepScope
    public ItemReader<Transaction> transactionCsvReader(
            @Value("#{stepExecutionContext['startLine']}") Long startLine,
            @Value("#{stepExecutionContext['endLine']}") Long endLine) {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.putLong("startLine", startLine);
        executionContext.putLong("endLine", endLine);
        return new CsvPartitionedItemReader(new FileSystemResource(batchProperties.getInputFile()), executionContext);
    }

    /**
     * Configures the ItemProcessor to process Transaction objects.
     */
    @Bean("partitioningTransactionProcessor")
    public ItemProcessor<Transaction, Transaction> transactionProcessor() {
        return new TransactionItemProcessor();
    }

    /**
     * Configures the ItemWriter to write Transaction objects to the database.
     */
    @Bean("partitioningTransactionWriter")
    public ItemWriter<Transaction> transactionWriter() {
        return new TransactionDbWriter(dataSource);
    }
    @Bean
    public TaskExecutorPartitionHandler partitionHandler() {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setTaskExecutor(taskExecutorConfig.taskExecutor());
        handler.setStep(slaveStep());
        handler.setGridSize(batchProperties.getPartitionCount());
        return handler;
    }
    @Bean
    public Partitioner transactionPartitioner() {
        return new FileSizePartitioner(new FileSystemResource(batchProperties.getInputFile()));
    }

    @Bean
    public Step slaveStep() {
        return new StepBuilder("slaveStep", jobRepository)
                .<Transaction, Transaction>chunk(batchProperties.getChunkSize(), transactionManager)
                .reader(transactionCsvReader(null,null)) // Injected at runtime
                .processor(transactionProcessor())
                .writer(transactionWriter())
                .build();
    }


    @Bean
    public Step masterStep() {
        return new StepBuilder("masterStep", jobRepository)
                .partitioner(slaveStep().getName(), transactionPartitioner())
                .partitionHandler(partitionHandler())
                .build();
    }

    @Bean("partitioningStep")
    public Step transactionCsvToDbStep() {
        return masterStep();
    }
}
