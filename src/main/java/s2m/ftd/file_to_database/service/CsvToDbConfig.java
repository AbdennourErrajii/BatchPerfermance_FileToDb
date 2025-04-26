package s2m.ftd.file_to_database.service;

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
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import s2m.ftd.file_to_database.config.BatchProperties;
import s2m.ftd.file_to_database.model.Transaction;
import s2m.ftd.file_to_database.partition.CsvPartitionedItemReader;
import s2m.ftd.file_to_database.partition.FileSizePartitioner;


import javax.sql.DataSource;
@Configuration
@RequiredArgsConstructor
public class CsvToDbConfig {

    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final BatchProperties batchProperties;

    /**
     * Configures the ItemReader to read Transaction objects from a CSV file.
     */

    @Bean
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
    @Bean
    ItemProcessor<Transaction, Transaction> transactionProcessor() {
        return new TransactionItemProcessor();
    }

    /**
     * Configures the ItemWriter to write Transaction objects to the database.
     */
    @Bean
    public JdbcBatchItemWriter<Transaction> transactionDbWriter() {
        JdbcBatchItemWriter<Transaction> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("INSERT INTO transaction (transaction_id, groupe, carte_id, date_transaction, montant, " +
                "devise, merchant, pays, type_carte, statut, canal, source_compte, destination_compte) " +
                "VALUES (:transactionId, :groupe, :carteId, :dateTransaction, :montant, :devise, :merchant, " +
                ":pays, :typeCarte, :statut, :canal, :sourceCompte, :destinationCompte)");
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        return writer;
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(batchProperties.getThreadCount());
        executor.setMaxPoolSize(batchProperties.getThreadCount());
        executor.setThreadNamePrefix("partition-exec-");
        executor.setThreadFactory(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        executor.initialize();
        return executor;
    }

    /**
     * Handle the partitioned data
     */


    @Bean
    public TaskExecutorPartitionHandler partitionHandler() {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setTaskExecutor(taskExecutor());
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
                .writer(transactionDbWriter())
                .build();
    }


    @Bean
    public Step masterStep() {
        return new StepBuilder("masterStep", jobRepository)
                .partitioner(slaveStep().getName(), transactionPartitioner())
                .partitionHandler(partitionHandler())
                .build();
    }

    @Bean
    public Step transactionCsvToDbStep() {
        return masterStep();
    }
}
