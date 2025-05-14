package s2m.ftd.file_to_database.asyncProcessing;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.transaction.PlatformTransactionManager;
import s2m.ftd.file_to_database.config.BatchProperties;
import s2m.ftd.file_to_database.config.TaskExecutorConfig;
import s2m.ftd.file_to_database.customs.CustomItemProessor;
import s2m.ftd.file_to_database.listeners.*;
import s2m.ftd.file_to_database.model.Transaction;
import s2m.ftd.file_to_database.services.TransactionCsvReader;
import s2m.ftd.file_to_database.services.TransactionDbWriter;
import s2m.ftd.file_to_database.services.TransactionItemProcessor;

import javax.sql.DataSource;
import java.util.concurrent.Future;

@Configuration
@RequiredArgsConstructor
public class CsvToDbConfigAsyncProcessing {

    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final BatchProperties batchProperties;
    private final TaskExecutorConfig taskExecutorConfig;
    private final TransactionItemProcessor transactionItemProcessor;

    /**
     * Configures the ItemReader to read Transaction objects from a CSV file.
     */
    @Bean("asyncProcessingTransactionCsvReader")
    public ItemReader<Transaction> transactionCsvReader() {
        return new TransactionCsvReader(batchProperties);
    }

    /**
     * Configures the ItemProcessor to process Transaction objects.
     */
    @Bean("asyncProcessingTransactionProcessor")
    public CustomItemProessor<Transaction, Transaction> transactionProcessor()  {
        return new CustomItemProessor<>(transactionItemProcessor);
    }

    /**
     * Configures the ItemWriter to write Transaction objects to the database.
     */
    @Bean("asyncProcessingTransactionWriter")
    public ItemWriter<Transaction> transactionWriter() {
        return new TransactionDbWriter(dataSource);
    }
    /**
     *  Process Transaction items asynchronously, delegating to the transactionProcessor and using the configured taskExecutor
     */
    @Bean
    public AsyncItemProcessor<Transaction, Transaction> asyncItemProcessor() {
        AsyncItemProcessor<Transaction, Transaction> asyncProcessor = new AsyncItemProcessor<>();
        asyncProcessor.setDelegate(transactionProcessor());
        asyncProcessor.setTaskExecutor(taskExecutorConfig.taskExecutor());
        return asyncProcessor;
    }


    /**
     * Write Transaction items asynchronously, delegating to the transactionDbWriter for database operations.
     */
    @Bean
    public AsyncItemWriter<Transaction> asyncItemWriter() { // Inject delegate
        AsyncItemWriter<Transaction> asyncWriter = new AsyncItemWriter<>();
        asyncWriter.setDelegate(transactionWriter());
        return asyncWriter;
    }

    /**
     * Configures the Step to read, process, and write Transaction objects.
     */
    @Bean("asyncProcessingStep")
    public Step transactionCsvToDbStep(
            @Qualifier("asyncProcessingTransactionCsvReader") ItemReader<Transaction> transactionCsvReader
    ) {
        return new StepBuilder("AsyncProcessingTransactionStep", jobRepository)
                .<Transaction, Future<Transaction>>chunk(batchProperties.getChunkSize(), transactionManager)
                .reader(transactionCsvReader)
                .processor(asyncItemProcessor())
                .writer(asyncItemWriter())
                //.listener(new CustomChunkListener())
                //.listener(new CustomReaderListener())
                //.listener(new CustomProcessorListener())
                //.listener(new CustomWriterListener())
                .build();
    }
}
