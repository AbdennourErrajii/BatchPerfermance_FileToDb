package s2m.ftd.file_to_database.multiThread;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.transaction.PlatformTransactionManager;
import s2m.ftd.file_to_database.config.BatchProperties;
import s2m.ftd.file_to_database.config.TaskExecutorConfig;
import s2m.ftd.file_to_database.listeners.CustomReaderListener;
import s2m.ftd.file_to_database.model.Transaction;
import s2m.ftd.file_to_database.services.TransactionCsvReader;
import s2m.ftd.file_to_database.services.TransactionDbWriter;
import s2m.ftd.file_to_database.services.TransactionItemProcessor;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class CsvToDbConfigMultiThread {

    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final BatchProperties batchProperties;
    private final TaskExecutorConfig taskExecutorConfig;

    /**
     * Configures the ItemReader to read Transaction objects from a CSV file.
     */

    @Bean("multiThreadTransactionCsvReader")
    public ItemReader<Transaction> transactionCsvReader() {
        return new TransactionCsvReader(batchProperties);
    }

    /**
     * Configures the ItemProcessor to process Transaction objects.
     */
    @Bean("multiThreadTransactionProcessor")
    public ItemProcessor<Transaction, Transaction> transactionProcessor() {
        return new TransactionItemProcessor();
    }

    /**
     * Configures the ItemWriter to write Transaction objects to the database.
     */
    @Bean("multiThreadTransactionWriter")
    public ItemWriter<Transaction> transactionWriter() {
        return new TransactionDbWriter(dataSource);
    }

    /**
     * Configures the Step to read, process, and write Transaction objects.
     */
    @Bean("multiThreadStep")
    public Step transactionCsvToDbStep(
            @Qualifier("multiThreadTransactionCsvReader") ItemReader<Transaction> transactionCsvReader,
            @Qualifier("multiThreadTransactionProcessor") ItemProcessor<Transaction, Transaction> transactionProcessor,
            @Qualifier("multiThreadTransactionWriter") ItemWriter<Transaction> transactionWriter
    ) {
        return new StepBuilder("multiThreadTransactionStep", jobRepository)
                .<Transaction, Transaction>chunk(batchProperties.getChunkSize(), transactionManager)
                .reader(transactionCsvReader)
                .processor(transactionProcessor)
                .writer(transactionWriter)
                .taskExecutor(taskExecutorConfig.taskExecutor())
                //.listener(new CustomReaderListener())
                .build();
    }
}
