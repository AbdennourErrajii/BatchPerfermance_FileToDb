package s2m.ftd.file_to_database.service;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import s2m.ftd.file_to_database.config.BatchProperties;
import s2m.ftd.file_to_database.listener.CustomReaderListener;
import s2m.ftd.file_to_database.model.Transaction;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;

import javax.sql.DataSource;
import java.beans.PropertyEditorSupport;
import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.Future;

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
    public ItemReader<Transaction> transactionCsvReader() {
        FlatFileItemReader<Transaction> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource(batchProperties.getInputFile()));
        reader.setLineMapper(lineMapper());
        reader.setLinesToSkip(1); // Skip header row
        reader.setStrict(true); // Fail if the file is not found
        return reader;
    }
    /**
     * Configures the LineMapper to map CSV lines to Transaction objects.
     */
    private DefaultLineMapper<Transaction> lineMapper() {
        DefaultLineMapper<Transaction> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[]{
                "transactionId", "groupe", "carteId", "dateTransaction",
                "montant", "devise", "merchant", "pays", "typeCarte",
                "statut", "canal", "sourceCompte", "destinationCompte"
        });
        BeanWrapperFieldSetMapper<Transaction> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Transaction.class);
        fieldSetMapper.setCustomEditors(Map.of(
                LocalDate.class, new PropertyEditorSupport() {
                    @Override
                    public void setAsText(String text) {
                        setValue(LocalDate.parse(text));
                    }
                }
        ));
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
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
     *  Process Transaction items asynchronously, delegating to the transactionProcessor and using the configured taskExecutor
     */
    @Bean
    public AsyncItemProcessor<Transaction, Transaction> asyncItemProcessor() {
        AsyncItemProcessor<Transaction, Transaction> asyncProcessor = new AsyncItemProcessor<>();
        asyncProcessor.setDelegate(transactionProcessor());
        asyncProcessor.setTaskExecutor(taskExecutor());
        return asyncProcessor;
    }


    /**
     * Write Transaction items asynchronously, delegating to the transactionDbWriter for database operations.
     */
    @Bean
    public AsyncItemWriter<Transaction> asyncItemWriter() { // Inject delegate
        AsyncItemWriter<Transaction> asyncWriter = new AsyncItemWriter<>();
        asyncWriter.setDelegate(transactionDbWriter());
        return asyncWriter;
    }

    /**
     * Configures the Step to read, process, and write Transaction objects.
     */
    @Bean
    public Step TransactionCsvToDbStep(
    ) {
        return new StepBuilder("AsyncProcessingTransactionStep", jobRepository)
                .<Transaction, Future<Transaction>>chunk(batchProperties.getChunkSize(), transactionManager)
                .reader(transactionCsvReader())
                .processor(asyncItemProcessor())
                .writer(asyncItemWriter())
                //.listener(new CustomReaderListener())
                .build();
    }
}
