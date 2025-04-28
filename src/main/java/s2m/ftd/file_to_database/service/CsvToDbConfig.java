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
import org.springframework.transaction.PlatformTransactionManager;
import s2m.ftd.file_to_database.config.BatchProperties;
import s2m.ftd.file_to_database.listener.CustomReaderListener;
import s2m.ftd.file_to_database.model.Transaction;

import javax.sql.DataSource;
import java.beans.PropertyEditorSupport;
import java.time.LocalDate;
import java.util.Map;

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
                "transactionId", "carteId", "dateTransaction",
                "montant", "devise", "merchant", "pays", "typeCarte",
                "statut", "canal", "sourceCompte", "destinationCompte"
        });
        BeanWrapperFieldSetMapper<Transaction> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Transaction.class);
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
    public JdbcBatchItemWriter<Transaction> itemWriter() {
        JdbcBatchItemWriter<Transaction> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("INSERT INTO transaction (transaction_id, carte_id, date_transaction, montant, " +
                "devise, merchant, pays, type_carte, statut, canal, source_compte, destination_compte) " +
                "VALUES (:transactionId, :carteId, :dateTransaction, :montant, :devise, :merchant, " +
                ":pays, :typeCarte, :statut, :canal, :sourceCompte, :destinationCompte)");
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        return writer;
    }

    /**
     * Configures the Step to read, process, and write Transaction objects.
     */
    @Bean
    public Step TransactionCsvToDbStep(
            ItemReader<Transaction> transactionCsvReader,
            ItemProcessor<Transaction, Transaction> transactionProcessor,
            ItemWriter<Transaction> transactionWriter
    ) {
        return new StepBuilder("simpleTransactionStep", jobRepository)
                .<Transaction, Transaction>chunk(batchProperties.getChunkSize(), transactionManager)
                .reader(transactionCsvReader)
                .processor(transactionProcessor)
                .writer(transactionWriter)
                //.listener(new CustomReaderListener())
                .build();
    }
}
