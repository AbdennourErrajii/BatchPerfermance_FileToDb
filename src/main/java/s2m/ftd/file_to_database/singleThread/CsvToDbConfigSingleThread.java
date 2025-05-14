        package s2m.ftd.file_to_database.singleThread;

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
        import org.springframework.core.io.FileSystemResource;
        import org.springframework.transaction.PlatformTransactionManager;
        import s2m.ftd.file_to_database.config.BatchProperties;
        import s2m.ftd.file_to_database.customs.CustomItemProessor;
        import s2m.ftd.file_to_database.model.Transaction;
        import s2m.ftd.file_to_database.services.*;

        import javax.sql.DataSource;

        @Configuration
        @RequiredArgsConstructor
        public class CsvToDbConfigSingleThread {

            private final DataSource dataSource;
            private final JobRepository jobRepository;
            private final PlatformTransactionManager transactionManager;
            private final BatchProperties batchProperties;
            private final TransactionItemProcessor transactionItemProcessor;

            /**
             * Configures the ItemReader to read Transaction objects from a CSV file.
             */
            @Bean("singleThreadTransactionCsvReader")
            public ItemReader<Transaction> transactionCsvReader() {
                return new TransactionCsvReader(batchProperties);
            }

            /**
             * Configures the ItemProcessor to process Transaction objects.
             */

            @Bean("singleThreadTransactionProcessor")
            public CustomItemProessor<Transaction, Transaction> transactionProcessor()  {
                return new CustomItemProessor<>(transactionItemProcessor);
            }
            /**
             * Configures the ItemWriter to write Transaction objects to the database.
             */
            @Bean("singleThreadTransactionWriter")
            public ItemWriter<Transaction> transactionWriter() {
                return new TransactionDbWriter(dataSource);
            }

            /**
             * Configures the Step to read, process, and write Transaction objects.
             */

            @Bean
            public TransactionCsvWriter csvWriter() {
                return new TransactionCsvWriter();
            }

            @Bean
            public TransactionTextWriter textWriter() {
                return new TransactionTextWriter();
            }
            @Bean("singleThreadStep")
            public Step transactionCsvToDbStep(
                    @Qualifier("singleThreadTransactionCsvReader") ItemReader<Transaction> transactionCsvReader,
                    @Qualifier("singleThreadTransactionProcessor") ItemProcessor<Transaction, Transaction> transactionProcessor,
                    @Qualifier("singleThreadTransactionWriter") ItemWriter<Transaction> transactionWriter
            ) {
                return new StepBuilder("singleThreadTransactionStep", jobRepository)
                        .<Transaction, Transaction>chunk(batchProperties.getChunkSize(), transactionManager)
                        .reader(transactionCsvReader)
                        .processor(transactionProcessor)
                        .writer(textWriter())
                        //.faultTolerant()
                        //.skip(Exception.class)
                        //.listener(new CustomSkipListener())
                        //.noRollback(Exception.class)
                        //.processorNonTransactional()
                        .build();
            }
        }
