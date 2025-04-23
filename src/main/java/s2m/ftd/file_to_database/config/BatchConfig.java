package s2m.ftd.file_to_database.config;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import s2m.ftd.file_to_database.listener.CustomChunkListener;
import s2m.ftd.file_to_database.listener.CustomStepListener;
import s2m.ftd.file_to_database.listener.CustomWriteListener;
import s2m.ftd.file_to_database.model.Transaction;
import s2m.ftd.file_to_database.processor.TransactionItemProcessor;
import s2m.ftd.file_to_database.reader.TransactionCsvReader;
import s2m.ftd.file_to_database.writer.TransactionItemWriter;

import javax.sql.DataSource;
import java.io.IOException;

@Configuration
@RequiredArgsConstructor
public class BatchConfig {
    private final PlatformTransactionManager transactionManager;
    private final JobRepository jobRepository;
    private final DataSource dataSource;

    @Value("${inputfilepath}")
    private String inputfilepath;

    @Value("${threadCount}")
    private int threadCount;

    @Value("${chunkSize}")
    private int chunkSize;

    @Value("${gridSize}")
    private int gridSize;

    // Configuration du Reader
    @Bean
    public TransactionCsvReader reader() throws IOException {
        Resource resource = new FileSystemResource(inputfilepath);
        return new TransactionCsvReader(resource);
    }

    // Configuration du Processor
    @Bean
    public TransactionItemProcessor processor() {
        return new TransactionItemProcessor();
    }

    // Configuration du Writer
    @Bean
    public TransactionItemWriter writer() {
        return new TransactionItemWriter(new JdbcTemplate(dataSource));
    }

    @Bean
    public Step step1() throws IOException {
        return new StepBuilder("CsvToDb_Step", jobRepository)
                .<Transaction, Transaction>chunk(chunkSize, transactionManager)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .listener(new CustomStepListener())
                .listener(new CustomChunkListener())
                .listener(new CustomWriteListener())
                .build();
    }
    @Bean
    public Job CsvToDbJob() throws Exception {
        return new JobBuilder("CsvToDbJob88", jobRepository)
                .start(step1())
                .build();
    }


}
