package s2m.ftd.file_to_database.config;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import s2m.ftd.file_to_database.listener.CustomStepListener;
import s2m.ftd.file_to_database.listener.CustomWriteListener;
import s2m.ftd.file_to_database.model.Transaction;
import s2m.ftd.file_to_database.partition.FileSizePartitioner;
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
    private final FileSizePartitioner partitioner;

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
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threadCount);
        executor.setMaxPoolSize(threadCount);
        executor.setThreadNamePrefix("partition-exec-");
        executor.initialize();
        return executor;
    }
    @Bean
    public TaskExecutor taskExecutor2(){
        SimpleAsyncTaskExecutor asyncTaskExecutor=new SimpleAsyncTaskExecutor("spring_batch");
        asyncTaskExecutor.setConcurrencyLimit(threadCount);
        return asyncTaskExecutor;
    }

    @Bean
    public PartitionHandler partitionHandler() throws Exception {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setGridSize(gridSize);
        handler.setTaskExecutor(taskExecutor());
        handler.setStep(slaveStep());
        handler.afterPropertiesSet();
        return handler;
    }

    @Bean
    public Step masterStep() throws Exception {
        partitioner.setResource(new FileSystemResource(inputfilepath));
        return new StepBuilder("masterStep", jobRepository)
                .partitioner("slaveStep", partitioner)
                .partitionHandler(partitionHandler())
                .build();
    }

    @Bean
    public Step slaveStep() throws IOException {
        return new StepBuilder("slaveStep", jobRepository)
                .<Transaction, Transaction>chunk(chunkSize, transactionManager)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                //.taskExecutor(taskExecutor2())
                //.listener(new CustomWriteListener())
                //.listener(new CustomStepListener())
                //.listener(new CustomStepListener())
                .build();
    }
    @Bean
    public Job CsvToDbJob() throws Exception {
        return new JobBuilder("CsvToDbJob86", jobRepository)
                .start(masterStep())
                .build();
    }


}
