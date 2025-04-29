package s2m.ftd.file_to_database.config;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class BatchConfig {
    @Value("${batch.mode}")
    private String batchMode;
    @Bean
    public Job transactionCsvToDbJob(
            JobRepository jobRepository,
            @Qualifier("transactionCsvToDbStep") Step transactionCsvToDbStep
    ) {
        return new JobBuilder("transactionCsvToDbJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .flow(transactionCsvToDbStep)
                .end()
                .build();
    }
    /**
     * Conditionally create Step bean based on the batch mode configuration.
     */
    @Bean("transactionCsvToDbStep")
    public Step transactionCsvToDbStep(
            @Qualifier("singleThreadStep") Step singleThreadStep,
            @Qualifier("multiThreadStep") Step multiThreadStep,
            @Qualifier("asyncProcessingStep") Step asyncProcessingStep,
            @Qualifier("partitioningStep") Step partitioningStep
    ) {
        switch (batchMode) {
            case "multiThread":
                return multiThreadStep;
            case "asyncProcessing":
                return asyncProcessingStep;
            case "partitioning":
                return partitioningStep;
            case "singleThread":
            default:
                return singleThreadStep;
        }
    }


}
