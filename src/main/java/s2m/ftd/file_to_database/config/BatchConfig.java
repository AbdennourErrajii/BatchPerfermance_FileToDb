package s2m.ftd.file_to_database.config;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class BatchConfig {
    @Bean
    public Job TransactionCsvToDbJob(
            JobRepository jobRepository,
            @Qualifier("TransactionCsvToDbStep") Step simpleTransactionStep
    ) {
        return new JobBuilder("MultiThreadTransactionJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .flow(simpleTransactionStep)
                .end()
                .build();
    }


}
