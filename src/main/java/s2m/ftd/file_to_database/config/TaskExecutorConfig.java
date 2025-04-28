package s2m.ftd.file_to_database.config;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class TaskExecutorConfig {
    private final BatchProperties batchProperties;

    public TaskExecutorConfig(BatchProperties batchProperties) {
        this.batchProperties = batchProperties;
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
}
