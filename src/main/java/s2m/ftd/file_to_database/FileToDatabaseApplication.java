package s2m.ftd.file_to_database;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.Bean;
import s2m.ftd.file_to_database.config.BatchProperties;
import s2m.ftd.file_to_database.utils.CsvGenerator;

@Slf4j
@EnableTask
@SpringBootApplication
@EnableConfigurationProperties(BatchProperties.class)
@RequiredArgsConstructor
public class FileToDatabaseApplication {
	private final BatchProperties batchProperties;
	public static void main(String[] args) {
		SpringApplication.run(FileToDatabaseApplication.class, args);
	}

	/**
	 * Generates a sample CSV file in the data folder.
	 */
	@Bean
	public CommandLineRunner generateCsvAtStartup() {
		return args -> {
			log.info("Generating CSV file: {}", batchProperties.getInputFile());
			CsvGenerator.generateCsv(batchProperties.getInputFile(), batchProperties.getRowsCsv());
			log.info("CSV file generated successfully");
		};
	}

}
