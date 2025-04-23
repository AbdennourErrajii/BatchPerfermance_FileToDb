package s2m.ftd.file_to_database;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.Bean;
import s2m.ftd.file_to_database.utils.CsvGenerator;

@Slf4j
@EnableTask
@SpringBootApplication
public class FileToDatabaseApplication {
	@Value("${inputfilepath}")
	private String inputFilePath;

	@Value("${csv.rows}")
	private int csvRows;
	public static void main(String[] args) {
		SpringApplication.run(FileToDatabaseApplication.class, args);
	}

	//@Bean
	public CommandLineRunner generateCsvAtStartup() {
		return args -> {
			log.warn("📄 Génération du fichier CSV : " + inputFilePath);
			CsvGenerator.generateCsv(inputFilePath, csvRows);
			log.info("✅ Fichier généré avec succès !");
		};
	}

}
