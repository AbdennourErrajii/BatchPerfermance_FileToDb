package s2m.ftd.file_to_database.services;


import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.FileSystemResource;
import s2m.ftd.file_to_database.model.Transaction;
import s2m.ftd.file_to_database.config.BatchProperties;

public class TransactionCsvReader implements ItemReader<Transaction> {

    private final FlatFileItemReader<Transaction> reader;

    public TransactionCsvReader(BatchProperties batchProperties) {
        this.reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource(batchProperties.getInputFile()));
        reader.setLineMapper(lineMapper());
        reader.setLinesToSkip(1); // Skip header row
        reader.setStrict(true); // Fail if the file is not found
    }

    private DefaultLineMapper<Transaction> lineMapper() {
        DefaultLineMapper<Transaction> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[]{
                "transactionId", "carteId", "dateTransaction", "montant", "devise", "merchant",
                "pays", "typeCarte", "statut", "canal", "sourceCompte", "destinationCompte"
        });
        BeanWrapperFieldSetMapper<Transaction> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Transaction.class);
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }

    @Override
    public Transaction read() throws Exception {
        return reader.read();
    }
}
