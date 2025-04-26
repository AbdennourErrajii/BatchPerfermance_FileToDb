package s2m.ftd.file_to_database.partition;

import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.Resource;
import s2m.ftd.file_to_database.model.Transaction;

import java.beans.PropertyEditorSupport;
import java.time.LocalDate;
import java.util.Map;

public class CsvPartitionedItemReader implements ItemReader<Transaction> {
    private final FlatFileItemReader<Transaction> delegate;
    private long currentLine = 0;
    private long start;
    private long end;
    private final Resource resource;
    private boolean initialized = false;

    // Constructor to initialize with Resource and ExecutionContext
    public CsvPartitionedItemReader(Resource resource, ExecutionContext executionContext) {
        this.resource = resource;
        this.start = executionContext.getLong("startLine");
        this.end = executionContext.getLong("endLine");
        this.delegate = new FlatFileItemReader<>();
        this.delegate.setResource(resource);
        this.delegate.setLineMapper(lineMapper());
        this.delegate.setLinesToSkip(1);
    }

    /**
     * Configures the LineMapper to map CSV lines to Transaction objects.
     */
    private DefaultLineMapper<Transaction> lineMapper() {
        DefaultLineMapper<Transaction> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[]{
                "transactionId", "groupe", "carteId", "dateTransaction",
                "montant", "devise", "merchant", "pays", "typeCarte",
                "statut", "canal", "sourceCompte", "destinationCompte"
        });
        BeanWrapperFieldSetMapper<Transaction> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Transaction.class);
        fieldSetMapper.setCustomEditors(Map.of(
                LocalDate.class, new PropertyEditorSupport() {
                    @Override
                    public void setAsText(String text) {
                        setValue(LocalDate.parse(text));
                    }
                }
        ));
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }

    @Override
    public Transaction read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        // Initialize the delegate reader on the first read
        if (!initialized) {
            delegate.open(new ExecutionContext());
            currentLine = 0;
            initialized = true;
        }

        // Skip lines until we reach the start line
        while (currentLine < start - 1) {
            delegate.read(); // Read and discard lines before the start
            currentLine++;
        }

        // Stop reading if we've exceeded the end line
        if (currentLine >= end) {
            // Close the delegate reader since we're done
            delegate.close();
            return null; // End of partition
        }

        Transaction transaction = delegate.read();
        currentLine++;

        // Return null if we've read past the end
        if (transaction == null || currentLine > end) {
            delegate.close();
            return null;
        }

        return transaction;
    }
}