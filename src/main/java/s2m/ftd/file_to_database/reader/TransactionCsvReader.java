package s2m.ftd.file_to_database.reader;

import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.Resource;
import s2m.ftd.file_to_database.model.Transaction;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Map;

public class TransactionCsvReader implements ItemReader<Transaction> , ItemStream{

    private final FlatFileItemReader<Transaction> delegate;
    public TransactionCsvReader(Resource resource) throws IOException {
        this.delegate = new FlatFileItemReader<>();
        this.delegate.setName("transactionItemReader");
        this.delegate.setResource(resource);
        this.delegate.setLinesToSkip(1);
        this.delegate.setLineMapper(this.lineMapper());
        this.delegate.setStrict(true);
        //this.delegate.open(new ExecutionContext());
    }
    private DefaultLineMapper<Transaction> lineMapper(){
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
                java.time.LocalDate.class, new java.beans.PropertyEditorSupport() {
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
    public Transaction read() throws Exception {
        return delegate.read();
    }

    @Override
    public void open(ExecutionContext ec) throws ItemStreamException {
        delegate.open(ec);
    }

    @Override
    public void update(ExecutionContext ec) throws ItemStreamException {
        delegate.update(ec);
    }

    @Override
    public void close() throws ItemStreamException {
        delegate.close();
    }

}
