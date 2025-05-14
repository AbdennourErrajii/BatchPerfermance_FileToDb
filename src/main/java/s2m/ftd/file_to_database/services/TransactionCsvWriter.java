package s2m.ftd.file_to_database.services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.WritableResource;
import s2m.ftd.file_to_database.customs.CustomFlatFileItemWriter;
import s2m.ftd.file_to_database.model.Transaction;

@Slf4j
public class  TransactionCsvWriter extends CustomFlatFileItemWriter<Transaction> {
    @Override
    protected void configureWriter(WritableResource resource, String[] columnNames, String delimiter) {
        setResource(new FileSystemResource("output/transactions.csv"));
        /*setColumnNames(new String[]{
                "transactionId", "carteId", "dateTransaction", "montant", "devise",
                "merchant", "pays", "typeCarte", "statut", "canal", "sourceCompte", "destinationCompte"
        });
        setDelimiter(";");*/
    }
}