package s2m.ftd.file_to_database.services;


import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import s2m.ftd.file_to_database.model.Transaction;

import javax.sql.DataSource;


public class TransactionDbWriter implements ItemWriter<Transaction> {

    private final JdbcBatchItemWriter<Transaction> writer;

    public TransactionDbWriter(DataSource dataSource) {
        writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("INSERT INTO transaction (transaction_id, carte_id, date_transaction, montant, " +
                "devise, merchant, pays, type_carte, statut, canal, source_compte, destination_compte) " +
                "VALUES (:transactionId, :carteId, :dateTransaction, :montant, :devise, :merchant, " +
                ":pays, :typeCarte, :statut, :canal, :sourceCompte, :destinationCompte)");
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
    }


    @Override
    public void write(Chunk<? extends Transaction> chunk) throws Exception {
        writer.write(chunk);
    }
}
