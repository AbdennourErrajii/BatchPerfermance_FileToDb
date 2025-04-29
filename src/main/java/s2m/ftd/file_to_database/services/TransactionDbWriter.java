package s2m.ftd.file_to_database.services;


import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import s2m.ftd.file_to_database.model.Transaction;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;


@Slf4j
public class TransactionDbWriter implements ItemWriter<Transaction> {

    private final JdbcBatchItemWriter<Transaction> writer;

    public TransactionDbWriter(DataSource dataSource) {
        writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("INSERT INTO transaction (" +
                "transaction_id, carte_id, date_transaction, montant, devise, merchant, " +
                "pays, type_carte, statut, canal, source_compte, destination_compte) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        writer.setItemPreparedStatementSetter(new ItemPreparedStatementSetter<Transaction>() {
            @Override
            public void setValues(Transaction transaction, PreparedStatement ps) throws SQLException {
                ps.setLong(1, transaction.getTransactionId());
                ps.setString(2, transaction.getCarteId());
                ps.setString(3, transaction.getDateTransaction());
                ps.setDouble(4, transaction.getMontant());
                ps.setString(5, transaction.getDevise());
                ps.setString(6, transaction.getMerchant());
                ps.setString(7, transaction.getPays());
                ps.setString(8, transaction.getTypeCarte());
                ps.setString(9, transaction.getStatut());
                ps.setString(10, transaction.getCanal());
                ps.setString(11, transaction.getSourceCompte());
                ps.setString(12, transaction.getDestinationCompte());
            }
        });

    }

    @Override
    public void write(Chunk<? extends Transaction> chunk) throws Exception {
        //log.info("Writing {} transactions", chunk.getItems());
        writer.write(chunk);
    }
}
