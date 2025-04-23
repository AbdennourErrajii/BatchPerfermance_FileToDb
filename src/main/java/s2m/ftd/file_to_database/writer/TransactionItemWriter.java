package s2m.ftd.file_to_database.writer;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.JdbcTemplate;
import s2m.ftd.file_to_database.model.Transaction;

import java.util.List;

public class TransactionItemWriter implements ItemWriter<Transaction> {
    private final JdbcTemplate jdbcTemplate;

    public TransactionItemWriter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    @Override
    public void write(Chunk<? extends Transaction> chunk) throws Exception {
        List<? extends Transaction> transactions = chunk.getItems();

        String sql = """
                INSERT INTO transaction (
                    transaction_id, groupe, carte_id, date_transaction, montant,
                    devise, merchant, pays, type_carte, statut, canal,
                    source_compte, destination_compte
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        for (Transaction t : transactions) {
            jdbcTemplate.update(sql,
                    t.getTransactionId(),
                    t.getGroupe(),
                    t.getCarteId(),
                    t.getDateTransaction(),
                    t.getMontant(),
                    t.getDevise(),
                    t.getMerchant(),
                    t.getPays(),
                    t.getTypeCarte(),
                    t.getStatut(),
                    t.getCanal(),
                    t.getSourceCompte(),
                    t.getDestinationCompte()
            );
        }

    }
}
