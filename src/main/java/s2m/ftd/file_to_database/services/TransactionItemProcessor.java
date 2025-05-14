package s2m.ftd.file_to_database.services;
import org.springframework.stereotype.Component;
import s2m.ftd.file_to_database.mapper.Mapper;
import s2m.ftd.file_to_database.model.Transaction;
@Component
public class TransactionItemProcessor implements Mapper<Transaction, Transaction> {

    @Override
    public Transaction map(Transaction source) {
        // Create a new Transaction to avoid modifying the source
       /* Transaction tr = new Transaction();
        tr.setTransactionId(source.getTransactionId());
        tr.setCarteId(source.getCarteId());
        tr.setDateTransaction(source.getDateTransaction());
        tr.setMontant(source.getMontant());
        tr.setDevise(source.getDevise() != null ? source.getDevise().toUpperCase() : null);
        tr.setMerchant(source.getMerchant());
        tr.setPays(source.getPays());
        tr.setTypeCarte(source.getTypeCarte());
        tr.setStatut(source.getStatut());
        tr.setCanal(source.getCanal());
        tr.setSourceCompte(source.getSourceCompte());
        tr.setDestinationCompte(source.getDestinationCompte());*/

        return source;
    }

}
