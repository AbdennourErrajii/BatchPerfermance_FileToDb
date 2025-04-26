package s2m.ftd.file_to_database.service;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import s2m.ftd.file_to_database.model.Transaction;
@Slf4j
public class TransactionItemProcessor implements ItemProcessor<Transaction,Transaction> {
    @Override
    public Transaction process(Transaction item) throws Exception {
        //log.info("ID: "+item.getTransactionId() +" | GRP : "+item.getGroupe());
        Thread.sleep(5);
        return item;
    }
}
