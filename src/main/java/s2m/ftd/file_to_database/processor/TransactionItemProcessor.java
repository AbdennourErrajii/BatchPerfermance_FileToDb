package s2m.ftd.file_to_database.processor;
import org.springframework.batch.item.ItemProcessor;
import s2m.ftd.file_to_database.model.Transaction;
public class TransactionItemProcessor implements ItemProcessor<Transaction,Transaction> {
    @Override
    public Transaction process(Transaction item) throws Exception {
        Thread.sleep(5);
        return item;
    }
}
