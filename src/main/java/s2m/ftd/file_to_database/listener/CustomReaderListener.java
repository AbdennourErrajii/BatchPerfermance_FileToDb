package s2m.ftd.file_to_database.listener;


import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemReadListener;
import s2m.ftd.file_to_database.model.Transaction;

@Slf4j
public class CustomReaderListener implements ItemReadListener<Transaction> {

    @Override
    public void beforeRead() {
        log.info(Thread.currentThread().getName()+"= Preparing to read item...");
    }

    @Override
    public void afterRead(Transaction item) {
        log.info(Thread.currentThread().getName()+"= Item read: " + item);
    }

    @Override
    public void onReadError(Exception ex) {
        log.error("Error reading item: " + ex.getMessage());
    }
}
