package s2m.ftd.file_to_database.listeners;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;
import s2m.ftd.file_to_database.model.Transaction;

@Slf4j
public class CustomWriterListener implements ItemWriteListener<Transaction> {

    @Override
    public void beforeWrite(Chunk<? extends Transaction> items) {
        log.info("[{}] Avant l'écriture des items : {}", Thread.currentThread().getName(), items);
    }

    @Override
    public void afterWrite(Chunk<? extends Transaction> items) {
        log.info("[{}] Items écrits : {}", Thread.currentThread().getName(), items);
    }

    @Override
    public void onWriteError(Exception exception, Chunk<? extends Transaction> items) {
        log.error("[{}] Erreur lors de l'écriture des items : {} - {}", Thread.currentThread().getName(), items, exception.getMessage());
    }


}
