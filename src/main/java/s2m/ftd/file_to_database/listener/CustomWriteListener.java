package s2m.ftd.file_to_database.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;
import s2m.ftd.file_to_database.model.Transaction;

@Slf4j
public class CustomWriteListener implements ItemWriteListener<Transaction> {
    private int beforeWriteCount = 0;
    private int afterWriteCount = 0;

    @Override
    public void beforeWrite(Chunk<? extends Transaction> items) {
        beforeWriteCount++;
        log.info("Nombre de commits avant écriture (beforeWrite): {} - Nombre d'items: {}", beforeWriteCount, items.size());
        ItemWriteListener.super.beforeWrite(items);
    }

    @Override
    public void afterWrite(Chunk<? extends Transaction> items) {
        afterWriteCount++;
        log.info("Nombre de commits après écriture (afterWrite): {}", afterWriteCount);
        ItemWriteListener.super.afterWrite(items);
    }
}