package s2m.ftd.file_to_database.listeners;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemProcessListener;
import s2m.ftd.file_to_database.model.Transaction;

@Slf4j
public class CustomProcessorListener implements ItemProcessListener<Transaction,Transaction> {
    @Override
    public void beforeProcess(Transaction item) {
        // Log before processing an item with thread name
        log.info("[{}] Avant le traitement de l'item : {}", Thread.currentThread().getName(), item);
    }

    @Override
    public void afterProcess(Transaction item, Transaction result) {
        log.info("[{}] Item traité : {} -> Résultat : {}", Thread.currentThread().getName(), item, result);
    }

    @Override
    public void onProcessError(Transaction item, Exception e) {
        log.error("[{}] Erreur lors du traitement de l'item : {} - {}", Thread.currentThread().getName(), item, e.getMessage());
    }
}
