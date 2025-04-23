package s2m.ftd.file_to_database.listeners;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomChunkListener implements ChunkListener {

    private static final Logger logger = LoggerFactory.getLogger(CustomChunkListener.class);

    @Override
    public void beforeChunk(ChunkContext context) {
        logger.info("[{}] Début du chunk (items précédents: {})",
                Thread.currentThread().getName(),
                context.getStepContext().getStepExecution().getReadCount());
    }

    @Override
    public void afterChunk(ChunkContext context) {
        logger.info("[{}] Fin du chunk ({} éléments traités)",
                Thread.currentThread().getName(),
                context.getStepContext().getStepExecution().getReadCount());
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        logger.error("[{}] Erreur lors du traitement du chunk (items traités: {})",
                Thread.currentThread().getName(),
                context.getStepContext().getStepExecution().getReadCount());
    }
}

