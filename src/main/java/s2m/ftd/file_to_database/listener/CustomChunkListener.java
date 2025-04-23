package s2m.ftd.file_to_database.listener;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

public class CustomChunkListener implements ChunkListener{

    @Override
    public void beforeChunk(ChunkContext context) {
        String threadName = Thread.currentThread().getName();
        System.out.printf("[%s] Début du chunk (items précédents: %d)%n",
                threadName,
                context.getStepContext().getStepExecution().getReadCount());
    }

    @Override
    public void afterChunk(ChunkContext context) {
        String threadName = Thread.currentThread().getName();
        System.out.printf("[%s] Fin du chunk (%d éléments traités)%n",
                threadName,
                context.getStepContext().getStepExecution().getReadCount());
    }

}
