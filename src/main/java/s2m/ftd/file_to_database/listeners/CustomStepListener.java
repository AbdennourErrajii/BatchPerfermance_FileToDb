package s2m.ftd.file_to_database.listeners;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

@Slf4j
public class CustomStepListener implements StepExecutionListener {

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info("Before Step - Thread: {}, Step: {} (id: {})",
                Thread.currentThread().getName(),
                stepExecution.getStepName(),
                stepExecution.getId());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.info("After Step - Thread: {}, Step: {} (id: {}), Status: {}, Read/Write counts: {}/{}, Filter: {}, Commit: {}",
                Thread.currentThread().getName(),
                stepExecution.getStepName(),
                stepExecution.getId(),
                stepExecution.getExitStatus().getExitCode(),
                stepExecution.getReadCount(),
                stepExecution.getWriteCount(),
                stepExecution.getFilterCount(),
                stepExecution.getCommitCount());

        return StepExecutionListener.super.afterStep(stepExecution);
    }
}
