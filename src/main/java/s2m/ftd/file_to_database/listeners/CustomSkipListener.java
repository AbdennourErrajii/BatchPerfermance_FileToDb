package s2m.ftd.file_to_database.listeners;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.SkipListener;

@Slf4j
public class CustomSkipListener implements SkipListener<Object, Object> {

    @Override
    public void onSkipInRead(Throwable t) {
        log.warn("@@@MySkipListener| On Skip in Read Error : " + t.getMessage());
    }

    @Override
    public void onSkipInWrite(Object item, Throwable t) {
        log.warn("@@@MySkipListener | Skipped in write due to : " + t.getMessage()+", Item ="+item);
    }

    @Override
    public void onSkipInProcess(Object item, Throwable t) {
        log.warn("@@@MySkipListener | Skipped in process due to: " + t.getMessage()+", Item ="+item);
    }
}