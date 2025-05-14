package s2m.ftd.file_to_database.abstracts;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;


public abstract class AbstractItemWriter<T> implements ItemWriter<T>, InitializingBean {

    protected boolean assertUpdates = true;

    protected abstract void writeToDataSource(Chunk<? extends T> items) throws Exception;

    public abstract void afterPropertiesSet();

    @Override
    public void write(Chunk<? extends T> items) throws Exception {
        if (items == null || items.isEmpty()) {
            return;
        }
        writeToDataSource(items);
        if (assertUpdates) {

        }
    }

    public void setAssertUpdates(boolean assertUpdates) {
        this.assertUpdates = assertUpdates;
    }
}