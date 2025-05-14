package s2m.ftd.file_to_database.customs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.FormatterLineAggregator;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import s2m.ftd.file_to_database.abstracts.AbstractItemWriter;
import s2m.ftd.file_to_database.utils.FlatFileFormatInspector;

import java.lang.reflect.Field;
import java.util.Arrays;

public abstract class CustomFlatFileItemWriter<T> extends AbstractItemWriter<T> {

    protected static final Log logger = LogFactory.getLog(CustomFlatFileItemWriter.class);

    private FlatFileItemWriter<T> flatFileItemWriter;
    private WritableResource resource;
    private LineAggregator<T> lineAggregator;
    private boolean initialized = false;

    private String[] columnNames;
    private String delimiter = ",";
    private final Class<T> clazz;

    @SuppressWarnings("unchecked")
    public CustomFlatFileItemWriter() {
        this.flatFileItemWriter = new FlatFileItemWriter<>();
        // Extract generic type T
        this.clazz = (Class<T>) ((java.lang.reflect.ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments()[0];
    }

    // Setter for column names
    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    // Setter for delimiter (default ",")
    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    // Setter for resource
    public void setResource(WritableResource resource) {
        this.resource = resource;
    }

    // Retrieve field names from class using reflection
    private String[] getFieldNames() {
        Field[] fields = clazz.getDeclaredFields();
        return Arrays.stream(fields)
                .map(Field::getName)
                .toArray(String[]::new);
    }

    @Override
    public void afterPropertiesSet() {
        // Call configureWriter to set resource, columnNames, and delimiter
        configureWriter(resource, columnNames, delimiter);

        // Use default column names if not set
        if (columnNames == null) {
            columnNames = getFieldNames();
            logger.info("Column names not provided; using default field names: " + Arrays.toString(columnNames));
        }

        try {
            // Detect file format
            String fileFormat = FlatFileFormatInspector.determineFileFormatWithTika(resource.getFile().getAbsolutePath());
            BeanWrapperFieldExtractor<T> fieldExtractor = new BeanWrapperFieldExtractor<>();
            fieldExtractor.setNames(columnNames);

            if ("CSV".equals(fileFormat)) {
                DelimitedLineAggregator<T> delimitedLineAggregator = new DelimitedLineAggregator<>();
                delimitedLineAggregator.setDelimiter(delimiter);
                delimitedLineAggregator.setFieldExtractor(fieldExtractor);
                this.lineAggregator = delimitedLineAggregator;

            } else if ("TEXT".equals(fileFormat)) {
                FormatterLineAggregator<T> formatterLineAggregator = new FormatterLineAggregator<>();
                formatterLineAggregator.setFormat("%-20s %-15s %-25s %-10.2f %-10s %-30s %-10s %-15s %-10s %-15s %-20s %-20s");
                formatterLineAggregator.setFieldExtractor(fieldExtractor);
                this.lineAggregator = formatterLineAggregator;

            } else {
                logger.warn("Unknown file format, defaulting to CSV.");
                DelimitedLineAggregator<T> defaultAggregator = new DelimitedLineAggregator<>();
                defaultAggregator.setDelimiter(delimiter);
                defaultAggregator.setFieldExtractor(fieldExtractor);
                this.lineAggregator = defaultAggregator;
            }

            flatFileItemWriter.setLineAggregator(lineAggregator);
            flatFileItemWriter.setResource(resource);

            // Add header callback
            flatFileItemWriter.setHeaderCallback(writer -> {
                String header = String.join(delimiter, columnNames);
                writer.write(header);
            });

            flatFileItemWriter.afterPropertiesSet();
        } catch (Exception e) {
            throw new IllegalStateException("Error initializing FlatFileItemWriter", e);
        }
    }

    protected abstract void configureWriter(WritableResource resource, String[] columnNames, String delimiter);

    @Override
    protected void writeToDataSource(Chunk<? extends T> items) throws Exception {
        if (!initialized) {
            flatFileItemWriter.open(new ExecutionContext());
            initialized = true;
        }
        flatFileItemWriter.write(items);
        if (assertUpdates) {
            validateWrites(items);
        }
    }

    private void validateWrites(Chunk<? extends T> items) {
        if (logger.isDebugEnabled()) {
            logger.debug("Validating writes for " + items.size() + " items.");
        }
    }

    public void close() {
        if (initialized) {
            flatFileItemWriter.close();
            initialized = false;
        }
    }
}