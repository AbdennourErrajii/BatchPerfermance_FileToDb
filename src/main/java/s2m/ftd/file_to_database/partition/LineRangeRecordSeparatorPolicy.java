package s2m.ftd.file_to_database.partition;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
public class LineRangeRecordSeparatorPolicy extends DefaultRecordSeparatorPolicy {
    private final long startLine;
    private final long endLine;
    private long currentLine;

    public LineRangeRecordSeparatorPolicy(ExecutionContext executionContext) {
        this.startLine = executionContext.getLong("startLine");
        this.endLine = executionContext.getLong("endLine");
        this.currentLine = 0; // Start from 0, as header is skipped by FlatFileItemReader
    }

    @Override
    public boolean isEndOfRecord(String line) {
        currentLine++;
        if (currentLine < startLine || currentLine > endLine) {
            return false; // Skip lines before startLine or after endLine
        }
        return super.isEndOfRecord(line);
    }

    @Override
    public String postProcess(String record) {
        if (currentLine < startLine || currentLine > endLine || record == null) {
            return null; // Stop reading if outside range or EOF
        }
        return super.postProcess(record);
    }
}