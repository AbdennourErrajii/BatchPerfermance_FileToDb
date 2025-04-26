package s2m.ftd.file_to_database.partition;

import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;

public class LineRangeRecordSeparatorPolicy extends DefaultRecordSeparatorPolicy {
    private final long startLine;
    private final long endLine;
    private long currentLine = 0;

    public LineRangeRecordSeparatorPolicy(long startLine, long endLine) {
        this.startLine = startLine;
        this.endLine = endLine;
    }

    @Override
    public boolean isEndOfRecord(String line) {
        currentLine++;
        return currentLine >= startLine && currentLine <= endLine && super.isEndOfRecord(line);
    }

    @Override
    public String postProcess(String record) {
        if (currentLine > endLine) {
            return null;
        }
        return super.postProcess(record);
    }
}