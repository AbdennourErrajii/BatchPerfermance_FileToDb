package s2m.ftd.file_to_database.mapper;

public interface Mapper<S, T> {
    T map(S source);
}
