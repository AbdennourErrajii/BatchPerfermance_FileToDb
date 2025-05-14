package s2m.ftd.file_to_database.customs;

import org.springframework.batch.item.ItemProcessor;
import s2m.ftd.file_to_database.mapper.Mapper;

public class CustomItemProessor<S, T> implements ItemProcessor<S, T>  {

        private final Mapper<S, T> mapper;

        public CustomItemProessor(Mapper<S, T> mapper) {
            this.mapper = mapper;
        }

        @Override
        public T process(S item) {
            return mapper.map(item);
        }
}
