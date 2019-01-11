package filtering;

import org.apache.spark.api.java.function.Function;
import trie.Query;

/**
 * Created by giannis on 11/01/19.
 */
public class FilterNullQueries implements Function<Query, Boolean> {
    @Override
    public Boolean call(Query query) throws Exception {
        return (query == null) ? false : true;
    }
}
