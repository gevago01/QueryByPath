package utilities;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by giannis on 26/12/18.
 */
public class StringPool implements Serializable{

    private ConcurrentMap<String, String> pool = new ConcurrentHashMap<>();

    public String getCanonicalVersion(String str) {
        String canon = pool.putIfAbsent(str, str);

        return (canon == null) ? str : canon;
    }
}
