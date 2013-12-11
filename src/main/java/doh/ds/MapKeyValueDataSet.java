package doh.ds;

import doh.crazy.KV;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;

public class MapKeyValueDataSet<KEY, VALUE> extends KeyValueDataSet<KEY, VALUE> {

    private Map<KEY, VALUE> inMemoryMap;

    public MapKeyValueDataSet(Path path) {
        super(path);
    }

    protected synchronized Map<KEY, VALUE> inMemoryMap() {
        if (inMemoryMap == null) {
            Map<KEY, VALUE> map = new HashMap<KEY, VALUE>();
            for (KV<KEY, VALUE> kv : this) {
                map.put(kv.key, kv.value);
            }
            inMemoryMap = map;
        }
        return inMemoryMap;
    }

    public VALUE get(KEY key) {
        return inMemoryMap().get(key);
    }

}
