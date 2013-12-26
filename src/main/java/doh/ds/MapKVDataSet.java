package doh.ds;

import doh.api.op.KV;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;

public class MapKVDataSet<KEY, VALUE> extends RealKVDataSet<KEY, VALUE> {

    private Map<KEY, VALUE> inMemoryMap;

    public MapKVDataSet(Path path) {
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
