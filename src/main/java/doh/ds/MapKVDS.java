package doh.ds;

import doh.api.ds.Location;
import doh.api.op.KV;

import java.util.HashMap;
import java.util.Map;

public class MapKVDS<KEY, VALUE> extends RealKVDS<KEY, VALUE> {

    private Map<KEY, VALUE> inMemoryMap;

    public MapKVDS(Location location) {
        super(location);
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
