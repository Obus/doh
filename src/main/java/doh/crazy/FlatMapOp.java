package doh.crazy;

import java.util.ArrayList;
import java.util.List;

public abstract class FlatMapOp<FromKey, FromValue, ToKey, ToValue>
        extends MapReduceOp<FromKey, FromValue, ToKey, ToValue>
        implements Op<KV<FromKey, FromValue>, Iterable<KV<ToKey, ToValue>>>{
    private final List<KV<ToKey, ToValue>> kvList = new ArrayList<doh.crazy.KV<ToKey, ToValue>>();

    @Override
    public Iterable<KV<ToKey, ToValue>> apply(KV<FromKey, FromValue> f) {
        return flatMap(f.key, f.value);
    }

    @Override
    protected KV<ToKey, ToValue> keyValue(ToKey toKey, ToValue toValue) {
        return new KV<ToKey, ToValue>().set(toKey, toValue);
    }

    protected List<KV<ToKey, ToValue>> newList() {
        if (!kvList.isEmpty()) {
            kvList.clear();
        }
        return kvList;
    }
    public abstract Iterable<KV<ToKey, ToValue>> flatMap(FromKey key, FromValue value);

}
