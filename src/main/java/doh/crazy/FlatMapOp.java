package doh.crazy;

import java.util.ArrayList;
import java.util.List;

public abstract class FlatMapOp<FromKey, FromValue, ToKey, ToValue>
        extends KVOp<FromKey, FromValue, ToKey, ToValue> {
    private final List<KV<ToKey, ToValue>> kvList = new ArrayList<doh.crazy.KV<ToKey, ToValue>>();


    @Override
    public Some<KV<ToKey, ToValue>> apply(Some<KV<FromKey, Some<FromValue>>> f) {
        kvList.clear();
        if (f.isOne()) {
            One<KV<FromKey, Some<FromValue>>> one = (One<KV<FromKey, Some<FromValue>>>) f;
            if (one.get().value.isOne()) {
                FromKey fk = one.get().key;
                FromValue fv = ((One<FromValue>) one.get().value).get();
                flatMap(fk, fv);
                return many(kvList);
            }
        }
        throw new UnsupportedOperationException();
    }

    public List<KV<ToKey, ToValue>> getKvList() {
        return kvList;
    }

    @Override
    protected KV<ToKey, ToValue> keyValue(ToKey toKey, ToValue toValue) {
        return new KV<ToKey, ToValue>().set(toKey, toValue);
    }

    protected void emitKeyValue(ToKey key, ToValue value) {
        kvList.add(keyValue(key, value));
    }

//    protected List<KV<ToKey, ToValue>> newList() {
//        if (!kvList.isEmpty()) {
//            kvList.clear();
//        }
//        return kvList;
//    }

     public abstract void flatMap(FromKey key, FromValue value);

}
