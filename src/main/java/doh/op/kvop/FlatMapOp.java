package doh.op.kvop;

import java.util.ArrayList;
import java.util.List;

public abstract class FlatMapOp<FromKey, FromValue, ToKey, ToValue>
        extends KVOneOp<FromKey, FromValue, ToKey, ToValue> {
    private final List<KV<ToKey, ToValue>> kvList = new ArrayList<KV<ToKey, ToValue>>();

    @Override
    public Some<KV<ToKey, ToValue>> applyOne(KV<FromKey, FromValue> f) {
        kvList.clear();
        FromKey fk = f.key;
        FromValue fv = f.value;
        flatMap(fk, fv);
        return many(kvList);
    }

    public List<KV<ToKey, ToValue>> getKvList() {
        return kvList;
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
