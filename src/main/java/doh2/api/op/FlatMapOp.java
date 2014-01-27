package doh2.api.op;

import doh2.impl.op.utils.ReflectionUtils;
import doh2.impl.op.kvop.KVUnoOp;
import doh2.impl.op.kvop.OpKVTransformer;

import java.util.ArrayList;
import java.util.List;

public abstract class FlatMapOp<FromKey, FromValue, ToKey, ToValue>
        extends KVUnoOp<FromKey, FromValue, ToKey, ToValue>
        implements OpKVTransformer<FromKey, FromValue, ToKey, ToValue> {

    public Class<FromKey> fromKeyClass() {
        return ReflectionUtils.getFromKeyClass(getClass());
    }

    public Class<FromValue> fromValueClass() {
        return ReflectionUtils.getFromValueClass(getClass());
    }

    public Class<ToKey> toKeyClass() {
        return ReflectionUtils.getToKeyClass(getClass());
    }

    public Class<ToValue> toValueClass() {
        return ReflectionUtils.getToValueClass(getClass());
    }


    private transient final List<KV<ToKey, ToValue>> kvList = new ArrayList<KV<ToKey, ToValue>>();

    @Override
    public Some<KV<ToKey, ToValue>> applyUno(KV<FromKey, FromValue> f) {
        kvList.clear();
        flatMap(f.key, f.value);
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
