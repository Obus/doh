package doh2.api.op;


import doh2.impl.op.utils.ReflectionUtils;
import doh2.impl.op.kvop.KVUnoOp;
import doh2.impl.op.kvop.OpKVTransformer;

public abstract class ReduceOp<FromKey, FromValue, ToKey, ToValue>
        extends KVUnoOp<FromKey, Iterable<FromValue>, ToKey, ToValue>
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

    protected transient final KV<ToKey, ToValue> kv = new KV<ToKey, ToValue>();

    @Override
    public Some<KV<ToKey, ToValue>> applyUno(KV<FromKey, Iterable<FromValue>> f) {
        return one(reduce(f.key, f.value));
    }

    public abstract KV<ToKey, ToValue> reduce(FromKey key, Iterable<FromValue> values);


}