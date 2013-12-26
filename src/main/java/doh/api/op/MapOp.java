package doh.api.op;


import doh.op.utils.ReflectionUtils;
import doh.op.kvop.KVUnoOp;
import doh.op.kvop.OpKVTransformer;

public abstract class MapOp<FromKey, FromValue, ToKey, ToValue>
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

    @Override
    public Some<KV<ToKey, ToValue>> applyUno(KV<FromKey, FromValue> f) {
        return one(map(f.key, f.value));
    }

    public abstract KV<ToKey, ToValue> map(FromKey key, FromValue value);


    protected transient final KV<ToKey, ToValue> kv = new KV<ToKey, ToValue>();
}
