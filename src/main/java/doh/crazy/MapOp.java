package doh.crazy;


public abstract class MapOp<FromKey, FromValue, ToKey, ToValue>
        extends MapReduceOp<FromKey, FromValue, ToKey, ToValue>
        implements Op<KV<FromKey, FromValue>, KV<ToKey, ToValue>>{
    protected final KV<ToKey, ToValue> kv = new KV<ToKey, ToValue>();

    @Override
    public KV<ToKey, ToValue> apply(KV<FromKey, FromValue> f) {
        return map(f.key, f.value);
    }

    @Override
    protected KV<ToKey, ToValue> keyValue(ToKey toKey, ToValue toValue) {
        return kv.set(toKey, toValue);
    }

    public abstract KV<ToKey, ToValue> map(FromKey key, FromValue value);


}
