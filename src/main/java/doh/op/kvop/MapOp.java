package doh.op.kvop;


public abstract class MapOp<FromKey, FromValue, ToKey, ToValue>
        extends KVOneOp<FromKey, FromValue, ToKey, ToValue> {
    protected final KV<ToKey, ToValue> kv = new KV<ToKey, ToValue>();


    @Override
    public Some<KV<ToKey, ToValue>> applyOne(KV<FromKey, FromValue> f) {
        return one(map(f.key, f.value));
    }

    public abstract KV<ToKey, ToValue> map(FromKey key, FromValue value);


}
