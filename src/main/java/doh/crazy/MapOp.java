package doh.crazy;


public abstract class MapOp<FromKey, FromValue, ToKey, ToValue>
        extends FlatMapOp<FromKey, FromValue, ToKey, ToValue> {
    protected final KV<ToKey, ToValue> kv = new KV<ToKey, ToValue>();

    @Override
    public Some<KV<ToKey, ToValue>> apply(Some<KV<FromKey, Some<FromValue>>> f) {
        if (f.isOne()) {
            One<KV<FromKey, Some<FromValue>>> one = (One<KV<FromKey, Some<FromValue>>>) f;
            if (one.get().value.isOne()) {
                FromKey fk = one.get().key;
                FromValue fv = ((One<FromValue>) one.get().value).get();
                return one(map(fk, fv));
            }
        }
        throw new UnsupportedOperationException();
    }

    @Override
    protected KV<ToKey, ToValue> keyValue(ToKey toKey, ToValue toValue) {
        return kv.set(toKey, toValue);
    }

    public abstract KV<ToKey, ToValue> map(FromKey key, FromValue value);




}
