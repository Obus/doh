package doh.op.kvop;


public abstract class ReduceOp<FromKey, FromValue, ToKey, ToValue>
        extends KVOp<FromKey, FromValue, ToKey, ToValue> {

    protected final KV<ToKey, ToValue> kv = new KV<ToKey, ToValue>();

    @Override
    public Some<KV<ToKey, ToValue>> apply(Some<KV<FromKey, Some<FromValue>>> f) {
        if (f.isOne()) {
            One<KV<FromKey, Some<FromValue>>> one = (One<KV<FromKey, Some<FromValue>>>) f;
            if (one.get().value.isMany()) {
                FromKey fk = one.get().key;
                Iterable<FromValue> fv = one.get().value;
                return one(reduce(fk, fv));
            }
        }
        throw new UnsupportedOperationException();
    }


    public abstract KV<ToKey, ToValue> reduce(FromKey key, Iterable<FromValue> values);


}