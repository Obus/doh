package doh.op.kvop;


public abstract class FlatReduceOp<FromKey, FromValue, ToKey, ToValue>
        extends KVOp<FromKey, FromValue, ToKey, ToValue> {
    @Override
    public Some<KV<ToKey, ToValue>> apply(Some<KV<FromKey, FromValue>> f) {

    }
}
