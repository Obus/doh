package doh.api.op;

import doh.api.op.KV;
import doh.api.op.ReduceOp;

public abstract class ValueOnlyReduceOp<Key, FromValue, ToValue> extends ReduceOp<Key, FromValue, Key, ToValue> {

    @Override
    public final KV<Key, ToValue> reduce(Key key, Iterable<FromValue> fromValues) {
        return keyValue(key, reduceValue(key, fromValues));
    }

    abstract public ToValue reduceValue(Key key, Iterable<FromValue> fromValues);
}
