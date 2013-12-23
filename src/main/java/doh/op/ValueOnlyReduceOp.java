package doh.op;

import doh.op.kvop.KV;
import doh.op.kvop.ReduceOp;

public abstract class ValueOnlyReduceOp<Key, FromValue, ToValue> extends ReduceOp<Key, FromValue, Key, ToValue> {

    @Override
    public final KV<Key, ToValue> reduce(Key key, Iterable<FromValue> fromValues) {
        return keyValue(key, reduceValue(key, fromValues));
    }

    abstract public ToValue reduceValue(Key key, Iterable<FromValue> fromValues);
}
