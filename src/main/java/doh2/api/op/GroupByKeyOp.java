package doh2.api.op;

import doh2.impl.op.kvop.KVUnoOp;

public class GroupByKeyOp<Key, Value> extends KVUnoOp<Key, Value, Key, Iterable<Value>> {
    @Override
    public Some<KV<Key, Iterable<Value>>> applyUno(KV<Key, Value> f) {
        throw new UnsupportedOperationException("Grouping operation should never be invoked");
    }
}
