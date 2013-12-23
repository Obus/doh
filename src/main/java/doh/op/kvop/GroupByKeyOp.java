package doh.op.kvop;


public class GroupByKeyOp<Key, Value>
        extends KVOp<Key, Value, Key, Iterable<Value>> {

    @Override
    public final Some<KV<Key, Iterable<Value>>> apply(Some<KV<Key, Value>> f) {
        throw new UnsupportedOperationException();
    }
}
