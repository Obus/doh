package doh.api.op;

import doh.op.kvop.KVUnoOp;
import doh.op.kvop.OpKVTransformer;
import doh.op.utils.ReflectionUtils;

public abstract class FilterOp <Key, Value>
        extends KVUnoOp<Key, Value, Key, Value>
        implements OpKVTransformer<Key, Value, Key, Value> {


    public Class<Key> fromKeyClass() {
        return ReflectionUtils.getFromKeyClass(getClass());
    }

    public Class<Value> fromValueClass() {
        return ReflectionUtils.getFromValueClass(getClass());
    }

    public Class<Key> toKeyClass() {
        return ReflectionUtils.getToKeyClass(getClass());
    }

    public Class<Value> toValueClass() {
        return ReflectionUtils.getToValueClass(getClass());
    }

    @Override
    public Some<KV<Key, Value>> applyUno(KV<Key, Value> f) {
        if (accept(f.key, f.value)) {
            return one(keyValue(f.key, f.value));
        }
        return none();
    }

    public abstract boolean accept(Key key, Value value);


    protected transient final KV<Key, Value> kv = new KV<Key, Value>();
}
