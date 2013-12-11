package doh.crazy;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.common.Pair;

import java.lang.reflect.Field;
import java.lang.reflect.Method;


public abstract class ReduceOp<FromKey, FromValue, ToKey, ToValue>
        extends MapReduceOp<FromKey, FromValue, ToKey, ToValue>
        implements Op<KV<FromKey, Iterable<FromValue>>, KV<ToKey, ToValue>> {

    protected final KV<ToKey, ToValue> kv = new KV<ToKey, ToValue>();

    @Override
    public KV<ToKey, ToValue> apply(KV<FromKey, Iterable<FromValue>> f) {
        return reduce(f.key, f.value);
    }

    @Override
    protected KV<ToKey, ToValue> keyValue(ToKey toKey, ToValue toValue) {
        return kv.set(toKey, toValue);
    }

    public abstract KV<ToKey, ToValue> reduce(FromKey key, Iterable<FromValue> values);




}