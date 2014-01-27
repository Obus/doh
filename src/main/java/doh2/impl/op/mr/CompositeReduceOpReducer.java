package doh2.impl.op.mr;

import doh2.impl.op.Op;
import doh2.impl.serde.OpSerializer;
import doh2.impl.op.WritableObjectDictionaryFactory;
import doh2.impl.op.kvop.CompositeReduceOp;
import doh2.api.op.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static doh2.impl.op.WritableObjectDictionaryFactory.createDictionary;
import static doh2.impl.op.mr.ReduceOpReducer.objectsIterable;

public class CompositeReduceOpReducer
        <
                WRITABLE_FROM_KEY extends WritableComparable,
                WRITABLE_FROM_VALUE extends Writable,
                WRITABLE_TO_KEY extends WritableComparable,
                WRITABLE_TO_VALUE extends Writable,
                FROM_KEY,
                FROM_VALUE,
                TO_KEY,
                TO_VALUE
                >
        extends Reducer<WRITABLE_FROM_KEY, WRITABLE_FROM_VALUE, WRITABLE_TO_KEY, WRITABLE_TO_VALUE> {

    private CompositeReduceOp<FROM_KEY, FROM_VALUE, ?, ?, TO_KEY, TO_VALUE> op;
    private WritableObjectDictionaryFactory.WritableObjectDictionary<FROM_KEY, WRITABLE_FROM_KEY> fromKeyDictionary;
    private WritableObjectDictionaryFactory.WritableObjectDictionary<FROM_VALUE, WRITABLE_FROM_VALUE> fromValueDictionary;
    private WritableObjectDictionaryFactory.WritableObjectDictionary<TO_KEY, WRITABLE_TO_KEY> toKeyDictionary;
    private WritableObjectDictionaryFactory.WritableObjectDictionary<TO_VALUE, WRITABLE_TO_VALUE> toValueDictionary;

    private OpSerializer opSerializer;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            Configuration conf = context.getConfiguration();
            opSerializer = OpSerializer.create(conf);
            op = (CompositeReduceOp) opSerializer.loadReducerOp(context.getConfiguration());
            fromKeyDictionary = createDictionary(op.fromKeyClass());
            fromValueDictionary = createDictionary(op.fromValueClass());
            toKeyDictionary = createDictionary(op.toKeyClass());
            toValueDictionary = createDictionary(op.toValueClass());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void reduce(WRITABLE_FROM_KEY key, Iterable<WRITABLE_FROM_VALUE> values, Context context) throws IOException, InterruptedException {
        Op.Some<KV<TO_KEY, TO_VALUE>> some = op.applyUno(new KV<FROM_KEY, Iterable<FROM_VALUE>>(
                fromKeyDictionary.getObject(key),
                objectsIterable(values, fromValueDictionary)));
        for (KV<TO_KEY, TO_VALUE> kv : some) {
            context.write(
                    toKeyDictionary.getWritable(kv.key),
                    toValueDictionary.getWritable(kv.value)
            );
        }
    }

}
