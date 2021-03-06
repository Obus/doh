package doh2.impl.op.mr;

import doh2.impl.serde.OpSerializer;
import doh2.api.op.KV;
import doh2.api.op.ReduceOp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

import static doh2.impl.op.WritableObjectDictionaryFactory.WritableObjectDictionary;
import static doh2.impl.op.WritableObjectDictionaryFactory.createDictionary;


public class ReduceOpReducer
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

    private ReduceOp<FROM_KEY, FROM_VALUE, TO_KEY, TO_VALUE> op;
    private WritableObjectDictionary<FROM_KEY, WRITABLE_FROM_KEY> fromKeyDictionary;
    private WritableObjectDictionary<FROM_VALUE, WRITABLE_FROM_VALUE> fromValueDictionary;
    private WritableObjectDictionary<TO_KEY, WRITABLE_TO_KEY> toKeyDictionary;
    private WritableObjectDictionary<TO_VALUE, WRITABLE_TO_VALUE> toValueDictionary;
    private OpSerializer opSerializer;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            Configuration conf = context.getConfiguration();
            opSerializer = OpSerializer.create(conf);
            op = (ReduceOp) opSerializer.loadReducerOp(context.getConfiguration());
            fromKeyDictionary = createDictionary(OpSerializer.loadMapOutputKeyClassFromConf(conf));
            fromValueDictionary = createDictionary(OpSerializer.loadMapOutputValueClassFromConf(conf));
            toKeyDictionary = createDictionary(OpSerializer.loadReduceOutputKeyClassFromConf(conf));
            toValueDictionary = createDictionary(OpSerializer.loadReduceOutputValueClassFromConf(conf));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void reduce(WRITABLE_FROM_KEY key, Iterable<WRITABLE_FROM_VALUE> values, Context context) throws IOException, InterruptedException {
        KV<TO_KEY, TO_VALUE> p = op.reduce(
                fromKeyDictionary.getObject(key),
                objectsIterable(values, fromValueDictionary));
        context.write(
                toKeyDictionary.getWritable(p.key),
                toValueDictionary.getWritable(p.value)
        );
    }

    public static <O, W extends Writable> Iterable<O> objectsIterable(final Iterable<W> writablesIterable,
                                                                      final WritableObjectDictionary<O, W> dictionary) {
        return new Iterable<O>() {
            @Override
            public Iterator<O> iterator() {
                final Iterator<W> writablesIterator = writablesIterable.iterator();
                return new Iterator<O>() {
                    @Override
                    public boolean hasNext() {
                        return writablesIterator.hasNext();
                    }

                    @Override
                    public O next() {
                        return dictionary.getObject(writablesIterator.next());
                    }

                    @Override
                    public void remove() {
                        writablesIterator.remove();
                    }
                };
            }
        };
    }
}