package doh.crazy;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.Pair;
import doh.crazy.WritableObjectDictionaryFactory.WritableObjectDictionary;

import java.io.IOException;

import static doh.crazy.WritableObjectDictionaryFactory.createDictionary;


public class SimpleOpMapper<
        WRITABLE_FROM_KEY extends WritableComparable,
        WRITABLE_FROM_VALUE extends Writable,
        WRITABLE_TO_KEY extends WritableComparable,
        WRITABLE_TO_VALUE extends Writable,
        FROM_KEY,
        FROM_VALUE,
        TO_KEY,
        TO_VALUE> extends Mapper<WRITABLE_FROM_KEY, WRITABLE_FROM_VALUE, WRITABLE_TO_KEY, WRITABLE_TO_VALUE> {

    private MapOp<FROM_KEY, FROM_VALUE, TO_KEY, TO_VALUE> op;
    private WritableObjectDictionary<FROM_KEY, WRITABLE_FROM_KEY> fromKeyDictionary;
    private WritableObjectDictionary<FROM_VALUE, WRITABLE_FROM_VALUE> fromValueDictionary;
    private WritableObjectDictionary<TO_KEY, WRITABLE_TO_KEY> toKeyDictionary;
    private WritableObjectDictionary<TO_VALUE, WRITABLE_TO_VALUE> toValueDictionary;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            op = OpSerializer.loadMapOpFromConf(context.getConfiguration());
            fromKeyDictionary = createDictionary(op.fromKeyClass());
            fromValueDictionary = createDictionary(op.fromValueClass());
            toKeyDictionary = createDictionary(op.toKeyClass());
            toValueDictionary = createDictionary(op.toValueClass());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void map(WRITABLE_FROM_KEY key, WRITABLE_FROM_VALUE value, Context context) throws IOException, InterruptedException {
        Pair<TO_KEY, TO_VALUE> p = op.map(
                fromKeyDictionary.getObject(key),
                fromValueDictionary.getObject(value));
        context.write(
                toKeyDictionary.getWritable(p.getFirst()),
                toValueDictionary.getWritable(p.getSecond())
        );
    }
}
