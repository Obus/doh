package doh2.impl.op.mr;

import doh2.impl.serde.OpSerializer;
import doh2.impl.op.WritableObjectDictionaryFactory;
import doh2.api.op.FlatMapOp;
import doh2.api.op.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static doh2.impl.op.WritableObjectDictionaryFactory.createDictionary;

public class FlatMapOpMapper
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
        extends Mapper<WRITABLE_FROM_KEY, WRITABLE_FROM_VALUE, WRITABLE_TO_KEY, WRITABLE_TO_VALUE> {

    private FlatMapOp<FROM_KEY, FROM_VALUE, TO_KEY, TO_VALUE> op;
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
            op = (FlatMapOp) opSerializer.loadMapperOp(context.getConfiguration());
            fromKeyDictionary = createDictionary(OpSerializer.loadMapInputKeyClassFromConf(conf));
            fromValueDictionary = createDictionary(OpSerializer.loadMapInputValueClassFromConf(conf));
            toKeyDictionary = createDictionary(OpSerializer.loadMapOutputKeyClassFromConf(conf));
            toValueDictionary = createDictionary(OpSerializer.loadMapOutputValueClassFromConf(conf));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void map(WRITABLE_FROM_KEY key, WRITABLE_FROM_VALUE value, Context context) throws IOException, InterruptedException {
        op.getKvList().clear();
        op.flatMap(
                fromKeyDictionary.getObject(key),
                fromValueDictionary.getObject(value));

        for (KV<TO_KEY, TO_VALUE> p : op.getKvList()) {
            context.write(
                    toKeyDictionary.getWritable(p.key),
                    toValueDictionary.getWritable(p.value)
            );
        }
    }
}
