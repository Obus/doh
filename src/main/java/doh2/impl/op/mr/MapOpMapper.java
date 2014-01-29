package doh2.impl.op.mr;

import doh2.impl.serde.OpSerializer;
import doh2.impl.op.WritableObjectDictionaryFactory.WritableObjectDictionary;
import doh2.api.op.KV;
import doh2.api.op.MapOp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static doh2.impl.op.WritableObjectDictionaryFactory.createDictionary;


public class MapOpMapper<
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

    private MapOp<FROM_KEY, FROM_VALUE, TO_KEY, TO_VALUE> op;
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
            op = (MapOp) opSerializer.loadMapperOp(context.getConfiguration());
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
        KV<TO_KEY, TO_VALUE> p = op.map(
                fromKeyDictionary.getObject(key),
                fromValueDictionary.getObject(value));
        context.write(
                toKeyDictionary.getWritable(p.key),
                toValueDictionary.getWritable(p.value)
        );
    }
}
