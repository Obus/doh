package doh.op.mr;

import doh.op.Op;
import doh.op.serde.OpSerializer;
import doh.op.WritableObjectDictionaryFactory;
import doh.op.kvop.CompositeMapOp;
import doh.api.op.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static doh.op.WritableObjectDictionaryFactory.createDictionary;


import static doh.op.serde.OpSerializer.loadMapInputKeyClassFromConf;
import static doh.op.serde.OpSerializer.loadMapInputValueClassFromConf;
import static doh.op.serde.OpSerializer.loadMapOutputKeyClassFromConf;
import static doh.op.serde.OpSerializer.loadMapOutputValueClassFromConf;

public class CompositeGeneralMapOpMapper <
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

    private CompositeMapOp<FROM_KEY, FROM_VALUE, TO_KEY, TO_VALUE> op;
    private WritableObjectDictionaryFactory.WritableObjectDictionary<FROM_KEY, WRITABLE_FROM_KEY> fromKeyDictionary;
    private WritableObjectDictionaryFactory.WritableObjectDictionary<FROM_VALUE, WRITABLE_FROM_VALUE> fromValueDictionary;
    private WritableObjectDictionaryFactory.WritableObjectDictionary<TO_KEY, WRITABLE_TO_KEY> toKeyDictionary;
    private WritableObjectDictionaryFactory.WritableObjectDictionary<TO_VALUE, WRITABLE_TO_VALUE> toValueDictionary;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            Configuration conf = context.getConfiguration();
            op = OpSerializer.loadCompositeMapOpConf(conf);
            fromKeyDictionary = createDictionary(loadMapInputKeyClassFromConf(conf));
            fromValueDictionary = createDictionary(loadMapInputValueClassFromConf(conf));
            toKeyDictionary = createDictionary(loadMapOutputKeyClassFromConf(conf));
            toValueDictionary = createDictionary(loadMapOutputValueClassFromConf(conf));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void map(WRITABLE_FROM_KEY key, WRITABLE_FROM_VALUE value, Context context) throws IOException, InterruptedException {
        Op.Some<KV<TO_KEY, TO_VALUE>> some = op.applyUno(new KV<FROM_KEY, FROM_VALUE>().set(
                fromKeyDictionary.getObject(key),
                fromValueDictionary.getObject(value)));

        for (KV<TO_KEY, TO_VALUE> p : some) {
            context.write(
                    toKeyDictionary.getWritable(p.key),
                    toValueDictionary.getWritable(p.value)
            );
        }
    }
}
