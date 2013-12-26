package doh.op.kvop;


import doh.op.OpParameter;
import doh.op.WritableObjectDictionaryFactory;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import static doh.op.ReflectionUtils.UNKNOWN_CLASS;
import static doh.op.ReflectionUtils.isUnknown;

public class CompositeReduceOp<FromKey, FromValue, IntermKey, IntermValue, ToKey, ToValue>
        extends KVUnoOp<FromKey, Iterable<FromValue>, ToKey, ToValue> implements Writable, OpKVTransformer<FromKey, FromValue, ToKey, ToValue> {

    @OpParameter
    private ReduceOp<FromKey, FromValue, IntermKey, IntermValue> reduceOp;
    @OpParameter
    private CompositeMapOp<IntermKey, IntermValue, ToKey, ToValue> compositeMapOp;


    public CompositeReduceOp() {}

    public CompositeReduceOp(ReduceOp<FromKey, FromValue, IntermKey, IntermValue> reduceOp, CompositeMapOp<IntermKey, IntermValue, ToKey, ToValue> compositeMapOp) {
        this.reduceOp = reduceOp;
        this.compositeMapOp = compositeMapOp;
    }

    @Override
    public Some<KV<ToKey, ToValue>> applyUno(KV<FromKey, Iterable<FromValue>> f) {
        KV<IntermKey, IntermValue> intermKV = reduceOp.reduce(f.key, f.value);
        return compositeMapOp.applyUno(intermKV);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableObjectDictionaryFactory.SimpleKVOpWritable w
                = new WritableObjectDictionaryFactory.SimpleKVOpWritable(reduceOp);
        w.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        WritableObjectDictionaryFactory.SimpleKVOpWritable w
                = new WritableObjectDictionaryFactory.SimpleKVOpWritable();
        w.readFields(dataInput);
        reduceOp = (ReduceOp) w.op;
    }

    @Override
    public Class<FromKey> fromKeyClass() {
        return reduceOp.fromKeyClass();
    }

    @Override
    public Class<FromValue> fromValueClass() {
        return reduceOp.fromValueClass();
    }

    @Override
    public Class<ToKey> toKeyClass() {
        if (!isUnknown(compositeMapOp.toKeyClass())) {
            return compositeMapOp.toKeyClass();
        }
        else if (!isUnknown(compositeMapOp.fromKeyClass())) {
            return (Class) compositeMapOp.fromKeyClass();
        }
        else if (!isUnknown(reduceOp.toKeyClass())) {
            return (Class) reduceOp.toKeyClass();
        }
        else if (!isUnknown(reduceOp.toValueClass())) {
            return (Class) reduceOp.toValueClass();
        }
        return UNKNOWN_CLASS;
    }

    @Override
    public Class<ToValue> toValueClass() {
        if (!isUnknown(compositeMapOp.toValueClass())) {
            return compositeMapOp.toValueClass();
        }
        else if (!isUnknown(compositeMapOp.fromValueClass())) {
            return (Class) compositeMapOp.fromValueClass();
        }
        else if (!isUnknown(reduceOp.toValueClass())) {
            return (Class) reduceOp.toValueClass();
        }
        else if (!isUnknown(reduceOp.toValueClass())) {
            return (Class) reduceOp.toValueClass();
        }
        return UNKNOWN_CLASS;
    }

    public ReduceOp<FromKey, FromValue, IntermKey, IntermValue> getReduceOp() {
        return reduceOp;
    }

    public CompositeMapOp<IntermKey, IntermValue, ToKey, ToValue> getCompositeMapOp() {
        return compositeMapOp;
    }
}
