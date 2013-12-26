package doh.op;

import doh.op.kvop.KVUnoOp;
import doh.op.kvop.OpKVTransformer;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static doh.op.utils.ReflectionUtils.UNKNOWN_CLASS;
import static doh.op.utils.ReflectionUtils.isUnknown;

public class OpSequence implements Writable, OpKVTransformer {

    private List<KVUnoOp> sequence;

    public OpSequence() {}

    public OpSequence(List<KVUnoOp> sequence) {
        this.sequence = sequence;
    }

    public List<KVUnoOp> getSequence() {
        return sequence;
    }


    @Override
    public Class fromKeyClass() {
        return ((OpKVTransformer)sequence.get(0)).fromKeyClass();
    }

    @Override
    public Class fromValueClass() {
        return ((OpKVTransformer)sequence.get(0)).fromValueClass();
    }

    @Override
    public Class toKeyClass() {
        for (int i = sequence.size() - 1; i >= 0; --i) {
            OpKVTransformer op = (OpKVTransformer) sequence.get(i);
            if (!isUnknown(op.toKeyClass())) {
                return op.toKeyClass();
            }
            else if( ! isUnknown(op.fromKeyClass())) {
                return op.fromKeyClass();
            }
        }
        return UNKNOWN_CLASS;
    }

    @Override
    public Class toValueClass() {
        for (int i = sequence.size() - 1; i >= 0; --i) {
            OpKVTransformer op = (OpKVTransformer) sequence.get(i);
            if (!isUnknown(op.toValueClass())) {
                return op.toValueClass();
            }
            else if( ! isUnknown(op.toValueClass())) {
                return op.toValueClass();
            }
        }
        return UNKNOWN_CLASS;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        try {
            dataOutput.writeInt(this.getSequence().size());
            for (KVUnoOp op : this.getSequence()) {
                WritableObjectDictionaryFactory.writeObject(op, dataOutput);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        List<KVUnoOp> sequence = new ArrayList<KVUnoOp>(size);
        for (int i = 0; i < size; ++i) {
            // todo : not only simple KVOPs could be there but I prefer to forget about this for a while
            WritableObjectDictionaryFactory.SimpleKVOpWritable w = new WritableObjectDictionaryFactory.SimpleKVOpWritable();
            w.readFields(dataInput);
            sequence.add((KVUnoOp) w.op);
        }
        this.sequence = sequence;
    }
}
