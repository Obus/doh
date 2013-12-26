package doh.op.kvop;

import com.google.common.collect.Lists;
import doh.op.OpParameter;
import doh.op.OpSequence;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class CompositeMapOp<FromKey, FromValue, ToKey, ToValue>
        extends KVUnoOp<FromKey, FromValue, ToKey, ToValue>
        implements Writable, OpKVTransformer<FromKey, FromValue, ToKey, ToValue> {

    @OpParameter
    public OpSequence sequence;

    public CompositeMapOp() {}

    public CompositeMapOp(List<KVUnoOp> sequence) {
        this.sequence = new OpSequence(sequence);
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        sequence.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sequence = new OpSequence();
        sequence.readFields(dataInput);
    }

    @Override
    public Some<KV<ToKey, ToValue>> applyUno(KV<FromKey, FromValue> f) {
        List<KV> currentKVList = Lists.newArrayList((KV) f);
        for (KVUnoOp op : sequence.getSequence()) {
            currentKVList = step(op, currentKVList);
        }
        return many((List<KV<ToKey, ToValue>>) (Object) currentKVList);
    }

    private List<KV> step(KVUnoOp unoOp, List<KV> currentKVList) {
        List<KV> nextKVList = Lists.newArrayList();
        for (KV kv : currentKVList) {
            expand(nextKVList, unoOp.applyUno(kv));
        }
        return nextKVList;
    }

    @Override
    public Class<FromKey> fromKeyClass() {
        return sequence.fromKeyClass();
    }

    @Override
    public Class<FromValue> fromValueClass() {
        return sequence.fromValueClass();
    }

    @Override
    public Class<ToKey> toKeyClass() {
        return  sequence.toKeyClass();
    }

    @Override
    public Class<ToValue> toValueClass() {
        return sequence.toValueClass();
    }

    public static <T> void expand(List<T> list, Iterable<T> it) {
        for (T t : it) {
            list.add(t);
        }
    }
}
