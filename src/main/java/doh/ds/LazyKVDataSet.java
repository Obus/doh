package doh.ds;

import com.google.common.collect.Lists;
import doh.op.Op;
import doh.op.kvop.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.common.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static doh.op.KVOpJobUtils.configureJob;

public class LazyKVDataSet<Key, Value, FromKey, FromValue> implements KVDataSet<Key, Value> {

    protected final LazyKVDataSet<FromKey, FromValue, ?, ?> parentDataSet;
    protected final KVOp<FromKey, FromValue, Key, Value> parentOperation;

    public LazyKVDataSet(LazyKVDataSet<FromKey, FromValue, ?, ?> parentDataSet, KVOp<FromKey, FromValue, Key, Value> parentOperation) {
        this.parentDataSet = parentDataSet;
        this.parentOperation = parentOperation;
    }

    protected RealKVDataSet<Key, Value> real = null;

    protected synchronized RealKVDataSet<Key, Value> real() {
        if (real == null) {
            try {
                beReady();//real = parentDataSet.real().applyMR(parentOperation);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return real;
    }

    public boolean isReady() {
        return real != null;
    }

    protected void beReady() throws Exception {
        Iterator<Pair<LazyKVDataSet, KVOp>> ancestorsIt = ancestors().iterator();
        List<KVOneOp> oneOpSequence = new ArrayList<KVOneOp>();
        while (ancestorsIt.hasNext()) {
            Pair<LazyKVDataSet, KVOp> p = ancestorsIt.next();
            if (p.getSecond() instanceof KVOneOp) {
                oneOpSequence.add((KVOneOp) p.getSecond());
            } else if (p.getSecond() instanceof ReduceOp) {
                final Job job;
                if (oneOpSequence.isEmpty()) {
                    job = configureJob((ReduceOp) p.getSecond());
                } else {
                    job = configureJob(new FlatSequenceOp(oneOpSequence), (ReduceOp) p.getSecond());
                }
            } else {
                throw new IllegalArgumentException("Unknown KV operation type: "
                        + p.getSecond().getClass());
            }

        }
    }


    protected List<Pair<LazyKVDataSet, KVOp>> ancestors() {
        List<Pair<LazyKVDataSet, KVOp>> ancestors = Lists.newArrayList();
        LazyKVDataSet current = this;
        while (!current.isReady()) {
            ancestors.add(0, new Pair<LazyKVDataSet, KVOp>(current.getParentDataSet(), current.getParentOperation()));
            current = current.getParentDataSet();
        }
        return ancestors;
    }

    protected Pair<LazyKVDataSet, List<KVOp>> originAndPathFromOrigin() {
        List<KVOp> pathFromOrigin = new LinkedList<KVOp>();
        LazyKVDataSet current = this;
        while (!current.isReady()) {
            pathFromOrigin.add(0, current.getParentOperation());
            current = current.getParentDataSet();
        }
        return new Pair<LazyKVDataSet, List<KVOp>>(current, pathFromOrigin);
    }


    public LazyKVDataSet<FromKey, FromValue, ?, ?> getParentDataSet() {
        return parentDataSet;
    }

    public KVOp<FromKey, FromValue, Key, Value> getParentOperation() {
        return parentOperation;
    }

    @Override
    public Iterator<KV<Key, Value>> iteratorChecked() throws IOException {
        return real().iteratorChecked();
    }

    @Override
    public MapKVDataSet<Key, Value> toMapKVDS() {
        return real().toMapKVDS();
    }

    @Override
    public Iterator<KV<Key, Value>> iterator() {
        return real().iterator();
    }

    @Override
    public <TORIGIN> DataSet<TORIGIN> apply(Op<KV<Key, Value>, TORIGIN> op) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public <ToKey, ToValue>
    LazyKVDataSet<ToKey, ToValue, Key, Value> map(MapOp<Key, Value, ToKey, ToValue> mapOp)
            throws Exception {
        return new LazyKVDataSet<ToKey, ToValue, Key, Value>(this, mapOp);
    }

    @Override
    public <ToKey, ToValue>
    LazyKVDataSet<ToKey, ToValue, Key, Value> flatMap(FlatMapOp<Key, Value, ToKey, ToValue> flatMapOp)
            throws Exception {
        return new LazyKVDataSet<ToKey, ToValue, Key, Value>(this, flatMapOp);
    }

    @Override
    public <ToKey, ToValue>
    LazyKVDataSet<ToKey, ToValue, Key, Value> reduce(ReduceOp<Key, Value, ToKey, ToValue> reduceOp)
            throws Exception {
        return new LazyKVDataSet<ToKey, ToValue, Key, Value>(this, reduceOp);
    }

    @Override
    public Class<?> writableKeyClass() throws IOException {
        return real().writableKeyClass();
    }

    @Override
    public Class<?> writableValueClass() throws IOException {
        return real().writableValueClass();
    }

    @Override
    public Class<Key> keyClass() throws IOException {
        return real().keyClass();
    }

    @Override
    public Class<Value> valueClass() throws IOException {
        return real().valueClass();
    }

    @Override
    public Path getPath() {
        return real().getPath();
    }


}
