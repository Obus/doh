package doh.ds;

import com.google.common.collect.Lists;
import doh.api.KVDataSet;
import doh.api.op.FlatMapOp;
import doh.api.op.KV;
import doh.api.op.MapOp;
import doh.api.op.ReduceOp;
import doh.op.Context;
import doh.op.Op;
import doh.op.kvop.*;
import doh.op.mr.LazyKVDataSetReadyMaker;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class LazyKVDataSet<Key, Value> implements KVDataSet<Key, Value> {

    protected final LazyKVDataSet<?, ?> parentDataSet;
    protected final KVOp<?, ?, Key, Value> parentOperation;
    protected final Context context;

    public <FromKey, FromValue> LazyKVDataSet(LazyKVDataSet<FromKey, FromValue> parentDataSet, KVOp<FromKey, FromValue, Key, Value> parentOperation, Context context) {
        this.parentDataSet = parentDataSet;
        this.parentOperation = parentOperation;
        this.context = context;
    }

    public LazyKVDataSet(Context context, RealKVDataSet<Key, Value> real) {
        this.parentOperation = null;
        this.parentDataSet = null;
        this.context = context;
        this.real = real;
    }

    protected RealKVDataSet<Key, Value> real = null;

    public synchronized RealKVDataSet<Key, Value> real() {
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

    public void setReal(RealKVDataSet<Key, Value> real) {
        this.real = real;
    }

    @Override
    public synchronized void beReady() throws Exception {
        new LazyKVDataSetReadyMaker(this).makeItReady();
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


    public LazyKVDataSet<?, ?> getParentDataSet() {
        return parentDataSet;
    }

    public KVOp<?, ?, Key, Value> getParentOperation() {
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
    LazyKVDataSet<ToKey, ToValue> map(MapOp<Key, Value, ToKey, ToValue> mapOp)
            throws Exception {
        return new LazyKVDataSet<ToKey, ToValue>(this, mapOp, context);
    }

    @Override
    public <ToKey, ToValue>
    LazyKVDataSet<ToKey, ToValue> flatMap(FlatMapOp<Key, Value, ToKey, ToValue> flatMapOp)
            throws Exception {
        return new LazyKVDataSet<ToKey, ToValue>(this, flatMapOp, context);
    }

    // todo: Change reducer
    @Override
    public <ToKey, ToValue>
    LazyKVDataSet<ToKey, ToValue> reduce(ReduceOp<Key, Value, ToKey, ToValue> reduceOp)
            throws Exception {
        return new LazyKVDataSet<ToKey, ToValue>(this, (KVOp<Key, Value, ToKey, ToValue>)reduceOp, context);
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
