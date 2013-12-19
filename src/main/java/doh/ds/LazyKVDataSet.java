package doh.ds;

import doh.crazy.FlatMapOp;
import doh.crazy.KV;
import doh.crazy.KVOp;
import doh.crazy.MapOp;
import doh.crazy.Op;
import doh.crazy.ReduceOp;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class LazyKVDataSet<Key, Value, FromKey, FromValue> implements KVDataSet<Key, Value>  {

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
                real = parentDataSet.real().applyMR(parentOperation);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return real;
    }

    public boolean isBackedByReal() {
        return real != null;
    }


    public static void collapse() {

    }

    protected Pair<LazyKVDataSet, List<KVOp>> originAndPathFromOrigin() {
        List<KVOp> pathFromOrigin = new LinkedList<KVOp>();
        LazyKVDataSet current = this;
        while (!current.isBackedByReal()) {
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
