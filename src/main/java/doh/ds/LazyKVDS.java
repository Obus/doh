package doh.ds;

import doh.api.Context;
import doh.api.ds.KVDS;
import doh.api.ds.KVDSFactory;
import doh.api.ds.Location;
import doh.api.op.FlatMapOp;
import doh.api.op.KV;
import doh.api.op.MapOp;
import doh.api.op.ReduceOp;
import doh.op.Op;
import doh.op.kvop.KVOp;
import doh.op.mr.LazyKVDataSetCooker;

import java.io.IOException;
import java.util.Iterator;

public class LazyKVDS<Key, Value> implements KVDS<Key, Value> {

    protected final LazyKVDS<?, ?> parentDataSet;
    protected final KVOp<?, ?, Key, Value> parentOperation;
    protected final Context context;

    public <FromKey, FromValue> LazyKVDS(LazyKVDS<FromKey, FromValue> parentDataSet, KVOp<FromKey, FromValue, Key, Value> parentOperation, Context context) {
        this.parentDataSet = parentDataSet;
        this.parentOperation = parentOperation;
        this.context = context;
    }

    public LazyKVDS(Context context, RealKVDS<Key, Value> real) {
        this.parentOperation = null;
        this.parentDataSet = null;
        this.context = context;
        this.real = real;
    }

    public LazyKVDS(RealKVDS<Key, Value> real) {
        this.parentOperation = null;
        this.parentDataSet = null;
        this.context = real.context;
        this.real = real;
    }

    @Override
    public void setContext(Context context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Context getContext() {
        throw new UnsupportedOperationException();
    }

    protected RealKVDS<Key, Value> real = null;

    public synchronized RealKVDS<Key, Value> real() {
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

    public void setReal(RealKVDS<Key, Value> real) {
        this.real = real;
    }

    @Override
    public synchronized LazyKVDS<Key, Value> beReady() throws Exception {
        new LazyKVDataSetCooker(this).cookIt();
        return this;
    }


    @Override
    public KVDS<Key, Value> comeTogetherRightNow(KVDS<Key, Value> other) {
        RealKVDS<Key, Value> a = real();
        RealKVDS<Key, Value> b = other instanceof RealKVDS ?
                (RealKVDS<Key, Value>) other :
                ((LazyKVDS<Key, Value>) other).real();
        return KVDSFactory.createLazy(a.comeTogetherRightNow(b));
    }

    public LazyKVDS<?, ?> getParentDataSet() {
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
    public MapKVDS<Key, Value> toMapKVDS() {
        return real().toMapKVDS();
    }

    @Override
    public Iterator<KV<Key, Value>> iterator() {
        return real().iterator();
    }

    @Override
    public <TORIGIN> DS<TORIGIN> apply(Op<KV<Key, Value>, TORIGIN> op) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public <ToKey, ToValue>
    LazyKVDS<ToKey, ToValue> map(MapOp<Key, Value, ToKey, ToValue> mapOp)
    throws Exception {
        return new LazyKVDS<ToKey, ToValue>(this, mapOp, context);
    }

    @Override
    public <ToKey, ToValue>
    LazyKVDS<ToKey, ToValue> flatMap(FlatMapOp<Key, Value, ToKey, ToValue> flatMapOp)
    throws Exception {
        return new LazyKVDS<ToKey, ToValue>(this, flatMapOp, context);
    }

    // todo: Change reducer
    @Override
    public <ToKey, ToValue>
    LazyKVDS<ToKey, ToValue> reduce(ReduceOp<Key, Value, ToKey, ToValue> reduceOp)
    throws Exception {
        return new LazyKVDS<ToKey, ToValue>(this, (KVOp<Key, Value, ToKey, ToValue>) reduceOp, context);
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
    public Location getLocation() {
        return real().getLocation();
    }


}
