package doh.api.ds;

import doh.api.Context;
import doh.api.op.FlatMapOp;
import doh.api.op.KV;
import doh.api.op.MapOp;
import doh.api.op.ReduceOp;
import doh.ds.DS;
import doh.ds.MapKVDS;
import doh.op.Op;

import java.io.IOException;
import java.util.Iterator;

public interface KVDS<KEY, VALUE> extends Iterable<KV<KEY, VALUE>>, DS<KV<KEY, VALUE>> {

    Iterator<KV<KEY, VALUE>> iteratorChecked() throws IOException;

    MapKVDS<KEY, VALUE> toMapKVDS();

    boolean isReady();

    KVDS<KEY, VALUE> beReady() throws Exception;

    KVDS<KEY, VALUE> comeTogetherRightNow(KVDS<KEY, VALUE> other);

    @Override
    Iterator<KV<KEY, VALUE>> iterator();

    <TORIGIN> DS<TORIGIN> apply(Op<KV<KEY, VALUE>, TORIGIN> op) throws Exception;

    <TKEY, TVALUE> KVDS<TKEY, TVALUE> map(
            MapOp<KEY, VALUE, TKEY, TVALUE> mapOp
    ) throws Exception;

    <TKEY, TVALUE> KVDS<TKEY, TVALUE> flatMap(
            FlatMapOp<KEY, VALUE, TKEY, TVALUE> flatMapOp
    ) throws Exception;

    <TKEY, TVALUE> KVDS<TKEY, TVALUE> reduce(
            ReduceOp<KEY, VALUE, TKEY, TVALUE> reduceOp
    ) throws Exception;

    Class<?> writableKeyClass() throws IOException;

    Class<?> writableValueClass() throws IOException;

    Class<KEY> keyClass() throws IOException;

    Class<VALUE> valueClass() throws IOException;

    void setContext(Context context);
    Context getContext();
}
