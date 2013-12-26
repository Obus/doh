package doh.ds;

import doh.op.Op;
import doh.op.kvop.FlatMapOp;
import doh.op.kvop.KV;
import doh.op.kvop.MapOp;
import doh.op.kvop.ReduceOp;

import java.io.IOException;
import java.util.Iterator;

public interface KVDataSet<KEY, VALUE> extends Iterable<KV<KEY, VALUE>>, DataSet<KV<KEY, VALUE>> {

    Iterator<KV<KEY, VALUE>> iteratorChecked() throws IOException;

    MapKVDataSet<KEY, VALUE> toMapKVDS();

    boolean isReady();

    void beReady() throws Exception ;

    @Override
    Iterator<KV<KEY, VALUE>> iterator();

    <TORIGIN> DataSet<TORIGIN> apply(Op<KV<KEY, VALUE>, TORIGIN> op) throws Exception;

    <TKEY, TVALUE> KVDataSet<TKEY, TVALUE> map(
            MapOp<KEY, VALUE, TKEY, TVALUE> mapOp
    ) throws Exception;

    <TKEY, TVALUE> KVDataSet<TKEY, TVALUE> flatMap(
            FlatMapOp<KEY, VALUE, TKEY, TVALUE> flatMapOp
    ) throws Exception;

    <TKEY, TVALUE> KVDataSet<TKEY, TVALUE> reduce(
            ReduceOp<KEY, VALUE, TKEY, TVALUE> reduceOp
    ) throws Exception;

    Class<?> writableKeyClass() throws IOException;

    Class<?> writableValueClass() throws IOException;

    Class<KEY> keyClass() throws IOException;

    Class<VALUE> valueClass() throws IOException;
}
