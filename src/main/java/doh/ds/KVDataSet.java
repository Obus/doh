package doh.ds;

import doh.crazy.FlatMapOp;
import doh.crazy.KV;
import doh.crazy.MapOp;
import doh.crazy.Op;
import doh.crazy.ReduceOp;

import java.io.IOException;
import java.util.Iterator;

public interface KVDataSet<KEY, VALUE> extends Iterable<KV<KEY, VALUE>>, DataSet<KV<KEY, VALUE>> {
    Iterator<KV<KEY, VALUE>> iteratorChecked() throws IOException;

    MapKVDataSet<KEY, VALUE> toMapKVDS();

    @Override
    Iterator<KV<KEY, VALUE>> iterator();

    <TORIGIN> DataSet<TORIGIN> apply(Op<KV<KEY, VALUE>, TORIGIN> op) throws Exception;

    <KEY, VALUE, TKEY, TVALUE> KVDataSet<TKEY, TVALUE> map(
            MapOp<KEY, VALUE, TKEY, TVALUE> mapOp
    ) throws Exception;

    <KEY, VALUE, TKEY, TVALUE> KVDataSet<TKEY, TVALUE> flatMap(
            FlatMapOp<KEY, VALUE, TKEY, TVALUE> flatMapOp
    ) throws Exception;

    <KEY, VALUE, TKEY, TVALUE> KVDataSet<TKEY, TVALUE> reduce(
            ReduceOp<KEY, VALUE, TKEY, TVALUE> reduceOp
    ) throws Exception;

    Class<?> writableKeyClass() throws IOException;

    Class<?> writableValueClass() throws IOException;

    Class<KEY> keyClass() throws IOException;

    Class<VALUE> valueClass() throws IOException;
}
