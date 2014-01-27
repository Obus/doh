package doh2.api;


import doh2.api.op.FilterOp;
import doh2.api.op.FlatMapOp;
import doh2.api.op.KV;
import doh2.api.op.MapOp;
import doh2.api.op.ReduceOp;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.util.Iterator;

public interface DS<KEY, VALUE> extends Iterable<KV<KEY, VALUE>> {

    Iterator<KV<KEY, VALUE>> iteratorChecked() throws Exception;

    MapDS<KEY, VALUE> toMapDS() throws Exception ;

    DS<KEY, VALUE> comeTogetherRightNow(DS<KEY, VALUE> other);

    @Override
    Iterator<KV<KEY, VALUE>> iterator();

    DS<KEY, VALUE> filter(
            FilterOp<KEY, VALUE> filterOp
    ) throws Exception;

    <TKEY, TVALUE> DS<TKEY, TVALUE> map(
            MapOp<KEY, VALUE, TKEY, TVALUE> mapOp
    ) throws Exception;

    <TKEY, TVALUE> DS<TKEY, TVALUE> flatMap(
            FlatMapOp<KEY, VALUE, TKEY, TVALUE> flatMapOp
    ) throws Exception;

    <TKEY, TVALUE> DS<TKEY, TVALUE> reduce(
            ReduceOp<KEY, VALUE, TKEY, TVALUE> reduceOp
    ) throws Exception;

    HDFSLocation getLocation();

    DS<KEY, VALUE> setOutputPath(Path path);

    DS<KEY, VALUE> setOutputFormatCLass(Class<? extends OutputFormat> outputFormatCLass);

    DS<KEY, VALUE> setNumReduceTasks(int numReduceTasks);

    DS<KEY, VALUE> breakJobHere();

    DS<KEY, VALUE> execute() throws Exception;

    boolean isReady();
}
