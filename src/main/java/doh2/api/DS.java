package doh2.api;


import doh.api.ds.KVDS;
import doh.api.op.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.io.IOException;
import java.util.Iterator;

public interface DS<KEY, VALUE> extends Iterable<KV<KEY, VALUE>> {

    Iterator<KV<KEY, VALUE>> iteratorChecked() throws IOException;

    MapDS<KEY, VALUE> toMapDS();

    KVDS<KEY, VALUE> comeTogetherRightNow(KVDS<KEY, VALUE> other);

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

    void setOutputPath(Path path);

    void setOutputFormatCLass(Class<? extends OutputFormat> outputFormatCLass);

    void setNumReduceTasks(int numReduceTasks);


    boolean isReady();
}
