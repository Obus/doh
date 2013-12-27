package doh.api.ds;

import com.google.common.collect.Lists;
import doh.ds.LazyKVDataSet;
import doh.ds.MapKVDataSet;
import doh.ds.RealKVDataSet;
import doh.op.Context;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.List;

public class KVDataSetFactory {

    public static  <Key, Value> LazyKVDataSet<Key, Value> createLazy(RealKVDataSet<Key, Value> real) {
        LazyKVDataSet<Key, Value> lazyKVDS =
                new LazyKVDataSet<Key, Value>(real);
        return lazyKVDS;
    }
    public static <Key, Value> RealKVDataSet<Key, Value> createReal(Context context, Path path) {
        RealKVDataSet<Key, Value> realKVDS = new RealKVDataSet<Key, Value>(path);
        realKVDS.setContext(context);
        return realKVDS;
    }
    public static <Key, Value> RealKVDataSet<Key, Value> createReal(Context context, Path[] path) {
        RealKVDataSet<Key, Value> realKVDS = new RealKVDataSet<Key, Value>(path);
        realKVDS.setContext(context);
        return realKVDS;
    }

    public static <Key, Value> MapKVDataSet<Key, Value> createMap(Context context, Path path) {
        return KVDataSetFactory.<Key, Value>createReal(context, path).toMapKVDS();
    }

    public static <Key, Value> RealKVDataSet<Key, Value> createReal(RealKVDataSet<Key, Value> a, RealKVDataSet<Key, Value> b) {
        List<Path> paths = Lists.newArrayList(RealKVDataSet.getPaths(a.getLocation()));
        paths.addAll(Arrays.asList(RealKVDataSet.getPaths(b.getLocation())));
        return createReal(a.getContext(), paths.toArray(new Path[paths.size()]));
    }


    public static <Key, Value> KVDataSet<Key, Value> create(Context context, Path path) {
        return createLazy(KVDataSetFactory.<Key, Value>createReal(context, path));
    }
    public static <Key, Value> KVDataSet<Key, Value> create(Context context, Path[] path) {
        return createLazy(KVDataSetFactory.<Key, Value>createReal(context, path));
    }
}
