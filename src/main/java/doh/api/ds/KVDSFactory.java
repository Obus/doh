package doh.api.ds;

import com.google.common.collect.Lists;
import doh.api.Context;
import doh.ds.LazyKVDS;
import doh.ds.MapKVDS;
import doh.ds.RealKVDS;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.List;

public class KVDSFactory {

    public static <Key, Value> LazyKVDS<Key, Value> createLazy(RealKVDS<Key, Value> real) {
        LazyKVDS<Key, Value> lazyKVDS =
                new LazyKVDS<Key, Value>(real);
        return lazyKVDS;
    }

    public static <Key, Value> RealKVDS<Key, Value> createReal(Context context, Path path) {
        RealKVDS<Key, Value> realKVDS = new RealKVDS<Key, Value>(path);
        realKVDS.setContext(context);
        return realKVDS;
    }

    public static <Key, Value> RealKVDS<Key, Value> createReal(Context context, Path[] path) {
        RealKVDS<Key, Value> realKVDS = new RealKVDS<Key, Value>(path);
        realKVDS.setContext(context);
        return realKVDS;
    }

    public static <Key, Value> MapKVDS<Key, Value> createMap(Context context, Path path) {
        return KVDSFactory.<Key, Value>createReal(context, path).toMapKVDS();
    }

    public static <Key, Value> RealKVDS<Key, Value> createReal(RealKVDS<Key, Value> a, RealKVDS<Key, Value> b) {
        List<Path> paths = Lists.newArrayList(RealKVDS.getPaths(a.getLocation()));
        paths.addAll(Arrays.asList(RealKVDS.getPaths(b.getLocation())));
        return createReal(a.getContext(), paths.toArray(new Path[paths.size()]));
    }


    public static <Key, Value> KVDS<Key, Value> create(Context context, Path path) {
        return createLazy(KVDSFactory.<Key, Value>createReal(context, path));
    }

    public static <Key, Value> KVDS<Key, Value> create(Context context, Path[] path) {
        return createLazy(KVDSFactory.<Key, Value>createReal(context, path));
    }
}
