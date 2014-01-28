package doh2.api;

import doh2.impl.ondemand.OnDemandDS;
import org.apache.hadoop.fs.Path;

public class DSFactory {

    public static  <KEY, VALUE> DS<KEY, VALUE> create(Path dsPath, DSContext dsContext) {
        return new OnDemandDS<KEY, VALUE>(dsContext, new SingleHDFSLocation(dsPath));
    }
}
