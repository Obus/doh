package doh.op.utils;

import doh.api.Context;
import doh.api.ds.HDFSLocation;
import doh.ds.RealKVDS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.FSUtils;

import java.io.IOException;

public class HDFSUtils {
    public static synchronized Path[] listDirectories(Configuration configuration, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(configuration);
        return FileUtil.stat2Paths(fs.listStatus(path, new FSUtils.DirFilter(fs)));
    }

    public static synchronized Path[] listOutputFiles(Configuration conf, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        return FileUtil.stat2Paths(fs.listStatus(path, new RealKVDS.OutputFilesFilter(conf)));
    }

    public static <KEY, VALUE> RealKVDS<KEY, VALUE> create(Context context, Path path) {
        RealKVDS<KEY, VALUE> kvds = new RealKVDS<KEY, VALUE>(new HDFSLocation.SingleHDFSLocation(path));
        kvds.setContext(context);
        return kvds;
    }
}
