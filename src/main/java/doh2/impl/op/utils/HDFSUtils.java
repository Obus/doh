package doh2.impl.op.utils;

import com.synqera.bigkore.rank.PlatformUtils;
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
        return FileUtil.stat2Paths(fs.listStatus(path, new PlatformUtils.OutputFilesFilter(conf)));
    }

}
