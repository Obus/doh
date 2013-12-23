package doh.op;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Utils;

import java.util.concurrent.atomic.AtomicInteger;

public class TempPathManager {
    private final Path tempDir;

    private volatile AtomicInteger atomic = new AtomicInteger(1);

    public TempPathManager(Path tempDir) {
        this.tempDir = tempDir;
    }


    public synchronized void loadState(Configuration conf) throws Exception {
        FileSystem fs = tempDir.getFileSystem(conf);
        FileStatus[] fileStatuses = fs.listStatus(tempDir, new Utils.OutputFileUtils.OutputFilesFilter());
        int max = 0;
        for (FileStatus fileStatus : fileStatuses) {
            max = Math.max(max, Integer.parseInt(fileStatus.getPath().getName()));
        }
        atomic = new AtomicInteger(max);
    }


    public synchronized Path getNextPath() {
        return new Path(tempDir, Integer.toString(atomic.incrementAndGet()));
    }
}
