package doh.crazy;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class Context {
    private final TempPathManager tempPathManager = null;
    private final JobRunner jobRunner = null;

    public Path nextTempPath() throws Exception {
        return tempPathManager.getNextPath();
    }

    public void runJob(Job job) throws Exception {

    }



}
