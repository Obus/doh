package doh.crazy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class Context {
    private final TempPathManager tempPathManager;
    private final JobRunner jobRunner;
    private final Configuration conf;

    public Context(TempPathManager tempPathManager, JobRunner jobRunner, Configuration conf) {
        this.tempPathManager = tempPathManager;
        this.jobRunner = jobRunner;
        this.conf = conf;
    }

    public static Context create(Configuration conf, Path tempDir) {
        TempPathManager tempPathManager = new TempPathManager(tempDir);
        return new Context(tempPathManager, new JobRunner(), conf);
    }

    public Path nextTempPath() throws Exception {
        return tempPathManager.getNextPath();
    }

    public void runJob(Job job) throws Exception {
       jobRunner.runJob(job);
    }

    public Configuration getConf() {
        return conf;
    }
}
