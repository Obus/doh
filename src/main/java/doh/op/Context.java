package doh.op;

import doh.op.serde.OpSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.common.Pair;

import java.util.List;

public class Context {
    private final TempPathManager tempPathManager;
    private final JobRunner jobRunner;
    private final Configuration conf;
    private final OpSerializer opSerializer;

    public Context(TempPathManager tempPathManager, JobRunner jobRunner, Configuration conf) {
        this.tempPathManager = tempPathManager;
        this.jobRunner = jobRunner;
        this.conf = conf;
        this.opSerializer = OpSerializer.create(conf, new StringSerDe.GsonStringSerDe());
    }

    public Context(TempPathManager tempPathManager, JobRunner jobRunner, Configuration conf, StringSerDe stringSerDe) {
        this.tempPathManager = tempPathManager;
        this.jobRunner = jobRunner;
        this.conf = conf;
        this.opSerializer = OpSerializer.create(conf, stringSerDe);
    }

    public static Context create(Configuration conf, Path tempDir) {
        TempPathManager tempPathManager = new TempPathManager(tempDir);
        return new Context(tempPathManager, new JobRunner(), conf);
    }
    public static Context create(Configuration conf) {
        return new Context(null, null, conf);
    }

    public static Context create(Configuration conf, TempPathManager tempPathManager) {
        return new Context(tempPathManager, new JobRunner(), conf);
    }

    public static Context create(Configuration conf, TempPathManager tempPathManager, StringSerDe stringSerDe) {
        return new Context(tempPathManager, new JobRunner(), conf, stringSerDe);
    }
    public static Context create(Configuration conf, TempPathManager tempPathManager, List<Pair<Class, Object>> typeAdapters) {
        return new Context(tempPathManager, new JobRunner(), conf, new StringSerDe.GsonStringSerDe(typeAdapters));
    }

    public OpSerializer opSerializer() {
        return opSerializer;
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
