package doh2.api;

import doh.api.TempPathManager;
import doh.op.JobRunner;
import doh2.impl.DSExecutor;
import doh2.impl.OnDemandDS;
import doh2.impl.OpJobMaker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class DSContext {
    private final Configuration conf;
    private final TempPathManager tempPathManager;
    private final DSExecutor dsExecutor;
    private final JobRunner jobRunner;

    public static final Class<? extends OutputFormat> DEFAULT_FORMAT_CLASS = SequenceFileOutputFormat.class;

    public DSContext(Configuration conf, JobRunner jobRunner, Path tempDir) {
        this.conf = conf;
        this.jobRunner = jobRunner;
        this.tempPathManager = new TempPathManager(tempDir);
        this.dsExecutor = new DSExecutor(tempPathManager, conf, DEFAULT_FORMAT_CLASS, jobRunner, new OpJobMaker());
    }

    public Configuration conf() {
        return conf;
    }

    public void execute(DS ... dses) throws Exception {
        OnDemandDS[] dsesOK = new OnDemandDS[dses.length];
        for (int i = 0; i < dses.length; ++i) {
            DS ds = dses[i];
            if (! (ds instanceof OnDemandDS)) {
                throw new IllegalArgumentException("Only OnDemandDS are supported, but got: " + ds.getClass().getName());
            }
            dsesOK[i] = (OnDemandDS) ds;
        }
        dsExecutor.execute(dsesOK);
    }


}
