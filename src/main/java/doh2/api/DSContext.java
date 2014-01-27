package doh2.api;

import doh.api.TempPathManager;
import doh.op.JobRunner;
import doh.op.StringSerDe;
import doh.op.serde.OpSerializer;
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

    private final OpSerializer opSerializer;

    public static final Class<? extends OutputFormat> DEFAULT_FORMAT_CLASS = SequenceFileOutputFormat.class;

    public DSContext(Configuration conf, JobRunner jobRunner, Path tempDir) {
        this.conf = conf;
        this.jobRunner = jobRunner;
        this.tempPathManager = new TempPathManager(tempDir);
        this.opSerializer = OpSerializer.create(conf, new StringSerDe.GsonStringSerDe());
        this.dsExecutor = new DSExecutor(this);
    }

    public Configuration conf() {
        return conf;
    }

    public static Class<? extends OutputFormat> getDefaultFormatClass() {
        return DEFAULT_FORMAT_CLASS;
    }

    public void execute(DS ... dses) throws Exception {
        OnDemandDS[] dsesOK = new OnDemandDS[dses.length];
        for (int i = 0; i < dses.length; ++i) {
            DS ds = dses[i];
            if (! (ds instanceof OnDemandDS)) {
                throw new IllegalArgumentException("Only OnDemandDS are supported, but got: " +
                        ds.getClass().getName());
            }
            dsesOK[i] = (OnDemandDS) ds;
        }
        dsExecutor.execute(dsesOK);
    }


    public TempPathManager getTempPathManager() {
        return tempPathManager;
    }

    public DSExecutor getDsExecutor() {
        return dsExecutor;
    }

    public JobRunner getJobRunner() {
        return jobRunner;
    }

    public OpSerializer getOpSerializer() {
        return opSerializer;
    }
}
