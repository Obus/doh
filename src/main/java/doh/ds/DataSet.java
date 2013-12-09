package doh.ds;

import doh.crazy.Context;
import doh.crazy.MapOp;
import doh.crazy.Op;
import doh.crazy.OpSerializer;
import doh.crazy.ReduceOp;
import doh.crazy.SimpleOpMapper;
import doh.crazy.SimpleOpReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.List;


public abstract class DataSet<ORIGIN> {
    protected final Context context;
    protected final Path path;

    protected DataSet(Context context, Path path) {
        this.context = context;
        this.path = path;
    }

    public abstract <TORIGIN> DataSet<TORIGIN> apply(Op<ORIGIN, TORIGIN> op) throws Exception ;


    public Path getPath() {
        return path;
    }

    public Configuration getConf() {
        return context.getConf();
    }

    public List<ORIGIN> read() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }


    public Path nextPath() throws Exception {
        return context.nextTempPath();
    }
}
