package doh.ds;

import doh.crazy.MapOp;
import doh.crazy.Op;
import doh.crazy.OpSerializer;
import doh.crazy.ReduceOp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.List;


public abstract class DataSet<ORIGIN> {
    protected final Path path;
    protected final Configuration conf;

    protected DataSet(Configuration conf, Path path) {
        this.path = path;
        this.conf = conf;
    }

    public <TORIGIN> DataSet<TORIGIN> apply(Op<ORIGIN, TORIGIN> op) {

    }


    public Path getPath() {
        return path;
    }

    public Configuration getConf() {
        return conf;
    }

    public List<ORIGIN> read() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    public void copy(Path to) {

    }

    public void move(Path to) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    public static <F, T> DataSet<T> applyOperation(DataSet<F> from, Op<F, T> op) throws Exception {
        Configuration conf = from.getConf();
        Path input = from.getPath();
        Path output = from.next();
        OpSerializer.saveOpFieldsToConf(conf, op);

        if (op instanceof MapOp) {
            Job job = new Job(conf, "Map only job");


            FileInputFormat.setInputPaths(job, input);
            FileOutputFormat.setOutputPath(job, output);
        } else if (op instanceof ReduceOp) {

        } else {
            throw new IllegalArgumentException("Unsupported Op type: " + op.getClass());
        }
    }


    public abstract Path next();
}
