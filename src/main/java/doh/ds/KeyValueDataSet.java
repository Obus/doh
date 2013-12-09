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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.Pair;

import static doh.crazy.WritableObjectDictionaryFactory.getWritableClass;

public class KeyValueDataSet<KEY, VALUE> extends DataSet<Pair<KEY, VALUE>> {
    public KeyValueDataSet(Context context, Path path) {
        super(context, path);
    }

    @Override
    public <TORIGIN> DataSet<TORIGIN> apply(Op<Pair<KEY, VALUE>, TORIGIN> op) throws Exception {
        if (op instanceof MapOp) {
            return map((MapOp) op);
        }
        else if (op instanceof ReduceOp) {
            return reduce((ReduceOp) op);
        }
        throw new IllegalArgumentException("Unsupported operation type " + op.getClass());
    }

    public <KEY, VALUE, BKEY, BVALUE, TKEY, TVALUE> KeyValueDataSet<TKEY, TVALUE> mapReduce(
            MapOp<KEY, VALUE, BKEY, BVALUE> mapOp,
            ReduceOp<BKEY, BVALUE, TKEY, TVALUE> reduceOp
    ) throws Exception {

        Configuration conf = this.getConf();
        Path input = this.getPath();
        Path output = context.nextTempPath();

        Job job = new Job(conf, "MapReduce job");
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        setUpMapOpJob(job, mapOp);
        job.setJobName(job.getJobName() + ".\n MapOp: " + mapOp.getClass().getSimpleName());

        setUpReduceOpJob(job, reduceOp);
        job.setJobName(job.getJobName() + ".\n ReduceOp: " + reduceOp.getClass().getSimpleName());

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        context.runJob(job);

        return new KeyValueDataSet<TKEY, TVALUE>(context, output);
    }

    public <KEY, VALUE, TKEY, TVALUE> KeyValueDataSet<TKEY, TVALUE> map(
            MapOp<KEY, VALUE, TKEY, TVALUE> mapOp
    ) throws Exception {

        Configuration conf = this.getConf();
        Path input = this.getPath();
        Path output = context.nextTempPath();

        Job job = new Job(conf, "Map only job");
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        setUpMapOnlyOpJob(job, mapOp);
        job.setJobName(job.getJobName() + ".\n MapOp: " + mapOp.getClass().getSimpleName());

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        context.runJob(job);

        return new KeyValueDataSet<TKEY, TVALUE>(context, output);
    }

    public <KEY, VALUE, TKEY, TVALUE> KeyValueDataSet<TKEY, TVALUE> reduce(
            ReduceOp<KEY, VALUE, TKEY, TVALUE> reduceOp
    ) throws Exception {

        Configuration conf = this.getConf();
        Path input = this.getPath();
        Path output = context.nextTempPath();

        Job job = new Job(conf, "Reduce only job");
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        setUpReduceOnlyOpJob(job, reduceOp);
        job.setJobName(job.getJobName() + ".\n ReduceOp: " + reduceOp.getClass().getSimpleName());

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        context.runJob(job);

        return new KeyValueDataSet<TKEY, TVALUE>(context, output);
    }


    public static void setUpMapOpJob(Job job, MapOp mapOp) throws Exception {
        OpSerializer.saveMapOpToConf(job.getConfiguration(), mapOp);
        job.setMapperClass(SimpleOpMapper.class);
        job.setMapOutputKeyClass(getWritableClass(mapOp.toKeyClass()));
        job.setMapOutputValueClass(getWritableClass(mapOp.toValueClass()));
    }

    public static void setUpMapOnlyOpJob(Job job, MapOp mapOp) throws Exception {
        setUpMapOpJob(job, mapOp);
        job.setOutputKeyClass(getWritableClass(mapOp.toKeyClass()));
        job.setOutputValueClass(getWritableClass(mapOp.toValueClass()));
    }

    public static void setUpReduceOpJob(Job job, ReduceOp reduceOp) throws Exception {
        OpSerializer.saveReduceOpToConf(job.getConfiguration(), reduceOp);
        job.setReducerClass(SimpleOpReducer.class);
        job.setOutputKeyClass(getWritableClass(reduceOp.toKeyClass()));
        job.setOutputValueClass(getWritableClass(reduceOp.toValueClass()));
    }

    public static void setUpReduceOnlyOpJob(Job job, ReduceOp reduceOp) throws Exception {
        setUpReduceOpJob(job, reduceOp);
        job.setOutputKeyClass(getWritableClass(reduceOp.toKeyClass()));
        job.setOutputValueClass(getWritableClass(reduceOp.toValueClass()));
    }

    public Class<KEY> keyClass() {
        return null;
    }

    public Class<VALUE> valueClass() {
        return null;
    }

}
