package doh.ds;

import com.synqera.bigkore.rank.PlatformUtils;
import doh.crazy.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static doh.crazy.WritableObjectDictionaryFactory.getObjectClass;
import static doh.crazy.WritableObjectDictionaryFactory.getWritable;
import static doh.crazy.WritableObjectDictionaryFactory.getWritableClass;

public class KeyValueDataSet<KEY, VALUE> extends DataSet<KV<KEY, VALUE>> implements Iterable<KV<KEY, VALUE>> {
    public KeyValueDataSet(Path path) {
        super(path);
    }

    public Iterator<KV<KEY, VALUE>> iteratorChecked() throws IOException {
        return new KeyValueIterator();
    }

    public MapKeyValueDataSet<KEY, VALUE> toMapKVDS() {
        MapKeyValueDataSet<KEY, VALUE> mapKVDS = new MapKeyValueDataSet<KEY, VALUE>(getPath());
        mapKVDS.setContext(context);
        return mapKVDS;
    }

    @Override
    public Iterator<KV<KEY, VALUE>> iterator() {
        try {
            return iteratorChecked();
        } catch (IOException e) {
            throw new RuntimeException("Failed to iterate over keyValueDatSet", e);
        }
    }

    @Override
    public <TORIGIN> DataSet<TORIGIN> apply(Op<KV<KEY, VALUE>, TORIGIN> op) throws Exception {
        if (op instanceof MapOp) {
            return map((MapOp) op);
        }
        else if (op instanceof FlatMapOp) {
            return flatMap((FlatMapOp) op);
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

        return create(context, output);
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

        return create(context, output);
    }


    public <KEY, VALUE, TKEY, TVALUE> KeyValueDataSet<TKEY, TVALUE> flatMap(
            FlatMapOp<KEY, VALUE, TKEY, TVALUE> flatMapOp
    ) throws Exception {

        Configuration conf = this.getConf();
        Path input = this.getPath();
        Path output = context.nextTempPath();

        Job job = new Job(conf, "FlatMap only job");
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        setUpFlatMapOnlyOpJob(job, flatMapOp);
        job.setJobName(job.getJobName() + ".\n FlatMapOp: " + flatMapOp.getClass().getSimpleName());

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        context.runJob(job);

        return create(context, output);
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

        return create(context, output);
    }


    public void setUpMapOpJob(Job job, MapOp mapOp) throws Exception {
        OpSerializer.saveMapOpToConf(job.getConfiguration(), mapOp);
        job.setMapperClass(SimpleMapOpMapper.class);
        job.setMapOutputKeyClass(getWritableClass(mapOp.toKeyClass()));
        job.setMapOutputValueClass(getWritableClass(mapOp.toValueClass()));
    }


    public void setUpMapOnlyOpJob(Job job, MapOp mapOp) throws Exception {
        setUpMapOpJob(job, mapOp);
        job.setOutputKeyClass(getWritableClass(mapOp.toKeyClass()));
        job.setOutputValueClass(getWritableClass(mapOp.toValueClass()));
    }

    public void setUpFlatMapOpJob(Job job, FlatMapOp mapOp) throws Exception {
        OpSerializer.saveFlatMapOpToConf(job.getConfiguration(), mapOp);
        job.setMapperClass(SimpleFlatMapOpMapper.class);
        job.setMapOutputKeyClass(getWritableClass(mapOp.toKeyClass()));
        job.setMapOutputValueClass(getWritableClass(mapOp.toValueClass()));
    }

    public void setUpFlatMapOnlyOpJob(Job job, FlatMapOp mapOp) throws Exception {
        setUpFlatMapOpJob(job, mapOp);
        job.setOutputKeyClass(getWritableClass(mapOp.toKeyClass()));
        job.setOutputValueClass(getWritableClass(mapOp.toValueClass()));
    }

    public void setUpReduceOpJob(Job job, ReduceOp reduceOp) throws Exception {
        OpSerializer.saveReduceOpToConf(job.getConfiguration(), reduceOp);
        job.setReducerClass(SimpleReduceOpReducer.class);
        if (reduceOp instanceof ValueOnlyReduceOp) {
            job.setOutputKeyClass(this.writableKeyClass());
        }
        else {
            job.setOutputKeyClass(getWritableClass(reduceOp.toKeyClass()));
        }
        job.setOutputValueClass(getWritableClass(reduceOp.toValueClass()));
    }

    public void setUpReduceOnlyOpJob(Job job, ReduceOp reduceOp) throws Exception {
        setUpReduceOpJob(job, reduceOp);
//        job.setOutputKeyClass(getWritableClass(reduceOp.toKeyClass()));
//        job.setOutputValueClass(getWritableClass(reduceOp.toValueClass()));
        job.setMapOutputKeyClass(this.writableKeyClass());
        job.setMapOutputValueClass(this.writableValueClass());
    }



    public Class<?> writableKeyClass() throws IOException {
        Path dataPath = PlatformUtils.listOutputFiles(context.getConf(), getPath())[0];
        SequenceFile.Reader r
                = new SequenceFile.Reader(dataPath.getFileSystem(context.getConf()), dataPath, context.getConf());
        return r.getKeyClass();
    }

    public Class<?> writableValueClass() throws IOException{
        Path dataPath = PlatformUtils.listOutputFiles(context.getConf(), getPath())[0];
        SequenceFile.Reader r
                = new SequenceFile.Reader(dataPath.getFileSystem(context.getConf()), dataPath, context.getConf());
        return r.getValueClass();
    }

    public Class<KEY> keyClass() throws IOException {
        return getObjectClass((Class<? extends Writable>) this.writableKeyClass());
    }

    public Class<VALUE> valueClass() throws IOException{
        return getObjectClass((Class<? extends Writable>) this.writableValueClass());
    }

    public class KeyValueIterator implements Iterator<KV<KEY, VALUE>> {
        private final SequenceFileDirIterator sequenceFileDirIterator;
        private final WritableObjectDictionaryFactory.WritableObjectDictionary<KEY, Writable> keyDictionary;
        private final WritableObjectDictionaryFactory.WritableObjectDictionary<VALUE, Writable> valueDictionary;

        public KeyValueIterator() throws IOException {
            sequenceFileDirIterator = new SequenceFileDirIterator(
                    PlatformUtils.listOutputFiles(context.getConf(), getPath()),
                    false,
                    getConf());
            keyDictionary = WritableObjectDictionaryFactory.createDictionary(keyClass());
            valueDictionary = WritableObjectDictionaryFactory.createDictionary(valueClass());
        }

        @Override
        public boolean hasNext() {
            return sequenceFileDirIterator.hasNext();
        }

        @Override
        public KV<KEY, VALUE> next() {
            Pair<Writable, Writable> pairOfWritables = (Pair<Writable, Writable>) sequenceFileDirIterator.next();
            return kv.set(
                    keyDictionary.getObject(pairOfWritables.getFirst()),
                    valueDictionary.getObject(pairOfWritables.getSecond())
            );
        }

        private final KV<KEY, VALUE> kv = new KV<KEY, VALUE>();

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }


    public static <KEY, VALUE> KeyValueDataSet<KEY, VALUE> create(Context context, Path path) {
        KeyValueDataSet<KEY, VALUE> kvds = new KeyValueDataSet<KEY, VALUE>(path);
        kvds.setContext(context);
        return kvds;
    }

}
