package doh.ds;

import com.synqera.bigkore.rank.PlatformUtils;
import doh.api.ds.HDFSLocation;
import doh.api.ds.KVDataSet;
import doh.api.ds.KVDataSetFactory;
import doh.api.ds.Location;
import doh.api.op.FlatMapOp;
import doh.api.op.KV;
import doh.api.op.MapOp;
import doh.api.op.ReduceOp;
import doh.op.*;
import doh.op.kvop.*;
import doh.op.mr.FlatMapOpMapper;
import doh.op.mr.MapOpMapper;
import doh.op.mr.ReduceOpReducer;
import doh.op.serde.OpSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

import static doh.op.WritableObjectDictionaryFactory.getObjectClass;
import static doh.op.WritableObjectDictionaryFactory.getWritableClass;

public class RealKVDataSet<Key, Value> implements KVDataSet<Key, Value> {
    protected final Location location;
    protected Context context;


    public RealKVDataSet(Path path) {
        location = new HDFSLocation.SingleHDFSLocation(path);
    }
    public RealKVDataSet(Path[] path) {
        location = new HDFSLocation.MultyHDFSLocation(path);
    }


    @Override
    public Context getContext() {
        return context;
    }

    public RealKVDataSet(Location location) {
        if (!location.isHDFS()) {
            throw new UnsupportedOperationException();
        }
        this.location = location;
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public void beReady() {
        // always ready
    }

    @Override
    public void setContext(Context context) {
        this.context = context;
    }

    @Override
    public Location getLocation() {
        return location;
    }

    @Override
    public Iterator<KV<Key, Value>> iteratorChecked() throws IOException {
        HDFSLocation hdfsLocation = hdfsLocation(getLocation());
        if (!hdfsLocation.isSingle()) {
            throw new UnsupportedOperationException();
        }
        Path path = ((HDFSLocation.SingleHDFSLocation) hdfsLocation).getPath();
        try {
            return new KeyValueIterator(
                    this.context.getConf(),
                    path, this.keyClass(), this.valueClass());
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    private static HDFSLocation hdfsLocation(Location location) {
        if (location.isHDFS()) {
            return (HDFSLocation) location;
        }
        throw new UnsupportedOperationException();
    }

    private Path[] paths() {
        return getPaths(getLocation());
    }

    private static Path[] paths(HDFSLocation hdfsLocation) {
        if (hdfsLocation.isSingle()) {
            return new Path[] {((HDFSLocation.SingleHDFSLocation)hdfsLocation).getPath()};
        }
        else if (!hdfsLocation.isSingle()) {
            return ((HDFSLocation.MultyHDFSLocation)hdfsLocation).getPaths();
        }
        else {
            throw new IllegalStateException();
        }
    }

    public static Path[] getPaths(Location location) {
        return paths(hdfsLocation(location));
    }

    @Override
    public RealKVDataSet<Key, Value> comeTogetherRightNow(KVDataSet<Key, Value> other) {
        RealKVDataSet<Key, Value> a = this;
        RealKVDataSet<Key, Value> b = other instanceof RealKVDataSet ?
                (RealKVDataSet<Key, Value>) other :
                ((LazyKVDataSet<Key, Value>) other).real();
        return KVDataSetFactory.createReal(a, b);
    }

    @Override
    public MapKVDataSet<Key, Value> toMapKVDS() {
        MapKVDataSet<Key, Value> mapKVDS = new MapKVDataSet<Key, Value>(getLocation());
        mapKVDS.setContext(context);
        return mapKVDS;
    }

    @Override
    public Iterator<KV<Key, Value>> iterator() {
        try {
            return iteratorChecked();
        } catch (IOException e) {
            throw new RuntimeException("Failed to iterate over keyValueDatSet", e);
        }
    }

    @Override
    public <TORIGIN> DataSet<TORIGIN> apply(Op<KV<Key, Value>, TORIGIN> op) throws Exception {
        if (op instanceof KVOp) {
            return applyMR((KVOp) op);
        }
        throw new IllegalArgumentException("Unsupported operation type " + op.getClass());
    }

    protected <KEY, VALUE, ToKey, ToValue> RealKVDataSet<ToKey, ToValue> applyMR(
            KVOp<KEY, VALUE, ToKey, ToValue> KVOp) throws Exception {
        if (KVOp instanceof MapOp) {
            return map((MapOp) KVOp);
        } else if (KVOp instanceof FlatMapOp) {
            return flatMap((FlatMapOp) KVOp);
        } else if (KVOp instanceof ReduceOp) {
            return reduce((ReduceOp) KVOp);
        }
        throw new IllegalArgumentException("Unsupported map-reduce operation type " + KVOp.getClass());
    }


    @Override
    public <ToKey, ToValue> RealKVDataSet<ToKey, ToValue> map(
            MapOp<Key, Value, ToKey, ToValue> mapOp
    ) throws Exception {

        Configuration conf = this.context.getConf();
        Path[] inputs = paths();

        Path output = context.nextTempPath();

        Job job = new Job(conf, "Map only job");
        FileInputFormat.setInputPaths(job, inputs);
        FileOutputFormat.setOutputPath(job, output);

        setUpMapOnlyOpJob(job, mapOp);
        job.setJobName(job.getJobName() + ".\n MapOp: " + mapOp.getClass().getSimpleName());

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        context.runJob(job);

        return create(context, output);
    }


    @Override
    public <ToKey, ToValue> RealKVDataSet<ToKey, ToValue> flatMap(
            FlatMapOp<Key, Value, ToKey, ToValue> flatMapOp
    ) throws Exception {

        Configuration conf = this.context.getConf();
        Path[] inputs = paths();
        Path output = context.nextTempPath();

        Job job = new Job(conf, "FlatMap only job");
        FileInputFormat.setInputPaths(job, inputs);
        FileOutputFormat.setOutputPath(job, output);

        setUpFlatMapOnlyOpJob(job, flatMapOp);
        job.setJobName(job.getJobName() + ".\n FlatMapOp: " + flatMapOp.getClass().getSimpleName());

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        context.runJob(job);

        return create(context, output);
    }


    @Override
    public <ToKey, ToValue> RealKVDataSet<ToKey, ToValue> reduce(
            ReduceOp<Key, Value, ToKey, ToValue> reduceOp
    ) throws Exception {

        Configuration conf = this.context.getConf();
        Path[] inputs = paths();
        Path output = context.nextTempPath();

        Job job = new Job(conf, "Reduce only job");
        FileInputFormat.setInputPaths(job, inputs);
        FileOutputFormat.setOutputPath(job, output);

        setUpReduceOnlyOpJob(job, reduceOp);
        job.setJobName(job.getJobName() + ".\n ReduceOp: " + reduceOp.getClass().getSimpleName());

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        context.runJob(job);

        return create(context, output);
    }


    public void setUpMapOpJob(Job job, MapOp mapOp) throws Exception {
        context.opSerializer().saveMapOpToConf(job.getConfiguration(), mapOp);
        job.setMapperClass(MapOpMapper.class);
        job.setMapOutputKeyClass(getWritableClass(mapOp.toKeyClass()));
        job.setMapOutputValueClass(getWritableClass(mapOp.toValueClass()));
    }


    public void setUpMapOnlyOpJob(Job job, MapOp mapOp) throws Exception {
        setUpMapOpJob(job, mapOp);
        job.setOutputKeyClass(getWritableClass(mapOp.toKeyClass()));
        job.setOutputValueClass(getWritableClass(mapOp.toValueClass()));
    }

    public void setUpFlatMapOpJob(Job job, FlatMapOp mapOp) throws Exception {
        context.opSerializer().saveFlatMapOpToConf(job.getConfiguration(), mapOp);
        job.setMapperClass(FlatMapOpMapper.class);
        job.setMapOutputKeyClass(getWritableClass(mapOp.toKeyClass()));
        job.setMapOutputValueClass(getWritableClass(mapOp.toValueClass()));
    }

    public void setUpFlatMapOnlyOpJob(Job job, FlatMapOp mapOp) throws Exception {
        setUpFlatMapOpJob(job, mapOp);
        job.setOutputKeyClass(getWritableClass(mapOp.toKeyClass()));
        job.setOutputValueClass(getWritableClass(mapOp.toValueClass()));
    }

    public void setUpReduceOpJob(Job job, ReduceOp reduceOp) throws Exception {
        context.opSerializer().saveReduceOpToConf(job.getConfiguration(), reduceOp);
        job.setReducerClass(ReduceOpReducer.class);
        if (reduceOp instanceof ValueOnlyReduceOp) {
            job.setOutputKeyClass(this.writableKeyClass());
        } else {
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


    @Override
    public Class<?> writableKeyClass() throws IOException {
        HDFSLocation hdfsLocation = hdfsLocation(getLocation());
        if (hdfsLocation.isSingle()) {
            Path path = ((HDFSLocation.SingleHDFSLocation) hdfsLocation).getPath();
            return keyClassOfDir(context.getConf(), path);
        }
        else if (!hdfsLocation.isSingle()) {
            Path path = ((HDFSLocation.MultyHDFSLocation) hdfsLocation).getPaths()[0];
            return keyClassOfDir(context.getConf(), path);
        }
        throw new UnsupportedOperationException();
    }

    public static Class<?> valueClassOfDir(Configuration conf, Path path) throws IOException {
        Path dataPath = PlatformUtils.listOutputFiles(conf, path)[0];
        SequenceFile.Reader r
                = new SequenceFile.Reader(dataPath.getFileSystem(conf), dataPath, conf);
        return r.getValueClass();
    }

    public static Class<?> keyClassOfDir(Configuration conf, Path path) throws IOException {
        Path dataPath = PlatformUtils.listOutputFiles(conf, path)[0];
        SequenceFile.Reader r
                = new SequenceFile.Reader(dataPath.getFileSystem(conf), dataPath, conf);
        return r.getKeyClass();
    }

    @Override
    public Class<?> writableValueClass() throws IOException {
        HDFSLocation hdfsLocation = hdfsLocation(getLocation());
        if (hdfsLocation.isSingle()) {
            Path path = ((HDFSLocation.SingleHDFSLocation) hdfsLocation).getPath();
            return valueClassOfDir(context.getConf(), path);
        }
        else if (!hdfsLocation.isSingle()) {
            Path path = ((HDFSLocation.MultyHDFSLocation) hdfsLocation).getPaths()[0];
            return valueClassOfDir(context.getConf(), path);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<Key> keyClass() throws IOException {
        return getObjectClass((Class<? extends Writable>) this.writableKeyClass());
    }

    @Override
    public Class<Value> valueClass() throws IOException {
        return getObjectClass((Class<? extends Writable>) this.writableValueClass());
    }


    public static <KEY, VALUE> RealKVDataSet<KEY, VALUE> create(Context context, Path path) {
        RealKVDataSet<KEY, VALUE> kvds = new RealKVDataSet<KEY, VALUE>(new HDFSLocation.SingleHDFSLocation(path));
        kvds.setContext(context);
        return kvds;
    }

}
