package doh.ds;

import doh.api.Context;
import doh2.api.HDFSLocation;
import doh.api.ds.KVDS;
import doh.api.ds.KVDSFactory;
import doh.api.ds.Location;
import doh.api.op.FlatMapOp;
import doh.api.op.KV;
import doh.api.op.MapOp;
import doh.api.op.ReduceOp;
import doh.api.op.ValueOnlyReduceOp;
import doh.op.Op;
import doh.op.kvop.KVOp;
import doh.op.mr.FlatMapOpMapper;
import doh.op.mr.MapOpMapper;
import doh.op.mr.ReduceOpReducer;
import doh.op.utils.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

import static doh.op.WritableObjectDictionaryFactory.getObjectClass;
import static doh.op.WritableObjectDictionaryFactory.getWritableClass;

public class RealKVDS<Key, Value> implements KVDS<Key, Value> {
    protected final Location location;
    protected Context context;


    public RealKVDS(Path path) {
        location = new HDFSLocation.SingleHDFSLocation(path);
    }

    public RealKVDS(Path[] path) {
        location = new HDFSLocation.MultiHDFSLocation(path);
    }


    @Override
    public Context getContext() {
        return context;
    }

    public RealKVDS(Location location) {
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
    public RealKVDS<Key, Value> beReady() {
        return this;// always ready
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
            return new Path[]{((HDFSLocation.SingleHDFSLocation) hdfsLocation).getPath()};
        } else if (!hdfsLocation.isSingle()) {
            return ((HDFSLocation.MultiHDFSLocation) hdfsLocation).getPaths();
        } else {
            throw new IllegalStateException();
        }
    }

    public static Path[] getPaths(Location location) {
        return paths(hdfsLocation(location));
    }

    @Override
    public RealKVDS<Key, Value> comeTogetherRightNow(KVDS<Key, Value> other) {
        RealKVDS<Key, Value> a = this;
        RealKVDS<Key, Value> b = other instanceof RealKVDS ?
                (RealKVDS<Key, Value>) other :
                ((LazyKVDS<Key, Value>) other).real();
        return KVDSFactory.createReal(a, b);
    }

    @Override
    public MapKVDS<Key, Value> toMapKVDS() {
        MapKVDS<Key, Value> mapKVDS = new MapKVDS<Key, Value>(getLocation());
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
    public <TORIGIN> DS<TORIGIN> apply(Op<KV<Key, Value>, TORIGIN> op) throws Exception {
        if (op instanceof KVOp) {
            return applyMR((KVOp) op);
        }
        throw new IllegalArgumentException("Unsupported operation type " + op.getClass());
    }

    protected <KEY, VALUE, ToKey, ToValue> RealKVDS<ToKey, ToValue> applyMR(
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
    public <ToKey, ToValue> RealKVDS<ToKey, ToValue> map(
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

        return HDFSUtils.create(context, output);
    }


    @Override
    public <ToKey, ToValue> RealKVDS<ToKey, ToValue> flatMap(
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

        return HDFSUtils.create(context, output);
    }


    @Override
    public <ToKey, ToValue> RealKVDS<ToKey, ToValue> reduce(
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

        return HDFSUtils.create(context, output);
    }


    public void setUpMapOpJob(Job job, MapOp mapOp) throws Exception {
        context.opSerializer().saveMapperOp(job.getConfiguration(), mapOp);
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
        context.opSerializer().saveMapperOp(job.getConfiguration(), mapOp);
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
        context.opSerializer().saveReducerOp(job.getConfiguration(), reduceOp);
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
        } else if (!hdfsLocation.isSingle()) {
            Path path = ((HDFSLocation.MultiHDFSLocation) hdfsLocation).getPaths()[0];
            return keyClassOfDir(context.getConf(), path);
        }
        throw new UnsupportedOperationException();
    }

    public static Class<?> valueClassOfDir(Configuration conf, Path path) throws IOException {
        Path dataPath = HDFSUtils.listOutputFiles(conf, path)[0];
        SequenceFile.Reader r
                = new SequenceFile.Reader(dataPath.getFileSystem(conf), dataPath, conf);
        return r.getValueClass();
    }

    public static Class<?> keyClassOfDir(Configuration conf, Path path) throws IOException {
        Path dataPath = HDFSUtils.listOutputFiles(conf, path)[0];
        SequenceFile.Reader r
                = new SequenceFile.Reader(dataPath.getFileSystem(conf), dataPath, conf);
        return r.getKeyClass();
    }

    public static class OutputFilesFilter implements PathFilter {
        private static final Logger LOGGER = LoggerFactory.getLogger(OutputFilesFilter.class);

        private final Configuration conf;

        public OutputFilesFilter(Configuration conf) {
            this.conf = new Configuration(conf);
        }

        @Override
        public boolean accept(Path path) {
            boolean isFile = false;
            try {
                FileSystem fs = path.getFileSystem(conf);
                isFile = fs.exists(path) && fs.isFile(path);
            } catch (IOException e) {
                LOGGER.error("Error while filtering file " + path + ". Pass", e);
            }
            isFile &= !path.getName().startsWith("_");
            return isFile;
        }
    }

    @Override
    public Class<?> writableValueClass() throws IOException {
        HDFSLocation hdfsLocation = hdfsLocation(getLocation());
        if (hdfsLocation.isSingle()) {
            Path path = ((HDFSLocation.SingleHDFSLocation) hdfsLocation).getPath();
            return valueClassOfDir(context.getConf(), path);
        } else if (!hdfsLocation.isSingle()) {
            Path path = ((HDFSLocation.MultiHDFSLocation) hdfsLocation).getPaths()[0];
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


}
