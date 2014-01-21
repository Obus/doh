package doh.op.mr;

import doh.api.ds.HDFSLocation;
import doh.api.ds.KVDS;
import doh.api.op.FlatMapOp;
import doh.api.op.MapOp;
import doh.api.op.ReduceOp;
import doh.ds.RealKVDS;
import doh.op.kvop.CompositeMapOp;
import doh.op.kvop.CompositeReduceOp;
import doh.op.kvop.KVUnoOp;
import doh.op.kvop.OpKVTransformer;
import doh.op.serde.OpSerializer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static doh.op.WritableObjectDictionaryFactory.getWritableClass;
import static doh.op.utils.ReflectionUtils.isUnknown;

public class KVOpJobUtils {


    private static HDFSLocation hdfsLocation(RealKVDS origin) {
        if (origin.getLocation().isHDFS()) {
            return (HDFSLocation) origin.getLocation();
        }
        throw new UnsupportedOperationException();
    }

    private static Path[] paths(RealKVDS origin) {
        if (hdfsLocation(origin).isSingle()) {
            return new Path[]{((HDFSLocation.SingleHDFSLocation) hdfsLocation(origin)).getPath()};
        } else if (!hdfsLocation(origin).isSingle()) {
            return ((HDFSLocation.MultyHDFSLocation) hdfsLocation(origin)).getPaths();
        } else {
            throw new IllegalStateException();
        }
    }

    public static Job createCompositeMapReduceJob(RealKVDS origin, CompositeMapOp compositeMapOp, ReduceOp reduceOp) throws Exception {
        Job job = new Job(origin.getContext().getConf(), "Composite map simple reduce job");
        configureJob(job, origin, compositeMapOp, reduceOp);
        FileInputFormat.setInputPaths(job, paths(origin));
        FileOutputFormat.setOutputPath(job, origin.getContext().nextTempPath());
        return job;
    }

    public static Job createCompositeMapCompositeReduceJob(RealKVDS origin, CompositeMapOp compositeMapOp, CompositeReduceOp compositeReduceOp) throws Exception {
        Job job = new Job(origin.getContext().getConf(), "Composite map simple reduce job");
        configureJob(job, origin, compositeMapOp, compositeReduceOp);
        FileInputFormat.setInputPaths(job, paths(origin));
        FileOutputFormat.setOutputPath(job, origin.getContext().nextTempPath());
        return job;
    }

    public static Job createReduceOnlyJob(RealKVDS origin, ReduceOp reduceOp) throws Exception {
        Job job = new Job(origin.getContext().getConf(), "Simple reduce only job");
        configureJob(job, origin, reduceOp);
        FileInputFormat.setInputPaths(job, paths(origin));
        FileOutputFormat.setOutputPath(job, origin.getContext().nextTempPath());
        return job;
    }

    public static Job createCompositeReduceOnlyJob(RealKVDS origin, CompositeReduceOp compositeReduceOp) throws Exception {
        Job job = new Job(origin.getContext().getConf(), "Composite reduce only job");
        configureJob(job, origin, compositeReduceOp);
        FileInputFormat.setInputPaths(job, paths(origin));
        FileOutputFormat.setOutputPath(job, origin.getContext().nextTempPath());
        return job;
    }

    public static Job createCompositeMapOnlyJob(RealKVDS origin, CompositeMapOp compositeMapOp) throws Exception {
        Job job = new Job(origin.getContext().getConf(), "Composite map only job");
        configureJob(job, origin, compositeMapOp);
        FileInputFormat.setInputPaths(job, paths(origin));
        FileOutputFormat.setOutputPath(job, origin.getContext().nextTempPath());
        return job;
    }

    public static Job createMapOnlyJob(RealKVDS origin, MapOp compositeMapOp) throws Exception {
        Job job = new Job(origin.getContext().getConf(), "Map only job");
        configureJob(job, origin, compositeMapOp);
        FileInputFormat.setInputPaths(job, paths(origin));
        FileOutputFormat.setOutputPath(job, origin.getContext().nextTempPath());
        return job;
    }


    public static void configureJob(Job job, KVDS origin, FlatMapOp flatMapOp) throws Exception {
        origin.getContext().opSerializer().saveFlatMapOpToConf(job.getConfiguration(), flatMapOp);
        job.setMapperClass(MapOpMapper.class);
        setKeyValueClassesBasedOnMap(job, origin, flatMapOp);
    }

    public static void configureJob(Job job, KVDS origin, MapOp mapOp) throws Exception {
        origin.getContext().opSerializer().saveMapOpToConf(job.getConfiguration(), mapOp);
        job.setMapperClass(MapOpMapper.class);
        setKeyValueClassesBasedOnMap(job, origin, mapOp);
    }

    public static void configureJob(Job job, KVDS origin, ReduceOp reduceOp) throws Exception {
        origin.getContext().opSerializer().saveReduceOpToConf(job.getConfiguration(), reduceOp);
        job.setReducerClass(ReduceOpReducer.class);
        setKeyValueClassesBasedOnReduce(job, origin, reduceOp);
    }

    public static void configureJob(Job job, KVDS origin, MapOp mapOp, ReduceOp reduceOp) throws Exception {
        origin.getContext().opSerializer().saveMapOpToConf(job.getConfiguration(), mapOp);
        job.setMapperClass(FlatMapOpMapper.class);
        origin.getContext().opSerializer().saveReduceOpToConf(job.getConfiguration(), reduceOp);
        job.setReducerClass(ReduceOpReducer.class);
        setKeyValueClassesBasedOnMapReduce(job, origin, mapOp, reduceOp);

    }

    public static void configureJob(Job job, KVDS origin, FlatMapOp flatMapOp, ReduceOp reduceOp) throws Exception {
        origin.getContext().opSerializer().saveFlatMapOpToConf(job.getConfiguration(), flatMapOp);
        job.setMapperClass(FlatMapOpMapper.class);
        origin.getContext().opSerializer().saveReduceOpToConf(job.getConfiguration(), reduceOp);
        job.setReducerClass(ReduceOpReducer.class);
        setKeyValueClassesBasedOnMapReduce(job, origin, flatMapOp, reduceOp);
    }


    public static void configureJob(Job job, KVDS origin, CompositeMapOp compositeMapOp, ReduceOp reduceOp) throws Exception {
        origin.getContext().opSerializer().saveCompositeMapOp(job.getConfiguration(), compositeMapOp);
        job.setMapperClass(CompositeGeneralMapOpMapper.class);
        origin.getContext().opSerializer().saveReduceOpToConf(job.getConfiguration(), reduceOp);
        job.setReducerClass(ReduceOpReducer.class);
        setKeyValueClassesBasedOnMapReduce(job, origin, compositeMapOp, reduceOp);
    }

    public static void configureJob(Job job, KVDS origin, CompositeMapOp compositeMapOp) throws Exception {
        origin.getContext().opSerializer().saveCompositeMapOp(job.getConfiguration(), compositeMapOp);
        job.setMapperClass(CompositeGeneralMapOpMapper.class);
        setKeyValueClassesBasedOnMap(job, origin, compositeMapOp);
    }

    public static void configureJob(Job job, KVDS origin, CompositeReduceOp reduceOp) throws Exception {
        origin.getContext().opSerializer().saveCompositeReduceOp(job.getConfiguration(), reduceOp);
        job.setReducerClass(CompositeReduceOpReducer.class);
        setKeyValueClassesBasedOnReduce(job, origin, reduceOp);
    }

    public static void configureJob(Job job, KVDS origin, CompositeMapOp compositeMapOp, CompositeReduceOp reduceOp) throws Exception {
        origin.getContext().opSerializer().saveCompositeMapOp(job.getConfiguration(), compositeMapOp);
        job.setMapperClass(CompositeGeneralMapOpMapper.class);
        origin.getContext().opSerializer().saveCompositeReduceOp(job.getConfiguration(), reduceOp);
        job.setReducerClass(CompositeReduceOpReducer.class);
        setKeyValueClassesBasedOnMapReduce(job, origin, compositeMapOp, reduceOp);
    }


    public static void setKeyValueClassesBasedOnMapReduce(Job job, KVDS origin, KVUnoOp mapOp, KVUnoOp reduceOp) throws Exception {
        Class<?> mapperInputKeyClass = origin.keyClass();
        Class<?> mapperInputValueClass = origin.valueClass();
        Class<?> mapperOutputKeyClass = ((OpKVTransformer) mapOp).toKeyClass();
        if (isUnknown(mapperOutputKeyClass)) {
            mapperOutputKeyClass = mapperInputKeyClass;
        }
        Class<?> mapperOutputValueClass = ((OpKVTransformer) mapOp).toValueClass();
        if (isUnknown(mapperOutputValueClass)) {
            mapperOutputValueClass = mapperInputValueClass;
        }
        Class<?> reduceOutputKeyClass = ((OpKVTransformer) reduceOp).toKeyClass();
        if (isUnknown(reduceOutputKeyClass)) {
            reduceOutputKeyClass = mapperOutputKeyClass;
        }
        Class<?> reduceOutputValueClass = ((OpKVTransformer) reduceOp).toValueClass();
        if (isUnknown(reduceOutputValueClass)) {
            reduceOutputValueClass = mapperOutputValueClass;
        }

        setUpJobKV(job,
                mapperInputKeyClass,
                mapperInputValueClass,
                mapperOutputKeyClass,
                mapperOutputValueClass,
                reduceOutputKeyClass,
                reduceOutputValueClass
        );
    }


    public static void setUpJobKV(Job job,
                                  Class<?> mapperInputKeyClass,
                                  Class<?> mapperInputValueClass,
                                  Class<?> mapperOutputKeyClass,
                                  Class<?> mapperOutputValueClass,
                                  Class<?> reduceOutputKeyClass,
                                  Class<?> reduceOutputValueClass) {
        job.setMapOutputKeyClass(getWritableClass(mapperOutputKeyClass));
        job.setMapOutputValueClass(getWritableClass(mapperOutputValueClass));

        job.setOutputKeyClass(getWritableClass(reduceOutputKeyClass));
        job.setOutputValueClass(getWritableClass(reduceOutputValueClass));

        OpSerializer.saveKVClassesToConf(job.getConfiguration(),
                mapperInputKeyClass,
                mapperInputValueClass,
                mapperOutputKeyClass,
                mapperOutputValueClass,
                reduceOutputKeyClass,
                reduceOutputValueClass
        );
    }

    public static void setKeyValueClassesBasedOnMap(Job job, KVDS origin, KVUnoOp mapOp) throws Exception {
        Class<?> mapperInputKeyClass = origin.keyClass();
        Class<?> mapperInputValueClass = origin.valueClass();
        Class<?> mapperOutputKeyClass = ((OpKVTransformer) mapOp).toKeyClass();
        if (isUnknown(mapperOutputKeyClass)) {
            mapperOutputKeyClass = mapperInputKeyClass;
        }
        Class<?> mapperOutputValueClass = ((OpKVTransformer) mapOp).toValueClass();
        if (isUnknown(mapperOutputValueClass)) {
            mapperOutputValueClass = mapperInputValueClass;
        }
        Class<?> reduceOutputKeyClass = mapperOutputKeyClass;
        Class<?> reduceOutputValueClass = mapperOutputValueClass;

        setUpJobKV(job,
                mapperInputKeyClass,
                mapperInputValueClass,
                mapperOutputKeyClass,
                mapperOutputValueClass,
                reduceOutputKeyClass,
                reduceOutputValueClass
        );
    }

    public static void setKeyValueClassesBasedOnReduce(Job job, KVDS origin, KVUnoOp reduceOp) throws Exception {
        Class<?> mapperInputKeyClass = origin.keyClass();
        Class<?> mapperInputValueClass = origin.valueClass();
        Class<?> mapperOutputKeyClass = mapperInputKeyClass;
        Class<?> mapperOutputValueClass = mapperInputValueClass;
        Class<?> reduceOutputKeyClass = ((OpKVTransformer) reduceOp).toKeyClass();
        if (isUnknown(reduceOutputKeyClass)) {
            reduceOutputKeyClass = mapperOutputKeyClass;
        }
        Class<?> reduceOutputValueClass = ((OpKVTransformer) reduceOp).toValueClass();
        if (isUnknown(reduceOutputValueClass)) {
            reduceOutputValueClass = mapperOutputValueClass;
        }

        setUpJobKV(job,
                mapperInputKeyClass,
                mapperInputValueClass,
                mapperOutputKeyClass,
                mapperOutputValueClass,
                reduceOutputKeyClass,
                reduceOutputValueClass
        );
    }


}
