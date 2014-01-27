package doh2.impl.ondemand;

import doh2.api.op.FilterOp;
import doh2.api.op.FlatMapOp;
import doh2.api.op.MapOp;
import doh2.api.op.ReduceOp;
import doh2.impl.op.kvop.CompositeMapOp;
import doh2.impl.op.kvop.CompositeReduceOp;
import doh2.impl.op.kvop.KVUnoOp;
import doh2.impl.op.kvop.OpKVTransformer;
import doh2.impl.op.mr.CompositeMapOpMapper;
import doh2.impl.op.mr.CompositeReduceOpReducer;
import doh2.impl.op.mr.FlatMapOpMapper;
import doh2.impl.op.mr.GeneralMapOpMapper;
import doh2.impl.op.mr.MapOpMapper;
import doh2.impl.op.mr.ReduceOpReducer;
import doh2.impl.serde.OpSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import static doh2.impl.op.WritableObjectDictionaryFactory.getWritableClass;
import static doh2.impl.op.utils.ReflectionUtils.isUnknown;

public class OpJobMaker {

    public Job makeJob(Configuration conf, Class fromKeyClass, Class fromValueClass, Path[] inputs,
                               KVUnoOp mapTaskOp, KVUnoOp reduceTaskOp) throws Exception {
        return new JobBuilding(conf, fromKeyClass, fromValueClass, inputs).mapTaskOp(mapTaskOp).reduceTaskOp(reduceTaskOp).make();
    }

    public static class JobBuilding {
        private final Configuration conf;
        private final Class fromKeyClass;
        private final Class fromValueClass;
        private final Path[] inputs;

        private KVUnoOp mapTaskOp;
        private KVUnoOp reduceTaskOp;

        public JobBuilding(Configuration conf, Class fromKeyClass, Class fromValueClass, Path[] inputs) {
            this.conf = conf;
            this.fromKeyClass = fromKeyClass;
            this.fromValueClass = fromValueClass;
            this.inputs = inputs;
        }
        public JobBuilding mapTaskOp(KVUnoOp mapTaskOp) {
            this.mapTaskOp = mapTaskOp;
            return this;
        }
        public JobBuilding reduceTaskOp(KVUnoOp reduceTaskOp) {
            this.reduceTaskOp = reduceTaskOp;
            return this;
        }
        public Job make() throws Exception {
            if (mapTaskOp == null && reduceTaskOp == null) {
                throw new IllegalArgumentException("Both map and reduce op could not be null");
            }
            Job job = new Job(conf);

            if (mapTaskOp == null && reduceTaskOp != null) {
                job.setJobName("Reduce job\n" +
                        "Reduce operation: " + reduceTaskOp.getClass());
                job.setReducerClass(reducerClass(reduceTaskOp));
                setKeyValueClassesBasedOnReduce(job, fromKeyClass, fromValueClass, reduceTaskOp);

            }
            else if (mapTaskOp != null && reduceTaskOp == null) {
                job.setJobName("Mapper job\n" +
                        "Map operation: " + mapTaskOp.getClass());
                job.setMapperClass(mapperClass(mapTaskOp));
                setKeyValueClassesBasedOnMap(job, fromKeyClass, fromValueClass, mapTaskOp);
            }
            else {
                job.setJobName("MapReduce job\n" +
                        "Map operation: " + mapTaskOp.getClass() + "\n" +
                        "Reduce operation: " + reduceTaskOp.getClass());

                job.setMapperClass(mapperClass(mapTaskOp));
                job.setReducerClass(reducerClass(reduceTaskOp));
                setKeyValueClassesBasedOnMapReduce(job, fromKeyClass, fromValueClass, mapTaskOp, reduceTaskOp);
            }

            FileInputFormat.setInputPaths(job, inputs);

            return job;
        }
    }


    public static Class<? extends Mapper> mapperClass(KVUnoOp mapOp) {
        if (mapOp instanceof MapOp) {
            return MapOpMapper.class;
        }
        if (mapOp instanceof FlatMapOp) {
            return FlatMapOpMapper.class;
        }
        if (mapOp instanceof CompositeMapOp) {
            return CompositeMapOpMapper.class;
        }
        if (mapOp instanceof FilterOp) {
            return GeneralMapOpMapper.class;
        }
        throw new IllegalArgumentException("Unknown type of map operation: " + mapOp.getClass());
    }

    public static Class<? extends Reducer> reducerClass(KVUnoOp reduceOp) {
        if (reduceOp instanceof ReduceOp) {
            return ReduceOpReducer.class;
        }
        if (reduceOp instanceof CompositeReduceOp) {
            return CompositeReduceOpReducer.class;
        }
        throw new IllegalArgumentException("Unknown type of reduce operation: " + reduceOp.getClass());
    }

    public static void setKeyValueClassesBasedOnMapReduce(Job job, Class fromKeyClass, Class fromValueClass, KVUnoOp mapOp, KVUnoOp reduceOp) throws Exception {
        Class mapperOutputKeyClass = ((OpKVTransformer) mapOp).toKeyClass();
        if (isUnknown(mapperOutputKeyClass)) {
            mapperOutputKeyClass = fromKeyClass;
        }
        Class mapperOutputValueClass = ((OpKVTransformer) mapOp).toValueClass();
        if (isUnknown(mapperOutputValueClass)) {
            mapperOutputValueClass = fromValueClass;
        }
        Class reduceOutputKeyClass = ((OpKVTransformer) reduceOp).toKeyClass();
        if (isUnknown(reduceOutputKeyClass)) {
            reduceOutputKeyClass = mapperOutputKeyClass;
        }
        Class reduceOutputValueClass = ((OpKVTransformer) reduceOp).toValueClass();
        if (isUnknown(reduceOutputValueClass)) {
            reduceOutputValueClass = mapperOutputValueClass;
        }

        setUpJobKV(job,
                fromKeyClass,
                fromValueClass,
                mapperOutputKeyClass,
                mapperOutputValueClass,
                reduceOutputKeyClass,
                reduceOutputValueClass
        );
    }

    public static void setKeyValueClassesBasedOnMap(Job job, Class fromKeyClass, Class fromValueClass, KVUnoOp mapOp) throws Exception {
        Class<?> mapperOutputKeyClass = ((OpKVTransformer) mapOp).toKeyClass();
        if (isUnknown(mapperOutputKeyClass)) {
            mapperOutputKeyClass = fromKeyClass;
        }
        Class<?> mapperOutputValueClass = ((OpKVTransformer) mapOp).toValueClass();
        if (isUnknown(mapperOutputValueClass)) {
            mapperOutputValueClass = fromValueClass;
        }
        Class<?> reduceOutputKeyClass = mapperOutputKeyClass;
        Class<?> reduceOutputValueClass = mapperOutputValueClass;

        setUpJobKV(job,
                fromKeyClass,
                fromValueClass,
                mapperOutputKeyClass,
                mapperOutputValueClass,
                reduceOutputKeyClass,
                reduceOutputValueClass
        );
    }

    public static void setKeyValueClassesBasedOnReduce(Job job, Class fromKeyClass, Class fromValueClass, KVUnoOp reduceOp) throws Exception {
        Class<?> reduceOutputKeyClass = ((OpKVTransformer) reduceOp).toKeyClass();
        if (isUnknown(reduceOutputKeyClass)) {
            reduceOutputKeyClass = fromKeyClass;
        }
        Class<?> reduceOutputValueClass = ((OpKVTransformer) reduceOp).toValueClass();
        if (isUnknown(reduceOutputValueClass)) {
            reduceOutputValueClass = fromValueClass;
        }

        setUpJobKV(job,
                fromKeyClass,
                fromValueClass,
                fromKeyClass,
                fromValueClass,
                reduceOutputKeyClass,
                reduceOutputValueClass
        );
    }

    public static void setUpJobKV(Job job,
                                  Class mapperInputKeyClass,
                                  Class mapperInputValueClass,
                                  Class mapperOutputKeyClass,
                                  Class mapperOutputValueClass,
                                  Class reduceOutputKeyClass,
                                  Class reduceOutputValueClass) {
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
}
