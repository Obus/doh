package doh2.impl;

import doh.api.TempPathManager;
import doh.api.op.GroupByKeyOp;
import doh.api.op.ReduceOp;
import doh.op.JobRunner;
import doh.op.kvop.CompositeMapOp;
import doh.op.kvop.CompositeReduceOp;
import doh.op.kvop.KVUnoOp;
import doh2.api.HDFSLocation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class OpExecutor {

    private final TempPathManager tempPathManager;
    private final Configuration conf;
    private final Class<? extends OutputFormat> defaultOutputFormatClass;
    private final JobRunner jobRunner;
    private final OpJobMaker opJobMaker;

    public OpExecutor(TempPathManager tempPathManager, Configuration conf, Class<? extends OutputFormat> defaultOutputFormatClass, JobRunner jobRunner, OpJobMaker opJobMaker) {
        this.tempPathManager = tempPathManager;
        this.conf = conf;
        this.defaultOutputFormatClass = defaultOutputFormatClass;
        this.jobRunner = jobRunner;
        this.opJobMaker = opJobMaker;
    }

    public void execute(ExecutionUnit executionUnit) throws Exception {
        Queue<KVUnoOp> opQueue = new LinkedList<KVUnoOp>(executionUnit.kvOpList);

        final KVUnoOp mapTaskOp = compressMapOp(compositeOpSequence(opQueue));
        final KVUnoOp reduceTaskOp = compressReduceOp(reducerOp(opQueue), compositeOpSequence(opQueue));

        final Job job = opJobMaker.makeJob(conf,
                executionUnit.input.getKeyClass(),
                executionUnit.input.getValueClass(),
                executionUnit.input.getLocation().getPaths(),
                mapTaskOp, reduceTaskOp);

        job.setInputFormatClass(executionUnit.input.details().inputFormatClass());

        specifyDatSetDetails(job, executionUnit.output.details());

        jobRunner.runJob(job);

        executionUnit.output.setReady();
    }

    private DSDetails specifyDatSetDetails(Job job, DSDetails details) {
        if (details.formatClass == null) {
            details.formatClass = defaultOutputFormatClass;
        }
        if (singleLocationPath(details.location) == null) {
            details.location = new HDFSLocation.SingleHDFSLocation(tempPathManager.getNextPath());
        }

        job.setOutputFormatClass(details.formatClass);
        FileOutputFormat.setOutputPath(job, singleLocationPath(details.location));
        if (details.numReducers != null) {
            job.setNumReduceTasks(details.numReducers);
        }
        return details;
    }


    private Path singleLocationPath(HDFSLocation location) {
        if (location == null) {
            return null;
        }
        if (location.isSingle()) {
            return ((HDFSLocation.SingleHDFSLocation)location).getPath();
        }
        else {
            throw new IllegalArgumentException("Output path should have single output path");
        }
    }

    public static KVUnoOp compressMapOp(List<KVUnoOp> compositeMapperOpSequence) {
        if (compositeMapperOpSequence.size() == 0) {
            return null;
        }
        if (compositeMapperOpSequence.size() == 1) {
            return compositeMapperOpSequence.get(0);
        }
        return new CompositeMapOp(compositeMapperOpSequence);
    }

    public static KVUnoOp compressReduceOp(ReduceOp reduceOp, List<KVUnoOp> compositeReducerOpSequence) {
        if (reduceOp == null) {
            return null;
        }
        if (compositeReducerOpSequence.size() == 0) {
            return reduceOp;
        }
        return new CompositeReduceOp(reduceOp, new CompositeMapOp(compositeReducerOpSequence));
    }


    public static List<KVUnoOp> compositeOpSequence(Queue<KVUnoOp> opQueue) {
        final List<KVUnoOp> compositeMapperOpSequence = new ArrayList<KVUnoOp>();
        while (!opQueue.isEmpty() && !isGroupingRequired(opQueue.peek())) {
            compositeMapperOpSequence.add(opQueue.poll());
        }
        return compositeMapperOpSequence;
    }

    public static ReduceOp reducerOp(Queue<KVUnoOp> opQueue) {
        if (isGroupingRequired(opQueue.peek())) {
            opQueue.poll();
            return (ReduceOp) opQueue.poll();
        }
        else {
            return null;
        }
    }

    public static boolean isGroupingRequired(KVUnoOp kvUnoOp) {
        return kvUnoOp instanceof GroupByKeyOp;
    }

}
