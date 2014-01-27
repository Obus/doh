package doh2.impl.ondemand;

import doh2.api.op.GroupByKeyOp;
import doh2.api.op.ReduceOp;
import doh2.impl.op.kvop.CompositeMapOp;
import doh2.impl.op.kvop.CompositeReduceOp;
import doh2.impl.op.kvop.KVUnoOp;
import doh2.api.DSContext;
import doh2.api.HDFSLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.common.Pair;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class OpExecutor {

    private final DSContext dsContext;
    private final OpJobMaker opJobMaker;

    public OpExecutor(DSContext dsContext) {
        this.dsContext = dsContext;
        this.opJobMaker = new OpJobMaker();
    }

    public void execute(ExecutionUnit executionUnit) throws Exception {
        RetainLastQueue<OnDemandDS.ExecutionNode> nodeQueue =
                new RetainLastQueue<OnDemandDS.ExecutionNode>(new LinkedList<OnDemandDS.ExecutionNode>(executionUnit.kvOpList));

        JobPack jobPack;
        while ((jobPack = packJob(nodeQueue)) != null) {
            if (jobPack.mapTaskOp != null) {
                dsContext.getOpSerializer().saveMapperOp(dsContext.conf(), jobPack.mapTaskOp);
            }
            if (jobPack.reduceTaskOp != null) {
                dsContext.getOpSerializer().saveReducerOp(dsContext.conf(), jobPack.reduceTaskOp);
            }

            final Job job = opJobMaker.makeJob(dsContext.conf(),
                    jobPack.input.getKeyClass(),
                    jobPack.input.getValueClass(),
                    jobPack.input.getLocation().getPaths(),
                    jobPack.mapTaskOp, jobPack.reduceTaskOp);

            specifyDatSetDetails(job, jobPack.output.details());

            job.setInputFormatClass(jobPack.input.details().inputFormatClass());

            dsContext.getJobRunner().runJob(job);

            jobPack.output.setReady();
        }

    }

    private JobPack packJob(RetainLastQueue<OnDemandDS.ExecutionNode> nodeQueue) {
        if (nodeQueue.isEmpty()) {
            return null;
        }
        final OnDemandDS input = nodeQueue.peek().dataSet();
        final KVUnoOp mapTaskOp = compressMapOp(compositeOpSequence(nodeQueue));
        final KVUnoOp reduceTaskOp = compressReduceOp(reducerOp(nodeQueue), compositeOpSequence(nodeQueue));
        final OnDemandDS output = nodeQueue.last.dataSet();
        return new JobPack(input, output, mapTaskOp, reduceTaskOp);
    }


    private DSDetails specifyDatSetDetails(Job job, DSDetails details) {
        if (details.formatClass == null) {
            details.formatClass = dsContext.getDefaultFormatClass();
        }
        if (singleLocationPath(details.location) == null) {
            details.location = new HDFSLocation.SingleHDFSLocation(dsContext.getTempPathManager().getNextPath());
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


    public static List<KVUnoOp> compositeOpSequence(RetainLastQueue<OnDemandDS.ExecutionNode> opQueue) {
        final List<KVUnoOp> compositeMapperOpSequence = new ArrayList<KVUnoOp>();
        while (!opQueue.isEmpty() && !isGroupingRequired(opQueue.peek().dataSetParentOp())) {
            compositeMapperOpSequence.add(opQueue.poll().dataSetParentOp());
        }
        return compositeMapperOpSequence;
    }

    public static ReduceOp reducerOp(RetainLastQueue<OnDemandDS.ExecutionNode> opQueue) {
        if (isGroupingRequired(opQueue.peek().dataSetParentOp())) {
            opQueue.poll();
            return (ReduceOp) opQueue.poll().dataSetParentOp();
        }
        else {
            return null;
        }
    }

    public static boolean isGroupingRequired(KVUnoOp kvUnoOp) {
        return kvUnoOp instanceof GroupByKeyOp;
    }


    public static class JobPack {
        public final OnDemandDS input;
        public final OnDemandDS output;
        public final KVUnoOp mapTaskOp;
        public final KVUnoOp reduceTaskOp;

        public JobPack(OnDemandDS input, OnDemandDS output, KVUnoOp mapTaskOp, KVUnoOp reduceTaskOp) {
            this.input = input;
            this.output = output;
            this.mapTaskOp = mapTaskOp;
            this.reduceTaskOp = reduceTaskOp;
        }
    }

    public static class RetainLastQueue<T>  {
        private final Queue<T> queue;
        private T last;

        public RetainLastQueue(Queue<T> queue) {
            this.queue = queue;
        }

        public T peek() {
            return queue.peek();
        }

        public T poll(){
            T poll = queue.poll();
            last = poll == null ? last : poll;
            return last;
        }
        public boolean isEmpty() {
            return queue.isEmpty();
        }
    }



}
