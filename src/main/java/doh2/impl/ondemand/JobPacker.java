package doh2.impl.ondemand;

import doh2.api.op.GroupByKeyOp;
import doh2.api.op.ReduceOp;
import doh2.impl.op.kvop.CompositeMapOp;
import doh2.impl.op.kvop.CompositeReduceOp;
import doh2.impl.op.kvop.KVUnoOp;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class JobPacker {
    public Iterable<JobPack> packJobs(ExecutionUnit<OnDemandDS.DSExecutionNode> executionUnit) {
        RetainLastQueue<OnDemandDS.DSExecutionNode> nodeQueue =
                new RetainLastQueue<OnDemandDS.DSExecutionNode>(executionUnit.executionNodesList);

        List<JobPack> jobPacks = new ArrayList<JobPack>();
        JobPack jobPack;
        while ((jobPack = packJob(nodeQueue)) != null) {
            jobPacks.add(jobPack);
        }
        return jobPacks;
    }

    public static JobPack packJob(RetainLastQueue<OnDemandDS.DSExecutionNode> nodeQueue) {
        if (nodeQueue.isEmpty()) {
            return null;
        }
        final OnDemandDS input = nodeQueue.peek().incomeNode().dataSet();
        final KVUnoOp mapTaskOp = compressMapOp(compositeOpSequence(nodeQueue));
        final KVUnoOp reduceTaskOp = compressReduceOp(reducerOp(nodeQueue), compositeOpSequence(nodeQueue));
        final OnDemandDS output = nodeQueue.getLast().dataSet();
        return new JobPack(input, output, mapTaskOp, reduceTaskOp);
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


    public static List<KVUnoOp> compositeOpSequence(RetainLastQueue<OnDemandDS.DSExecutionNode> opQueue) {
        final List<KVUnoOp> compositeMapperOpSequence = new ArrayList<KVUnoOp>();
        while (!opQueue.isEmpty() && !isGroupingRequired(opQueue.peek().dataSetParentOp())) {
            compositeMapperOpSequence.add(valid(opQueue.poll().dataSetParentOp()));
        }
        return compositeMapperOpSequence;
    }

    public static ReduceOp reducerOp(RetainLastQueue<OnDemandDS.DSExecutionNode> opQueue) {
        if (opQueue.isEmpty()) {
            return null;
        }
        if (isGroupingRequired(opQueue.peek().dataSetParentOp())) {
            opQueue.poll();
            if (! (opQueue.peek().dataSetParentOp() instanceof ReduceOp)) {
                throw new IllegalArgumentException("No reducer after group by");
            }
            return (ReduceOp) opQueue.poll().dataSetParentOp();
        }
        return null;
    }

    public static boolean isGroupingRequired(KVUnoOp kvUnoOp) {
        return kvUnoOp instanceof GroupByKeyOp;
    }

    public static KVUnoOp valid(KVUnoOp kvUnoOp) {
        if (kvUnoOp instanceof ReduceOp) {
            throw new IllegalArgumentException("ReduceOp without GroupBy");
        }
        return kvUnoOp;
    }


}
