package doh2.impl;

import com.google.common.collect.Lists;
import doh.api.ds.KVDS;
import doh.api.op.*;
import doh.op.kvop.KVUnoOp;
import doh2.api.DS;
import doh2.api.HDFSLocation;
import doh2.api.MapDS;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class OnDemandDS<KEY, VALUE> implements DS<KEY, VALUE> {

    private final ExecutionNode node;
    private final DSDetails details = new DSDetails();
    private volatile boolean isReady;

    private OnDemandDS(OnDemandDS parentDS, KVUnoOp parentOp) {
        this.node = new ExecutionNode(parentOp,
                Lists.newArrayList(parentDS.node),
                Lists.<ExecutionNode>newArrayList());
        this.node.dataSet = this;
    }

    public OnDemandDS parent() {
        return node.incomeNodes().get(0).dataSet;
    }

    @Override
    public Iterator<KV<KEY, VALUE>> iteratorChecked() throws IOException {
        return null;
    }

    @Override
    public MapDS<KEY, VALUE> toMapDS() {
        return null;
    }

    @Override
    public KVDS<KEY, VALUE> comeTogetherRightNow(KVDS<KEY, VALUE> other) {
        return null;
    }

    @Override
    public Iterator<KV<KEY, VALUE>> iterator() {
        return null;
    }

    @Override
    public <TKEY, TVALUE> OnDemandDS<TKEY, TVALUE> map(MapOp<KEY, VALUE, TKEY, TVALUE> mapOp) throws Exception {
        return applyUnoOp(mapOp);
    }

    @Override
    public OnDemandDS<KEY, VALUE> filter(FilterOp<KEY, VALUE> filterOp) throws Exception {
        return applyUnoOp(filterOp);
    }

    @Override
    public <TKEY, TVALUE> OnDemandDS<TKEY, TVALUE> flatMap(FlatMapOp<KEY, VALUE, TKEY, TVALUE> flatMapOp) throws Exception {
        return applyUnoOp(flatMapOp);
    }

    @Override
    public <TKEY, TVALUE> OnDemandDS<TKEY, TVALUE> reduce(ReduceOp<KEY, VALUE, TKEY, TVALUE> reduceOp) throws Exception {
        GroupByKeyOp<KEY, VALUE> groupByKeyOp = new GroupByKeyOp<KEY, VALUE>();
        return applyUnoOp(groupByKeyOp).applyUnoOp(reduceOp);
    }

    private <TKEY, TVALUE> OnDemandDS<TKEY, TVALUE> applyUnoOp(KVUnoOp<KEY, VALUE, TKEY, TVALUE> kvUnoOp) throws Exception {
        OnDemandDS<TKEY, TVALUE> childDS = new OnDemandDS<TKEY, TVALUE>(this, kvUnoOp);
        node.outcomeNodes.add(childDS.node);
        return childDS;
    }

    @Override
    public HDFSLocation getLocation() {
        if (!isReady) {
            throw new IllegalStateException("No location yet provided");
        }
        return details.location;
    }

    DSDetails details() {
        return details;
    }

    ExecutionNode node() {
        return node;
    }

    public OnDemandDS<KEY, VALUE> breakJobHere() {
        node.sequenceBreak = true;
        return this;
    }

    public boolean isReady() {
        return isReady;
    }

    void setReady() {
        this.isReady = true;
    }

    @Override
    public void setOutputPath(Path path) {
        this.details.location = new HDFSLocation.SingleHDFSLocation(path);
    }

    @Override
    public void setOutputFormatCLass(Class<? extends OutputFormat> outputFormatCLass) {
        this.details.formatClass = outputFormatCLass;
    }

    @Override
    public void setNumReduceTasks(int numReduceTasks) {
        this.details.numReducers = numReduceTasks;
    }

    Class<KEY> getKeyClass() {
        if (!isReady) {
            throw new IllegalArgumentException("The data set is not ready yet");
        }
    }

    Class<VALUE> getValueClass() {
        if (!isReady) {
            throw new IllegalArgumentException("The data set is not ready yet");
        }

    }

    /**
     * Node of execution graph
     * Consist of
     * - associated data set
     * - operation which produce the associated data set
     * - lists of income and outcome nodes
     *
     * @warning: No more than one income node is supported at the moment
     */
    public static class ExecutionNode {
        private final KVUnoOp dsParentOp;
        private final List<ExecutionNode> incomeNodes;
        private final List<ExecutionNode> outcomeNodes;
        private boolean sequenceBreak = false;
        private OnDemandDS dataSet;

        public ExecutionNode(KVUnoOp dsParentOp, List<ExecutionNode> incomeNodes, List<ExecutionNode> outcomeNodes) {
            if (incomeNodes.size() > 1) {
                throw new UnsupportedOperationException("Only one income node supported.");
            }
            this.dsParentOp = dsParentOp;
            this.incomeNodes = incomeNodes;
            this.outcomeNodes = outcomeNodes;
        }

        public boolean isSequenceBreak() {
            return sequenceBreak;
        }

        public OnDemandDS dataSet() {
            return dataSet;
        }

        public KVUnoOp dataSetParentOp() {
            return dsParentOp;
        }

        public List<ExecutionNode> incomeNodes() {
            return incomeNodes;
        }

        public List<ExecutionNode> outcomeNodes() {
            return outcomeNodes;
        }
    }
}
