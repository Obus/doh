package doh2.impl;

import com.google.common.collect.Lists;
import doh.api.ds.KVDS;
import doh.api.op.*;
import doh.op.kvop.KVUnoOp;
import doh2.api.DS;
import doh2.api.DSContext;
import doh2.api.HDFSLocation;
import doh2.api.MapDS;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class OnDemandDS<KEY, VALUE> implements DS<KEY, VALUE> {

    private final DSContext context;
    private final ExecutionNode node;
    private final DSDetails details = new DSDetails();
    private volatile boolean isReady;

    private OnDemandDS(OnDemandDS parentDS, KVUnoOp parentOp) {
        this.context = parentDS.context;
        this.node = new ExecutionNode(parentOp,
                Lists.newArrayList(parentDS.node),
                Lists.<ExecutionNode>newArrayList());
        this.node.dataSet = this;
    }

    public OnDemandDS(DSContext context, HDFSLocation location) {
        this.context = context;
        this.node = new ExecutionNode(null,
                Lists.<ExecutionNode>newArrayList(),
                Lists.<ExecutionNode>newArrayList());
        this.details.location = location;
    }

    @Override
    public Iterator<KV<KEY, VALUE>> iteratorChecked() throws IOException {
        return null;
    }

    @Override
    public MapDS<KEY, VALUE> toMapDS() throws Exception {
        context.execute(this);

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

    @Override
    public HDFSLocation getLocation() {
        if (!isReady) {
            throw new IllegalStateException("No location yet provided");
        }
        return details.location;
    }

    @Override
    public OnDemandDS<KEY, VALUE> breakJobHere() {
        node.sequenceBreak = true;
        return this;
    }

    @Override
    public boolean isReady() {
        return isReady;
    }

    @Override
    public OnDemandDS<KEY, VALUE> setOutputPath(Path path) {
        this.details.location = new HDFSLocation.SingleHDFSLocation(path);
        return this;
    }

    @Override
    public OnDemandDS<KEY, VALUE> setOutputFormatCLass(Class<? extends OutputFormat> outputFormatCLass) {
        this.details.formatClass = outputFormatCLass;
        return this;
    }

    @Override
    public OnDemandDS<KEY, VALUE> setNumReduceTasks(int numReduceTasks) {
        this.details.numReducers = numReduceTasks;
        return this;
    }


    DSDetails details() {
        return details;
    }

    ExecutionNode node() {
        return node;
    }

    OnDemandDS parent() {
        return node.incomeNodes().get(0).dataSet;
    }

    Class<KEY> getKeyClass() {
        if (!isReady) {
            throw new IllegalArgumentException("The data set is not ready yet");
        }
        return null;
    }

    Class<VALUE> getValueClass() {
        if (!isReady) {
            throw new IllegalArgumentException("The data set is not ready yet");
        }
        return null;
    }

    void setReady() {
        this.isReady = true;
    }

    private <TKEY, TVALUE> OnDemandDS<TKEY, TVALUE> applyUnoOp(KVUnoOp<KEY, VALUE, TKEY, TVALUE> kvUnoOp) throws Exception {
        OnDemandDS<TKEY, TVALUE> childDS = new OnDemandDS<TKEY, TVALUE>(this, kvUnoOp);
        node.outcomeNodes.add(childDS.node);
        return childDS;
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

    public class OnDemandMapDS implements MapDS<KEY, VALUE> {
        @Override
        public VALUE get(KEY key) {
            return null;
        }

        @Override
        public boolean contains(KEY key) {
            return false;
        }

        @Override
        public Iterator<KV<KEY, VALUE>> iteratorChecked() throws IOException {
            return null;
        }

        @Override
        public MapDS<KEY, VALUE> toMapDS() throws Exception {
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
        public DS<KEY, VALUE> filter(FilterOp<KEY, VALUE> filterOp) throws Exception {
            return null;
        }

        @Override
        public <TKEY, TVALUE> DS<TKEY, TVALUE> map(MapOp<KEY, VALUE, TKEY, TVALUE> mapOp) throws Exception {
            return null;
        }

        @Override
        public <TKEY, TVALUE> DS<TKEY, TVALUE> flatMap(FlatMapOp<KEY, VALUE, TKEY, TVALUE> flatMapOp) throws Exception {
            return null;
        }

        @Override
        public <TKEY, TVALUE> DS<TKEY, TVALUE> reduce(ReduceOp<KEY, VALUE, TKEY, TVALUE> reduceOp) throws Exception {
            return null;
        }

        @Override
        public HDFSLocation getLocation() {
            return null;
        }

        @Override
        public DS<KEY, VALUE> setOutputPath(Path path) {
            return null;
        }

        @Override
        public DS<KEY, VALUE> setOutputFormatCLass(Class<? extends OutputFormat> outputFormatCLass) {
            return null;
        }

        @Override
        public DS<KEY, VALUE> setNumReduceTasks(int numReduceTasks) {
            return null;
        }

        @Override
        public DS<KEY, VALUE> breakJobHere() {
            return null;
        }

        @Override
        public boolean isReady() {
            return false;
        }
    }
}
