package doh2.impl;

import com.google.common.collect.Lists;
import doh.api.ds.KVDS;
import doh.api.ds.Location;
import doh.api.op.*;
import doh.ds.KeyValueIterator;
import doh.op.kvop.KVUnoOp;
import doh.op.utils.HDFSUtils;
import doh2.api.DS;
import doh2.api.DSContext;
import doh2.api.HDFSLocation;
import doh2.api.MapDS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static doh.op.WritableObjectDictionaryFactory.getObjectClass;

public class OnDemandDS<KEY, VALUE> implements DS<KEY, VALUE>, MapDS<KEY, VALUE> {

    private final DSContext context;
    private final ExecutionNode node;
    private final DSDetails details = new DSDetails();
    private volatile boolean isReady;

    private Map<KEY, VALUE> inMemoryMap;

    private OnDemandDS(OnDemandDS parentDS, KVUnoOp parentOp) {
        this.context = parentDS.context;
        this.node = new ExecutionNode(parentOp,
                Lists.newArrayList(parentDS.node),
                Lists.<ExecutionNode>newArrayList(), this);
    }


    public OnDemandDS(DSContext context, HDFSLocation location) {
        this.context = context;
        this.node = new ExecutionNode(null,
                Lists.<ExecutionNode>newArrayList(),
                Lists.<ExecutionNode>newArrayList(), this);
        this.details.location = location;
        this.details.formatClass = SequenceFileOutputFormat.class;
        this.isReady = true;
    }


    protected synchronized Map<KEY, VALUE> inMemoryMap() {
        if (inMemoryMap == null) {
            Map<KEY, VALUE> map = new HashMap<KEY, VALUE>();
            for (KV<KEY, VALUE> kv : this) {
                map.put(kv.key, kv.value);
            }
            inMemoryMap = map;
        }
        return inMemoryMap;
    }

    @Override
    public VALUE get(KEY key) {
        return inMemoryMap().get(key);
    }

    @Override
    public Iterator<KV<KEY, VALUE>> iteratorChecked() throws Exception {
        context.execute(this);
        if (!getLocation().isSingle()) {
            throw new UnsupportedOperationException();
        }
        Path path = ((HDFSLocation.SingleHDFSLocation) getLocation()).getPath();
        try {
            return new KeyValueIterator<KEY, VALUE>(
                    this.context.conf(),
                    path, this.getKeyClass(), this.getValueClass());
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    @Override
    public MapDS<KEY, VALUE> toMapDS() throws Exception {
        context.execute(this);
        return this;
    }

    @Override
    public KVDS<KEY, VALUE> comeTogetherRightNow(KVDS<KEY, VALUE> other) {
        return null;
    }

    @Override
    public Iterator<KV<KEY, VALUE>> iterator() {
        try {
            return iteratorChecked();
        } catch (Exception e) {
            throw new RuntimeException("Failed to iterate over keyValueDatSet", e);
        }
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

    Class<KEY> getKeyClass() throws IOException {
        if (!isReady) {
            throw new IllegalArgumentException("The data set is not ready yet");
        }
        return getObjectClass((Class<? extends Writable>) this.getWritableKeyClass());
    }

    Class<VALUE> getValueClass() throws IOException {
        if (!isReady) {
            throw new IllegalArgumentException("The data set is not ready yet");
        }
        return getObjectClass((Class<? extends Writable>) this.getWritableValueClass());
    }

    Class<?> getWritableValueClass() throws IOException {
        if (getLocation().isSingle()) {
            Path path = ((HDFSLocation.SingleHDFSLocation) getLocation()).getPath();
            return valueClassOfDir(context.conf(), path);
        } else if (!getLocation().isSingle()) {
            Path path = ((HDFSLocation.MultiHDFSLocation) getLocation()).getPaths()[0];
            return valueClassOfDir(context.conf(), path);
        }
        throw new UnsupportedOperationException();
    }

    Class<?> getWritableKeyClass() throws IOException {
        if (getLocation().isSingle()) {
            Path path = ((HDFSLocation.SingleHDFSLocation) getLocation()).getPath();
            return keyClassOfDir(context.conf(), path);
        } else if (!getLocation().isSingle()) {
            Path path = ((HDFSLocation.MultiHDFSLocation) getLocation()).getPaths()[0];
            return keyClassOfDir(context.conf(), path);
        }
        throw new UnsupportedOperationException();
    }

    public static Class<?> keyClassOfDir(Configuration conf, Path path) throws IOException {
        Path dataPath = HDFSUtils.listOutputFiles(conf, path)[0];
        SequenceFile.Reader r
                = new SequenceFile.Reader(dataPath.getFileSystem(conf), dataPath, conf);
        return r.getKeyClass();
    }

    public static Class<?> valueClassOfDir(Configuration conf, Path path) throws IOException {
        Path dataPath = HDFSUtils.listOutputFiles(conf, path)[0];
        SequenceFile.Reader r
                = new SequenceFile.Reader(dataPath.getFileSystem(conf), dataPath, conf);
        return r.getValueClass();
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
        private final OnDemandDS dataSet;

        public ExecutionNode(KVUnoOp dsParentOp, List<ExecutionNode> incomeNodes, List<ExecutionNode> outcomeNodes, OnDemandDS dataSet) {
            if (incomeNodes.size() > 1) {
                throw new UnsupportedOperationException("Only one income node supported.");
            }
            this.dsParentOp = dsParentOp;
            this.incomeNodes = incomeNodes;
            this.outcomeNodes = outcomeNodes;
            this.dataSet = dataSet;
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
