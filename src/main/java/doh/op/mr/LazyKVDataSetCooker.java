package doh.op.mr;

import com.google.common.collect.Lists;
import doh2.api.HDFSLocation;
import doh.api.op.FlatMapOp;
import doh.api.op.MapOp;
import doh.api.op.ReduceOp;
import doh.ds.LazyKVDS;
import doh.ds.RealKVDS;
import doh.op.kvop.CompositeMapOp;
import doh.op.kvop.CompositeReduceOp;
import doh.op.kvop.KVOp;
import doh.op.kvop.KVUnoOp;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.common.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LazyKVDataSetCooker {


    public static class ParentOpChild {
        private final LazyKVDS parent;
        private final KVOp op;
        private final LazyKVDS child;

        private ParentOpChild(LazyKVDS parent, KVOp op, LazyKVDS child) {
            this.parent = parent;
            this.op = op;
            this.child = child;
        }
    }

    private final List<ParentOpChild> dataSetPath;

    public LazyKVDataSetCooker(LazyKVDS lazy) {
        dataSetPath = Lists.newArrayList();
        LazyKVDS child = lazy;
        while (! child.isReady()) {
            dataSetPath.add(0, new ParentOpChild(child.getParentDataSet(), child.getParentOperation(), child));
            child = child.getParentDataSet();
        }
    }

    public void cookIt() throws Exception {
        RealKVDS origin = dataSetPath.get(0).parent.real();

        Pair<LazyKVDS, Job> lazyJobPair;
        Iterator<ParentOpChild> parentOpChildIt = dataSetPath.iterator();

        while ((lazyJobPair = nextJob(parentOpChildIt)) != null) {
            Path outputPath = FileOutputFormat.getOutputPath(lazyJobPair.getSecond());
            origin.getContext().runJob(lazyJobPair.getSecond());
            RealKVDS real = new RealKVDS(new HDFSLocation.SingleHDFSLocation(outputPath));
            real.setContext(origin.getContext());
            lazyJobPair.getFirst().setReal(real);
        }
    }

    private Pair<LazyKVDS, Job> nextJob(Iterator<ParentOpChild> it) throws Exception {
        if (!it.hasNext()) {
            return null;
        }

        ParentOpChild first = it.next();
        RealKVDS origin = first.parent.real();

        ParentOpChild current = first;

        List<KVUnoOp> compositeMapperOpSequence = new ArrayList<KVUnoOp>();
        while (isMapperPartOp(current.op)) {
            compositeMapperOpSequence.add((KVUnoOp) current.op);
            if (!it.hasNext()) {
                Job job = mapOnlyJob(origin, compositeMapperOpSequence);
                return new Pair<LazyKVDS, Job>(current.child, job);
            }
            current = it.next();
        }


        final ReduceOp reduceOp;
        if (isReducePartOp(current.op)) {
            reduceOp = (ReduceOp) current.op;
        }
        else {
            throw new IllegalArgumentException("Unknown operation type " + current.op.getClass());
        }

        if (it.hasNext()) current = it.next();

        List<KVUnoOp> compositeReducerOpSequence = new ArrayList<KVUnoOp>();
        while (isMapperPartOp(current.op)) {
            compositeReducerOpSequence.add((KVUnoOp) current.op);
            if (!it.hasNext()) {
                break;
            }
            current = it.next();
        }

        Job job = mapReduceJob(origin, compositeMapperOpSequence, reduceOp, compositeReducerOpSequence);
        return new Pair<LazyKVDS, Job>(current.child, job);
    }


    private Job mapReduceJob(RealKVDS origin, List<KVUnoOp> compositeMapperOpSequence, ReduceOp reduceOp, List<KVUnoOp> compositeReducerOpSequence) throws Exception {
        if (compositeMapperOpSequence.isEmpty()) {
            if (compositeReducerOpSequence.isEmpty()) {
                return KVOpJobUtils.createReduceOnlyJob(origin, reduceOp);
            }
            else {
                CompositeReduceOp compositeReduceOp = new CompositeReduceOp(reduceOp, new CompositeMapOp(compositeReducerOpSequence));
                return KVOpJobUtils.createCompositeReduceOnlyJob(origin, compositeReduceOp);
            }
        }
        else {
            CompositeMapOp compositeMapOp = new CompositeMapOp(compositeMapperOpSequence);
            if (compositeReducerOpSequence.isEmpty()) {
                return KVOpJobUtils.createCompositeMapReduceJob(origin, compositeMapOp, reduceOp);
            }
            else {
                CompositeReduceOp compositeReduceOp = new CompositeReduceOp(reduceOp, new CompositeMapOp(compositeReducerOpSequence));
                return KVOpJobUtils.createCompositeMapCompositeReduceJob(origin, compositeMapOp, compositeReduceOp);
            }
        }
    }

    private Job mapOnlyJob(RealKVDS origin, List<KVUnoOp> mapperOpSequence) throws Exception {
        if (mapperOpSequence.isEmpty()) {
            throw new IllegalArgumentException("No mapper operations");
        }
        Job job = KVOpJobUtils.createCompositeMapOnlyJob(origin, new CompositeMapOp(mapperOpSequence));
        return job;
    }

    public static boolean isMapperPartOp(KVOp kvOp) throws Exception {
        return kvOp instanceof MapOp || kvOp instanceof FlatMapOp;
    }

    public static boolean isReducePartOp(KVOp kvOp) throws Exception {
        return kvOp instanceof ReduceOp;
    }
}
