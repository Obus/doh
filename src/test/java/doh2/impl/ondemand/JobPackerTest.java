package doh2.impl.ondemand;

import com.google.common.collect.Lists;
import doh2.api.DSFactory;
import doh2.api.SingleHDFSLocation;
import doh2.api.op.FilterOp;
import doh2.api.op.FlatMapOp;
import doh2.api.op.GroupByKeyOp;
import doh2.api.op.MapOp;
import doh2.api.op.ReduceOp;
import doh2.impl.op.kvop.CompositeMapOp;
import doh2.impl.op.kvop.CompositeReduceOp;
import doh2.impl.op.kvop.KVUnoOp;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertEquals;

public class JobPackerTest {

    @Test(expected = IllegalArgumentException.class)
    public void testPackJobs_ReduceWithoutGroupBy() throws Exception {
        OnDemandDS.DSExecutionNode rootNode = new OnDemandDS.DSExecutionNode(null, new OnDemandDS(null, new SingleHDFSLocation(new Path("root"))));
        List<OnDemandDS.DSExecutionNode> nodeList = new ArrayList<OnDemandDS.DSExecutionNode>();
        nodeList.add(executionNode(FilterOp.class, rootNode));
        nodeList.add(executionNode(ReduceOp.class, nodeList.get(nodeList.size() - 1), "reduce1"));

        JobPacker jobPacker = new JobPacker();
        jobPacker.packJobs(new ExecutionUnit<OnDemandDS.DSExecutionNode>(nodeList));
        fail();
    }

    @Test
    public void testPackJob_Complex() throws Exception {
        OnDemandDS.DSExecutionNode rootNode = new OnDemandDS.DSExecutionNode(null, new OnDemandDS(null, new SingleHDFSLocation(new Path("root"))));
        List<OnDemandDS.DSExecutionNode> nodeList = new ArrayList<OnDemandDS.DSExecutionNode>();
        nodeList.add(executionNode(MapOp.class, rootNode));
        nodeList.add(executionNode(FlatMapOp.class, nodeList.get(nodeList.size() - 1)));
        nodeList.add(executionNode(FilterOp.class, nodeList.get(nodeList.size() - 1)));
        nodeList.add(executionNode(GroupByKeyOp.class, nodeList.get(nodeList.size() - 1)));
        nodeList.add(executionNode(ReduceOp.class, nodeList.get(nodeList.size() - 1), "reduce1"));
        nodeList.add(executionNode(FlatMapOp.class, nodeList.get(nodeList.size() - 1)));
        nodeList.add(executionNode(FlatMapOp.class, nodeList.get(nodeList.size() - 1)));
        nodeList.add(executionNode(FlatMapOp.class, nodeList.get(nodeList.size() - 1), "fin1"));
        nodeList.add(executionNode(GroupByKeyOp.class, nodeList.get(nodeList.size() - 1)));
        nodeList.add(executionNode(ReduceOp.class, nodeList.get(nodeList.size() - 1), "reduce1"));
        nodeList.add(executionNode(FlatMapOp.class, nodeList.get(nodeList.size() - 1)));
        nodeList.add(executionNode(MapOp.class, nodeList.get(nodeList.size() - 1)));
        nodeList.add(executionNode(FilterOp.class, nodeList.get(nodeList.size() - 1), "fin2"));

        JobPacker jobPacker = new JobPacker();
        List<JobPack> jobPacks = Lists.newArrayList(jobPacker.packJobs(new ExecutionUnit<OnDemandDS.DSExecutionNode>(nodeList)));

        assertEquals(2, jobPacks.size());

        JobPack jobPack = jobPacks.get(0);
        assertEquals("root", dsId(jobPack.input));
        assertEquals("fin1", dsId(jobPack.output));
        assertEquals(3, ((CompositeMapOp) jobPack.mapTaskOp).sequence.getSequence().size());
        assertTrue(((CompositeMapOp) jobPack.mapTaskOp).sequence.getSequence().get(0) instanceof MapOp);
        assertTrue(((CompositeMapOp) jobPack.mapTaskOp).sequence.getSequence().get(1) instanceof FlatMapOp);
        assertTrue(((CompositeMapOp) jobPack.mapTaskOp).sequence.getSequence().get(2) instanceof FilterOp);
        assertEquals(3, ((CompositeReduceOp) jobPack.reduceTaskOp).getCompositeMapOp().sequence.getSequence().size());
        assertTrue(((CompositeReduceOp) jobPack.reduceTaskOp).getCompositeMapOp().sequence.getSequence().get(0) instanceof FlatMapOp);
        assertTrue(((CompositeReduceOp) jobPack.reduceTaskOp).getCompositeMapOp().sequence.getSequence().get(1) instanceof FlatMapOp);
        assertTrue(((CompositeReduceOp) jobPack.reduceTaskOp).getCompositeMapOp().sequence.getSequence().get(2) instanceof FlatMapOp);

        jobPack = jobPacks.get(1);
        assertEquals("fin1", dsId(jobPack.input));
        assertEquals("fin2", dsId(jobPack.output));
        assertNull(jobPack.mapTaskOp);
        assertEquals(3, ((CompositeReduceOp) jobPack.reduceTaskOp).getCompositeMapOp().sequence.getSequence().size());
        assertTrue(((CompositeReduceOp) jobPack.reduceTaskOp).getCompositeMapOp().sequence.getSequence().get(0) instanceof FlatMapOp);
        assertTrue(((CompositeReduceOp) jobPack.reduceTaskOp).getCompositeMapOp().sequence.getSequence().get(1) instanceof MapOp);
        assertTrue(((CompositeReduceOp) jobPack.reduceTaskOp).getCompositeMapOp().sequence.getSequence().get(2) instanceof FilterOp);

    }

    @Test
    public void testPackJob_NoReducer() throws Exception {
        OnDemandDS.DSExecutionNode rootNode = new OnDemandDS.DSExecutionNode(null, new OnDemandDS(null, new SingleHDFSLocation(new Path("root"))));
        List<OnDemandDS.DSExecutionNode> nodeList = new ArrayList<OnDemandDS.DSExecutionNode>();
        nodeList.add(executionNode(MapOp.class, rootNode));
        nodeList.add(executionNode(FlatMapOp.class, nodeList.get(nodeList.size() - 1)));
        nodeList.add(executionNode(FilterOp.class, nodeList.get(nodeList.size() - 1), "fin"));

        JobPacker jobPacker = new JobPacker();
        List<JobPack> jobPacks = Lists.newArrayList(jobPacker.packJobs(new ExecutionUnit<OnDemandDS.DSExecutionNode>(nodeList)));

        assertEquals(1, jobPacks.size());

        JobPack jobPack = jobPacks.get(0);
        assertEquals("root", dsId(jobPack.input));
        assertEquals("fin", dsId(jobPack.output));
        assertEquals(3, ((CompositeMapOp) jobPack.mapTaskOp).sequence.getSequence().size());
        assertTrue(((CompositeMapOp) jobPack.mapTaskOp).sequence.getSequence().get(0) instanceof MapOp);
        assertTrue(((CompositeMapOp) jobPack.mapTaskOp).sequence.getSequence().get(1) instanceof FlatMapOp);
        assertTrue(((CompositeMapOp) jobPack.mapTaskOp).sequence.getSequence().get(2) instanceof FilterOp);
        assertNull(jobPack.reduceTaskOp);
    }

    @Test
    public void testPackJob_NoMapper() throws Exception {
        OnDemandDS.DSExecutionNode rootNode = new OnDemandDS.DSExecutionNode(null, new OnDemandDS(null, new SingleHDFSLocation(new Path("root"))));
        List<OnDemandDS.DSExecutionNode> nodeList = new ArrayList<OnDemandDS.DSExecutionNode>();
        nodeList.add(executionNode(GroupByKeyOp.class, rootNode));
        nodeList.add(executionNode(ReduceOp.class, nodeList.get(nodeList.size() - 1), "reduce1"));
        nodeList.add(executionNode(FlatMapOp.class, nodeList.get(nodeList.size() - 1)));
        nodeList.add(executionNode(FlatMapOp.class, nodeList.get(nodeList.size() - 1)));
        nodeList.add(executionNode(FlatMapOp.class, nodeList.get(nodeList.size() - 1), "fin1"));

        JobPacker jobPacker = new JobPacker();
        List<JobPack> jobPacks = Lists.newArrayList(jobPacker.packJobs(new ExecutionUnit<OnDemandDS.DSExecutionNode>(nodeList)));

        assertEquals(1, jobPacks.size());

        JobPack jobPack = jobPacks.get(0);
        assertEquals("root", dsId(jobPack.input));
        assertEquals("fin1", dsId(jobPack.output));
        assertNull(jobPack.mapTaskOp);
        assertEquals(3, ((CompositeReduceOp) jobPack.reduceTaskOp).getCompositeMapOp().sequence.getSequence().size());
        assertTrue(((CompositeReduceOp) jobPack.reduceTaskOp).getCompositeMapOp().sequence.getSequence().get(0) instanceof FlatMapOp);
        assertTrue(((CompositeReduceOp) jobPack.reduceTaskOp).getCompositeMapOp().sequence.getSequence().get(1) instanceof FlatMapOp);
        assertTrue(((CompositeReduceOp) jobPack.reduceTaskOp).getCompositeMapOp().sequence.getSequence().get(2) instanceof FlatMapOp);

    }

    @Test
    public void testPackJob_Simple() throws Exception {
        OnDemandDS.DSExecutionNode rootNode = new OnDemandDS.DSExecutionNode(null, new OnDemandDS(null, new SingleHDFSLocation(new Path("root"))));
        List<OnDemandDS.DSExecutionNode> nodeList = new ArrayList<OnDemandDS.DSExecutionNode>();
        nodeList.add(executionNode(FilterOp.class, rootNode));
        nodeList.add(executionNode(GroupByKeyOp.class, nodeList.get(nodeList.size() - 1)));
        nodeList.add(executionNode(ReduceOp.class, nodeList.get(nodeList.size() - 1), "reduce1"));

        JobPacker jobPacker = new JobPacker();
        List<JobPack> jobPacks = Lists.newArrayList(jobPacker.packJobs(new ExecutionUnit<OnDemandDS.DSExecutionNode>(nodeList)));

        assertEquals(1, jobPacks.size());

        JobPack jobPack = jobPacks.get(0);
        assertEquals("root", dsId(jobPack.input));
        assertEquals("reduce1", dsId(jobPack.output));
        assertTrue(jobPack.mapTaskOp instanceof FilterOp);
        assertTrue(jobPack.reduceTaskOp instanceof ReduceOp);
    }


    public static String dsId(OnDemandDS ds) {
        return ((SingleHDFSLocation)ds.details().location).getPath().toString();
    }
    
    public static OnDemandDS.DSExecutionNode executionNode(Class<? extends KVUnoOp> opClass, OnDemandDS.DSExecutionNode parent) throws Exception {
        return executionNode(opClass, parent, "fake path");
    }
    public static OnDemandDS.DSExecutionNode executionNode(Class<? extends KVUnoOp> opClass, OnDemandDS.DSExecutionNode parent, String dsId) throws Exception {
        KVUnoOp op = mock(opClass);
        return new OnDemandDS.DSExecutionNode(op, parent, 
                parent.dataSet().applyUnoOp(op).setOutputPath(new Path(dsId))
        );
    }

    public static OnDemandDS ds(String path) {
        return (OnDemandDS)DSFactory.create(new Path(path), null);
    }
    public static OnDemandDS ds() {
        return (OnDemandDS)DSFactory.create(new Path("fake path"), null);
    }

}
