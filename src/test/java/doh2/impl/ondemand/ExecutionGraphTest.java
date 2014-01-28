package doh2.impl.ondemand;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ExecutionGraphTest {


    @Test
    public void test_twoTargetForks_intermediateReady() throws Exception {
        Iterator<ExecutionUnit<TestNode>> gi;
        ExecutionUnit<TestNode> eu;

        TestNode n0 = TestNode.done("0");
        TestNode n00 = n0.child();
        TestNode n000 = n00.readyChild();
        TestNode n0000 = n000.child();
        TestNode n0001 = n000.child();
        TestNode n00010 = n0001.child();
        TestNode n000100 = n00010.child();
        TestNode n00000 = n0000.child();
        TestNode n000000 = n00000.child();
        TestNode n000001 = n00000.child();
        TestNode n0000010 = n000001.readyChild();
        TestNode n00000100 = n0000010.child();
        TestNode n0000000 = n000000.child();


        gi = new ExecutionGraph<TestNode>(Arrays.asList(n000100, n00000100, n0000000)).executionUnits().iterator();
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(4, eu.executionNodesList.size());
        assertEquals(n0000, eu.executionNodesList.get(0));
        assertEquals(n00000, eu.executionNodesList.get(1));
        assertEquals(n000000, eu.executionNodesList.get(2));
        assertEquals(n0000000, eu.executionNodesList.get(3));
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(3, eu.executionNodesList.size());
        assertEquals(n0001, eu.executionNodesList.get(0));
        assertEquals(n00010, eu.executionNodesList.get(1));
        assertEquals(n000100, eu.executionNodesList.get(2));
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(1, eu.executionNodesList.size());
        assertEquals(n00000100, eu.executionNodesList.get(0));
        assertFalse(gi.hasNext());

    }


    @Test
    public void test_oneTargetFork_oneNonTargetFork_intermediateReady() throws Exception {
        Iterator<ExecutionUnit<TestNode>> gi;
        ExecutionUnit<TestNode> eu;

        TestNode n0 = TestNode.done("0");
        TestNode n00 = n0.child();
        TestNode n000 = n00.child();
        TestNode n0000 = n000.child();
        TestNode n0001 = n000.child();
        TestNode n00010 = n0001.child();
        TestNode n000100 = n00010.child();
        TestNode n00000 = n0000.readyChild();
        TestNode n000000 = n00000.child();
        TestNode n000001 = n00000.child();
        TestNode n0000010 = n000001.child();
        TestNode n00000100 = n0000010.child();
        TestNode n0000000 = n000000.child();


        gi = new ExecutionGraph<TestNode>(Arrays.asList(n000100, n00000100)).executionUnits().iterator();
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(3, eu.executionNodesList.size());
        assertEquals(n000001, eu.executionNodesList.get(0));
        assertEquals(n0000010, eu.executionNodesList.get(1));
        assertEquals(n00000100, eu.executionNodesList.get(2));
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(5, eu.executionNodesList.size());
        assertEquals(n00, eu.executionNodesList.get(0));
        assertEquals(n000, eu.executionNodesList.get(1));
        assertEquals(n0001, eu.executionNodesList.get(2));
        assertEquals(n00010, eu.executionNodesList.get(3));
        assertEquals(n000100, eu.executionNodesList.get(4));
        assertFalse(gi.hasNext());
    }

    @Test
    public void test_oneNoneTargetFork_multiExecution() throws Exception {
        Iterator<ExecutionUnit<TestNode>> gi;
        ExecutionUnit<TestNode> eu;

        TestNode n0 = TestNode.done("0");
        TestNode n00 = n0.child();
        TestNode n000 = n00.child();
        TestNode n0000 = n000.child();
        TestNode n0001 = n000.child();
        TestNode n00010 = n0001.child();
        TestNode n000100 = n00010.child();
        TestNode n00000 = n0000.child();


        gi = new ExecutionGraph<TestNode>(Arrays.asList(n000100, n000100, n000100)).executionUnits().iterator();
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertFalse(gi.hasNext());
        assertEquals(5, eu.executionNodesList.size());
        assertEquals(n00, eu.executionNodesList.get(0));
        assertEquals(n000, eu.executionNodesList.get(1));
        assertEquals(n0001, eu.executionNodesList.get(2));
        assertEquals(n00010, eu.executionNodesList.get(3));
        assertEquals(n000100, eu.executionNodesList.get(4));
    }


    @Test(expected = IllegalArgumentException.class)
    public void test_noReadyNodes() throws Exception {
        TestNode n0 = TestNode.notDone("0");
        TestNode n00 = n0.child();
        TestNode n000 = n00.child();
        TestNode n0000 = n000.child();
        TestNode n0001 = n000.child();
        TestNode n00010 = n0001.child();
        TestNode n000100 = n00010.child();
        TestNode n00000 = n0000.child();

        new ExecutionGraphIterator<TestNode>(Arrays.asList(n000100));
        fail();
    }

    @Test
    public void test_oneNode() throws Exception {
        Iterator<ExecutionUnit<TestNode>> gi;
        TestNode n0 = TestNode.done("0");

        gi = new ExecutionGraph<TestNode>(Arrays.asList(n0)).executionUnits().iterator();
        assertFalse(gi.hasNext());
    }

    @Test
    public void test_twoNodes() throws Exception {
        Iterator<ExecutionUnit<TestNode>> gi;
        ExecutionUnit<TestNode> eu;
        TestNode n0 = TestNode.done("0");
        TestNode n00 = n0.child();


        gi = new ExecutionGraph<TestNode>(Arrays.asList(n00)).executionUnits().iterator();
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertFalse(gi.hasNext());
        assertEquals(1, eu.executionNodesList.size());
        assertEquals(n00, eu.executionNodesList.get(0));
    }

    @Test
    public void test_oneNoneTargetFork() throws Exception {
        Iterator<ExecutionUnit<TestNode>> gi;
        ExecutionUnit<TestNode> eu;

        TestNode n0 = TestNode.done("0");
        TestNode n00 = n0.child();
        TestNode n000 = n00.child();
        TestNode n0000 = n000.child();
        TestNode n0001 = n000.child();
        TestNode n00010 = n0001.child();
        TestNode n000100 = n00010.child();
        TestNode n00000 = n0000.child();


        gi = new ExecutionGraph<TestNode>(Arrays.asList(n000100)).executionUnits().iterator();
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertFalse(gi.hasNext());
        assertEquals(5, eu.executionNodesList.size());
        assertEquals(n00, eu.executionNodesList.get(0));
        assertEquals(n000, eu.executionNodesList.get(1));
        assertEquals(n0001, eu.executionNodesList.get(2));
        assertEquals(n00010, eu.executionNodesList.get(3));
        assertEquals(n000100, eu.executionNodesList.get(4));
    }

    @Test
    public void test_withBreak() throws Exception {
        Iterator<ExecutionUnit<TestNode>> gi;
        ExecutionUnit<TestNode> eu;

        TestNode n0 = TestNode.done("0");
        TestNode n00 = n0.child();
        TestNode n000 = n00.child();
        TestNode n0000 = n000.child().breakHere();
        TestNode n0001 = n000.child().breakHere();
        TestNode n00010 = n0001.child();
        TestNode n000100 = n00010.child();
        TestNode n00000 = n0000.child();


        gi = new ExecutionGraph<TestNode>(Arrays.asList(n000100)).executionUnits().iterator();
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(3, eu.executionNodesList.size());
        assertEquals(n00, eu.executionNodesList.get(0));
        assertEquals(n000, eu.executionNodesList.get(1));
        assertEquals(n0001, eu.executionNodesList.get(2));
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(2, eu.executionNodesList.size());
        assertEquals(n00010, eu.executionNodesList.get(0));
        assertEquals(n000100, eu.executionNodesList.get(1));
        assertFalse(gi.hasNext());
    }


    @Test
    public void test_oneTargetFork() throws Exception {
        Iterator<ExecutionUnit<TestNode>> gi;
        ExecutionUnit<TestNode> eu;

        TestNode n0 = TestNode.done("0");
        TestNode n00 = n0.child();
        TestNode n000 = n00.child();
        TestNode n0000 = n000.child();
        TestNode n0001 = n000.child();
        TestNode n00010 = n0001.child();
        TestNode n000100 = n00010.child();
        TestNode n00000 = n0000.child();


        gi = new ExecutionGraph<TestNode>(Arrays.asList(n000100, n00000)).executionUnits().iterator();
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(2, eu.executionNodesList.size());
        assertEquals(n00, eu.executionNodesList.get(0));
        assertEquals(n000, eu.executionNodesList.get(1));
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(2, eu.executionNodesList.size());
        assertEquals(n0000, eu.executionNodesList.get(0));
        assertEquals(n00000, eu.executionNodesList.get(1));
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(3, eu.executionNodesList.size());
        assertEquals(n0001, eu.executionNodesList.get(0));
        assertEquals(n00010, eu.executionNodesList.get(1));
        assertEquals(n000100, eu.executionNodesList.get(2));
        assertFalse(gi.hasNext());
    }


    @Test
    public void test_oneTargetFork_oneNonTargetFork() throws Exception {
        Iterator<ExecutionUnit<TestNode>> gi;
        ExecutionUnit<TestNode> eu;

        TestNode n0 = TestNode.done("0");
        TestNode n00 = n0.child();
        TestNode n000 = n00.child();
        TestNode n0000 = n000.child();
        TestNode n0001 = n000.child();
        TestNode n00010 = n0001.child();
        TestNode n000100 = n00010.child();
        TestNode n00000 = n0000.child();
        TestNode n000000 = n00000.child();
        TestNode n000001 = n00000.child();
        TestNode n0000010 = n000001.child();
        TestNode n00000100 = n0000010.child();
        TestNode n0000000 = n000000.child();


        gi = new ExecutionGraph<TestNode>(Arrays.asList(n000100, n00000100)).executionUnits().iterator();
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(2, eu.executionNodesList.size());
        assertEquals(n00, eu.executionNodesList.get(0));
        assertEquals(n000, eu.executionNodesList.get(1));
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(5, eu.executionNodesList.size());
        assertEquals(n0000, eu.executionNodesList.get(0));
        assertEquals(n00000, eu.executionNodesList.get(1));
        assertEquals(n000001, eu.executionNodesList.get(2));
        assertEquals(n0000010, eu.executionNodesList.get(3));
        assertEquals(n00000100, eu.executionNodesList.get(4));
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(3, eu.executionNodesList.size());
        assertEquals(n0001, eu.executionNodesList.get(0));
        assertEquals(n00010, eu.executionNodesList.get(1));
        assertEquals(n000100, eu.executionNodesList.get(2));
        assertFalse(gi.hasNext());
    }

    @Test
    public void test_twoTargetForks() throws Exception {
        Iterator<ExecutionUnit<TestNode>> gi;
        ExecutionUnit<TestNode> eu;

        TestNode n0 = TestNode.done("0");
        TestNode n00 = n0.child();
        TestNode n000 = n00.child();
        TestNode n0000 = n000.child();
        TestNode n0001 = n000.child();
        TestNode n00010 = n0001.child();
        TestNode n000100 = n00010.child();
        TestNode n00000 = n0000.child();
        TestNode n000000 = n00000.child();
        TestNode n000001 = n00000.child();
        TestNode n0000010 = n000001.child();
        TestNode n00000100 = n0000010.child();
        TestNode n0000000 = n000000.child();


        gi = new ExecutionGraph<TestNode>(Arrays.asList(n000100, n00000100, n0000000)).executionUnits().iterator();
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(2, eu.executionNodesList.size());
        assertEquals(n00, eu.executionNodesList.get(0));
        assertEquals(n000, eu.executionNodesList.get(1));
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(2, eu.executionNodesList.size());
        assertEquals(n0000, eu.executionNodesList.get(0));
        assertEquals(n00000, eu.executionNodesList.get(1));
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(3, eu.executionNodesList.size());
        assertEquals(n0001, eu.executionNodesList.get(0));
        assertEquals(n00010, eu.executionNodesList.get(1));
        assertEquals(n000100, eu.executionNodesList.get(2));
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(2, eu.executionNodesList.size());
        assertEquals(n000000, eu.executionNodesList.get(0));
        assertEquals(n0000000, eu.executionNodesList.get(1));
        assertTrue(gi.hasNext());
        eu = gi.next();
        assertEquals(3, eu.executionNodesList.size());
        assertEquals(n000001, eu.executionNodesList.get(0));
        assertEquals(n0000010, eu.executionNodesList.get(1));
        assertEquals(n00000100, eu.executionNodesList.get(2));
        assertFalse(gi.hasNext());

    }



    public static class TestNode implements ExecutionNode<TestNode> {
        final String id;
        final TestNode incomeNode;
        final List<TestNode> outcomeNodes;
        boolean done;
        boolean breakHere = false;

        private TestNode(String id, TestNode parent) {
            this.id = id;
            incomeNode = parent;
            outcomeNodes = new ArrayList<TestNode>();
            done = false;
        }


        private TestNode(String id, boolean done) {
            this.id = id;
            incomeNode = null;
            outcomeNodes = new ArrayList<TestNode>();
            this.done = done;
        }

        @Override
        public TestNode incomeNode() {
            return incomeNode;
        }

        @Override
        public List<TestNode> outcomeNodes() {
            return outcomeNodes;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        public boolean isBreakHere() {
            return breakHere;
        }

        private TestNode child(boolean done) {
            TestNode child = new TestNode(this.id + this.outcomeNodes.size(), this);
            this.outcomeNodes.add(child);
            child.done = done;
            return child;
        }

        TestNode child() {
            return child(false);
        }
        TestNode readyChild() {
            return child(true);
        }

        static TestNode done(String id) {
            return new TestNode(id, true);
        }
        static TestNode notDone(String id) {
            return new TestNode(id, false);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof TestNode)) {
                return false;
            }
            return id.equals(((TestNode)obj).id);
        }

        public TestNode breakHere() {
            this.breakHere = true;
            return this;
        }
    }
}
