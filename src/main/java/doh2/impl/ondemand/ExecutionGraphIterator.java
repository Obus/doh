package doh2.impl.ondemand;

import doh2.impl.op.kvop.KVUnoOp;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

class ExecutionGraphIterator implements Iterator<ExecutionUnit> {
    private final Queue<OnDemandDS.ExecutionNode> executionNodeQueue;
    private final List<OnDemandDS.ExecutionNode> targetNodes;

    ExecutionGraphIterator(OnDemandDS.ExecutionNode rootNode, List<OnDemandDS.ExecutionNode> targetNodes) {
        this.targetNodes = new ArrayList<OnDemandDS.ExecutionNode>();
        for (OnDemandDS.ExecutionNode targetNode : targetNodes) {
            if (!targetNode.dataSet().isReady()) {
                this.targetNodes.add(targetNode);
            }
        }
        this.executionNodeQueue = new LinkedList<OnDemandDS.ExecutionNode>();
        for (OnDemandDS.ExecutionNode executionNode : rootNode.outcomeNodes()) {
            if (!executionNode.dataSet().isReady() && isBranchContainsTargets(executionNode)) {
                executionNodeQueue.add(executionNode);
            }
        }
    }

    @Override
    public boolean hasNext() {
        return !executionNodeQueue.isEmpty();
    }

    @Override
    public ExecutionUnit next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        OnDemandDS.ExecutionNode currentNode = executionNodeQueue.poll();

        final List<OnDemandDS.ExecutionNode> nodeSequence = new ArrayList<OnDemandDS.ExecutionNode>();
        nodeSequence.add(currentNode);

        while (currentNode.outcomeNodes().size() == 1 && !targetNodes.contains(currentNode)) {
            currentNode = currentNode.outcomeNodes().get(0);
            nodeSequence.add(currentNode);
        }

        for (OnDemandDS.ExecutionNode executionNode : currentNode.outcomeNodes()) {
            if (!executionNode.dataSet().isReady() && isBranchContainsTargets(executionNode)) {
                executionNodeQueue.add(executionNode);
            }
        }
        return new ExecutionUnit(nodeSequence);
    }

    private boolean isBranchContainsTargets(OnDemandDS.ExecutionNode executionNode) {
        for (OnDemandDS.ExecutionNode target : targetNodes) {
            if (target.equals(executionNode)) {
                return true;
            }
        }
        for (OnDemandDS.ExecutionNode child : executionNode.outcomeNodes()) {
            if (isBranchContainsTargets(child)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
