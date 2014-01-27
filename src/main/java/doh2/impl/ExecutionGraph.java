package doh2.impl;

import doh.op.kvop.KVUnoOp;

import java.util.*;

/**
 * ExecutionGraph constructs a graph of ExecutionNodes from "input" or "root" ExecutionNodes and slice it to ExecutionUnits
 */

public class ExecutionGraph {
    private final List<OnDemandDS.ExecutionNode> targetNodes;
    private final List<OnDemandDS.ExecutionNode> rootNodes;
    private final OpExecutor opExecutor;


    public ExecutionGraph(List<OnDemandDS.ExecutionNode> targetNodes, OpExecutor opExecutor) {
        Set<OnDemandDS.ExecutionNode> origins = new HashSet<OnDemandDS.ExecutionNode>();
        for (OnDemandDS.ExecutionNode targetNode : targetNodes) {
            origins.add(findOrigin(targetNode));
        }
        this.rootNodes = new ArrayList<OnDemandDS.ExecutionNode>(origins.size());
        for (OnDemandDS.ExecutionNode o : origins) {
            rootNodes.add(o);
        }
        this.targetNodes = targetNodes;
        this.opExecutor = opExecutor;
    }

    public static OnDemandDS.ExecutionNode findOrigin(OnDemandDS.ExecutionNode executionNode) {
        OnDemandDS.ExecutionNode ancestorNode = executionNode;
        while (ancestorNode.incomeNodes().size() > 0) {
            ancestorNode = ancestorNode.incomeNodes().get(0);
        }
        return ancestorNode;
    }

    public Iterable<ExecutionUnit> executionUnits() {
        if (rootNodes.size() == 0) {
            throw new IllegalArgumentException("Zero amount of nodes");
        }
        if (rootNodes.size() > 1) {
            throw new UnsupportedOperationException("Only one income node supported.");
        }

        return iterableExecutions(rootNodes.get(0), targetNodes);
    }

    private Iterable<ExecutionUnit> iterableExecutions(final OnDemandDS.ExecutionNode rootNode,
                                                       final List<OnDemandDS.ExecutionNode> targetNodes) {
        if (rootNode.incomeNodes().size() > 0) {
            throw new IllegalArgumentException("Root node should have zero income nodes");
        }
        return new Iterable<ExecutionUnit>() {
            @Override
            public Iterator<ExecutionUnit> iterator() {
                return new ExecutionGraphIterator(rootNode, targetNodes);
            }
        };
    }


    static class ExecutionGraphIterator implements Iterator<ExecutionUnit> {
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

            final List<KVUnoOp> kvOpList = new ArrayList<KVUnoOp>();
            kvOpList.add(currentNode.dataSetParentOp());
            final OnDemandDS input = currentNode.incomeNodes().get(0).dataSet();

            while (currentNode.outcomeNodes().size() == 1 && !targetNodes.contains(currentNode)) {
                currentNode = currentNode.outcomeNodes().get(0);
                kvOpList.add(currentNode.dataSetParentOp());
            }

            final OnDemandDS output = currentNode.dataSet();

            for (OnDemandDS.ExecutionNode executionNode : currentNode.outcomeNodes()) {
                if (!executionNode.dataSet().isReady() && isBranchContainsTargets(executionNode)) {
                    executionNodeQueue.add(executionNode);
                }
            }
            return new ExecutionUnit(input, output, kvOpList);
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
}
