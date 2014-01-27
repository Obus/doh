package doh2.impl.ondemand;

import java.util.*;

/**
 * ExecutionGraph constructs a graph of ExecutionNodes from "input" or "root" ExecutionNodes and slice it to ExecutionUnits
 */

class ExecutionGraph {
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


}
