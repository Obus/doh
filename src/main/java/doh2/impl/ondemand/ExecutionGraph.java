package doh2.impl.ondemand;

import java.util.*;

/**
 * ExecutionGraph constructs a graph of ExecutionNodes from "input" or "root" ExecutionNodes and slice it to ExecutionUnits
 */

class ExecutionGraph<Node extends ExecutionNode<Node>> {
    private final List<Node> targetNodes;


    public ExecutionGraph(List<Node> targetNodes) {
        this.targetNodes = targetNodes;
    }

    public Iterable<ExecutionUnit<Node>> executionUnits() {
        return iterableExecutions(targetNodes);
    }

    private Iterable<ExecutionUnit<Node>> iterableExecutions(final List<Node> targetNodes) {
        return new Iterable<ExecutionUnit<Node>>() {
            @Override
            public Iterator<ExecutionUnit<Node>> iterator() {
                return new ExecutionGraphIterator<Node>(targetNodes);
            }
        };
    }


}
