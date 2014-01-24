package doh2.impl;

import doh.op.kvop.KVUnoOp;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * ExecutionGraph constructs a graph of ExecutionNodes from "input" or "root" ExecutionNodes and slice it to ExecutionUnits
 */

public class ExecutionGraph {
    private final List<OnDemandDS.ExecutionNode> rootNodes;
    private final OpExecutor opExecutor;


    public ExecutionGraph(List<OnDemandDS.ExecutionNode> rootNodes, OpExecutor opExecutor) {
        this.rootNodes = rootNodes;
        this.opExecutor = opExecutor;
    }

    public Iterable<ExecutionUnit> executionUnits() {
        if (rootNodes.size() == 0) {
            throw new IllegalArgumentException("Zero amount of nodes");
        }
        if (rootNodes.size() > 1) {
            throw new UnsupportedOperationException("Only one income node supported.");
        }

        return iterableExecutions(rootNodes.get(0));
    }

    private Iterable<ExecutionUnit> iterableExecutions(final OnDemandDS.ExecutionNode rootNode) {
        if (rootNode.incomeNodes().size() > 0) {
            throw new IllegalArgumentException("Root node should have zero income nodes");
        }
        return new Iterable<ExecutionUnit>() {
            @Override
            public Iterator<ExecutionUnit> iterator() {
                return new ExecutionGraphIterator(rootNode);
            }
        };
    }

    static class ExecutionGraphIterator implements Iterator<ExecutionUnit> {
        private OnDemandDS.ExecutionNode currentNode ;

        private ExecutionGraphIterator(OnDemandDS.ExecutionNode currentNode) {
            this.currentNode = currentNode;
        }
        @Override
        public boolean hasNext() {
            return currentNode.outcomeNodes().size() > 0;
        }

        @Override
        public ExecutionUnit next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final List<KVUnoOp> kvOpList = new ArrayList<KVUnoOp>();
            final OnDemandDS input = currentNode.dataSet();
            while (currentNode.outcomeNodes().size() == 1) {
                currentNode = currentNode.outcomeNodes().get(0);
                kvOpList.add(currentNode.dataSetParentOp());
            }
            final OnDemandDS output = currentNode.dataSet();
            return new ExecutionUnit(input, output, kvOpList);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
