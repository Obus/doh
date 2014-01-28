package doh2.impl.ondemand;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

class ExecutionGraphIterator<Node extends ExecutionNode<Node>> implements Iterator<ExecutionUnit<Node>> {
    private final Queue<Node> executionNodeQueue;
    private final List<Node> targetNodes;

    ExecutionGraphIterator(List<Node> targetNodes) {
        final Set<Node> rootNodes = new HashSet<Node>();
        for (Node targetNode : targetNodes) {
            rootNodes.add(findOrigin(targetNode));
        }

        this.targetNodes = new ArrayList<Node>();
        for (Node targetNode : targetNodes) {
            if (!targetNode.isDone()) {
                this.targetNodes.add(targetNode);
            }
        }
        this.executionNodeQueue = new LinkedList<Node>();
        for (Node rootNode : rootNodes) {
            updateNodeQueue(rootNode.outcomeNodes());
        }
    }

    @Override
    public boolean hasNext() {
        return !executionNodeQueue.isEmpty();
    }

    @Override
    public ExecutionUnit<Node> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Node currentNode = executionNodeQueue.poll();

        final List<Node> nodeSequence = new ArrayList<Node>();
        nodeSequence.add(currentNode);

        while (!targetNodes.contains(currentNode) && !currentNode.isBreakHere()) {
            List<Node> currentOutcomeNodes = targetFullNodes(currentNode.outcomeNodes());
            if (currentOutcomeNodes.size() != 1) {
                break;
            }
            currentNode = currentOutcomeNodes.get(0);
            nodeSequence.add(currentNode);
        }

        updateNodeQueue(currentNode.outcomeNodes());

        return new ExecutionUnit<Node>(nodeSequence);
    }

    private void updateNodeQueue(Iterable<Node> nodes) {
        for (Node node : targetFullNodes(nodes)) {
            executionNodeQueue.add(node);
        }
    }

    private List<Node> targetFullNodes(Iterable<Node> nodes) {
        List<Node> targetFullNodes = new ArrayList<Node>();
        for (Node node : nodes) {
            if (isBranchForTarget(node)) {
                targetFullNodes.add(node);
            }
        }
        return targetFullNodes;
    }

    private boolean isBranchForTarget(Node executionNode) {
        for (Node target : targetNodes) {
            if (target.equals(executionNode)) {
                return true;
            }
        }
        if (executionNode.isDone()) {
            return false;
        }
        for (Node child : executionNode.outcomeNodes()) {
            if (isBranchForTarget(child)) {
                return true;
            }
        }
        return false;
    }




    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }


    public static <T extends ExecutionNode<T>> T findOrigin(T executionNode) {
        T ancestorNode = executionNode;
        while (!ancestorNode.isDone()) {
            ancestorNode = ancestorNode.incomeNode();
            if (ancestorNode == null) {
                throw new IllegalArgumentException("No done ancestor node for node " + executionNode);
            }
        }
        return ancestorNode;
    }
}
