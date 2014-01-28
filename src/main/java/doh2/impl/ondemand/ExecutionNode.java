package doh2.impl.ondemand;

import java.util.List;

public interface ExecutionNode<T extends ExecutionNode> {
    T incomeNode();
    List<T> outcomeNodes();
    boolean isDone();
    boolean isBreakHere();
}
