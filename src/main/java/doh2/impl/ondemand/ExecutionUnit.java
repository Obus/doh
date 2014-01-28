package doh2.impl.ondemand;

import java.util.List;


/**
 * Unit of execution (in graph terminology).
 */
class ExecutionUnit<T extends ExecutionNode<T>> {
    final List<T> executionNodesList;

    ExecutionUnit(List<T> executionNodesList) {
        this.executionNodesList = executionNodesList;
    }
}
