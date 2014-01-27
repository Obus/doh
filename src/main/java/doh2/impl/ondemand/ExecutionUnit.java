package doh2.impl.ondemand;

import doh2.impl.op.kvop.KVUnoOp;

import java.util.List;


/**
 * Unit of execution (in graph terminology).
 */
class ExecutionUnit {
    final List<OnDemandDS.ExecutionNode> kvOpList;

    ExecutionUnit(List<OnDemandDS.ExecutionNode> kvOpList) {
        this.kvOpList = kvOpList;
    }
}
