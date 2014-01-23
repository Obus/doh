package doh2.impl;

import doh.op.kvop.KVOp;
import doh.op.kvop.KVUnoOp;

import java.util.List;


/**
 * Unit of execution (in graph terminology).
 * Consist of
 *      - 'input' data set (start node),
 *      - 'output' data set (finish node) and
 *      - sequence of operations (edges) from one to another
 *
 */
public class ExecutionUnit {
    final OnDemandDS input;
    final OnDemandDS output;
    final List<KVUnoOp> kvOpList;

    public ExecutionUnit(OnDemandDS input, OnDemandDS output, List<KVUnoOp> kvOpList) {
        this.input = input;
        this.output = output;
        this.kvOpList = kvOpList;
    }
}
