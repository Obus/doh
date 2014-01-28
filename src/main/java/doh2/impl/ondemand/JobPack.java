package doh2.impl.ondemand;

import doh2.impl.op.kvop.KVUnoOp;

public class JobPack {
    public final OnDemandDS input;
    public final OnDemandDS output;
    public final KVUnoOp mapTaskOp;
    public final KVUnoOp reduceTaskOp;

    public JobPack(OnDemandDS input, OnDemandDS output, KVUnoOp mapTaskOp, KVUnoOp reduceTaskOp) {
        this.input = input;
        this.output = output;
        this.mapTaskOp = mapTaskOp;
        this.reduceTaskOp = reduceTaskOp;
    }
}
