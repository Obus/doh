package doh.op;


import doh.op.kvop.FlatSequenceOp;
import doh.op.kvop.KV;
import doh.op.kvop.ReduceOp;

public class ReduceThanFlatSequenceOp<FromKey, FromValue, IntermKey, IntermValue, ToKey, ToValue>
        extends ReduceOp<FromKey, FromValue, ToKey, ToValue> {
    @OpParameter
    private FlatSequenceOp<IntermKey, IntermValue, ToKey, ToValue> flatSequenceOp;
    @OpParameter
    private ReduceOp<FromKey, FromValue, IntermKey, IntermValue> reduceOp;

    @Override
    public KV<ToKey, ToValue> reduce(FromKey fromKey, Iterable<FromValue> fromValues) {
        KV<IntermKey, IntermValue> ntermKV = reduceOp.reduce(fromKey, fromValues);

    }


}
