package doh.op;

import doh.op.kvop.FlatSequenceOp;
import doh.op.kvop.KVOneOp;
import doh.op.kvop.KVOp;
import doh.op.kvop.ReduceOp;

import java.util.ArrayList;
import java.util.List;

public class OpCompositor {

    public List<KVOp> compose(List<KVOp> opList) throws Exception {
        List<KVOp> composedOpList = new ArrayList<KVOp>();
        List<KVOneOp> currentCompositionOpList = new ArrayList<KVOneOp>();
        for (KVOp op : opList) {
            if (op instanceof ReduceOp) {
                if (currentCompositionOpList.size() > 0) {
                    composedOpList.add(new FlatSequenceOp(currentCompositionOpList));
                    currentCompositionOpList = new ArrayList<KVOneOp>();
                }
            } else if (op instanceof KVOneOp) {
                currentCompositionOpList.add((KVOneOp) op);
            } else {
                throw new IllegalArgumentException();
            }
        }
        if (currentCompositionOpList.size() > 0) {
            composedOpList.add(new FlatSequenceOp(currentCompositionOpList));
        }
        return composedOpList;
    }

}

