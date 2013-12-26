package doh.op;

import doh.op.kvop.CompositeMapOp;
import doh.op.kvop.KVUnoOp;
import doh.op.kvop.KVOp;
import doh.op.kvop.ReduceOp;

import java.util.ArrayList;
import java.util.List;

public class OpCompositor {

    public List<KVOp> compose(List<KVOp> opList) throws Exception {
        List<KVOp> composedOpList = new ArrayList<KVOp>();
        List<KVUnoOp> currentCompositionOpList = new ArrayList<KVUnoOp>();
        for (KVOp op : opList) {
            if (op instanceof ReduceOp) {
                if (currentCompositionOpList.size() > 0) {
                    composedOpList.add(new CompositeMapOp(currentCompositionOpList));
                    currentCompositionOpList = new ArrayList<KVUnoOp>();
                }
            } else if (op instanceof KVUnoOp) {
                currentCompositionOpList.add((KVUnoOp) op);
            } else {
                throw new IllegalArgumentException();
            }
        }
        if (currentCompositionOpList.size() > 0) {
            composedOpList.add(new CompositeMapOp(currentCompositionOpList));
        }
        return composedOpList;
    }

}

