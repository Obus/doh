package doh.crazy.op;

import doh.crazy.FlatMapOp;
import doh.crazy.KV;
import doh.crazy.MapOp;
import doh.crazy.KVOp;
import doh.crazy.ReduceOp;

import java.util.List;

public class OpCompositor {

    public boolean isCompatible(KVOp op1, KVOp op2) throws Exception {
        return  !(op1 instanceof ReduceOp && op2 instanceof ReduceOp);
    }

    public KVOp compose(KVOp op1, KVOp op2) {

    }

    public KVOp composeMapMap(MapOp op1, MapOp op2) {

    }

    public static class CompositeMapOp extends MapOp {
        List<MapOp> mapOpComposites;

        @Override
        public KV map(Object key, Object value) {
            for (MapOp op : mapOpComposites) {
                KV kv = op.map(key, value);
                key = kv.key;
                value = kv.value;
            }
            return keyValue(key, value);
        }
    }

    public static class CompositeFlatMapOp extends FlatMapOp {
        List<KVOp> mapOrFlatMapComposites;

        @Override
        public void flatMap(Object key, Object value) {
            for (KVOp op : mapOrFlatMapComposites) {
            }
            return keyValue(key, value);
        }
    }
}

