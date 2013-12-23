package doh.op.kvop;

import com.google.common.collect.Lists;

import java.util.List;

public class FlatSequenceOp<FromKey, FromValue, ToKey, ToValue>
        extends KVOneOp<FromKey, FromValue, ToKey, ToValue> {
    private final List<KVOneOp> sequence;

    public FlatSequenceOp(List<KVOneOp> sequence) {
        this.sequence = sequence;
    }

    @Override
    public Some<KV<ToKey, ToValue>> applyOne(KV<FromKey, FromValue> f) {
        List<KV> currentKVList = Lists.newArrayList((KV) f);
        for (KVOneOp op : sequence) {
            List<KV> nextKVList = Lists.newArrayList();
            for (KV kv : currentKVList) {
                expand(nextKVList, op.apply(one(kv)));
            }
            currentKVList = nextKVList;
        }
        return many((List<KV<ToKey, ToValue>>) (Object) currentKVList);
    }

    public static <T> void expand(List<T> list, Iterable<T> it) {
        for (T t : it) {
            list.add(t);
        }
    }
}
