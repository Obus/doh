package doh.op.kvop;

import doh.api.op.KV;

public abstract class KVUnoOp<FromKey, FromValue, ToKey, ToValue> extends KVOp<FromKey, FromValue, ToKey, ToValue> {

    @Override
    public Some<KV<ToKey, ToValue>> apply(Some<KV<FromKey, FromValue>> f) {
        if (!f.isOne()) {
            throw new IllegalArgumentException();
        }
        KV<FromKey, FromValue> firstKV = ((One<KV<FromKey, FromValue>>) f).get();
        return applyUno(keyValue(firstKV.key, firstKV.value));
    }

    public abstract Some<KV<ToKey, ToValue>> applyUno(KV<FromKey, FromValue> f);
}
