package doh.op.kvop;

public abstract class KVOneOp<FromKey, FromValue, ToKey, ToValue> extends KVOp<FromKey, FromValue, ToKey, ToValue> {
    @Override
    public final Some<KV<ToKey, ToValue>> apply(Some<KV<FromKey, Some<FromValue>>> f) {
        if (!f.isOne()) {
            throw new IllegalArgumentException();
        }
        KV<FromKey, Some<FromValue>> firstKV = ((One<KV<FromKey, Some<FromValue>>>) f).get();
        if (!firstKV.value.isOne()) {
            throw new IllegalArgumentException();
        }
        return applyOne(keyValue(firstKV.key, ((One<FromValue>) firstKV.value).get()));
    }

    public abstract Some<KV<ToKey, ToValue>> applyOne(KV<FromKey, FromValue> f);
}
