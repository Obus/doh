package doh.crazy;

public abstract class MapReduceOp<FromKey, FromValue, ToKey, ToValue> {

    private final KV<ToKey, ToValue> KV = new KV<ToKey, ToValue>();

    public Class<FromKey> fromKeyClass() {
        return ReflectionUtils.getFromKeyClass(getClass());
    }

    public Class<FromValue> fromValueClass() {
        return ReflectionUtils.getFromValueClass(getClass());
    }

    public Class<ToKey> toKeyClass() {
        return ReflectionUtils.getToKeyClass(getClass());
    }

    public Class<ToValue> toValueClass() {
        return ReflectionUtils.getToValueClass(getClass());
    }


    protected abstract KV<ToKey, ToValue> keyValue(ToKey key, ToValue value);


}
