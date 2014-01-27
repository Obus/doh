package doh2.impl.op.kvop;

public interface OpKVTransformer<FromKey, FromValue, ToKey, ToValue> {
    public Class<FromKey> fromKeyClass();
    public Class<FromValue> fromValueClass();
    public Class<ToKey> toKeyClass();
    public Class<ToValue> toValueClass();
}
