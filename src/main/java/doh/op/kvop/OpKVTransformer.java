package doh.op.kvop;

import doh.op.ReflectionUtils;

public interface OpKVTransformer<FromKey, FromValue, ToKey, ToValue> {
    public Class<FromKey> fromKeyClass();
    public Class<FromValue> fromValueClass();
    public Class<ToKey> toKeyClass();
    public Class<ToValue> toValueClass();
}
