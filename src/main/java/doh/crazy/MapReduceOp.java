package doh.crazy;

public interface MapReduceOp<FromKey, FromValue, ToKey, ToValue> {

    Class<FromKey> fromKeyClass();

    Class<FromValue> fromValueClass();

    Class<ToKey> toKeyClass();

    Class<ToValue> toValueClass();


}
