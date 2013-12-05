package doh.crazy;

/**
 * Created by Alexander A. Senov
 * Synqera, 2012.
 */
public interface MapReduceOp<FromKey, FromValue, ToKey, ToValue> {

    Class<FromKey> fromKeyClass();

    Class<FromValue> fromValueClass();

    Class<ToKey> toKeyClass();

    Class<ToValue> toValueClass();


}
