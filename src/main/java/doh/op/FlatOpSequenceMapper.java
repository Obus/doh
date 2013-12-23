package doh.op;


import org.apache.hadoop.mapreduce.Mapper;

public class FlatOpSequenceMapper<FromKey, FromValue, ToKey, ToValue>
        extends Mapper<FromKey, FromValue, ToKey, ToValue> {
}
