package doh.api.op.impl;

import com.synqera.bigkore.model.fact.Consumer;
import doh.ds.MapKVDS;

public class OpFactory {
    public static RawUSToConsumerPaymentsOp rawUserStoryToConsumerPayments() {
        return new RawUSToConsumerPaymentsOp();
    }

    public static ValueAvgOp.ConsumerValuesAvgOp valuesAvg() {
        return new ValueAvgOp.ConsumerValuesAvgOp();
    }

    public static ValueStdOp.ConsumerValuesStdOp valuesStd(MapKVDS<Consumer, Double> keysAvg) {
        ValueStdOp.ConsumerValuesStdOp op = new ValueStdOp.ConsumerValuesStdOp();
        op.keysAvg = keysAvg;
        return op;
    }
}
