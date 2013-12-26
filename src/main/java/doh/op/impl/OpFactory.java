package doh.op.impl;

import com.synqera.bigkore.model.fact.Consumer;
import doh.ds.MapKVDataSet;
import doh.op.impl.RawUSToConsumerPaymentsOp;
import doh.op.impl.ValueAvgOp;
import doh.op.impl.ValueStdOp;

public class OpFactory {
    public static RawUSToConsumerPaymentsOp rawUserStoryToConsumerPayments() {
        return new RawUSToConsumerPaymentsOp();
    }

    public static ValueAvgOp.ConsumerValuesAvgOp valuesAvg() {
        return new ValueAvgOp.ConsumerValuesAvgOp();
    }

    public static ValueStdOp.ConsumerValuesStdOp valuesStd(MapKVDataSet<Consumer, Double> keysAvg) {
        ValueStdOp.ConsumerValuesStdOp op = new ValueStdOp.ConsumerValuesStdOp();
        op.keysAvg = keysAvg;
        return op;
    }
}
