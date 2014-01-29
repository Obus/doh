package doh2.impl.op;

import com.synqera.bigkore.model.fact.Consumer;
import doh2.api.MapDS;

public class TestOpFactory {
    public static TestOps.RawUSToConsumerPaymentsOp rawUserStoryToConsumerPayments() {
        return new TestOps.RawUSToConsumerPaymentsOp();
    }

    public static TestOps.ValueAvgOp.ConsumerValuesAvgOp valuesAvg() {
        return new TestOps.ValueAvgOp.ConsumerValuesAvgOp();
    }

    public static TestOps.ValueStdOp.ConsumerValuesStdOp valuesStd(MapDS<Consumer, Double> keysAvg) {
        TestOps.ValueStdOp.ConsumerValuesStdOp op = new TestOps.ValueStdOp.ConsumerValuesStdOp();
        op.keysAvg = keysAvg;
        return op;
    }
}
