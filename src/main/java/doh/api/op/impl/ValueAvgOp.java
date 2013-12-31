package doh.api.op.impl;

import com.synqera.bigkore.model.fact.Consumer;
import doh.api.op.ValueOnlyReduceOp;

public class ValueAvgOp<Key> extends ValueOnlyReduceOp<Key, Long, Double> {
    @Override
    public Double reduceValue(Key key, Iterable<Long> values) {
        long count = 0;
        long sum = 0;
        for (Long v : values) {
            sum += v;
            count++;
        }
        return (double) sum / count;
    }

    public static class ConsumerValuesAvgOp extends ValueAvgOp<Consumer> {
    }
}
