package doh.crazy.op;

import com.synqera.bigkore.model.fact.Consumer;
import doh.crazy.KV;
import doh.crazy.ReduceOp;

public class ValueAvgOp<Key> extends ReduceOp<Key, Long, Key, Double> {
    @Override
    public KV<Key, Double> reduce(Key o, Iterable<Long> values) {
        long count = 0;
        long sum = 0;
        for (Long v : values) {
            sum += v;
            count++;
        }
        return keyValue(o, (double) sum / count);
    }

    public static class ConsumerValuesAvgOp extends ValueAvgOp<Consumer> {}
}
