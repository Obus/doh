package doh.crazy.op;

import com.synqera.bigkore.model.fact.Consumer;
import doh.crazy.OpParameter;
import doh.crazy.ValueOnlyReduceOp;
import doh.ds.MapKVDataSet;

public class ValueStdOp<Key> extends ValueOnlyReduceOp<Key, Long, Double> {
    @OpParameter
    public MapKVDataSet<Key, Double> keysAvg;

    @Override
    public Double reduceValue(Key key, Iterable<Long> values) {
        double avg = keysAvg.get(key);
        double std = 0;
        double count = 0;
        for (Long v : values) {
            std += Math.pow(v - avg, 2);
            count++;
        }
        return Math.sqrt(std / count);
    }

    public static class ConsumerValuesStdOp extends ValueStdOp<Consumer> {}
}
