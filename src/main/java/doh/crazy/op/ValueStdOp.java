package doh.crazy.op;

import com.synqera.bigkore.model.fact.Consumer;
import doh.crazy.OpParameter;
import doh.crazy.ReduceOp;
import doh.ds.MapKeyValueDataSet;

public class ValueStdOp<Key> extends ReduceOp<Key, Long, Key, Double> {
    @OpParameter
    public MapKeyValueDataSet<Key, Double> keysAvg;

    @Override
    public doh.crazy.KV<Key, Double> reduce(Key key, Iterable<Long> values) {
        double avg = keysAvg.get(key);
        double std = 0;
        double count = 0;
        for (Long v : values) {
            std += Math.pow(v - avg, 2);
            count++;
        }
        return keyValue(key, Math.sqrt(std / count));
    }

    public static class ConsumerValuesStdOp extends ValueStdOp<Consumer> {}
}
