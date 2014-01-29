package doh2.impl.op;

import com.synqera.bigkore.model.UserStory;
import com.synqera.bigkore.model.fact.Consumer;
import com.synqera.bigkore.model.fact.Fact;
import com.synqera.bigkore.model.fact.Payment;
import doh2.api.MapDS;
import doh2.api.OpParameter;
import doh2.api.op.FilterOp;
import doh2.api.op.FlatMapOp;
import doh2.api.op.KV;
import doh2.api.op.MapOp;
import doh2.api.op.ReduceOp;
import doh2.api.op.ValueOnlyReduceOp;
import org.apache.hadoop.io.BytesWritable;
import org.apache.mahout.common.IntPairWritable;

public class TestOps {
    public static class IdentityMapOp<FK, FV> extends MapOp<FK, FV, FK, FV> {
        @Override
        public KV<FK, FV> map(FK fk, FV fv) {
            return keyValue(fk, fv);
        }
    }

    public static class ValueAvgOp<Key> extends ValueOnlyReduceOp<Key, Long, Double> {
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

    public static class ValueStdOp<Key> extends ValueOnlyReduceOp<Key, Long, Double> {
        @OpParameter
        public MapDS<Key, Double> keysAvg;

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

        public static class ConsumerValuesStdOp extends ValueStdOp<Consumer> {

            public ConsumerValuesStdOp() {
            }
        }
    }

    public static class RawUSToConsumerPaymentsOp extends FlatMapOp<BytesWritable, String, Consumer, Long> {
        private static final UserStory us = new UserStory();

        @Override
        public void flatMap(BytesWritable bytesWritable, String s) {
            us.fromString(s);
            Consumer c = us.consumer();
            for (Payment p : Fact.findFacts(us.getFacts(), Payment.class)) {
                emitKeyValue(c, p.getValue());
            }
        }

    }

    public static class MapOpKeyValueSwap<FK, FV> extends MapOp<FK, FV, FV, FK> {
        @Override
        public KV<FV, FK> map(FK fk, FV fv) {
            return keyValue(fv, fk);
        }
    }

    public static class MapOpLongKeyToStringStringValueToInteger extends MapOp<Long, String, String, Integer> {
        @Override
        public KV<String, Integer> map(Long aLong, String s) {
            return keyValue(aLong.toString(), Integer.parseInt(s));
        }
    }

    public static class FlatMapOpValueLineToWords extends FlatMapOp<Integer, String, IntPairWritable, String> {
        @Override
        public void flatMap(Integer lineNum, String line) {
            String[] words = line.split(" ");
            int i = 0;
            for (String word : words) {
                if (word.trim().isEmpty()) {
                    continue;
                }
                emitKeyValue(new IntPairWritable(lineNum, i), word);
                ++i;
            }
        }
    }

    public static class FilterOpFilterPositiveIntValuesAndNonEmptyStringKeys extends FilterOp<String, Integer> {
        @Override
        public boolean accept(String s, Integer integer) {
            return (!s.trim().isEmpty() && integer > 0);
        }
    }

    public static class ReduceOpIntegerValuesSum<K> extends ReduceOp<K, Integer, K, Integer> {
        @Override
        public KV<K, Integer> reduce(K k, Iterable<Integer> integers) {
            Integer sum = 0;
            for (Integer i : integers) {
                sum += i;
            }
            return keyValue(k, sum);
        }
    }
}
