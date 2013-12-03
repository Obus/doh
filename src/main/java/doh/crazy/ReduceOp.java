package doh.crazy;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.Vector;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Created by Alexander A. Senov
 * Synqera, 2012.
 */
public abstract class ReduceOp <FromKey, FromValue, ToKey, ToValue>  implements Op<Pair<FromKey, Iterable<FromValue>>, Pair<ToKey, ToValue>> {

    @Override
    public Pair<ToKey, ToValue> apply(Pair<FromKey, Iterable<FromValue>> f) {
        return reduce(f.getFirst(), f.getSecond());
    }

    public abstract Pair<ToKey, ToValue> reduce(FromKey key, Iterable<FromValue> values);

    public Pair<ToKey, ToValue> pair(ToKey key, ToValue value) {
        return new Pair<ToKey, ToValue>(key, value);
    }

    public static class ClusterDiameterReduceOp extends ReduceOp<String, Double, WritableComparable, String> {
        private int intValue;
        private Integer integerValue;

        @Override
        public Pair<WritableComparable, String> reduce(String s, Iterable<Double> doubles) {
            return pair(new BytesWritable(), s + "," + 2 * max(doubles));
        }
    }

    public static void main(String[] args) {
        Class c = ClusterDiameterReduceOp.class;
        Method[] methods = c.getDeclaredMethods();
        Method m = methods[0];
        Field[] fields = c.getDeclaredFields();
        Field f = fields[0];
    }

    public static <T extends Comparable> T max(Iterable<T> iterable) {
        T result = null;
        for (T i : iterable) {
            if (result == null) {
                result = i;
            }
            else {
                result = result.compareTo(i) > 0 ? result : i;
            }
        }
        return result;
    }
}