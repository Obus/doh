package doh.crazy;

import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

public abstract class MapOp<FromKey, FromValue, ToKey, ToValue> implements Op<Pair<FromKey, FromValue>, Pair<ToKey, ToValue>> {

    @Override
    public Pair<ToKey, ToValue> apply(Pair<FromKey, FromValue> f) {
        return map(f.getFirst(), f.getSecond());
    }

    public abstract Pair<ToKey, ToValue> map(FromKey key, FromValue value);

    public Pair<ToKey, ToValue> pair(ToKey key, ToValue value) {
        return new Pair<ToKey, ToValue>(key, value);
    }

    public static class ClusterDiameterMapOp extends MapOp<WritableComparable, Vector, String, Double> {

        @OpParameter
        private DistanceMeasure dm;
        @OpParameter
        private Vector center;
        @OpParameter
        private String id;


        @Override
        public Pair<String, Double> map(WritableComparable writableComparable, Vector point) {
            return pair(id, dm.distance(center, point));
        }
    }

}
