package doh.op;

import com.synqera.bigkore.model.fact.Consumer;
import doh.ds.LazyKVDataSet;
import doh.ds.MapKVDataSet;
import doh.api.ds.RawUserStories;
import doh.api.op.KV;
import doh.api.op.MapOp;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import java.util.Iterator;

import static doh.api.op.impl.OpFactory.rawUserStoryToConsumerPayments;
import static doh.api.op.impl.OpFactory.valuesAvg;
import static doh.api.op.impl.OpFactory.valuesStd;
import static org.junit.Assert.assertEquals;

public class LazyExample {


    public static class IdentityMapOp<FK, FV> extends MapOp<FK, FV, FK, FV> {
        @Override
        public KV<FK, FV> map(FK fk, FV fv) {
            return keyValue(fk, fv);
        }
    }

    @Test
    public void testLazy() throws Exception {
        RawUserStories rawUS = Example.make();
        LazyKVDataSet<BytesWritable, String> lazyRawUS = new LazyKVDataSet<BytesWritable, String>(rawUS.getContext(), rawUS);
        LazyKVDataSet<BytesWritable, String> lazyRawUS1 = lazyRawUS.map(new IdentityMapOp<BytesWritable, String>());
        LazyKVDataSet<BytesWritable, String> lazyRawUS2 = lazyRawUS1.map(new IdentityMapOp<BytesWritable, String>());

        LazyKVDataSet<Consumer, Long> lazyConsumerPayments
                = lazyRawUS2.flatMap(rawUserStoryToConsumerPayments());

        MapKVDataSet<Consumer, Double> consumerPaymentsAvg
                = lazyConsumerPayments.reduce(valuesAvg()).toMapKVDS();

        LazyKVDataSet<Consumer, Double> lazyConsumerPaymentsStd
                = lazyConsumerPayments.reduce(valuesStd(consumerPaymentsAvg));

        LazyKVDataSet<Consumer, Double> lazyConsumerPaymentsStd1
                = lazyConsumerPaymentsStd.map(new IdentityMapOp<Consumer, Double>());


        Iterator<KV<Consumer, Double>> cpIt = lazyConsumerPaymentsStd1.iterator();

        KV<Consumer, Double> kv;

        kv= cpIt.next();
        assertEquals(new Consumer("Elton"), kv.key);
        assertEquals(74100.0, kv.value, 0.1);

        kv= cpIt.next();
        assertEquals(new Consumer("Emma"), kv.key);
        assertEquals(55.57777333511022, kv.value, 0.1);

        kv= cpIt.next();
        assertEquals(new Consumer("Johny"), kv.key);
        assertEquals(574.1785088280474, kv.value, 0.1);

    }
}
