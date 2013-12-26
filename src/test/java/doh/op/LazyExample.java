package doh.op;

import com.synqera.bigkore.model.fact.Consumer;
import doh.ds.LazyKVDataSet;
import doh.ds.MapKVDataSet;
import doh.ds.RawUserStories;
import doh.ds.RealKVDataSet;
import doh.op.kvop.KV;
import doh.op.kvop.MapOp;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import static doh.op.impl.OpFactory.rawUserStoryToConsumerPayments;
import static doh.op.impl.OpFactory.valuesAvg;
import static doh.op.impl.OpFactory.valuesStd;

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

        for (KV<Consumer, Double> kv : lazyConsumerPaymentsStd1) {
            System.out.println(kv);
        }

    }
}
