package doh.op;

import com.synqera.bigkore.model.UserStory;
import com.synqera.bigkore.model.fact.Consumer;
import com.synqera.bigkore.model.fact.Payment;
import com.synqera.bigkore.model.fact.Product;
import com.synqera.bigkore.model.fact.Time;
import doh.api.ds.RawUserStories;
import doh.api.op.KV;
import doh.api.op.MapOp;
import doh.ds.LazyKVDS;
import doh.ds.MapKVDS;
import doh2.api.DS;
import doh2.api.DSContext;
import doh2.api.DSFactory;
import doh2.api.MapDS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.HadoopUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static doh.api.op.impl.OpFactory.rawUserStoryToConsumerPayments;
import static doh.api.op.impl.OpFactory.valuesAvg;
import static doh.api.op.impl.OpFactory.valuesStd;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LazyExample {


    public static class IdentityMapOp<FK, FV> extends MapOp<FK, FV, FK, FV> {
        @Override
        public KV<FK, FV> map(FK fk, FV fv) {
            return keyValue(fk, fv);
        }
    }

    @Test
    public void testOnDemandDS() throws Exception {
        DS<BytesWritable, String> rawUS = make();
        DS<BytesWritable, String> lazyRawUS1 = rawUS.map(new IdentityMapOp<BytesWritable, String>());
        DS<BytesWritable, String> lazyRawUS2 = lazyRawUS1.map(new IdentityMapOp<BytesWritable, String>());

        DS<Consumer, Long> lazyConsumerPayments
                = lazyRawUS2.flatMap(rawUserStoryToConsumerPayments());

        MapDS<Consumer, Double> consumerPaymentsAvg
                = lazyConsumerPayments.reduce(valuesAvg()).toMapDS();

        DS<Consumer, Double> lazyConsumerPaymentsStd
                = lazyConsumerPayments.reduce(valuesStd(consumerPaymentsAvg));

        DS<Consumer, Double> lazyConsumerPaymentsStd1
                = lazyConsumerPaymentsStd.map(new IdentityMapOp<Consumer, Double>());


        Iterator<KV<Consumer, Double>> cpIt = lazyConsumerPaymentsStd1.iterator();
        KV<Consumer, Double> kv;
        kv = cpIt.next();
        assertEquals(new Consumer("Elton"), kv.key);
        assertEquals(74100.0, kv.value, 0.1);
        kv = cpIt.next();
        assertEquals(new Consumer("Emma"), kv.key);
        assertEquals(55.57777333511022, kv.value, 0.1);
        kv = cpIt.next();
        assertEquals(new Consumer("Johny"), kv.key);
        assertEquals(574.1785088280474, kv.value, 0.1);
        assertFalse(cpIt.hasNext());

        assertTrue(lazyConsumerPaymentsStd1.isReady());
        assertFalse(lazyConsumerPaymentsStd.isReady());
        assertTrue(consumerPaymentsAvg.isReady());
        assertTrue(lazyConsumerPayments.isReady());
        assertFalse(lazyRawUS2.isReady());
        assertFalse(lazyRawUS1.isReady());

        Iterator<KV<Consumer, Double>> cpaIt = consumerPaymentsAvg.iterator();
        kv = cpaIt.next();
        assertEquals(new Consumer("Elton"), kv.key);
        assertEquals(75900.0, kv.value, 0.1);
        kv = cpaIt.next();
        assertEquals(new Consumer("Emma"), kv.key);
        assertEquals(113.33333333333333, kv.value, 0.1);
        kv = cpaIt.next();
        assertEquals(new Consumer("Johny"), kv.key);
        assertEquals(353.2, kv.value, 0.1);
        assertFalse(cpaIt.hasNext());

    }

    public static DS<BytesWritable, String> make() throws Exception {
        Configuration conf = new Configuration();
        Path tempDir = new Path("tempDir");
        Path usData = new Path(tempDir, "usData");
        HadoopUtil.delete(conf, tempDir);
        DSContext dsContext = new DSContext(conf, new JobRunner(), tempDir);
        DS<BytesWritable, String> rawUserStories = DSFactory.create(usData, dsContext);

        SequenceFile.Writer writer =
                SequenceFile.createWriter(usData.getFileSystem(conf), conf, usData, BytesWritable.class, Text.class);

        writer.append(
                new BytesWritable(),
                new Text(new UserStory(Arrays.asList(
                        new Time(12345l),
                        new Consumer("Johny"),
                        new Product("six snickers pack"),
                        new Payment(66, Payment.Type.cash)
                )).toString()));
        writer.append(
                new BytesWritable(),
                new Text(new UserStory(Arrays.asList(
                        new Time(12345l),
                        new Consumer("Johny"),
                        new Product("pepsi"),
                        new Payment(30, Payment.Type.cash)
                )).toString()));
        writer.append(
                new BytesWritable(),
                new Text(new UserStory(Arrays.asList(
                        new Time(12345l),
                        new Consumer("Johny"),
                        new Product("frozen pizza"),
                        new Payment(120, Payment.Type.cash)
                )).toString()));
        writer.append(
                new BytesWritable(),
                new Text(new UserStory(Arrays.asList(
                        new Time(12345l),
                        new Consumer("Johny"),
                        new Product("100 cotton socks for tough guys"),
                        new Payment(1500, Payment.Type.cash)
                )).toString()));
        writer.append(
                new BytesWritable(),
                new Text(new UserStory(Arrays.asList(
                        new Time(12345l),
                        new Consumer("Johny"),
                        new Product("lucky strike"),
                        new Payment(50, Payment.Type.cash)
                )).toString()));


        writer.append(
                new BytesWritable(),
                new Text(new UserStory(Arrays.asList(
                        new Time(12345l),
                        new Consumer("Elton"),
                        new Product("strazed strings"),
                        new Payment(1800, Payment.Type.cash)
                )).toString()));
        writer.append(
                new BytesWritable(),
                new Text(new UserStory(Arrays.asList(
                        new Time(12345l),
                        new Consumer("Elton"),
                        new Product("piano"),
                        new Payment(150000, Payment.Type.cash)
                )).toString()));


        writer.append(
                new BytesWritable(),
                new Text(new UserStory(Arrays.asList(
                        new Time(12345l),
                        new Consumer("Emma"),
                        new Product("useless thing # 1"),
                        new Payment(100, Payment.Type.cash)
                )).toString()));
        writer.append(
                new BytesWritable(),
                new Text(new UserStory(Arrays.asList(
                        new Time(12345l),
                        new Consumer("Emma"),
                        new Product("useless thing # 2"),
                        new Payment(110, Payment.Type.cash)
                )).toString()));
        writer.append(
                new BytesWritable(),
                new Text(new UserStory(Arrays.asList(
                        new Time(12345l),
                        new Consumer("Emma"),
                        new Product("useless thing # 3"),
                        new Payment(90, Payment.Type.cash)
                )).toString()));
        writer.append(
                new BytesWritable(),
                new Text(new UserStory(Arrays.asList(
                        new Time(12345l),
                        new Consumer("Emma"),
                        new Product("useless thing # 4"),
                        new Payment(230, Payment.Type.cash)
                )).toString()));
        writer.append(
                new BytesWritable(),
                new Text(new UserStory(Arrays.asList(
                        new Time(12345l),
                        new Consumer("Emma"),
                        new Product("useless thing # 5"),
                        new Payment(50, Payment.Type.cash)
                )).toString()));
        writer.append(
                new BytesWritable(),
                new Text(new UserStory(Arrays.asList(
                        new Time(12345l),
                        new Consumer("Emma"),
                        new Product("useless thing # 6"),
                        new Payment(100, Payment.Type.cash)
                )).toString()));

        writer.close();

        return rawUserStories;
    }

//    @Test
//    public void testLazy() throws Exception {
//        RawUserStories rawUS = RealExample.make();
//        LazyKVDS<BytesWritable, String> lazyRawUS = new LazyKVDS<BytesWritable, String>(rawUS.getContext(), rawUS);
//        LazyKVDS<BytesWritable, String> lazyRawUS1 = lazyRawUS.map(new IdentityMapOp<BytesWritable, String>());
//        LazyKVDS<BytesWritable, String> lazyRawUS2 = lazyRawUS1.map(new IdentityMapOp<BytesWritable, String>());
//
//        LazyKVDS<Consumer, Long> lazyConsumerPayments
//                = lazyRawUS2.flatMap(rawUserStoryToConsumerPayments());
//
//        MapKVDS<Consumer, Double> consumerPaymentsAvg
//                = lazyConsumerPayments.reduce(valuesAvg()).toMapKVDS();
//
//        LazyKVDS<Consumer, Double> lazyConsumerPaymentsStd
//                = lazyConsumerPayments.reduce(valuesStd(consumerPaymentsAvg));
//
//        LazyKVDS<Consumer, Double> lazyConsumerPaymentsStd1
//                = lazyConsumerPaymentsStd.map(new IdentityMapOp<Consumer, Double>());
//
//
//        Iterator<KV<Consumer, Double>> cpIt = lazyConsumerPaymentsStd1.iterator();
//        KV<Consumer, Double> kv;
//        kv = cpIt.next();
//        assertEquals(new Consumer("Elton"), kv.key);
//        assertEquals(74100.0, kv.value, 0.1);
//        kv = cpIt.next();
//        assertEquals(new Consumer("Emma"), kv.key);
//        assertEquals(55.57777333511022, kv.value, 0.1);
//        kv = cpIt.next();
//        assertEquals(new Consumer("Johny"), kv.key);
//        assertEquals(574.1785088280474, kv.value, 0.1);
//        assertFalse(cpIt.hasNext());
//
//        assertTrue(lazyConsumerPaymentsStd1.isReady());
//        assertFalse(lazyConsumerPaymentsStd.isReady());
//        assertTrue(consumerPaymentsAvg.isReady());
//        assertFalse(lazyConsumerPayments.isReady());
//        assertFalse(lazyRawUS2.isReady());
//        assertFalse(lazyRawUS1.isReady());
//        assertTrue(lazyRawUS.isReady());
//
//        Iterator<KV<Consumer, Double>> cpaIt = consumerPaymentsAvg.iterator();
//        kv = cpaIt.next();
//        assertEquals(new Consumer("Elton"), kv.key);
//        assertEquals(75900.0, kv.value, 0.1);
//        kv = cpaIt.next();
//        assertEquals(new Consumer("Emma"), kv.key);
//        assertEquals(113.33333333333333, kv.value, 0.1);
//        kv = cpaIt.next();
//        assertEquals(new Consumer("Johny"), kv.key);
//        assertEquals(353.2, kv.value, 0.1);
//        assertFalse(cpaIt.hasNext());
//
//
//    }
//



}
