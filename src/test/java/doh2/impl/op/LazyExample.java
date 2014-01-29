package doh2.impl.op;

import com.synqera.bigkore.model.UserStory;
import com.synqera.bigkore.model.fact.Consumer;
import com.synqera.bigkore.model.fact.Payment;
import com.synqera.bigkore.model.fact.Product;
import com.synqera.bigkore.model.fact.Time;
import doh2.api.op.KV;
import doh2.api.DS;
import doh2.api.DSContext;
import doh2.api.DSFactory;
import doh2.api.MapDS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.common.HadoopUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static doh2.impl.op.TestOpFactory.rawUserStoryToConsumerPayments;
import static doh2.impl.op.TestOpFactory.valuesAvg;
import static doh2.impl.op.TestOpFactory.valuesStd;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LazyExample {


    @Test
    public void testOnDemandDS() throws Exception {
        DS<BytesWritable, String> rawUS = make();
        DS<BytesWritable, String> lazyRawUS1 = rawUS.map(new TestOps.IdentityMapOp<BytesWritable, String>());
        DS<BytesWritable, String> lazyRawUS2 = lazyRawUS1.map(new TestOps.IdentityMapOp<BytesWritable, String>());

        DS<Consumer, Long> lazyConsumerPayments
                = lazyRawUS2.flatMap(rawUserStoryToConsumerPayments());

        MapDS<Consumer, Double> consumerPaymentsAvg
                = lazyConsumerPayments.reduce(valuesAvg()).toMapDS();

        DS<Consumer, Double> lazyConsumerPaymentsStd
                = lazyConsumerPayments.reduce(valuesStd(consumerPaymentsAvg));

        DS<Consumer, Double> lazyConsumerPaymentsStd1
                = lazyConsumerPaymentsStd.map(new TestOps.IdentityMapOp<Consumer, Double>()).breakJobHere();

        DS<Consumer, Double> lazyConsumerPaymentsStd2
                = lazyConsumerPaymentsStd1.map(new TestOps.IdentityMapOp<Consumer, Double>());

        DS<Consumer, Double> lazyConsumerPaymentsStd3
                = lazyConsumerPaymentsStd2.map(new TestOps.IdentityMapOp<Consumer, Double>());


        Iterator<KV<Consumer, Double>> cpIt = lazyConsumerPaymentsStd3.iterator();
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

        assertTrue(lazyConsumerPaymentsStd3.isReady());
        assertFalse(lazyConsumerPaymentsStd2.isReady());
        assertTrue(lazyConsumerPaymentsStd1.isReady());
        assertFalse(lazyConsumerPaymentsStd.isReady());
        assertTrue(consumerPaymentsAvg.isReady());
        assertFalse(lazyConsumerPayments.isReady());
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
        DSContext dsContext = new DSContext(conf, new JobRunner() {
            @Override
            public void runJob(Job job) throws Exception {
                job.waitForCompletion(true);
            }
        }, tempDir);
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

}
