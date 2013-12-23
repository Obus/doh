package doh.op;

import com.synqera.bigkore.model.UserStory;
import com.synqera.bigkore.model.fact.Consumer;
import com.synqera.bigkore.model.fact.Payment;
import com.synqera.bigkore.model.fact.Product;
import com.synqera.bigkore.model.fact.Time;
import doh.ds.MapKVDataSet;
import doh.ds.RawUserStories;
import doh.ds.RealKVDataSet;
import doh.op.kvop.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.HadoopUtil;
import org.junit.Test;

import java.util.Arrays;

import static doh.op.OpFactory.*;


public class Example {


    @Test
    public void testExample() throws Exception {
        RawUserStories rawUS = make();

        RealKVDataSet<Consumer, Long> consumerPayments
                = rawUS.flatMap(rawUserStoryToConsumerPayments());

        MapKVDataSet<Consumer, Double> consumerPaymentsAvg
                = consumerPayments.reduce(valuesAvg()).toMapKVDS();

        RealKVDataSet<Consumer, Double> consumerPaymentsStd
                = consumerPayments.reduce(valuesStd(consumerPaymentsAvg));

        for (KV<Consumer, Double> kv : consumerPaymentsStd) {
            System.out.println(kv);
        }
    }

    public static RawUserStories make() throws Exception {
        Path tempDir = new Path("tempDir");
        Configuration conf = new Configuration();
        HadoopUtil.delete(conf, tempDir);
        Context context = Context.create(conf, tempDir);
        Path usPath = context.nextTempPath();
        RawUserStories rawUserStories = new RawUserStories(usPath);
        rawUserStories.setContext(context);

        Path usData = new Path(usPath, "data");
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
