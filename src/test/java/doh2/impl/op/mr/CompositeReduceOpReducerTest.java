package doh2.impl.op.mr;

import doh2.impl.ondemand.OpJobMaker;
import doh2.impl.op.TestOps;
import doh2.impl.op.kvop.CompositeMapOp;
import doh2.impl.op.kvop.CompositeReduceOp;
import doh2.impl.serde.GsonStringSerDe;
import doh2.impl.serde.OpSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.mahout.common.IntPairWritable;
import org.junit.Test;

import java.util.Arrays;

public class CompositeReduceOpReducerTest {
    @Test
    public void testReduce() throws Exception {
        CompositeReduceOp compositeReduceOp = new CompositeReduceOp(
                new TestOps.ReduceOpIntegerValuesSum<String>(),
                new CompositeMapOp(Arrays.asList(
                        new TestOps.MapOpKeyValueSwap<String, Integer>(),
                        new TestOps.FlatMapOpValueLineToWords()
                ))
        );
        Configuration conf = new Configuration();
        OpSerializer opSerializer = OpSerializer.create(conf, new GsonStringSerDe());
        opSerializer.saveReducerOp(conf, compositeReduceOp);
        Job job = new Job(conf);
        OpJobMaker.setKeyValueClassesBasedOnReduce(job, String.class, Integer.class, compositeReduceOp);//Text.class, IntWritable.class, filterOp);
        conf = job.getConfiguration();

        CompositeReduceOpReducer reducer = new CompositeReduceOpReducer();
        final ReduceDriver<Text, IntWritable, IntPairWritable, Text> reduceDriver =
                ReduceDriver.<Text, IntWritable, IntPairWritable, Text>newReduceDriver(reducer).withConfiguration(conf);

        reduceDriver.
                withInput(new Text("May the Force be with you"), Arrays.asList(
                        new IntWritable(10),
                        new IntWritable(4),
                        new IntWritable(300)
                )).
                withOutput(new IntPairWritable(314, 0), new Text("May")).
                withOutput(new IntPairWritable(314, 1), new Text("the")).
                withOutput(new IntPairWritable(314, 2), new Text("Force")).
                withOutput(new IntPairWritable(314, 3), new Text("be")).
                withOutput(new IntPairWritable(314, 4), new Text("with")).
                withOutput(new IntPairWritable(314, 5), new Text("you")).
                runTest();
        reduceDriver.resetOutput();

        reduceDriver.
                withInput(new Text("    "), Arrays.asList(
                        new IntWritable(10),
                        new IntWritable(4),
                        new IntWritable(300)
                )).
                runTest();
        reduceDriver.resetOutput();

    }
}
