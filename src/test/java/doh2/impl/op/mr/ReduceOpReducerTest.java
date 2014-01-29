package doh2.impl.op.mr;

import doh2.impl.ondemand.OpJobMaker;
import doh2.impl.op.TestOps;
import doh2.impl.serde.GsonStringSerDe;
import doh2.impl.serde.OpSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.Arrays;

public class ReduceOpReducerTest {
    @Test
    public void testReduce() throws Exception {
        TestOps.ReduceOpIntegerValuesSum<String> reduceOp = new TestOps.ReduceOpIntegerValuesSum<String>();
        Configuration conf = new Configuration();
        OpSerializer opSerializer = OpSerializer.create(conf, new GsonStringSerDe());
        opSerializer.saveReducerOp(conf, reduceOp);
        Job job = new Job(conf);
        OpJobMaker.setKeyValueClassesBasedOnReduce(job, String.class, Integer.class, reduceOp);//Text.class, IntWritable.class, filterOp);
        conf = job.getConfiguration();

        ReduceOpReducer reducer = new ReduceOpReducer();
        final ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver =
                ReduceDriver.<Text, IntWritable, Text, IntWritable>newReduceDriver(reducer).withConfiguration(conf);

        reduceDriver.
                withInput(new Text("key123"), Arrays.asList(
                        new IntWritable(-13),
                        new IntWritable(5),
                        new IntWritable(0),
                        new IntWritable(100),
                        new IntWritable(33),
                        new IntWritable(2),
                        new IntWritable(-4)
                )).
                withOutput(new Text("key123"), new IntWritable(123)).
                runTest();
        reduceDriver.resetOutput();

        reduceDriver.
                withInput(new Text(""), Arrays.asList(
                        new IntWritable(314)
                )).
                withOutput(new Text(""), new IntWritable(314)).
                runTest();
        reduceDriver.resetOutput();

    }
}
