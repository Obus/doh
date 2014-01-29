package doh2.impl.op.mr;

import doh2.impl.ondemand.OpJobMaker;
import doh2.impl.op.TestOps;
import doh2.impl.op.kvop.CompositeMapOp;
import doh2.impl.serde.GsonStringSerDe;
import doh2.impl.serde.OpSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.mahout.common.IntPairWritable;
import org.junit.Test;

import java.util.Arrays;

public class CompositeMapOpMapperTest {

    @Test
    public void testMapper() throws Exception {
        CompositeMapOp compositeMapOp = new CompositeMapOp(Arrays.asList(
                new TestOps.FilterOpFilterPositiveIntValuesAndNonEmptyStringKeys(),
                new TestOps.MapOpKeyValueSwap<String, Integer>(),
                new TestOps.FlatMapOpValueLineToWords()
        ));
        Configuration conf = new Configuration();
        OpSerializer opSerializer = OpSerializer.create(conf, new GsonStringSerDe());
        opSerializer.saveMapperOp(conf, compositeMapOp);
        Job job = new Job(conf);
        OpJobMaker.setKeyValueClassesBasedOnMap(job, String.class, Integer.class, compositeMapOp);//Text.class, IntWritable.class, filterOp);
        conf = job.getConfiguration();

        CompositeMapOpMapper mapper = new CompositeMapOpMapper();
        final MapDriver<Text, IntWritable, IntPairWritable, Text> mapDriver =
                MapDriver.<Text, IntWritable, IntPairWritable, Text>newMapDriver(mapper).withConfiguration(conf);

        mapDriver.
                withInput(new Text("May the  \t Force be     with  you"), new IntWritable(314)).
                withOutput(new IntPairWritable(314, 0), new Text("May")).
                withOutput(new IntPairWritable(314, 1), new Text("the")).
                withOutput(new IntPairWritable(314, 2), new Text("Force")).
                withOutput(new IntPairWritable(314, 3), new Text("be")).
                withOutput(new IntPairWritable(314, 4), new Text("with")).
                withOutput(new IntPairWritable(314, 5), new Text("you")).
                runTest();
        mapDriver.resetOutput();


        mapDriver.
                withInput(new Text("May_the_Force_be_with_you"), new IntWritable(314)).
                withOutput(new IntPairWritable(314, 0), new Text("May_the_Force_be_with_you")).
                runTest();
        mapDriver.resetOutput();

        mapDriver.
                withInput(new Text(""), new IntWritable(314)).
                runTest();
        mapDriver.resetOutput();

        mapDriver.
                withInput(new Text("May_the_Force_be_with_you"), new IntWritable(0)).
                runTest();
        mapDriver.resetOutput();

    }
}
