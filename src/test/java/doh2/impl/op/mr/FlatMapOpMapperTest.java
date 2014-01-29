package doh2.impl.op.mr;

import doh2.impl.ondemand.OpJobMaker;
import doh2.impl.op.TestOps;
import doh2.impl.serde.GsonStringSerDe;
import doh2.impl.serde.OpSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.mahout.common.IntPairWritable;
import org.junit.Test;

public class FlatMapOpMapperTest {

    @Test
    public void testMapper() throws Exception {
        TestOps.FlatMapOpValueLineToWords flatMapOp = new TestOps.FlatMapOpValueLineToWords();
        Configuration conf = new Configuration();
        OpSerializer opSerializer = OpSerializer.create(conf, new GsonStringSerDe());
        opSerializer.saveMapperOp(conf, flatMapOp);
        Job job = new Job(conf);
        OpJobMaker.setKeyValueClassesBasedOnMap(job, Integer.class, String.class, flatMapOp);//Text.class, IntWritable.class, filterOp);
        conf = job.getConfiguration();

        FlatMapOpMapper mapper = new FlatMapOpMapper();
        final MapDriver<IntWritable, Text, IntPairWritable, Text> mapDriver =
                MapDriver.<IntWritable, Text, IntPairWritable, Text>newMapDriver(mapper).withConfiguration(conf);

        mapDriver.
                withInput(new IntWritable(314), new Text("May the Force be with you")).
                withOutput(new IntPairWritable(314, 0), new Text("May")).
                withOutput(new IntPairWritable(314, 1), new Text("the")).
                withOutput(new IntPairWritable(314, 2), new Text("Force")).
                withOutput(new IntPairWritable(314, 3), new Text("be")).
                withOutput(new IntPairWritable(314, 4), new Text("with")).
                withOutput(new IntPairWritable(314, 5), new Text("you")).
                runTest();
        mapDriver.resetOutput();

        mapDriver.
                withInput(new IntWritable(314), new Text("May_the_Force_be_with_you")).
                withOutput(new IntPairWritable(314, 0), new Text("May_the_Force_be_with_you")).
                runTest();
        mapDriver.resetOutput();

        mapDriver.
                withInput(new IntWritable(314), new Text("")).
                runTest();
        mapDriver.resetOutput();
    }
}
