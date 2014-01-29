package doh2.impl.op.mr;

import doh2.impl.ondemand.OpJobMaker;
import doh2.impl.op.TestOps;
import doh2.impl.serde.GsonStringSerDe;
import doh2.impl.serde.OpSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.mahout.common.IntPairWritable;
import org.junit.Test;

public class GeneralMapOpMapperTest {


    @Test
    public void testMapperWithFilterOp() throws Exception {
        TestOps.FilterOpFilterPositiveIntValuesAndNonEmptyStringKeys filterOp
                = new TestOps.FilterOpFilterPositiveIntValuesAndNonEmptyStringKeys();
        Configuration conf = new Configuration();
        OpSerializer opSerializer = OpSerializer.create(conf, new GsonStringSerDe());
        opSerializer.saveMapperOp(conf, filterOp);
        Job job = new Job(conf);
        OpJobMaker.setKeyValueClassesBasedOnMap(job, String.class, Integer.class, filterOp);//Text.class, IntWritable.class, filterOp);
        conf = job.getConfiguration();

        GeneralMapOpMapper mapper = new GeneralMapOpMapper();
        final MapDriver<Text, IntWritable, Text, IntWritable> mapDriver =
                MapDriver.<Text, IntWritable, Text, IntWritable>newMapDriver(mapper).withConfiguration(conf);

        mapDriver.
                withInput(new Text("Boba"), new IntWritable(-1)).
                runTest();
        mapDriver.resetOutput();

        mapDriver.
                withInput(new Text("    "), new IntWritable(100)).
                runTest();
        mapDriver.resetOutput();

        mapDriver.
                withInput(new Text("Fett"), new IntWritable(1)).
                withOutput(new Text("Fett"), new IntWritable(1)).
                runTest();
        mapDriver.resetOutput();
    }


    @Test
    public void testMapperWithFlatOp() throws Exception {
        TestOps.FlatMapOpValueLineToWords flatMapOp = new TestOps.FlatMapOpValueLineToWords();
        Configuration conf = new Configuration();
        OpSerializer opSerializer = OpSerializer.create(conf, new GsonStringSerDe());
        opSerializer.saveMapperOp(conf, flatMapOp);

        Job job = new Job(conf);
        OpJobMaker.setKeyValueClassesBasedOnMap(job, Integer.class, String.class, flatMapOp);//Text.class, IntWritable.class, filterOp);
        conf = job.getConfiguration();


        GeneralMapOpMapper mapper = new GeneralMapOpMapper();
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

    @Test
    public void testMapperWithMapOp() throws Exception {
        TestOps.MapOpLongKeyToStringStringValueToInteger mapOp = new TestOps.MapOpLongKeyToStringStringValueToInteger();
        Configuration conf = new Configuration();
        OpSerializer opSerializer = OpSerializer.create(conf, new GsonStringSerDe());
        opSerializer.saveMapperOp(conf, mapOp);
        Job job = new Job(conf);
        OpJobMaker.setKeyValueClassesBasedOnMap(job, Long.class, String.class, mapOp);//Text.class, IntWritable.class, filterOp);
        conf = job.getConfiguration();


        GeneralMapOpMapper mapper = new GeneralMapOpMapper();
        final MapDriver <LongWritable, Text, Text, IntWritable> mapDriver =
                MapDriver.<LongWritable, Text, Text, IntWritable>newMapDriver(mapper).withConfiguration(conf);

        mapDriver.
                withInput(new LongWritable(-1234l), new Text("4321")).
                withOutput(new Text("-1234"), new IntWritable(4321)).
                runTest();
        mapDriver.resetOutput();

        mapDriver.
                withInput(new LongWritable(1234567891011121314l), new Text("-11")).
                withOutput(new Text("1234567891011121314"), new IntWritable(-11))
                .runTest();
    }
}
