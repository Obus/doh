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
import org.junit.Test;

public class MapOpMapperTest {

    @Test
    public void testMapper() throws Exception {
        TestOps.MapOpLongKeyToStringStringValueToInteger mapOp = new TestOps.MapOpLongKeyToStringStringValueToInteger();
        Configuration conf = new Configuration();
        OpSerializer opSerializer = OpSerializer.create(conf, new GsonStringSerDe());
        opSerializer.saveMapperOp(conf, mapOp);
        Job job = new Job(conf);
        OpJobMaker.setKeyValueClassesBasedOnMap(job, Long.class, String.class, mapOp);//Text.class, IntWritable.class, filterOp);
        conf = job.getConfiguration();

        MapOpMapper mapper = new MapOpMapper();
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
