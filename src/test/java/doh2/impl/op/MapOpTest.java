package doh2.impl.op;

import doh2.api.DS;
import doh2.api.DSContext;
import doh2.api.OpParameter;
import doh2.api.SingleHDFSLocation;
import doh2.impl.ondemand.OnDemandDS;
import doh2.api.op.KV;
import doh2.api.op.MapOp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class MapOpTest {

    public static class StringParseMapOp extends MapOp<Long, String, String, Double> {
        @Override
        public KV<String, Double> map(Long aLong, String s) {
            return keyValue("Line number: " + aLong, Double.parseDouble(s));
        }
    }

    public abstract static class CSVParseMapOp<TK> extends MapOp<Long, String, String, TK> {
        @Override
        public KV<String, TK> map(Long aLong, String s) {
            return keyValue("Line number: " + aLong, makeValue(aLong, s.split(",")));
        }

        protected abstract TK makeValue(Long aLong, String[] fields);
    }

    public static class IntegerSumCSVParseMapOp extends CSVParseMapOp<Integer> {
        @Override
        protected Integer makeValue(Long aLong, String[] fields) {
            Integer sum = 0;
            for (String s : fields) {
                sum += Integer.parseInt(s);
            }
            return sum;
        }
    }

    public static class SimpleParametrizedMapOp extends CSVParseMapOp<Integer> {

        @OpParameter
        private Integer mul;

        public SimpleParametrizedMapOp() {
        }

        public SimpleParametrizedMapOp(Integer mul) {
            this.mul = mul;
        }

        @Override
        protected Integer makeValue(Long aLong, String[] fields) {
            Integer sum = 0;
            for (String s : fields) {
                sum += Integer.parseInt(s);
            }
            return sum * mul;
        }
    }

    @Test
    public void testMapOpGetFromToKeyValueSimple() {
        MapOp mapOp = new StringParseMapOp();

        assertEquals(Long.class, mapOp.fromKeyClass());
        assertEquals(String.class, mapOp.fromValueClass());
        assertEquals(String.class, mapOp.toKeyClass());
        assertEquals(Double.class, mapOp.toValueClass());
    }

    @Test
    public void testMapOpGetFromToKeyValueWithAncestor() {
        MapOp mapOp = new IntegerSumCSVParseMapOp();

        assertEquals(Long.class, mapOp.fromKeyClass());
        assertEquals(String.class, mapOp.fromValueClass());
        assertEquals(String.class, mapOp.toKeyClass());
        assertEquals(Integer.class, mapOp.toValueClass());
    }

    @Test
    public void testUnparameterized() throws Exception {
        Configuration conf = new Configuration();
        Path input = new Path("input");
        Path tempDir = new Path("temp");
        HadoopUtil.delete(conf, input, tempDir);

        JobRunner jobRunner = new JobRunner() {
            @Override
            public void runJob(Job job) throws Exception {
                job.waitForCompletion(true);
            }
        };
        DSContext dsContext = new DSContext(conf, jobRunner, tempDir);

        Path inputData = new Path(input, "data");
        SequenceFile.Writer writer = SequenceFile.
                createWriter(inputData.getFileSystem(conf), conf, inputData, LongWritable.class, Text.class);

        writer.append(new LongWritable(1), new Text("1"));
        writer.append(new LongWritable(2), new Text("1,2"));
        writer.append(new LongWritable(2), new Text("1,2,3"));

        writer.close();

        DS<Long, String> csv = new OnDemandDS<Long, String>(dsContext, new SingleHDFSLocation(input));
        DS<String, Integer> res = csv.
                map(new SimpleParametrizedMapOp(3));

        // Path resData = ;
        Path path = ((SingleHDFSLocation) res.getLocation()).getPath();
        FileSystem fs = path.getFileSystem(conf);
        FileStatus[] statuses = fs.listStatus(path, new Utils.OutputFileUtils.OutputFilesFilter());
        assertEquals(1, statuses.length);
        Path resData = statuses[0].getPath();


        Iterator<Pair<Text, IntWritable>> it = new SequenceFileIterator<Text, IntWritable>(resData, false, conf);

        Pair<Text, IntWritable> p = it.next();
        assertEquals("Line number: 1", p.getFirst().toString());
        assertEquals(3, p.getSecond().get());
        p = it.next();
        assertEquals("Line number: 2", p.getFirst().toString());
        assertEquals(9, p.getSecond().get());
        p = it.next();
        assertEquals("Line number: 2", p.getFirst().toString());
        assertEquals(18, p.getSecond().get());
        assertFalse(it.hasNext());


    }

}
