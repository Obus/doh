package doh.op;

import doh.ds.RealKVDataSet;
import doh.api.op.FlatMapOp;
import doh.api.op.KV;
import doh.api.op.ReduceOp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.StringTokenizer;

public class WordCountExample {

    public static class LineToWords extends FlatMapOp<Long, String, String, Long> {
        @Override
        public void flatMap(Long aLong, String line) {
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                emitKeyValue(tokenizer.nextToken(), 1l);
            }
        }
    }

    public static class CountWords extends ReduceOp<String, Long, String, Long> {
        @Override
        public KV<String, Long> reduce(String word, Iterable<Long> values) {
            Long count = 0l;
            for (Long v : values) {
                count += v;
            }
            return keyValue(word, count);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path input = new Path(args[0]);
        Path tempDir = new Path(args[1]);
        RealKVDataSet<Long, String> lines = RealKVDataSet.create(Context.create(conf, tempDir), input);
        RealKVDataSet<String, Long> words = lines.flatMap(new LineToWords());
        RealKVDataSet<String, Long> wordsCount = words.reduce(new CountWords());
    }

}
