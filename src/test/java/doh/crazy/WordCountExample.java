package doh.crazy;

import doh.ds.KeyValueDataSet;
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
        KeyValueDataSet<Long, String> lines = KeyValueDataSet.create(Context.create(conf, tempDir), input);
        KeyValueDataSet<String, Long> words = lines.flatMap(new LineToWords());
        KeyValueDataSet<String, Long> wordsCount = words.reduce(new CountWords());
    }

}
