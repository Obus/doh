DOH - I don't know what does it mean.
================================

Intro
-------------------------
This framework based on two main concepts: 
1. KeyValueDataSet - represents the sequence file on hdfs, controls its data and provide usefull method to manipulate it
2. Operation - operation which transform on KeyValueDataSet to another (Map-, Reduce-, or MapReduce- Job).

The main goal of this framework is 
1. To abstract from the Hadoop routines like: Jobs, Writables, Configuration, IO routines, etc.
2. To handle data stored in HDFS as data-driven collection-like object rather then Path object and operate on it by high-order functions (like map and reduce).

Why it is cool?
-------------------------
WordCount is the most popular illustration of MapReduce paradigm benefits. Despite that it hide all of MapReduce defects, we will use it as an example.
WordCount take directory with text files and create a new directory with files contain all occured words with their frequencies.
Classic WordCount implementation using Hadoop MapReduce could be found [here](http://wiki.apache.org/hadoop/WordCount).
Below you can see WordCount implemented using DOH.

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

  
It's easy to see that this implementation is much more clear and concise than the Hadoop MapReduce one.





