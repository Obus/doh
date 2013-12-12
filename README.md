DOH — I don't know what does it mean.
================================

Intro
-------------------------
This framework based on two main concepts: 
1. KeyValueDataSet — represents the sequence file on hdfs, controls its data and provide usefull method to manipulate it
2. Operation — operation which transform on KeyValueDataSet to another (Map-, Reduce-, or MapReduce- Job).

The main goal of this framework is 
1. To abstract from the Hadoop routines like: Jobs, Writables, Configuration, IO routines, etc.
2. To handle data stored in HDFS as data-driven collection-like object rather then Path object and operate on it by high-order functions (like map and reduce).

DOH was inspired by [Spark](http://spark.incubator.apache.org/) (scala-based distributed computing framework). Spark overwhelm DOH in both codability (because of scala) and computational perfomance (cause Spark supports in-memory computations). However, Spark suffers from two major disadvantages:

1. Scala. Scala is much less common than Java and it is much harder to learn. Almost everyone who use Hadoop knows Java and if you not using it — you definity don't need DOH. 
2. Spark has really poor Hadoop support.

So, if you like the idea beside the Spark, but you can't aford it due to some limitations — try DOH!

Why is it cool? (introductionary example)
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
                  emitKeyValue(tokenizer.nextToken(), 1L);
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
In fact, you should make this code even more concise, by make oneliner the last two operations:

    KeyValueDataSet<String, Long> wordsCount = lines.flatMap(new LineToWords()).reduce(new CountWords());

Note, that all the typechecking of KeyValueDataSets and Operations Key/Value types is done Java.

