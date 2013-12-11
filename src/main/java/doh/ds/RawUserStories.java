package doh.ds;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;

public class RawUserStories extends KeyValueDataSet<BytesWritable, String> {
    public RawUserStories(Path path) {
        super(path);
    }
}
