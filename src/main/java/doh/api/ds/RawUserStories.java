package doh.api.ds;


import doh.ds.RealKVDataSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;

public class RawUserStories extends RealKVDataSet<BytesWritable, String> {
    public RawUserStories(Path path) {
        super(path);
    }
}
