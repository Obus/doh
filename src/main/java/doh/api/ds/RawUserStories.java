package doh.api.ds;


import doh.ds.RealKVDS;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;

public class RawUserStories extends RealKVDS<BytesWritable, String> {
    public RawUserStories(Path path) {
        super(path);
    }
}
