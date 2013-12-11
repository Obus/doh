package doh.ds;

import doh.crazy.Context;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;

public class KeyDataSet<KEY> extends KeyValueDataSet<KEY, NullWritable> {
    protected KeyDataSet(Path path) {
        super(path);
    }


}
