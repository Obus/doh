package doh.ds;

import doh.crazy.Context;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;

public class KeyDataSet<KEY> extends KeyValueDataSet<KEY, NullWritable> {
    protected KeyDataSet(Context context, Path path) {
        super(context, path);
    }


}
