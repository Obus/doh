package doh.ds;

import org.apache.hadoop.fs.Path;


public class Vectors extends DataSet {
    protected Vectors(Path path, long size) {
        super(size, path);
    }


}
