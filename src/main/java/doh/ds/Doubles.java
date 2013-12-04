package doh.ds;

import org.apache.hadoop.fs.Path;

import java.util.List;


public class Doubles extends DataSet {
    protected Doubles(Path path, long size) {
        super(path, size);
    }

    public List<Double> read() {
        return null;
    }
}
