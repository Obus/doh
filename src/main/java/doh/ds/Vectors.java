package doh.ds;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;

import java.util.List;

/**
 * Created by Alexander A. Senov
 * Synqera, 2012.
 *
 * In-hdfs abstraction for vectors
 */
public class Vectors extends DataSet {
    private final Path path;

    public long size() {
        return 0;
    }

    public void write(Path path) {
        return;
    }

    public List<Vector> read() {
        return null;
    }

    public Vectors(Path path) {
        this.path = path;
    }
}
