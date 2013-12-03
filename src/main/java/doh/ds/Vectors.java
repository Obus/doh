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
    protected Vectors(Path path, long size) {
        super(path, size);
    }


}
