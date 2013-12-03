package doh.ds;

import com.sun.javafx.PlatformUtil;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;

import java.util.List;

/**
 * Created by Alexander A. Senov
 * Synqera, 2012.
 */
public abstract class DataSet<ORIGIN> {
    protected final Path path;
    protected final long size;

    protected DataSet(Path path, long size) {
        this.path = path;
        this.size = size;
    }

    public long size() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    public List<ORIGIN> read() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    public void copy(Path to){

    }
    public void move(Path to){
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
