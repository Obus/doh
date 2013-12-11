package doh.ds;

import doh.crazy.Context;
import doh.crazy.Op;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.List;


public abstract class DataSet<ORIGIN> {
    protected Context context;
    protected final Path path;

    protected DataSet(Path path) {
        this.path = path;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public abstract <TORIGIN> DataSet<TORIGIN> apply(Op<ORIGIN, TORIGIN> op) throws Exception ;


    public Path getPath() {
        return path;
    }

    public Configuration getConf() {
        return context.getConf();
    }

    public List<ORIGIN> read() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }


    public Path nextPath() throws Exception {
        return context.nextTempPath();
    }
}
