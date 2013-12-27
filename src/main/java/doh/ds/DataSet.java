package doh.ds;

import doh.op.Op;
import doh.api.ds.Location;


public interface DataSet<ORIGIN> {
    //protected Context context;
    //protected final Path path;

//    protected DataSet(Path path) {
//        this.path = path;
//    }

//    public void setContext(Context context) {
//        this.context = context;
//    }

    public <TORIGIN> DataSet<TORIGIN> apply(Op<ORIGIN, TORIGIN> op) throws Exception;


    public Location getLocation();

    // public Configuration getConf()
}
