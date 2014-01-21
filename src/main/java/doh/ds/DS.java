package doh.ds;

import doh.api.ds.Location;
import doh.op.Op;


public interface DS<ORIGIN> {
    //protected Context context;
    //protected final Path path;

//    protected DS(Path path) {
//        this.path = path;
//    }

//    public void setContext(Context context) {
//        this.context = context;
//    }

    public <TORIGIN> DS<TORIGIN> apply(Op<ORIGIN, TORIGIN> op) throws Exception;


    public Location getLocation();

    // public Configuration getConf()
}
