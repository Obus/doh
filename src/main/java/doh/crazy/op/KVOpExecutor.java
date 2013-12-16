package doh.crazy.op;

import doh.crazy.Op;
import doh.crazy.OpExecutor;
import doh.ds.DataSet;
import doh.ds.RealKVDataSet;

public abstract class KVOpExecutor implements OpExecutor {
    @Override
    public <F, T> DataSet<T> apply(DataSet<F> origin, Op<F, T> operation) throws Exception {
        return null;
    }

    //public RealKVDataSet<,>
}
