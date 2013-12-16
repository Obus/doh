package doh.crazy;

import doh.ds.DataSet;

public interface OpExecutor {
    <F, T> DataSet<T> apply(DataSet<F> origin, Op<F, T> operation) throws Exception;
}
