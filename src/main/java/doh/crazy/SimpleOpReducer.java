package doh.crazy;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.Pair;

import java.io.IOException;

/**
 * Created by Alexander A. Senov
 * Synqera, 2012.
 */
public class SimpleOpReducer extends Reducer {

    private ReduceOp op;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            op = OpSerializer.loadReduceOpFromConf(context.getConfiguration());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        Pair p = op.reduce(key, values);
        context.write(p.getFirst(), p.getSecond());
    }
}