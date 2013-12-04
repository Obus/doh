package doh.crazy;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.Pair;

import java.io.IOException;

/**
 * Created by Alexander A. Senov
 * Synqera, 2012.
 */
public class SimpleOpMapper extends Mapper {

    private MapOp op;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            op = OpSerializer.loadOpFromConf(context.getConfiguration());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        Pair p = op.map(key, value);
        context.write(p.getFirst(), p.getSecond());
    }
}
