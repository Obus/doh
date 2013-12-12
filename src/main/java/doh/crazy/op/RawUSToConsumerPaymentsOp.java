package doh.crazy.op;

import com.synqera.bigkore.model.UserStory;
import com.synqera.bigkore.model.fact.Consumer;
import com.synqera.bigkore.model.fact.Fact;
import com.synqera.bigkore.model.fact.Payment;
import doh.crazy.FlatMapOp;
import doh.crazy.KV;
import org.apache.hadoop.io.BytesWritable;

import java.util.List;

public class RawUSToConsumerPaymentsOp extends FlatMapOp<BytesWritable, String, Consumer, Long> {
    private static final UserStory us = new UserStory();
    @Override
    public void flatMap(BytesWritable bytesWritable, String s) {
        us.fromString(s);
        Consumer c = us.consumer();
        for (Payment p : Fact.findFacts(us.getFacts(), Payment.class)) {
            emitKeyValue(c, p.getValue());
        }
    }

}
