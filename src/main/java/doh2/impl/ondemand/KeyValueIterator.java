package doh2.impl.ondemand;

import com.synqera.bigkore.rank.PlatformUtils;
import doh2.impl.op.WritableObjectDictionaryFactory;
import doh2.api.op.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;

import java.io.IOException;
import java.util.Iterator;

class KeyValueIterator<KEY, VALUE> implements Iterator<KV<KEY, VALUE>> {
    private final SequenceFileDirIterator sequenceFileDirIterator;
    private final WritableObjectDictionaryFactory.WritableObjectDictionary<KEY, Writable> keyDictionary;
    private final WritableObjectDictionaryFactory.WritableObjectDictionary<VALUE, Writable> valueDictionary;

    public KeyValueIterator(Configuration conf, Path path, Class<KEY> keyClass, Class<VALUE> valueClass) throws IOException {
        sequenceFileDirIterator = new SequenceFileDirIterator(
                PlatformUtils.listOutputFiles(conf, path),
                false,
                conf);
        keyDictionary = WritableObjectDictionaryFactory.createDictionary(keyClass);
        valueDictionary = WritableObjectDictionaryFactory.createDictionary(valueClass);
    }

    @Override
    public boolean hasNext() {
        return sequenceFileDirIterator.hasNext();
    }

    @Override
    public KV<KEY, VALUE> next() {
        Pair<Writable, Writable> pairOfWritables = (Pair<Writable, Writable>) sequenceFileDirIterator.next();
        return kv.set(
                keyDictionary.getObject(pairOfWritables.getFirst()),
                valueDictionary.getObject(pairOfWritables.getSecond())
        );
    }

    private final KV<KEY, VALUE> kv = new KV<KEY, VALUE>();

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
