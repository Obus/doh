package doh2.impl.op;

import com.google.common.base.Splitter;
import doh2.impl.serde.GsonStringSerDe;
import org.apache.mahout.common.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class StringSerDe {
    public abstract <T> String serialize(T o) throws Exception;

    public abstract <T> T deserialize(String s) throws Exception;

    public abstract String serializeSelf() throws Exception;

    public static StringSerDe deserializeSerDe(String s) throws Exception {
        Iterator<String> it = Splitter.on(",").split(s).iterator();
        Class serDeClass = Class.forName(it.next());
        if (serDeClass.equals(GsonStringSerDe.class)) {
            final List<Pair<Class, Object>> customTypeAdapters = new ArrayList<Pair<Class, Object>>();
            while (it.hasNext()) {
                Class type = Class.forName(it.next());
                Object serDe = Class.forName(it.next()).newInstance();
                customTypeAdapters.add(new Pair<Class, Object>(type, serDe));
            }
            return new GsonStringSerDe(customTypeAdapters);
        } else {
            throw new IllegalArgumentException();
        }
    }

}
