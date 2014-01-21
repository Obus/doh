package doh.op;

import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import doh.ds.MapKVDS;
import doh.op.kvop.CompositeReduceOp;
import doh.op.serde.CompositeReduceOpJsonSerDe;
import doh.op.serde.MapKVDataSetJsonSerDe;
import doh.op.serde.OpSequenceJsonSerDe;
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

    public static class GsonStringSerDe extends StringSerDe {
        private final static List<Pair<Class, Object>> defaultTypeAdapters;

        static {
            defaultTypeAdapters = new ArrayList<Pair<Class, Object>>();
            defaultTypeAdapters.add(new Pair<Class, Object>(OpSequence.class, new OpSequenceJsonSerDe()));
            defaultTypeAdapters.add(new Pair<Class, Object>(MapKVDS.class, new MapKVDataSetJsonSerDe()));
            defaultTypeAdapters.add(new Pair<Class, Object>(CompositeReduceOp.class, new CompositeReduceOpJsonSerDe()));
        }

        private final List<Pair<Class, Object>> customTypeAdapters;
        private final Gson gson;

        public GsonStringSerDe() {
            customTypeAdapters = new ArrayList<Pair<Class, Object>>();
            gson = gsonWithAdapters(defaultTypeAdapters);
        }

        public GsonStringSerDe(List<Pair<Class, Object>> customTypeAdapters) {
            this.customTypeAdapters = customTypeAdapters;
            List<Pair<Class, Object>> allTypeAdapters = new ArrayList<Pair<Class, Object>>();
            allTypeAdapters.addAll(defaultTypeAdapters);
            allTypeAdapters.addAll(customTypeAdapters);
            gson = gsonWithAdapters(allTypeAdapters);
        }

        @Override
        public String serializeSelf() throws Exception {
            String s = getClass().getName();
            for (Pair<Class, Object> p : customTypeAdapters) {
                s += "," + p.getFirst().getName() + "," + p.getSecond().getClass().getName();
            }
            return s;
        }

        private Gson gsonWithAdapters(List<Pair<Class, Object>> typeAdapters) {
            final GsonBuilder gsonBuilder = new GsonBuilder();
            for (Pair<Class, Object> p : typeAdapters) {
                gsonBuilder.registerTypeAdapter(p.getFirst(), p.getSecond());
            }
            return gsonBuilder.create();
        }

        @Override
        public <T> String serialize(T o) {
            return o.getClass().getName() + ";" + gson.toJson(o);
        }

        @Override
        public <T> T deserialize(String json) throws Exception {
            int firstSep = json.indexOf(";");
            String classStr = json.substring(0, firstSep);
            String dataStr = json.substring(firstSep + 1);
            return (T) gson.fromJson(dataStr, Class.forName(classStr));
        }


    }
}
