package doh.op.serde;

import com.google.common.base.Splitter;
import doh.api.OpParameter;
import doh.op.Op;
import doh.op.StringSerDe;
import doh.op.kvop.KVUnoOp;
import doh.op.utils.ReflectionUtils;
import doh.op.WritableObjectDictionaryFactory;
import doh.op.kvop.CompositeMapOp;
import doh.op.kvop.CompositeReduceOp;
import doh.api.op.FlatMapOp;
import doh.op.kvop.KVOp;
import doh.api.op.MapOp;
import doh.api.op.ReduceOp;
import org.apache.hadoop.conf.Configuration;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OpSerializer {
    private static final String FLAT_MAP_PARAM_NAME = "tmp.op.flatmap";
    private static final String MAP_PARAM_NAME = "tmp.op.map";
    private static final String COMPOSITE_MAP_PARAM_NAME = "tmp.op.composite.map";
    private static final String REDUCE_PARAM_NAME = "tmp.op.reduce";
    private static final String COMPOSITE_REDUCE_PARAM_NAME = "tmp.op.composite.reduce";
    private static final String MAPPER_OP = "tmp.op.mapper";
    private static final String REDUCER_OP = "tmp.op.reducer";

    private static final String KEY_VALUE_CLASSES_NAME = "tmp.op.keyValue.classes";
    private static final String SELF_NAME = "tmp.op.serializer";

    private final StringSerDe stringSerDe;
    // private final Configuration conf;

    private OpSerializer(StringSerDe stringSerDe) {
        this.stringSerDe = stringSerDe;
    }

    public static OpSerializer create(Configuration conf, StringSerDe stringSerDe) {
        OpSerializer opSerializer = new OpSerializer(stringSerDe);
        saveToConf(opSerializer, conf);
        return opSerializer;
    }


    public static void saveToConf(OpSerializer opSerializer, Configuration conf) {
        try {
            conf.set(SELF_NAME, opSerializer.stringSerDe.serializeSelf());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static OpSerializer create(Configuration conf) {
        try {
            return new OpSerializer(StringSerDe.deserializeSerDe(conf.get(SELF_NAME)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }





    public static void saveKVClassesToConf(Configuration conf,
                                           Class<?> mapperInputKeyClass,
                                           Class<?> mapperInputValueClass,
                                           Class<?> mapperOutputKeyClass,
                                           Class<?> mapperOutputValueClass,
                                           Class<?> reduceOutputKeyClass,
                                           Class<?> reduceOutputValueClass) {
        conf.set(KEY_VALUE_CLASSES_NAME,
                        mapperInputKeyClass.getName() + "," +
                        mapperInputValueClass.getName() + "," +
                        mapperOutputKeyClass.getName() + "," +
                        mapperOutputValueClass.getName() + "," +
                        reduceOutputKeyClass.getName() + "," +
                        reduceOutputValueClass.getName()
        );
    }
    private static Class<?>[] loadKVClassesFromConf(Configuration conf) throws Exception {
        String value = conf.get(KEY_VALUE_CLASSES_NAME);
        Class<?>[] kvClasses = new Class<?>[6];
        Iterator<String> it = Splitter.on(",").split(value).iterator();
        kvClasses[0] = Class.forName(it.next());
        kvClasses[1] = Class.forName(it.next());
        kvClasses[2] = Class.forName(it.next());
        kvClasses[3] = Class.forName(it.next());
        kvClasses[4] = Class.forName(it.next());
        kvClasses[5] = Class.forName(it.next());
        if (it.hasNext()) {
            throw new IllegalStateException();
        }
        return kvClasses;
    }

    public static Class<?> loadMapInputKeyClassFromConf(Configuration conf) throws Exception {
        return loadKVClassesFromConf(conf)[0];
    }
    public static Class<?> loadMapInputValueClassFromConf(Configuration conf) throws Exception {
        return loadKVClassesFromConf(conf)[1];
    }
    public static Class<?> loadMapOutputKeyClassFromConf(Configuration conf) throws Exception {
        return loadKVClassesFromConf(conf)[2];
    }
    public static Class<?> loadMapOutputValueClassFromConf(Configuration conf) throws Exception {
        return loadKVClassesFromConf(conf)[3];
    }
    public static Class<?> loadReduceOutputKeyClassFromConf(Configuration conf) throws Exception {
        return loadKVClassesFromConf(conf)[4];
    }
    public static Class<?> loadReduceOutputValueClassFromConf(Configuration conf) throws Exception {
        return loadKVClassesFromConf(conf)[5];
    }


    private  void saveObjectToConf(Configuration conf, String paramName, Object obj) throws Exception {
        String value = stringSerDe.serialize(obj);
        System.out.println();
        System.out.println();
        System.out.println("Saving parameter: " + paramName);
        System.out.println("Value: \n" + value);
        conf.set(paramName, value);
    }


    private  Object loadObjectFromConf(Configuration conf, String paramName) throws Exception {
        return stringSerDe.deserialize(conf.get(paramName));
    }


    public void saveMapperOp(Configuration conf, KVUnoOp op) throws Exception {
        saveObjectToConf(conf, MAPPER_OP, op);
    }
    public void saveReducerOp(Configuration conf, KVUnoOp op) throws Exception {
        saveObjectToConf(conf, REDUCER_OP, op);
    }

    public KVUnoOp loadMapperOp(Configuration conf) throws Exception {
        return (KVUnoOp) loadObjectFromConf(conf, MAPPER_OP);
    }
    public KVUnoOp loadReducerOp(Configuration conf) throws Exception {
        return (KVUnoOp) loadObjectFromConf(conf, REDUCER_OP);
    }


    public static List<Field> opParametersAccessible(List<Field> fields) {
        List<Field> opParameters = new ArrayList<Field>();
        for (Field f : fields) {
            if (isOpParameter(f)) {
                f.setAccessible(true);
                opParameters.add(f);
            }
        }
        return opParameters;
    }

    private static boolean isOpParameter(Field f) {
        Annotation[] annotations = f.getDeclaredAnnotations();
        if (annotations.length == 0) {
            return false;
        }
        for (Annotation a : annotations) {
            if (a instanceof OpParameter) {
                return true;
            }
        }
        return false;
    }
}
