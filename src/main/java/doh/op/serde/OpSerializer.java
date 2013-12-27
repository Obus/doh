package doh.op.serde;

import com.google.common.base.Splitter;
import doh.api.OpParameter;
import doh.op.Op;
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

    private static final String KEY_VALUE_CLASSES_NAME = "tmp.op.keyValue.classes";

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


    private static void saveObjectToConf(Configuration conf, String paramName, Object obj) throws Exception {
        String value = WritableObjectDictionaryFactory.objectToString(obj);
        System.out.println();
        System.out.println();
        System.out.println("Saving parameter: " + paramName);
        System.out.println("Value: \n" + value);
        conf.set(paramName, value);
    }


    private static Object loadObjectFromConf(Configuration conf, String paramName) throws Exception {
        return WritableObjectDictionaryFactory.stringToObject(conf.get(paramName));
    }


    public static <T extends MapOp> T loadMapOpFromConf(Configuration conf) throws Exception {
        return (T) loadObjectFromConf(conf, MAP_PARAM_NAME); //loadOpFromConf(conf, MAP_PARAM_NAME);
    }

    public static <T extends FlatMapOp> T loadFlatMapOpFromConf(Configuration conf) throws Exception {
        return (T) loadObjectFromConf(conf, FLAT_MAP_PARAM_NAME); //loadOpFromConf(conf, FLAT_MAP_PARAM_NAME);
    }

    public static <T extends ReduceOp> T loadReduceOpFromConf(Configuration conf) throws Exception {
        return (T) loadObjectFromConf(conf, REDUCE_PARAM_NAME); //loadOpFromConf(conf, REDUCE_PARAM_NAME);
    }

    public static <T extends MapOp> void saveMapOpToConf(Configuration conf, T op) throws Exception {
        saveObjectToConf(conf, MAP_PARAM_NAME, op);
//        conf.set(MAP_PARAM_NAME, op.getClass().getName());
//        saveOpFieldsToConf(conf, op);
    }

    public static <T extends FlatMapOp> void saveFlatMapOpToConf(Configuration conf, T op) throws Exception {
        saveObjectToConf(conf, FLAT_MAP_PARAM_NAME, op);
//        conf.set(FLAT_MAP_PARAM_NAME, op.getClass().getName());
//        saveOpFieldsToConf(conf, op);
    }

    public static <T extends ReduceOp> void saveReduceOpToConf(Configuration conf, T op) throws Exception {
        saveObjectToConf(conf, REDUCE_PARAM_NAME, op);
//        conf.set(REDUCE_PARAM_NAME, op.getClass().getName());
//        saveOpFieldsToConf(conf, op);
    }

    public static <T extends CompositeMapOp> T loadCompositeMapOpConf(Configuration conf) throws Exception {
        return (T) loadObjectFromConf(conf, COMPOSITE_MAP_PARAM_NAME); //throw new UnsupportedOperationException();
    }


    public static <T extends CompositeReduceOp> T loadCompositeReduceOpConf(Configuration conf) throws Exception {
        return (T) loadObjectFromConf(conf, COMPOSITE_REDUCE_PARAM_NAME); //throw new UnsupportedOperationException();
    }

    public static void saveCompositeMapOp(Configuration conf, CompositeMapOp op) throws Exception {
        saveObjectToConf(conf, COMPOSITE_MAP_PARAM_NAME, op);//throw new UnsupportedOperationException();
    }

    public static void saveCompositeReduceOp(Configuration conf, CompositeReduceOp op) throws Exception {
        saveObjectToConf(conf, COMPOSITE_REDUCE_PARAM_NAME, op);//throw new UnsupportedOperationException();
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
