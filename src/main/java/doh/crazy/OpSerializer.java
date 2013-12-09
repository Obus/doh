package doh.crazy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static doh.crazy.ClassUtils.*;

public class OpSerializer {

    public static <T extends MapOp> T loadMapOpFromConf(Configuration conf) throws Exception {
        String opClassStr = conf.get("tmp.op.map");
        Class opClass = Class.forName(opClassStr);
        T op = (T) opClass.newInstance();
        loadOpFieldsFromConf(conf, op);
        return op;
    }

    public static <T extends ReduceOp> T loadReduceOpFromConf(Configuration conf) throws Exception {
        String opClassStr = conf.get("tmp.op.reduce");
        Class opClass = Class.forName(opClassStr);
        T op = (T) opClass.newInstance();
        loadOpFieldsFromConf(conf, op);
        return op;
    }

    public static <T extends MapOp> void saveMapOpToConf(Configuration conf, T op) throws Exception {
        conf.set("tmp.op.map", op.getClass().getName());
        saveOpFieldsToConf(conf, op);
    }

    public static <T extends ReduceOp> void saveReduceOpToConf(Configuration conf, T op) throws Exception {
        conf.set("tmp.op.redyce", op.getClass().getName());
        saveOpFieldsToConf(conf, op);
    }

    public static <T extends Op> T loadOpFieldsFromConf(Configuration conf, T op) throws Exception {
        Class opClass = op.getClass();
        List<Field> opParameters = opParametersAccessible(opClass.getDeclaredFields());
        for (Field f : opParameters) {
            loadFieldFromConf(conf, op, f);
        }
        return op;
    }

    public static <T extends Op> T saveOpFieldsToConf(Configuration conf, T op) throws Exception {
        Class opClass = op.getClass();
        List<Field> opParameters = opParametersAccessible(opClass.getDeclaredFields());
        for (Field f : opParameters) {
            saveFieldToConf(conf, op, f);
        }
        return op;
    }

    public static <T> T loadFieldFromConf(Configuration conf, T op, Field f) throws Exception {
        Object value = load(conf, parameterForOpField(op, f), fieldClass(f));
        setFieldValue(op, f, value);
        return op;
    }

    public static void saveFieldToConf(Configuration conf, Object op, Field f) throws Exception {
        Object value = fieldValue(op, f);
        save(conf, parameterForOpField(op, f), value);
    }

    public static Class fieldClass(Field f) {
        return f.getType();
    }

    public static List<Field> opParametersAccessible(Field[] fields) {
        List<Field> opParameters = new ArrayList<Field>();
        for (Field f : fields) {
            if (isOpParameter(f)) {
                f.setAccessible(true);
                opParameters.add(f);
            }
        }
        return opParameters;
    }

    public static boolean isOpParameter(Field f) {
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

    public static String parameterForOpField(Object op, Field f) {
        return "tmp." + op.getClass().getSimpleName() + "." + f.getName();
    }

    public static String parameterForOp(Object op) {
        return "tmp." + op.getClass().getSimpleName();
    }

    public static Object fieldValue(Object instance, Field f) throws Exception {
        return f.get(instance);
    }

    public static void setFieldValue(Object instance, Field f, Object value) throws Exception {
        f.set(instance, value);
    }

    public static <T> T load(Configuration conf, String paramName, Class<T> clazz) throws Exception {
        String s = conf.get(paramName);
        if (isWritable(clazz)) {
            return (T) new OpParameterSerDe.WritableOpParameterSerDe().de(s);
        }
        if (isInteger(clazz)) {
            return (T) (Integer) Integer.parseInt(s);
        }
        if (isDouble(clazz)) {
            return (T) (Double) Double.parseDouble(s);
        }
        if (isString(clazz)) {
            return (T) s;
        }
        throw new IllegalArgumentException();
    }


    public static <T> void save(Configuration conf, String paramName, T value) throws Exception {
        if (value instanceof Writable) {
            conf.set(paramName, new OpParameterSerDe.WritableOpParameterSerDe().ser((Writable) value));
            return;
        }
        if (value instanceof Integer) {
            conf.setInt(paramName, (Integer) value);
            return;
        }
        if (value instanceof String) {
            conf.set(paramName, (String) value);
            return;
        }
        if (value instanceof Double) {
            conf.set(paramName, value.toString());
            return;
        }
        throw new IllegalArgumentException("Unsupported parameter class: " + value.getClass());
    }





}
