package doh.crazy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Arrays;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

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
        List<Field> opParameters = opParameters(opClass.getDeclaredFields());
        for (Field f : opParameters) {
            loadFieldFromConf(conf, op, f);
        }
        return op;
    }

    public static <T extends Op> T saveOpFieldsToConf(Configuration conf, T op) throws Exception {
        Class opClass = op.getClass();
        List<Field> opParameters = opParameters(opClass.getDeclaredFields());
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

    public static List<Field> opParameters(Field[] fields) {
        List<Field> opParameters = new ArrayList<Field>();
        for (Field f : fields) {
            if (isOpParameter(f)) {
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
        return loadWritable(conf, paramName, clazz);
    }

    public static <T> T loadWritable(Configuration conf, String paramName, Class<T> clazz) throws Exception {
        Writable w = (Writable) clazz.newInstance();
        byte[] data = conf.get(paramName).getBytes(charset);
        DataInputBuffer buffer = new DataInputBuffer();
        buffer.reset(data, data.length);
        w.readFields(buffer);
        return (T) w;
    }

    public static <T> void save(Configuration conf, String paramName, T value) throws Exception {
        saveWritable(conf, paramName, (Writable) value);
    }

    public static void saveWritable(Configuration conf, String paramName, Writable value) throws Exception {
        DataOutputBuffer buffer = new DataOutputBuffer();
        value.write(buffer);
        String data = new String(Arrays.trimToCapacity(buffer.getData(), buffer.getLength()), charset);
        conf.set(paramName, data);
    }

    public static Charset charset = Charset.forName("UTF8");
}
