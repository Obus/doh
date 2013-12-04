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

/**
 * Created by Alexander A. Senov
 * Synqera, 2012.
 */
public class OpSerializer {

    public static <T extends Op> T makeMapOp(Configuration conf, Class<T> opClass) throws Exception {
        T op = opClass.newInstance();
        List<Field> opParameters = opParameters(opClass.getDeclaredFields());
        for (Field f : opParameters) {
            setFieldFromConf(conf, op, f);
        }
        return op;
    }

    public static <T> T setFieldFromConf(Configuration conf, T op, Field f) throws Exception {
        Object value = load(conf, parameterName(op, f), fieldClass(f));
        setFieldValue(op, f, value);
        return op;
    }

    public static Class fieldClass(Field f) {
        return f.getType();
    }

    public static List<Field> opParameters(Field[] fields) {
        List<Field> opParameters = new ArrayList<Field>(5);
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

    public static String parameterName(Object op, Field f) {
        return "tmp." + f.getName();
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
    }

    public static Charset charset = Charset.forName("UTF8");
}
