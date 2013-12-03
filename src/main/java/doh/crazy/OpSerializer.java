package doh.crazy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Arrays;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import sun.nio.cs.

/**
 * Created by Alexander A. Senov
 * Synqera, 2012.
 */
public class OpSerializer {

    public static <T extends Op> T makeMapOp(Configuration conf, Class<T> opClass) throws Exception {
        T op = opClass.newInstance();
        List<Field> opParameters = opParameters(opClass.getDeclaredFields());
        for (Field f : opParameters) {
            setField(op, f);
        }
        return op;
    }

    public static <T> T setField(Configuration conf, T instance, Field f) {
        return null;
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

    public void setFieldValueToConf(Configuration conf, Object op, Field f) {
        String name = parameterName(op, f);

    }

    public static Object getFieldValue(Object instance, Field f) throws Exception {
        return f.get(instance);
    }

    public static void setFieldValue(Object instance, Field f, Object value) throws Exception {
        f.set(instance, value);
    }

    public static <T> T load(Configuration conf, String paramName, Class<T> clazz) {
        return null;
    }

    public static <T> void save(Configuration conf, String paramName, T value) {

    }

    public static void saveWritable(Configuration conf, String paramName, Writable value) throws Exception {
        DataOutputBuffer buffer = new DataOutputBuffer();
        value.write(buffer);
        String data = new String(Arrays.trimToCapacity(buffer.getData(), buffer.getLength()), new US_ASCII());
    }

}
