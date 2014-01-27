package doh2.impl.op.utils;

import org.apache.hadoop.io.Writable;

public class ClassUtils {

    public static boolean isWritable(Class clazz) {
        return Writable.class.isAssignableFrom(clazz);
    }

    public static boolean isString(Class clazz) {
        return clazz.equals(String.class);
    }

    public static boolean isInteger(Class clazz) {
        return clazz.equals(Integer.class);
    }

    public static boolean isLong(Class clazz) {
        return clazz.equals(Long.class);
    }

    public static boolean isDouble(Class clazz) {
        return clazz.equals(Double.class);
    }
}
