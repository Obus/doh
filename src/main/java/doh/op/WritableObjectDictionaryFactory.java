package doh.op;

import org.apache.hadoop.io.*;

import static doh.op.utils.ClassUtils.*;


public class WritableObjectDictionaryFactory {
    public interface WritableObjectDictionary<O, W extends Writable> {
        public O getObject(W writable);

        public W getWritable(O obj);
    }

    public static <O, W extends Writable> WritableObjectDictionary<O, W> createDictionary(Class clazz) {
        if (Writable.class.isAssignableFrom(clazz)) {
            return (WritableObjectDictionary<O, W>) new IdentityWOD<W>();
        }
        if (Integer.class.isAssignableFrom(clazz)) {
            return (WritableObjectDictionary<O, W>) new IntegerWOD();
        }
        if (Long.class.isAssignableFrom(clazz)) {
            return (WritableObjectDictionary<O, W>) new LongWOD();
        }
        if (Double.class.isAssignableFrom(clazz)) {
            return (WritableObjectDictionary<O, W>) new DoubleWOD();
        }
        if (String.class.isAssignableFrom(clazz)) {
            return (WritableObjectDictionary<O, W>) new StringWOD();
        }
        throw new UnsupportedOperationException();
    }

    public static Object getObject(Writable writable) {
        return null;
    }

    public static Writable getWritable(Object obj) {
        return null;
    }

    public static Class getObjectClass(Class<? extends Writable> writableClass) {
        if (writableClass.equals(IntWritable.class)) {
            return Integer.class;
        }
        if (writableClass.equals(DoubleWritable.class)) {
            return Double.class;
        }
        if (writableClass.equals(Text.class)) {
            return String.class;
        }
        if (isWritable(writableClass)) {
            return writableClass;
        }
        throw new IllegalArgumentException("Unsupported parameter writable class: " + writableClass);
    }

    public static Class<? extends Writable> getWritableClass(Class objClass) {
        if (isWritable(objClass)) {
            return objClass;
        }
        if (isInteger(objClass)) {
            return IntWritable.class;
        }
        if (isLong(objClass)) {
            return LongWritable.class;
        }
        if (isDouble(objClass)) {
            return DoubleWritable.class;
        }
        if (isString(objClass)) {
            return Text.class;
        }
        throw new IllegalArgumentException("Unsupported parameter class: " + objClass);
    }


    public static class IdentityWOD<W extends Writable> implements WritableObjectDictionary<W, W> {
        @Override
        public W getObject(W writable) {
            return writable;
        }

        @Override
        public W getWritable(W obj) {
            return obj;
        }
    }

    public static class IntegerWOD implements WritableObjectDictionary<Integer, IntWritable> {
        @Override
        public Integer getObject(IntWritable writable) {
            return writable.get();
        }

        @Override
        public IntWritable getWritable(Integer obj) {
            return new IntWritable(obj);
        }
    }

    public static class LongWOD implements WritableObjectDictionary<Long, LongWritable> {
        @Override
        public Long getObject(LongWritable writable) {
            return writable.get();
        }

        @Override
        public LongWritable getWritable(Long obj) {
            return new LongWritable(obj);
        }
    }

    public static class DoubleWOD implements WritableObjectDictionary<Double, DoubleWritable> {
        @Override
        public Double getObject(DoubleWritable writable) {
            return writable.get();
        }

        @Override
        public DoubleWritable getWritable(Double obj) {
            return new DoubleWritable(obj);
        }
    }

    public static class StringWOD implements WritableObjectDictionary<String, Text> {
        @Override
        public String getObject(Text writable) {
            return writable.toString();
        }

        @Override
        public Text getWritable(String obj) {
            return new Text(obj);
        }
    }


}
