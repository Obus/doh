package doh.op;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import doh.ds.MapKVDataSet;
import doh.op.kvop.CompositeReduceOp;
import doh.api.op.FlatMapOp;
import doh.op.kvop.KVOp;
import doh.api.op.MapOp;
import doh.api.op.ReduceOp;
import doh.op.serde.CompositeReduceOpJsonSerDe;
import doh.op.serde.MapKVDataSetJsonSerDe;
import doh.op.serde.OpSequenceJsonSerDe;
import doh.op.serde.OpSerializer;
import doh.op.utils.ReflectionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.mahout.math.Arrays;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

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
//        if (isSimpleKVOpClass(clazz)) {
//
//        }
        throw new UnsupportedOperationException("Unknown class: " + clazz);
    }

    public static boolean isSimpleKVOpClass(Class clazz) {
        return MapOp.class.isAssignableFrom(clazz) ||
                FlatMapOp.class.isAssignableFrom(clazz) ||
                ReduceOp.class.isAssignableFrom(clazz);
    }
//
    public static void writeObject(Object obj, DataOutput dataOutput) throws Exception {
        if (obj instanceof Integer) {
            dataOutput.writeInt((Integer) obj);
            return;
        }
        else if (obj instanceof Long) {
            dataOutput.writeLong((Long) obj);
            return;
        }
        else if (obj instanceof Double) {
            dataOutput.writeDouble((Double) obj);
            return;
        }
        else if (obj instanceof String) {
            WritableUtils.writeString(dataOutput, (String) obj);
            return;
        }
        else if (obj instanceof Writable) {
            ((Writable)obj).write(dataOutput);
            return;
        }
        else if (isSimpleKVOpClass(obj.getClass())) {
            new SimpleKVOpWritable((KVOp) obj).write(dataOutput);
            return;
        }
        throw new UnsupportedOperationException("Unknown object class: " + obj.getClass());
    }

    public static Object readObject(Class clazz, DataInput dataInput) throws Exception {
        if (Writable.class.isAssignableFrom(clazz)) {
            Writable obj = (Writable) clazz.newInstance();
            obj.readFields(dataInput);
        }
        if (Integer.class.isAssignableFrom(clazz)) {
            return dataInput.readInt();
        }
        if (Long.class.isAssignableFrom(clazz)) {
            return dataInput.readLong();
        }
        if (Double.class.isAssignableFrom(clazz)) {
            return dataInput.readDouble();
        }
        if (String.class.isAssignableFrom(clazz)) {
            return WritableUtils.readString(dataInput);
        }
        if (isSimpleKVOpClass(clazz)) {
            SimpleKVOpWritable w = new SimpleKVOpWritable();
            w.readFields(dataInput);
            return w.op;
        }
        throw new UnsupportedOperationException();

    }


    public static class AnyObjectWritable implements Writable{
        public Object inst;

        public AnyObjectWritable(Object inst) { this.inst = inst; }
        public AnyObjectWritable() {};

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            try {
                WritableUtils.writeString(dataOutput, inst.getClass().getName());
                writeObject(inst, dataOutput);
            } catch (Exception e) {
                throw new IOException();
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            try {
                inst = readObject(Class.forName(WritableUtils.readString(dataInput)), dataInput);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
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

    public static String objectToString(Object o) throws Exception {
        if (1 == 1) return objectToJson(o);
        Writable w = new AnyObjectWritable(o);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(baos);
        w.write(dataOutput);
        return baos.toString("UTF-8");

        //String s = dataOutputToString(buffer);// new String(Arrays.trimToCapacity(buffer.getData(), buffer.getLength()), OpParameterSerDe.charset);//writableToString(w);
        //return s;//s.replaceAll("\"&#", "MOAR_BIGKOAR");
    }


    private static final GsonBuilder gsonBuilder = new GsonBuilder();
    static {
        gsonBuilder.registerTypeAdapter(OpSequence.class, new OpSequenceJsonSerDe());
        gsonBuilder.registerTypeAdapter(MapKVDataSet.class, new MapKVDataSetJsonSerDe());
        gsonBuilder.registerTypeAdapter(CompositeReduceOp.class, new CompositeReduceOpJsonSerDe());
    }
    private static final Gson gson = gsonBuilder.create();//= new Gson();

    public static String objectToJson(Object o) throws Exception {
        return o.getClass().getName() + ";" + gson.toJson(o);
    }

    public static Object jsonToObject(String json) throws Exception {
        int firstSep = json.indexOf(";");
        String classStr = json.substring(0, firstSep);
        String dataStr = json.substring(firstSep + 1);
        return gson.fromJson(dataStr, Class.forName(classStr));
    }

    public static String dataOutputToString(DataOutputBuffer dataOutput) throws IOException {
        DataInputBuffer dataInput = new DataInputBuffer();
        dataInput.reset(dataOutput.getData(), dataOutput.getLength());
        return dataInput.readLine();
    }

    public static Object stringToObject(String s) throws Exception {
        if (1==1) return jsonToObject(s);
        //s = s.replaceAll("MOAR_BIGKOAR", "\"&#");
        AnyObjectWritable w = new AnyObjectWritable();// stringToWritable(s);
        byte[] data = Bytes.toBytes(s); //s.getBytes(OpParameterSerDe.charset);
        DataInputBuffer buffer = new DataInputBuffer();
        buffer.reset(data, data.length);
        w.readFields(buffer);
        return w.inst;
    }


    public static String writableToString(Writable w) throws Exception {
        DataOutputBuffer buffer = new DataOutputBuffer();
        w.write(buffer);
        return w.getClass().getName() + ";" + new String(Arrays.trimToCapacity(buffer.getData(), buffer.getLength()), OpParameterSerDe.charset);
    }

    public static <T extends Writable> T stringToWritable(String s) throws Exception {
        int firstSep = s.indexOf(";");
        String classStr = s.substring(0, firstSep);
        String dataStr = s.substring(firstSep + 1);
        T w = (T) Class.forName(classStr).newInstance();
        byte[] data = dataStr.getBytes(OpParameterSerDe.charset);
        DataInputBuffer buffer = new DataInputBuffer();
        buffer.reset(data, data.length);
        w.readFields(buffer);
        return w;
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


    public static class SimpleKVOpWritable implements Writable {
        public KVOp op;

        public SimpleKVOpWritable(KVOp op) {
            this.op = op;
        }

        public SimpleKVOpWritable(){}

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            try {
                WritableUtils.writeString(dataOutput, op.getClass().getName());
                List<Field> fields = OpSerializer.opParametersAccessible(ReflectionUtils.getAllDeclaredFields(op.getClass()));
                for (Field f : fields) {
                    Object obj = f.get(op);
                    writeObject(obj, dataOutput);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            try {
                op = (KVOp) Class.forName(WritableUtils.readString(dataInput)).newInstance();
                List<Field> fields = OpSerializer.opParametersAccessible(ReflectionUtils.getAllDeclaredFields(op.getClass()));
                for (Field f : fields) {
                    Object obj = readObject(f.getType(), dataInput);
                    f.set(op, obj);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


}
