package doh.op;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Arrays;

import java.nio.charset.Charset;

public interface OpParameterSerDe<T> {

    T de(String s) throws Exception;

    String ser(T parameter) throws Exception;

    public static class IntegerOpParameterSerDe implements OpParameterSerDe<Integer> {
        @Override
        public Integer de(String s) throws Exception {
            return Integer.parseInt(s);
        }

        @Override
        public String ser(Integer parameter) throws Exception {
            return parameter.toString();
        }
    }

    public static class DoubleOpParameterSerDe implements OpParameterSerDe<Double> {
        @Override
        public Double de(String s) throws Exception {
            return Double.parseDouble(s);
        }

        @Override
        public String ser(Double parameter) throws Exception {
            return parameter.toString();
        }
    }

    public static class StringOpParameterSerDe implements OpParameterSerDe<String> {
        @Override
        public String de(String s) throws Exception {
            return s;
        }

        @Override
        public String ser(String parameter) throws Exception {
            return parameter;
        }
    }


    public static class WritableOpParameterSerDe<W extends Writable> implements OpParameterSerDe<W> {

        @Override
        public W de(String s) throws Exception {
            int firstSep = s.indexOf(";");
            String classStr = s.substring(0, firstSep);
            String dataStr = s.substring(firstSep + 1);
            W w = (W) Class.forName(classStr).newInstance();
            byte[] data = dataStr.getBytes(charset);
            DataInputBuffer buffer = new DataInputBuffer();
            buffer.reset(data, data.length);
            w.readFields(buffer);
            return w;
        }

        @Override
        public String ser(W parameter) throws Exception {
            DataOutputBuffer buffer = new DataOutputBuffer();
            parameter.write(buffer);
            return parameter.getClass().getName() + ";" + new String(Arrays.trimToCapacity(buffer.getData(), buffer.getLength()), charset);
        }


        public static void saveWritable(Configuration conf, String paramName, Writable value) throws Exception {
            DataOutputBuffer buffer = new DataOutputBuffer();
            value.write(buffer);
            String data = new String(Arrays.trimToCapacity(buffer.getData(), buffer.getLength()), charset);
            conf.set(paramName, data);
        }

        public static <T> T loadWritable(Configuration conf, String paramName, Class<T> clazz) throws Exception {
            Writable w = (Writable) clazz.newInstance();
            byte[] data = conf.get(paramName).getBytes(charset);
            DataInputBuffer buffer = new DataInputBuffer();
            buffer.reset(data, data.length);
            w.readFields(buffer);
            return (T) w;
        }


    }
    public static Charset charset = Charset.forName("UTF8");


}
