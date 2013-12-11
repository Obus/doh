package doh.crazy;

import org.apache.hadoop.conf.Configuration;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class OpSerializer {
    private static final String FLAT_MAP_PARAM_NAME = "tmp.op.flatmap";
    private static final String MAP_PARAM_NAME = "tmp.op.map";
    private static final String REDUCE_PARAM_NAME = "tmp.op.reduce";

    public static <T extends MapOp> T loadMapOpFromConf(Configuration conf) throws Exception {
        return loadOpFromConf(conf, MAP_PARAM_NAME);
    }

    public static <T extends FlatMapOp> T loadFlatMapOpFromConf(Configuration conf) throws Exception {
        return loadOpFromConf(conf, FLAT_MAP_PARAM_NAME);
    }

    public static <T extends ReduceOp> T loadReduceOpFromConf(Configuration conf) throws Exception {
        return loadOpFromConf(conf, REDUCE_PARAM_NAME);
    }

    private static <T extends Op> T loadOpFromConf(Configuration conf, String paramName) throws Exception {
        String opClassStr = conf.get(paramName);
        Class opClass = Class.forName(opClassStr);
        T op = (T) opClass.newInstance();
        loadOpFieldsFromConf(conf, op);
        return op;
    }

    public static <T extends MapOp> void saveMapOpToConf(Configuration conf, T op) throws Exception {
        conf.set(MAP_PARAM_NAME, op.getClass().getName());
        saveOpFieldsToConf(conf, op);
    }
    public static <T extends FlatMapOp> void saveFlatMapOpToConf(Configuration conf, T op) throws Exception {
        conf.set(FLAT_MAP_PARAM_NAME, op.getClass().getName());
        saveOpFieldsToConf(conf, op);
    }

    public static <T extends ReduceOp> void saveReduceOpToConf(Configuration conf, T op) throws Exception {
        conf.set(REDUCE_PARAM_NAME, op.getClass().getName());
        saveOpFieldsToConf(conf, op);
    }

    public static <T extends Op> T loadOpFieldsFromConf(Configuration conf, T op) throws Exception {
        Class opClass = op.getClass();
        List<Field> opParameters = opParametersAccessible(ReflectionUtils.getAllDeclaredFields(opClass));
        for (Field f : opParameters) {
            loadFieldFromConf(conf, op, f);
        }
        return op;
    }

    public static <T extends Op> T saveOpFieldsToConf(Configuration conf, T op) throws Exception {
        Class opClass = op.getClass();
        List<Field> opParameters = opParametersAccessible(ReflectionUtils.getAllDeclaredFields(opClass));
        for (Field f : opParameters) {
            OpFieldSerializer.saveFieldToConf(conf, op, f);
        }
        return op;
    }

    public static <T> T loadFieldFromConf(Configuration conf, T op, Field f) throws Exception {
        Object value = OpFieldSerializer.load(conf, OpFieldSerializer.parameterForOpField(op, f), OpFieldSerializer.fieldClass(f));
        OpFieldSerializer.setFieldValue(op, f, value);
        return op;
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



}
