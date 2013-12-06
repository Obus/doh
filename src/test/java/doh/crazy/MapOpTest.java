package doh.crazy;

import org.apache.mahout.common.Pair;
import org.junit.Test;


import static org.junit.Assert.assertEquals;

/**
 * Created by Alexander A. Senov
 * Synqera, 2012.
 */
public class MapOpTest {
    public static class StringParseMapOp extends MapOp<Long, String, String, Double> {
        @Override
        public Pair<String, Double> map(Long aLong, String s) {
            return pair("Line number: " + aLong, Double.parseDouble(s));
        }
    }

    public abstract static class CSVParseMapOp<TK> extends MapOp<Long, String, String, TK> {
        @Override
        public Pair<String, TK> map(Long aLong, String s) {
            return pair("Line number: " + aLong, makeValue(aLong, s.split(",")));
        }

        protected abstract TK makeValue(Long aLong, String[] fields);
    }

    public static class IntegerSumCSVParseMapOp extends CSVParseMapOp<Integer> {
        @Override
        protected Integer makeValue(Long aLong, String[] fields) {
            Integer sum = 0;
            for (String s : fields) {
                sum += Integer.parseInt(s);
            }
            return sum;
        }
    }


    @Test
    public void testMapOpGetFromToKeyValueSimple() {
        MapOp mapOp = new StringParseMapOp();

        assertEquals(Long.class , mapOp.fromKeyClass());
        assertEquals(String.class , mapOp.fromValueClass());
        assertEquals(String.class , mapOp.toKeyClass());
        assertEquals(Double.class , mapOp.toValueClass());
    }

    @Test
    public void testMapOpGetFromToKeyValueWithAncestor() {
        MapOp mapOp = new IntegerSumCSVParseMapOp();

        assertEquals(Long.class , mapOp.fromKeyClass());
        assertEquals(String.class , mapOp.fromValueClass());
        assertEquals(String.class , mapOp.toKeyClass());
        assertEquals(Integer.class , mapOp.toValueClass());
    }

    @Test
    public void testUnparameterized() throws Exception {

    }

}
