package doh.op.utils;

import doh.api.op.FilterOp;
import doh.api.op.FlatMapOp;
import doh.api.op.KV;
import doh.api.op.MapOp;
import doh.api.op.ReduceOp;
import doh.op.Op;
import org.junit.Test;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import static doh.op.utils.ReflectionUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class ReflectionUtilsTest {
    @Test
    public void testIsUnknown() throws Exception {
        assertTrue(isUnknown(UNKNOWN_CLASS));
        assertFalse(isUnknown(Object.class));
    }

    @Test
    public void testGetFromKeyClass() throws Exception {
         assertEquals(Double.class, getFromKeyClass(ReduceImplOp.class));
         assertEquals(UNKNOWN_CLASS, getFromKeyClass(MapImplOp.class));
    }

    @Test
    public void testGetFromValueClass() throws Exception {
         assertEquals(Long.class, getFromValueClass(ReduceImplOp.class));
         assertEquals(UNKNOWN_CLASS, getFromValueClass(MapImplOp.class));
    }

    @Test
    public void testGetToKeyClass() throws Exception {
         assertEquals(String.class, getToKeyClass(ReduceImplOp.class));
         assertEquals(UNKNOWN_CLASS, getToKeyClass(MapImplOp.class));
    }

    @Test
    public void testGetToValueClass() throws Exception {
         assertEquals(Integer.class, getToValueClass(ReduceImplOp.class));
         assertEquals(UNKNOWN_CLASS, getToValueClass(MapImplOp.class));
    }

    @Test
    public void testGetOpAncestor() throws Exception {
        assertEquals(ReduceOp.class, getOpAncestor(ReduceImplOp.class));
        assertEquals(MapOp.class, getOpAncestor(MapImplOp.class));
        assertEquals(FlatMapOp.class, getOpAncestor(FlatMapImplOp.class));
        assertEquals(FilterOp.class, getOpAncestor(FilterImplOp.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetOpAncestorUnsopported() throws Exception {
        getOpAncestor(ImplOp.class);
    }




    public static class ReduceImplOp extends ReduceOp<Double, Long, String, Integer> {
        @Override
        public KV<String, Integer> reduce(Double aDouble, Iterable<Long> longs) {
            return null;
        }
    }
    public static class MapImplOp extends MapOp {
        @Override
        public KV map(Object o, Object o2) {
            return null;
        }
    }
    public static class FlatMapImplOp extends FlatMapOp {
        @Override
        public void flatMap(Object o, Object o2) {

        }
    }
    public static class FilterImplOp extends FilterOp {
        @Override
        public boolean accept(Object o, Object o2) {
            return false;
        }
    }
    public static class ImplOp implements Op {
        @Override
        public Some apply(Some f) {
            return null;
        }
    }


}
