package doh2.impl.op.utils;

import doh2.api.op.FilterOp;
import doh2.api.op.FlatMapOp;
import doh2.api.op.KV;
import doh2.api.op.MapOp;
import doh2.api.op.ReduceOp;
import doh2.impl.op.Op;
import doh2.impl.op.kvop.KVUnoOp;
import org.junit.Test;

import static doh2.impl.op.utils.ReflectionUtils.*;
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
        assertEquals(KVUnoOp.class, getOpAncestor(MapImplOp.class));
        assertEquals(KVUnoOp.class, getOpAncestor(FlatMapImplOp.class));
        assertEquals(KVUnoOp.class, getOpAncestor(FilterImplOp.class));
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
