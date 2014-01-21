package doh.op.utils;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import static doh.op.utils.ClassUtils.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class ClassUtilsTest {

    @Test
    public void testIsWritable() throws Exception {
        assertTrue(isWritable(Writable.class));
        assertTrue(isWritable(DoubleWritable.class));
        assertTrue(isWritable(Text.class));

        assertFalse(isWritable(Object.class));
        assertFalse(isWritable(Double.class));
    }

    @Test
    public void testIsString() throws Exception {
        assertTrue(isString(String.class));
        assertFalse(isString(Text.class));
        assertFalse(isString(Object.class));
    }

    @Test
    public void testIsInteger() throws Exception {
        assertTrue(isInteger(Integer.class));
        assertFalse(isInteger(IntWritable.class));
        assertFalse(isInteger(Object.class));
    }

    @Test
    public void testIsLong() throws Exception {
        assertTrue(isLong(Long.class));
        assertFalse(isLong(LongWritable.class));
        assertFalse(isLong(Object.class));
    }

    @Test
    public void testIsDouble() throws Exception {
        assertTrue(isDouble(Double.class));
        assertFalse(isDouble(DoubleWritable.class));
        assertFalse(isDouble(Object.class));
    }
}
