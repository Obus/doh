package doh2.impl.op;

import doh2.api.OpParameter;
import doh2.api.SingleHDFSLocation;
import doh2.impl.serde.GsonStringSerDe;
import doh2.api.op.FlatMapOp;
import doh2.api.op.KV;
import doh2.api.op.MapOp;
import doh2.api.op.ReduceOp;
import doh2.impl.op.kvop.CompositeMapOp;
import doh2.impl.op.kvop.CompositeReduceOp;
import doh2.api.MapDS;
import doh2.impl.ondemand.OnDemandDS;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GsonStringSerDeTest {


    public static class ReduceOpTestImpl extends ReduceOp {
        //@OpParameter
        public MapDS aMap;
        @OpParameter
        public int aInteger;
        @OpParameter
        public double aDouble;
        @OpParameter
        public long aLong;
        @OpParameter
        public String aString;
        public String aNotSerializedString;

        @Override
        public KV reduce(Object o, Iterable iterable) {
            throw new AssertionError();
        }
    }
    public static class MapOpTestImpl1 extends MapOp {

        @Override
        public KV map(Object o, Object o2) {
            throw new AssertionError();
        }
    }
    public static class FlatMapOpTestImpl1 extends FlatMapOp {
        @OpParameter
        public String aString;
        public String aNotSerializedString;

        @Override
        public void flatMap(Object o, Object o2) {
            throw new AssertionError();
        }
    }

    @Test
    public void testSerializeSelf() throws Exception {
        GsonStringSerDe gsonStringSerDe = new GsonStringSerDe();
        assertTrue(StringSerDe.deserializeSerDe(gsonStringSerDe.serializeSelf()) instanceof GsonStringSerDe);
    }

    @Test
    public void testSerDe() throws Exception {

        FlatMapOpTestImpl1 flatMapOpTestImpl1 = new FlatMapOpTestImpl1();
        flatMapOpTestImpl1.aNotSerializedString = "not serialized";
        flatMapOpTestImpl1.aString = "serialized";
        CompositeMapOp compositeMapOp = new CompositeMapOp(Arrays.asList(new MapOpTestImpl1(), flatMapOpTestImpl1));


        MapDS mapMock = new OnDemandDS(null, new SingleHDFSLocation(new Path("tempDir/map.map")));
        ReduceOpTestImpl reduceOp = new ReduceOpTestImpl();
        reduceOp.aMap = mapMock;
        reduceOp.aDouble = 3.141592;
        reduceOp.aInteger = 314;
        reduceOp.aLong = 3141592;
        reduceOp.aNotSerializedString = "not serialized";
        reduceOp.aString = "serialized";

        CompositeReduceOp compositeReduceOp = new CompositeReduceOp(reduceOp, compositeMapOp);

        GsonStringSerDe gsonStringSerDe = new GsonStringSerDe();

        String serialization = gsonStringSerDe.serialize(compositeReduceOp);

        compositeReduceOp = gsonStringSerDe.deserialize(serialization);

        ReduceOpTestImpl reduceOpTest = (ReduceOpTestImpl) compositeReduceOp.getReduceOp();
        assertEquals(new Path("tempDir/map.map"), ((SingleHDFSLocation) reduceOpTest.aMap.getLocation()).getPath());
        assertEquals(3.141592, reduceOpTest.aDouble, 0.0000001);
        assertEquals(314, reduceOp.aInteger);
        assertEquals(3141592l, reduceOp.aLong);
        assertEquals("serialized", reduceOp.aString);
//        assertNull(reduceOpTest.aNotSerializedString);

        CompositeMapOp compositeMapOpTest = compositeReduceOp.getCompositeMapOp();
        assertEquals(2, compositeMapOpTest.sequence.getSequence().size());
        assertTrue(compositeMapOpTest.sequence.getSequence().get(0) instanceof MapOpTestImpl1);
        FlatMapOpTestImpl1 flatMapOpTest = (FlatMapOpTestImpl1) compositeMapOpTest.sequence.getSequence().get(1);
        assertEquals("serialized", flatMapOpTest.aString);
 //       assertNull(flatMapOpTest.aNotSerializedString);
    }

}
