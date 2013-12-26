package doh.op.kvop;

import doh.api.op.KV;
import doh.op.Op;

import java.util.Iterator;

public abstract class KVOp<FromKey, FromValue, ToKey, ToValue>
        implements Op<KV<FromKey, FromValue>, KV<ToKey, ToValue>> {



    protected <A, B> KV<A, B> keyValue(A key, B value) {
        return new KV<A, B>().set(key, value);
    }

    protected <T> One<T> one(T value) {
        OneImpl<T> one = new OneImpl<T>();
        one.value = value;
        return one;
    }

    protected <T> Many<T> many(Iterable<T> value) {
        ManyImpl<T> many = new ManyImpl<T>();
        many.valueIt = value;
        return many;
    }

//    private final OneImpl one = new OneImpl();
//    private final ManyImpl many = new ManyImpl();


    public static class OneImpl<T> extends One<T> {
        private T value;

        @Override
        public T get() {
            return value;
        }
    }

    public static class ManyImpl<T> extends Many<T> {
        private Iterable<T> valueIt;

        @Override
        public Iterator<T> iterator() {
            return valueIt.iterator();
        }
    }

    public static <T> None<T> none() {
        return None.none();
    }

}
