package doh.crazy;

import java.util.Iterator;

import static doh.crazy.Op.Some;

public abstract class KVOp<FromKey, FromValue, ToKey, ToValue>
        implements Op<KV<FromKey, Some<FromValue>>, KV<ToKey, ToValue>> {



    public Class<FromKey> fromKeyClass() {
        return ReflectionUtils.getFromKeyClass(getClass());
    }

    public Class<FromValue> fromValueClass() {
        return ReflectionUtils.getFromValueClass(getClass());
    }

    public Class<ToKey> toKeyClass() {
        return ReflectionUtils.getToKeyClass(getClass());
    }

    public Class<ToValue> toValueClass() {
        return ReflectionUtils.getToValueClass(getClass());
    }


    protected KV<ToKey, ToValue> keyValue(ToKey key, ToValue value) {
        return new KV<ToKey, ToValue>().set(key, value);
    }


    protected <T> One<T> one(T value) {
        one.value = value;
        return one;
    }

    protected <T> Many<T> many(Iterable<T> value) {
        many.valueIt = value;
        return many;
    }

    private final OneImpl one = new OneImpl();
    private final ManyImpl many = new ManyImpl();


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

}
