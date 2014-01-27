package doh2.impl.op;

import com.google.common.collect.Lists;

import java.util.Iterator;

public interface Op<From, To> {

    Some<To> apply(Some<From> f);

    public static interface Some<T> extends Iterable<T> {
        boolean isOne();

        boolean isMany();

        boolean isNone();
    }

    public abstract class One<T> implements Some<T> {
        public abstract T get();

        @Override
        public Iterator<T> iterator() {
            return Lists.newArrayList(get()).iterator();
        }

        @Override
        public final boolean isOne() {
            return true;
        }

        @Override
        public final boolean isMany() {
            return false;
        }

        @Override
        public final boolean isNone() {
            return false;
        }
    }


    public abstract class Many<T> implements Some<T> {

        @Override
        public boolean isOne() {
            return false;
        }

        @Override
        public boolean isMany() {
            return true;
        }

        @Override
        public final boolean isNone() {
            return false;
        }

    }

    public class None<T> implements Some<T> {
        private static final None instance = new None();

        public static <T> None<T> none() {
            return (None<T>) instance;
        }

        private static final Iterator it = new Iterator() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Object next() {
                throw new RuntimeException();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        @Override
        public boolean isOne() {
            return false;
        }

        @Override
        public boolean isMany() {
            return false;
        }

        @Override
        public boolean isNone() {
            return true;
        }

        @Override
        public Iterator<T> iterator() {
            return (Iterator<T>) it;
        }
    }
}
