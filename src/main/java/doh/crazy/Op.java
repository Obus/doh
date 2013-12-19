package doh.crazy;

import com.google.common.collect.Lists;

import java.util.Iterator;

public interface Op<From, To> {

    Some<To> apply(Some<From> f);


    public static interface Some<T> extends Iterable<T>{
        boolean isOne();
        boolean isMany();
    }

    public abstract class One<T> implements Some<T> {
        public abstract T get();

        @Override
        public Iterator<T> iterator() {
            return Lists.newArrayList(get()).iterator();
        }

        @Override
        public boolean isOne() {
            return true;
        }

        @Override
        public boolean isMany() {
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

    }
}
