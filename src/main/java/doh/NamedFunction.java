package doh;


public interface NamedFunction<F, T> {
    String name();
    T apply(F arg);
}
