package doh;

/**
 * Created by Alexander A. Senov
 * Synqera, 2012.
 */
public interface NamedFunction<F, T> {
    String name();
    T apply(F arg);
}
