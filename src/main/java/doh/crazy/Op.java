package doh.crazy;

/**
 * Created by Alexander A. Senov
 * Synqera, 2012.
 */
public interface Op<From, To> {
    To apply(From f);
}
