package doh.crazy;

public interface Op<From, To> {
    To apply(From f);
}
