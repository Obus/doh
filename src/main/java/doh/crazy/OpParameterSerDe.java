package doh.crazy;

public interface OpParameterSerDe<T> {
    T de(String s) throws Exception;
    String ser(T parameter) throws Exception;
}
