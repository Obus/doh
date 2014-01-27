package doh2.api;

public interface MapDS<KEY, VALUE> extends DS<KEY, VALUE> {
    VALUE get(KEY key);
}
