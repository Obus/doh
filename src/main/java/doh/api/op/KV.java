package doh.api.op;

public class KV<Key, Value> {
    public Key key;
    public Value value;

    public KV() {
    }

    public KV(Key key, Value value) {
        this.key = key;
        this.value = value;
    }

    public static <Key, Value> KV<Key, Value> one() {
        return new KV<Key, Value>();
    }

    public KV<Key, Value> set(Key key, Value value) {
        this.key = key;
        this.value = value;
        return this;
    }

    @Override
    public String toString() {
        return key + "\t" + value;
    }
}
