package doh2.api;

import org.apache.hadoop.fs.Path;

public abstract class HDFSLocation {

    public abstract boolean isSingle();

    public abstract Path[] getPaths();
}
